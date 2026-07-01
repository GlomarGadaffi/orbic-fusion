//! v1 reference sink. Plain TCP syslog (RFC 5424 message format, RFC 6587
//! octet-counting framing) — no auth, no SDK, the lowest-friction thing that
//! every real log backend (rsyslog, syslog-ng, Splunk's own syslog input,
//! Logstash's syslog input) can already ingest. See the architecture note in
//! the repo README for why the daemon doesn't talk to Splunk/BigQuery/ELK
//! natively: this decouples a RAM-starved device from ever needing HEC
//! tokens or GCP service-account auth of its own.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;

use crate::envelope::FusionEnvelope;
use crate::sink::SinkError;

pub struct RsyslogConfig {
    pub host: String,
    pub port: u16,
    /// RFC 5424 APP-NAME field.
    pub app_name: String,
    /// RFC 5424 HOSTNAME field — the Orbic's own identity, not the producer's.
    pub hostname: String,
    /// Bounded retry queue depth. On overflow the OLDEST entry is dropped —
    /// deliberately the opposite of the ESP32-side firmware's drop-NEWEST
    /// queues (those protect a live scan loop from blocking; this queue is
    /// only ever draining during a network outage, where keeping recent
    /// state beats keeping stale queued state).
    pub max_queue: usize,
}

impl Default for RsyslogConfig {
    fn default() -> Self {
        RsyslogConfig {
            host: "127.0.0.1".to_string(),
            port: 514,
            app_name: "orbic-fusion".to_string(),
            hostname: "orbic-rc400l".to_string(),
            max_queue: 500,
        }
    }
}

pub struct RsyslogSink {
    config_app_name: String,
    config_hostname: String,
    queue: Arc<Mutex<VecDeque<String>>>,
    max_queue: usize,
    notify: Arc<Notify>,
}

impl RsyslogSink {
    pub fn new(config: RsyslogConfig) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::with_capacity(config.max_queue)));
        let notify = Arc::new(Notify::new());
        tokio::spawn(run_sender(
            config.host.clone(),
            config.port,
            queue.clone(),
            notify.clone(),
        ));
        RsyslogSink {
            config_app_name: config.app_name,
            config_hostname: config.hostname,
            queue,
            max_queue: config.max_queue,
            notify,
        }
    }

    pub async fn send(&self, envelope: &FusionEnvelope) -> Result<(), SinkError> {
        let line = format_rfc5424(envelope, &self.config_app_name, &self.config_hostname);
        enqueue_drop_oldest(&self.queue, self.max_queue, line);
        self.notify.notify_one();
        Ok(())
    }
}

/// Push `line`, dropping the oldest entry first if the queue is already at
/// capacity. Split out as a plain function (no tokio needed) so it's cheap
/// to unit test directly.
fn enqueue_drop_oldest(queue: &Mutex<VecDeque<String>>, max_queue: usize, line: String) {
    let mut q = queue.lock().unwrap();
    if q.len() >= max_queue {
        q.pop_front();
    }
    q.push_back(line);
}

/// RFC 5424: `<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID
/// STRUCTURED-DATA MSG`. PRI 14 = facility 1 (user-level), severity 6
/// (informational) — this is telemetry relay, not an error channel.
fn format_rfc5424(envelope: &FusionEnvelope, app_name: &str, hostname: &str) -> String {
    let msg = serde_json::to_string(envelope).unwrap_or_else(|_| "{}".to_string());
    format!(
        "<14>1 {} {} {} - {} - {}",
        envelope.recv_ts_utc, hostname, app_name, envelope.sensor_id, msg
    )
}

async fn run_sender(
    host: String,
    port: u16,
    queue: Arc<Mutex<VecDeque<String>>>,
    notify: Arc<Notify>,
) {
    let mut backoff = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        match TcpStream::connect((host.as_str(), port)).await {
            Ok(mut stream) => {
                backoff = Duration::from_millis(500);
                loop {
                    let next = {
                        let mut q = queue.lock().unwrap();
                        q.pop_front()
                    };
                    match next {
                        Some(line) => {
                            // RFC 6587 octet-counting framing: "<len> <msg>"
                            let framed = format!("{} {}", line.len(), line);
                            if let Err(_e) = stream.write_all(framed.as_bytes()).await {
                                let mut q = queue.lock().unwrap();
                                q.push_front(line);
                                break; // reconnect
                            }
                        }
                        None => notify.notified().await,
                    }
                }
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn sample_envelope() -> FusionEnvelope {
        FusionEnvelope::from_mqtt(
            "mockingbird-scrivener/board-01/sighting",
            br#"{"t":"ble","rssi":-70}"#,
            "2026-07-01T00:00:00Z".to_string(),
        )
    }

    #[test]
    fn formats_rfc5424_with_expected_fields() {
        let e = sample_envelope();
        let line = format_rfc5424(&e, "orbic-fusion", "orbic-rc400l");
        assert!(line.starts_with("<14>1 2026-07-01T00:00:00Z orbic-rc400l orbic-fusion - board-01 - "));
        assert!(line.contains("\"schema\":1"));
    }

    #[test]
    fn enqueue_drops_oldest_on_overflow() {
        let queue = Mutex::new(VecDeque::new());
        for i in 0..5 {
            enqueue_drop_oldest(&queue, 3, format!("line-{i}"));
        }
        let q = queue.lock().unwrap();
        // capacity 3, pushed 0..5 -> only the last 3 survive
        assert_eq!(*q, VecDeque::from(vec![
            "line-2".to_string(),
            "line-3".to_string(),
            "line-4".to_string(),
        ]));
    }

    #[test]
    fn enqueue_under_capacity_keeps_everything() {
        let queue = Mutex::new(VecDeque::new());
        enqueue_drop_oldest(&queue, 10, "only-one".to_string());
        let q = queue.lock().unwrap();
        assert_eq!(q.len(), 1);
    }

    #[tokio::test]
    async fn send_delivers_octet_framed_line_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            buf.truncate(n);
            String::from_utf8(buf).unwrap()
        });

        let sink = RsyslogSink::new(RsyslogConfig {
            host: "127.0.0.1".to_string(),
            port: addr.port(),
            app_name: "orbic-fusion".to_string(),
            hostname: "orbic-rc400l".to_string(),
            max_queue: 10,
        });

        sink.send(&sample_envelope()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), accept)
            .await
            .expect("timed out waiting for syslog line")
            .unwrap();

        // "<len> <rfc5424 line>"
        let (len_str, rest) = received.split_once(' ').unwrap();
        let declared_len: usize = len_str.parse().unwrap();
        assert_eq!(rest.len(), declared_len);
        assert!(rest.starts_with("<14>1 "));
        assert!(rest.contains("board-01"));
    }
}
