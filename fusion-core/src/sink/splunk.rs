//! Planned, not implemented. Splunk's HTTP Event Collector needs a token and
//! an HTTPS client stack — real weight on a device with ~22MB free RAM.
//! Until this is built, point rsyslog itself at Splunk's syslog input
//! (a Splunk universal forwarder or a syslog-listening HEC-adjacent input)
//! rather than waiting on this sink.

use crate::envelope::FusionEnvelope;
use crate::sink::SinkError;

pub struct SplunkConfig {
    pub hec_url: String,
    pub hec_token: String,
}

pub struct SplunkSink {
    #[allow(dead_code)]
    config: SplunkConfig,
}

impl SplunkSink {
    pub fn new(config: SplunkConfig) -> Self {
        SplunkSink { config }
    }

    pub async fn send(&self, _envelope: &FusionEnvelope) -> Result<(), SinkError> {
        Err(SinkError::NotImplemented(
            "SplunkSink: HEC POST not yet implemented, use RsyslogSink -> Splunk syslog input for now",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_reports_not_implemented() {
        let sink = SplunkSink::new(SplunkConfig {
            hec_url: "https://splunk.example/services/collector".to_string(),
            hec_token: "unused".to_string(),
        });
        let e = FusionEnvelope::from_mqtt("src/sensor/topic", b"{}", "2026-07-01T00:00:00Z".to_string());
        match sink.send(&e).await {
            Err(SinkError::NotImplemented(_)) => {}
            other => panic!("expected NotImplemented, got {other:?}"),
        }
    }
}
