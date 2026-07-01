//! Planned, not implemented. No existing repo in this fleet uses
//! Elasticsearch today, so this is genuinely new ground rather than a
//! consolidation of an existing stack. Until this is built, route through
//! rsyslog and Logstash's syslog input plugin instead.

use crate::envelope::FusionEnvelope;
use crate::sink::SinkError;

pub struct ElkConfig {
    pub es_url: String,
    pub index: String,
}

pub struct ElkSink {
    #[allow(dead_code)]
    config: ElkConfig,
}

impl ElkSink {
    pub fn new(config: ElkConfig) -> Self {
        ElkSink { config }
    }

    pub async fn send(&self, _envelope: &FusionEnvelope) -> Result<(), SinkError> {
        Err(SinkError::NotImplemented(
            "ElkSink: bulk API POST not yet implemented, use RsyslogSink -> Logstash syslog input for now",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_reports_not_implemented() {
        let sink = ElkSink::new(ElkConfig {
            es_url: "https://es.example:9200".to_string(),
            index: "fusion-events".to_string(),
        });
        let e = FusionEnvelope::from_mqtt("src/sensor/topic", b"{}", "2026-07-01T00:00:00Z".to_string());
        match sink.send(&e).await {
            Err(SinkError::NotImplemented(_)) => {}
            other => panic!("expected NotImplemented, got {other:?}"),
        }
    }
}
