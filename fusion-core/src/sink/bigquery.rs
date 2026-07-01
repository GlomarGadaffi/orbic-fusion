//! Planned, not implemented. Streaming inserts need a GCP service-account
//! auth flow (JWT signing, token refresh) — meshtap's own Python subscriber
//! already does this off-device; there's no reason to duplicate that auth
//! stack on the Orbic itself. Until this is built, route through rsyslog and
//! a downstream Fluentd/syslog-to-BigQuery bridge instead.

use crate::envelope::FusionEnvelope;
use crate::sink::SinkError;

pub struct BigQueryConfig {
    pub project_id: String,
    pub dataset: String,
    pub table: String,
}

pub struct BigQuerySink {
    #[allow(dead_code)]
    config: BigQueryConfig,
}

impl BigQuerySink {
    pub fn new(config: BigQueryConfig) -> Self {
        BigQuerySink { config }
    }

    pub async fn send(&self, _envelope: &FusionEnvelope) -> Result<(), SinkError> {
        Err(SinkError::NotImplemented(
            "BigQuerySink: streaming insert not yet implemented, use RsyslogSink -> BQ bridge for now",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_reports_not_implemented() {
        let sink = BigQuerySink::new(BigQueryConfig {
            project_id: "example-project".to_string(),
            dataset: "fusion".to_string(),
            table: "events".to_string(),
        });
        let e = FusionEnvelope::from_mqtt("src/sensor/topic", b"{}", "2026-07-01T00:00:00Z".to_string());
        match sink.send(&e).await {
            Err(SinkError::NotImplemented(_)) => {}
            other => panic!("expected NotImplemented, got {other:?}"),
        }
    }
}
