//! Pluggable output sinks. Deliberately an enum + match, not `dyn Sink` +
//! `async_trait` — there are exactly four sinks, known at compile time, and
//! static dispatch avoids an extra proc-macro dependency on a device with
//! ~22MB of free RAM to spare.

use std::fmt;

use crate::envelope::FusionEnvelope;

pub mod bigquery;
pub mod elk;
pub mod rsyslog;
pub mod splunk;

#[derive(Debug)]
pub enum SinkError {
    NotImplemented(&'static str),
    Io(std::io::Error),
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkError::NotImplemented(what) => write!(f, "not implemented: {what}"),
            SinkError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for SinkError {}

impl From<std::io::Error> for SinkError {
    fn from(e: std::io::Error) -> Self {
        SinkError::Io(e)
    }
}

pub enum Sink {
    Rsyslog(rsyslog::RsyslogSink),
    Splunk(splunk::SplunkSink),
    BigQuery(bigquery::BigQuerySink),
    Elk(elk::ElkSink),
}

impl Sink {
    /// "Accepted for delivery," not "confirmed delivered" — RsyslogSink
    /// enqueues onto its own bounded retry buffer and hands off to a
    /// background task; a return of `Ok(())` here means the envelope was
    /// queued, which is the right contract for a best-effort telemetry relay
    /// on a link (LTE) that can drop out at any time.
    pub async fn send(&self, envelope: &FusionEnvelope) -> Result<(), SinkError> {
        match self {
            Sink::Rsyslog(s) => s.send(envelope).await,
            Sink::Splunk(s) => s.send(envelope).await,
            Sink::BigQuery(s) => s.send(envelope).await,
            Sink::Elk(s) => s.send(envelope).await,
        }
    }
}
