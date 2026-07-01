//! Canonical event envelope. Every message ingested by the MQTT broker gets
//! wrapped into one of these before it reaches a `Sink` — see
//! `spec/envelope-schema.md` for the full spec this module implements.

use base64::Engine;
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;

/// A producer-agnostic event, built from a raw MQTT (topic, payload) pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FusionEnvelope {
    pub schema: u32,
    /// RFC3339 timestamp, set by the daemon at receipt time (not the
    /// producer's own clock — ESP32 boards don't reliably have wall-clock
    /// time, mono timestamps like `ts_mono_ms` stay inside `payload`).
    pub recv_ts_utc: String,
    /// First topic segment, e.g. "mockingbird-scrivener".
    pub source: String,
    /// Second topic segment, e.g. a board's client id.
    pub sensor_id: String,
    /// Full original MQTT topic, unmodified.
    pub topic: String,
    /// Producer-specific type tag, lifted from the payload's own `t`/`kind`
    /// field when the payload is JSON. `"raw"` for anything that didn't
    /// parse as JSON (e.g. meshtap's Meshtastic protobuf bytes).
    pub kind: String,
    pub payload: PayloadBody,
}

/// JSON payloads pass through unmodified; anything else (raw protobuf,
/// binary structs) is base64-encoded rather than dropped, so no producer's
/// data is silently lost just because it isn't JSON on the wire.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PayloadBody {
    Json(serde_json::Value),
    Base64(String),
}

impl FusionEnvelope {
    pub fn from_mqtt(topic: &str, payload: &[u8], recv_ts_utc: String) -> Self {
        let (source, sensor_id) = parse_topic(topic);
        let (kind, body) = match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(v) => {
                let kind = v
                    .get("t")
                    .or_else(|| v.get("kind"))
                    .and_then(|k| k.as_str())
                    .unwrap_or("json")
                    .to_string();
                (kind, PayloadBody::Json(v))
            }
            Err(_) => (
                "raw".to_string(),
                PayloadBody::Base64(base64::engine::general_purpose::STANDARD.encode(payload)),
            ),
        };
        FusionEnvelope {
            schema: SCHEMA_VERSION,
            recv_ts_utc,
            source,
            sensor_id,
            topic: topic.to_string(),
            kind,
            payload: body,
        }
    }
}

/// Topic convention: `<source>/<sensor_id>/<...rest ignored...>`.
/// Missing segments fall back to `"unknown"` rather than panicking — a
/// malformed topic from a misconfigured producer shouldn't crash the daemon.
fn parse_topic(topic: &str) -> (String, String) {
    let mut parts = topic.splitn(3, '/');
    let source = parts.next().filter(|s| !s.is_empty()).unwrap_or("unknown");
    let sensor_id = parts.next().filter(|s| !s.is_empty()).unwrap_or("unknown");
    (source.to_string(), sensor_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_topic() {
        let e = FusionEnvelope::from_mqtt(
            "mockingbird-scrivener/board-01/sighting",
            br#"{"t":"ble","mac":"aa:bb:cc:dd:ee:ff","rssi":-70}"#,
            "2026-07-01T00:00:00Z".to_string(),
        );
        assert_eq!(e.source, "mockingbird-scrivener");
        assert_eq!(e.sensor_id, "board-01");
        assert_eq!(e.topic, "mockingbird-scrivener/board-01/sighting");
        assert_eq!(e.kind, "ble");
        match e.payload {
            PayloadBody::Json(v) => assert_eq!(v["mac"], "aa:bb:cc:dd:ee:ff"),
            PayloadBody::Base64(_) => panic!("expected JSON payload"),
        }
    }

    #[test]
    fn falls_back_to_unknown_for_short_topic() {
        let e = FusionEnvelope::from_mqtt("bare-topic", b"{}", "2026-07-01T00:00:00Z".to_string());
        assert_eq!(e.source, "bare-topic");
        assert_eq!(e.sensor_id, "unknown");
    }

    #[test]
    fn falls_back_to_unknown_for_empty_topic() {
        let e = FusionEnvelope::from_mqtt("", b"{}", "2026-07-01T00:00:00Z".to_string());
        assert_eq!(e.source, "unknown");
        assert_eq!(e.sensor_id, "unknown");
    }

    #[test]
    fn non_json_payload_becomes_base64_raw() {
        let raw = vec![0x08, 0x01, 0x12, 0x04, 0xde, 0xad, 0xbe, 0xef];
        let e = FusionEnvelope::from_mqtt(
            "meshtap/node-01/proto",
            &raw,
            "2026-07-01T00:00:00Z".to_string(),
        );
        assert_eq!(e.kind, "raw");
        match e.payload {
            PayloadBody::Base64(b64) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .unwrap();
                assert_eq!(decoded, raw);
            }
            PayloadBody::Json(_) => panic!("expected base64 payload"),
        }
    }

    #[test]
    fn kind_falls_back_to_json_when_no_type_field() {
        let e = FusionEnvelope::from_mqtt(
            "wigle-wardriver/board-a/scan",
            br#"{"mac":"11:22:33:44:55:66","ssid":"test"}"#,
            "2026-07-01T00:00:00Z".to_string(),
        );
        assert_eq!(e.kind, "json");
    }

    #[test]
    fn round_trips_through_serde_json() {
        let e = FusionEnvelope::from_mqtt(
            "deauth-detector/base-01/alert",
            br#"{"t":"alert","sig":"evil_twin","rssi":-40}"#,
            "2026-07-01T00:00:00Z".to_string(),
        );
        let s = serde_json::to_string(&e).unwrap();
        let back: FusionEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
