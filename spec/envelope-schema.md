# Envelope schema (v1)

Every MQTT `(topic, payload)` pair the broker receives is converted into one
`FusionEnvelope` before it reaches a `Sink`. This is the wire/storage shape;
see `fusion-core/src/envelope.rs` for the implementation.

## Topic convention

```
<source>/<sensor_id>/<...rest, producer-defined, ignored by the daemon...>
```

- `source` — first segment, identifies which repo/producer this is (e.g.
  `mockingbird-scrivener`, `wigle-wardriver`, `deauth-detector`, `meshtap`).
- `sensor_id` — second segment, the individual board/node's own identifier.
- Missing segments fall back to `"unknown"` rather than rejecting the
  message — a misconfigured producer shouldn't lose data, just get a less
  useful label on it.

## Envelope fields

| Field          | Type          | Meaning |
|----------------|---------------|---------|
| `schema`       | u32           | Envelope schema version. `1` today. |
| `recv_ts_utc`  | string        | RFC3339 UTC, stamped by the daemon at receipt — not the producer's clock. ESP32 boards emit mono timestamps (`ts_mono_ms`) inside `payload` instead; they don't reliably have wall-clock time. |
| `source`       | string        | First topic segment. |
| `sensor_id`    | string        | Second topic segment. |
| `topic`        | string        | Full original MQTT topic, unmodified. |
| `kind`         | string        | Producer-specific type tag. If `payload` is JSON with a `t` or `kind` field, that value is lifted here (e.g. `"ble"`, `"hb"`, `"alert"`, `"scan"`). If the JSON object doesn't have either field, `"json"`. If the payload didn't parse as JSON at all, `"raw"`. |
| `payload`      | JSON or string | The producer's own payload. JSON payloads pass through byte-for-byte reparsed as a `serde_json::Value` (no reshaping, no field renaming). Anything that isn't valid JSON (e.g. meshtap's Meshtastic `ServiceEnvelope` protobuf bytes) is base64-encoded rather than dropped. |

## Non-goals

- No attempt to unify or validate producer-specific payload shapes — a
  `mockingbird-scrivener` BLE sighting and a `wigle-wardriver` scan row look
  nothing alike inside `payload`, and that's fine. The envelope's job is
  provenance (source/sensor/topic/kind) and delivery, not schema
  normalization across producers.
- No QoS/delivery guarantees beyond MQTT QoS 0 in, best-effort out. See
  `RsyslogSink`'s doc comment for what "accepted" means downstream.

## Producer payload shapes (as of this writing)

- **mockingbird-scrivener**: NDJSON, `{"t":"ble"|"hb", "schema":1, ...}` —
  the wire format already used by `lawndale_courier.c`'s existing UDP
  syslog/SD paths, unchanged for MQTT.
- **wigle-wardriver**: new JSON encoding of the same fields already written
  to its Wigle-CSV rows (MAC/SSID/AuthMode/RSSI/lat/lon/...).
- **deauth-detector**: hand-formatted JSON matching the on-wire `alert_t`
  struct fields (`sig`, `channel`, `rssi`, `src`, `count`, `uptime_s`).
- **meshtap**: raw Meshtastic `ServiceEnvelope` protobuf bytes — arrives as
  `kind: "raw"`, `payload: Base64(...)`. Decoding stays meshtap's own
  subscriber's job; the fusion daemon doesn't parse protobuf.
