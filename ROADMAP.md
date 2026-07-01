# Roadmap

Honest accounting of what's actually built vs. planned — see each sink
module's own doc comment for the specific reasoning.

## Done

- `fusion-core`: envelope schema (`FusionEnvelope`, `PayloadBody`), built and
  unit-tested (topic parsing, JSON/base64 fallback, serde round-trip).
- `mqtt-broker`: hand-rolled MQTT 3.1.1 subset (CONNECT/CONNACK/PUBLISH/
  SUBSCRIBE/SUBACK/PINGREQ/PINGRESP/DISCONNECT), QoS 0 only. Unit-tested
  (codec round-trips, fixed-header edge cases) and integration-tested (real
  TCP connections, a malformed client doesn't take down the broker).
- `RsyslogSink`: TCP, RFC 5424 message format, RFC 6587 octet-counting
  framing, reconnect with exponential backoff, bounded retry queue
  (drop-oldest on overflow). Integration-tested against a real TCP listener.
- `fusion-daemon`: wires broker + envelope construction + sink + a small
  status UI (`/`, `/api/status`). End-to-end smoke-tested locally (native
  build): real MQTT CONNECT/PUBLISH in, correctly-framed syslog line out,
  status counters correct.
- Cross-compiles clean for `armv7-unknown-linux-musleabihf` (the real
  target): 775KB static stripped binary.

## Not yet done

- **`SplunkSink`, `BigQuerySink`, `ElkSink`** — all three are stubs that
  return `SinkError::NotImplemented`. Route through `RsyslogSink` and each
  backend's own syslog-ingestion path for now (see README).
- **On-device deployment and verification.** Cross-compiled and smoke-tested
  locally; not yet installed on the physical Orbic. Blocked on the device
  being connected — see the repo's issue tracker for current status.
- **Producer-side MQTT publishing** in `mockingbird-scrivener`,
  `wigle-wardriver`, and `deauth-detector` — tracked in each of those repos,
  not here. `meshtap` needs no firmware change, only a broker-address config
  change for co-located deployments.
- **MQTT wildcard fan-out to external subscribers.** SUBSCRIBE is ACKed
  today but not actually routed anywhere — the one real subscriber is the
  daemon's own internal envelope pipeline. Fine for the current fleet (all
  producers are publish-only); would need real implementation if an
  external MQTT subscriber ever needs to listen in.
