# orbic-fusion

**A telemetry fusion point for a rooted Orbic RC400L: ingest MQTT from a
fleet of ESP32 sensor nodes over WiFi, forward upstream over the Orbic's own
LTE modem.**

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Why

The Orbic RC400L (see [`orbic-toolkit`](https://github.com/GlomarGadaffi/orbic-toolkit)
for how to get root on one) is a rooted ARMv7 Linux box with its own WiFi AP
and its own cellular uplink. That combination makes it a natural aggregation
point for a fleet of ESP32 sensors that individually have no cellular of
their own: `mockingbird-scrivener` (BLE/WiFi sightings),
`wigle-wardriver` (WiFi wardriving), `deauth-detector` (802.11 attack
alerts), and `meshtap` (Meshtastic capture) can all report to the Orbic over
its local WiFi, and it backhauls everything over LTE — one cellular
connection instead of several.

`orbic-fusion` is **not** a replacement for any of those ESP32 boards — it
has no BLE radio and its WiFi chip has no monitor/injection mode, so it
can't do the actual RF sensing they do. It's the aggregation/backhaul layer
that sits around them.

## Architecture

```
mockingbird-scrivener ─┐
deauth-detector (base) ─┼─ MQTT (WiFi) ──▶ orbic-fusion (Orbic RC400L)
wigle-wardriver (A)    ─┘                    │
meshtap CLIENT_MUTE ───(co-located only)────▶│
                                              ├─ mqtt-broker :1883 (hand-rolled, QoS0)
                                              ├─ fusion-core: envelope + Sink
                                              ├─ RsyslogSink (v1, TCP, RFC5424/RFC6587)
                                              └─ status UI :8090
                                                     │
                                                     ▼ LTE
                                          rsyslog → Splunk / ELK / BigQuery
                                          via standard syslog input plugins
```

Picking rsyslog as the v1 sink doesn't foreclose Splunk/ELK/BigQuery —
all three have mature syslog *input* plugins (Splunk universal forwarder,
Logstash's syslog input, any syslog→Fluentd/BQ bridge). That keeps this
device — ~160MB total RAM, ~22MB free with other services running — out of
the business of HEC tokens or GCP service-account auth. See `ROADMAP.md`
for what's actually implemented vs. planned, and `spec/envelope-schema.md`
for the wire format.

## Why a hand-rolled MQTT broker

QoS 0 only, no retained messages, no persistent sessions, no auth — the
trust boundary is "reachable on the Orbic's own WiFi AP," the same posture
`radio-cc` and `mvp-daemon` already run under on this device. Written from
the OASIS MQTT 3.1.1 spec rather than pulling in an existing broker crate,
to keep the binary and RAM footprint small and predictable.

## Deployment

Deployed via [`orbic-toolkit`](https://github.com/GlomarGadaffi/orbic-toolkit):

```sh
cargo build-firmware
orbic-toolkit --password <PASS> install payload.toml \
    --binary target/armv7-unknown-linux-musleabihf/firmware/fusion-daemon
```

Ports: **1883** (MQTT broker), **8090** (status UI). Confirmed clear of
`mvp-daemon` (8080) and `radio-cc` (7878) as of this writing —
`orbic-toolkit`'s own port-conflict check only compares against a hardcoded
list, not the live device, so if you're running other services check
manually.

### Known `orbic-toolkit` gaps this deployment works around

- **Boot persistence isn't automatic.** `orbic-toolkit install` writes
  `/etc/init.d/{name}` but never creates the `/etc/rc5.d/S99{name}` symlink
  that actually activates it on boot. Do it manually after install:
  ```sh
  orbic-toolkit --password <PASS> run "ln -s /etc/init.d/orbic-fusion /etc/rc5.d/S99orbic-fusion"
  ```
- **`uninstall` derives the data directory from the service name, not the
  manifest.** `payload.toml` here deliberately sets `name = "orbic-fusion"`
  and `data_dir = "/data/orbic-fusion"` with matching last path segments so
  `uninstall orbic-fusion` actually removes the right directory.

## Building

```sh
cargo test --workspace          # unit + integration tests, no hardware needed
cargo build-firmware             # release, size-optimized, armv7-unknown-linux-musleabihf
cargo build-firmware-devel       # faster-building, less size-optimized, for iteration
```

## License

MIT.
