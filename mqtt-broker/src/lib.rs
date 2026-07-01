//! Deliberately scoped small: QoS 0 only, no retained messages, no
//! persistent sessions, no auth. The trust boundary is "reachable on the
//! Orbic's own WiFi AP" — the same posture `radio-cc`'s server and
//! `mvp-daemon` already run under on this device. The one real subscriber
//! is internal (the fusion daemon itself, wanting everything); external
//! SUBSCRIBE is acked but not fanned out to other MQTT clients.

pub mod codec;
pub mod broker;

pub use broker::{Broker, PublishedMessage};
pub use codec::Packet;
