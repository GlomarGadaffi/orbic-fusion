//! MQTT 3.1.1 fixed-header + a QoS-0-only subset of packet types, hand-
//! rolled against the OASIS spec (not a vendored client library) — matches
//! this account's established practice of building protocol handling from
//! the spec rather than reusing an existing implementation.

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect { client_id: String, keep_alive: u16 },
    ConnAck { session_present: bool, return_code: u8 },
    Publish { topic: String, payload: Vec<u8> },
    Subscribe { packet_id: u16, filters: Vec<String> },
    SubAck { packet_id: u16, return_codes: Vec<u8> },
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, PartialEq)]
pub enum CodecError {
    Malformed(&'static str),
    UnsupportedPacketType(u8),
}

pub const CONNACK_ACCEPTED: u8 = 0x00;

/// Largest remaining length the broker will buffer for one packet. The
/// spec allows about 256 MB; this device has about 22 MB free. Worst case
/// queued for the fusion-daemon sink is 500 x this, so keep it small.
pub const MAX_REMAINING_LEN: usize = 16 * 1024;
const SUBACK_QOS0_GRANTED: u8 = 0x00;

// ---- variable-length "remaining length" encoding (MQTT 2.2.3) ----

pub fn encode_varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Returns `(value, bytes_consumed)`, or `None` if `buf` doesn't yet contain
/// a complete varint (caller should wait for more bytes from the stream).
pub fn decode_varint(buf: &[u8]) -> Option<(usize, usize)> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut i = 0;
    loop {
        if i >= buf.len() || i >= 4 {
            return None;
        }
        let byte = buf[i];
        value += (byte as usize & 0x7F) * multiplier;
        multiplier *= 128;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Some((value, i))
}

fn decode_string(buf: &[u8]) -> Result<(String, usize), CodecError> {
    if buf.len() < 2 {
        return Err(CodecError::Malformed("string length prefix truncated"));
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + len {
        return Err(CodecError::Malformed("string body truncated"));
    }
    let s = std::str::from_utf8(&buf[2..2 + len])
        .map_err(|_| CodecError::Malformed("string is not valid utf-8"))?
        .to_string();
    Ok((s, 2 + len))
}

#[cfg(test)]
fn encode_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Try to parse one complete packet from the front of `buf`. Returns
/// `Ok(None)` if more bytes are needed (streaming TCP, not a datagram) —
/// the caller should read more and retry rather than treat it as an error.
pub fn try_parse(buf: &[u8]) -> Result<Option<(Packet, usize)>, CodecError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let packet_type = buf[0] >> 4;
    let flags = buf[0] & 0x0F;

    let Some((remaining_len, varint_len)) = decode_varint(&buf[1..]) else {
        return Ok(None);
    };
    // Refuse before buffering the body. Otherwise one client can make the
    // broker hold up to about 256 MB by sending a large header and then slow data.
    if remaining_len > MAX_REMAINING_LEN {
        return Err(CodecError::Malformed("remaining length over limit"));
    }
    let header_len = 1 + varint_len;
    let total_len = header_len + remaining_len;
    if buf.len() < total_len {
        return Ok(None);
    }
    let body = &buf[header_len..total_len];

    let packet = match packet_type {
        1 => parse_connect(body)?,
        3 => parse_publish(body, flags)?,
        8 => parse_subscribe(body)?,
        12 => Packet::PingReq,
        14 => Packet::Disconnect,
        other => return Err(CodecError::UnsupportedPacketType(other)),
    };
    Ok(Some((packet, total_len)))
}

fn parse_connect(body: &[u8]) -> Result<Packet, CodecError> {
    let (protocol_name, mut off) = decode_string(body)?;
    if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
        return Err(CodecError::Malformed("unexpected protocol name"));
    }
    if body.len() < off + 4 {
        return Err(CodecError::Malformed("CONNECT variable header truncated"));
    }
    let _protocol_level = body[off];
    let connect_flags = body[off + 1];
    let keep_alive = u16::from_be_bytes([body[off + 2], body[off + 3]]);
    off += 4;

    let (client_id, consumed) = decode_string(&body[off..])?;
    off += consumed;

    let will_flag = connect_flags & 0x04 != 0;
    if will_flag {
        let (_will_topic, c1) = decode_string(&body[off..])?;
        off += c1;
        let (_will_msg, c2) = decode_string(&body[off..])?;
        off += c2;
    }
    let has_username = connect_flags & 0x80 != 0;
    if has_username {
        let (_user, c) = decode_string(&body[off..])?;
        off += c;
    }
    let has_password = connect_flags & 0x40 != 0;
    if has_password {
        let (_pass, _c) = decode_string(&body[off..])?;
    }

    Ok(Packet::Connect { client_id, keep_alive })
}

fn parse_publish(body: &[u8], flags: u8) -> Result<Packet, CodecError> {
    let qos = (flags >> 1) & 0x03;
    let (topic, mut off) = decode_string(body)?;
    if qos != 0 {
        // We only support QoS 0. Still parse correctly (skip the packet id)
        // so a client that defaults to QoS1 doesn't desync the stream.
        if body.len() < off + 2 {
            return Err(CodecError::Malformed("PUBLISH packet id truncated"));
        }
        off += 2;
    }
    let payload = body[off..].to_vec();
    Ok(Packet::Publish { topic, payload })
}

fn parse_subscribe(body: &[u8]) -> Result<Packet, CodecError> {
    if body.len() < 2 {
        return Err(CodecError::Malformed("SUBSCRIBE packet id truncated"));
    }
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let mut off = 2;
    let mut filters = Vec::new();
    while off < body.len() {
        let (filter, consumed) = decode_string(&body[off..])?;
        off += consumed;
        if off >= body.len() {
            return Err(CodecError::Malformed("SUBSCRIBE missing requested QoS byte"));
        }
        off += 1; // requested QoS byte, ignored — we only ever grant QoS0
        filters.push(filter);
    }
    Ok(Packet::Subscribe { packet_id, filters })
}

// ---- encoding (broker -> client) ----

pub fn encode_connack(session_present: bool, return_code: u8) -> Vec<u8> {
    let mut out = vec![0x20];
    encode_varint(2, &mut out);
    out.push(if session_present { 0x01 } else { 0x00 });
    out.push(return_code);
    out
}

pub fn encode_suback(packet_id: u16, num_filters: usize) -> Vec<u8> {
    let mut out = vec![0x90];
    encode_varint(2 + num_filters, &mut out);
    out.extend_from_slice(&packet_id.to_be_bytes());
    out.extend(std::iter::repeat_n(SUBACK_QOS0_GRANTED, num_filters));
    out
}

pub fn encode_pingresp() -> Vec<u8> {
    vec![0xD0, 0x00]
}

#[cfg(test)]
pub fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    encode_string(topic, &mut body);
    body.extend_from_slice(payload);
    let mut out = vec![0x30]; // PUBLISH, QoS0, no DUP/RETAIN
    encode_varint(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_small_and_multibyte_values() {
        for &v in &[0usize, 1, 127, 128, 16383, 16384, 2097151] {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let (decoded, consumed) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn decode_varint_returns_none_when_truncated() {
        // 0x80 alone has the continuation bit set with no following byte
        assert_eq!(decode_varint(&[0x80]), None);
    }

    #[test]
    fn try_parse_returns_none_when_buffer_incomplete() {
        // CONNECT fixed header claiming remaining_len=20 but body is short
        let mut buf = vec![0x10];
        encode_varint(20, &mut buf);
        buf.extend_from_slice(b"MQTT"); // way short of 20 bytes
        assert_eq!(try_parse(&buf).unwrap(), None);
    }

    fn build_connect(client_id: &str, keep_alive: u16) -> Vec<u8> {
        let mut body = Vec::new();
        encode_string("MQTT", &mut body);
        body.push(4); // protocol level 3.1.1
        body.push(0x02); // clean session, no will/user/pass
        body.extend_from_slice(&keep_alive.to_be_bytes());
        encode_string(client_id, &mut body);

        let mut out = vec![0x10];
        encode_varint(body.len(), &mut out);
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn parses_minimal_connect() {
        let buf = build_connect("board-01", 60);
        let (packet, consumed) = try_parse(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(
            packet,
            Packet::Connect { client_id: "board-01".to_string(), keep_alive: 60 }
        );
    }

    #[test]
    fn parses_publish_qos0() {
        let buf = encode_publish("mockingbird-scrivener/board-01/sighting", b"{\"t\":\"ble\"}");
        let (packet, consumed) = try_parse(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        match packet {
            Packet::Publish { topic, payload } => {
                assert_eq!(topic, "mockingbird-scrivener/board-01/sighting");
                assert_eq!(payload, b"{\"t\":\"ble\"}");
            }
            other => panic!("expected Publish, got {other:?}"),
        }
    }

    #[test]
    fn parses_publish_qos1_and_skips_packet_id() {
        let mut body = Vec::new();
        encode_string("topic/x", &mut body);
        body.extend_from_slice(&42u16.to_be_bytes()); // packet id
        body.extend_from_slice(b"payload");
        let mut buf = vec![0x32]; // PUBLISH, QoS1
        encode_varint(body.len(), &mut buf);
        buf.extend_from_slice(&body);

        let (packet, _) = try_parse(&buf).unwrap().unwrap();
        match packet {
            Packet::Publish { topic, payload } => {
                assert_eq!(topic, "topic/x");
                assert_eq!(payload, b"payload");
            }
            other => panic!("expected Publish, got {other:?}"),
        }
    }

    #[test]
    fn parses_subscribe_with_multiple_filters() {
        let mut body = Vec::new();
        body.extend_from_slice(&7u16.to_be_bytes()); // packet id
        encode_string("mockingbird-scrivener/+/sighting", &mut body);
        body.push(0);
        encode_string("#", &mut body);
        body.push(0);
        let mut buf = vec![0x82];
        encode_varint(body.len(), &mut buf);
        buf.extend_from_slice(&body);

        let (packet, _) = try_parse(&buf).unwrap().unwrap();
        assert_eq!(
            packet,
            Packet::Subscribe {
                packet_id: 7,
                filters: vec!["mockingbird-scrivener/+/sighting".to_string(), "#".to_string()],
            }
        );
    }

    #[test]
    fn parses_pingreq_and_disconnect() {
        assert_eq!(try_parse(&[0xC0, 0x00]).unwrap().unwrap().0, Packet::PingReq);
        assert_eq!(try_parse(&[0xE0, 0x00]).unwrap().unwrap().0, Packet::Disconnect);
    }

    #[test]
    fn rejects_unsupported_packet_type() {
        // type 6 (PUBREL) isn't something a QoS0-only broker needs to handle
        assert_eq!(try_parse(&[0x60, 0x00]), Err(CodecError::UnsupportedPacketType(6)));
    }

    #[test]
    fn encode_connack_matches_spec_layout() {
        let buf = encode_connack(false, CONNACK_ACCEPTED);
        assert_eq!(buf, vec![0x20, 0x02, 0x00, 0x00]);
    }

    #[test]
    fn encode_suback_grants_qos0_for_every_filter() {
        let buf = encode_suback(7, 2);
        assert_eq!(buf, vec![0x90, 0x04, 0x00, 0x07, 0x00, 0x00]);
    }

    #[test]
    fn try_parse_handles_two_packets_back_to_back() {
        let mut buf = build_connect("board-01", 60);
        buf.extend_from_slice(&[0xC0, 0x00]); // PINGREQ right after
        let (first, consumed1) = try_parse(&buf).unwrap().unwrap();
        assert!(matches!(first, Packet::Connect { .. }));
        let (second, consumed2) = try_parse(&buf[consumed1..]).unwrap().unwrap();
        assert_eq!(second, Packet::PingReq);
        assert_eq!(consumed1 + consumed2, buf.len());
    }
}

#[cfg(test)]
mod limit_tests {
    use super::*;

    #[test]
    fn rejects_remaining_length_over_limit_before_buffering() {
        // Header claims one byte over the limit. No body bytes are sent.
        let mut buf = vec![0x30];
        encode_varint(MAX_REMAINING_LEN + 1, &mut buf);
        assert_eq!(
            try_parse(&buf),
            Err(CodecError::Malformed("remaining length over limit"))
        );
    }

    #[test]
    fn accepts_remaining_length_at_limit() {
        let mut buf = vec![0x30];
        encode_varint(MAX_REMAINING_LEN, &mut buf);
        // Still incomplete (no body yet), but not rejected.
        assert_eq!(try_parse(&buf), Ok(None));
    }
}
