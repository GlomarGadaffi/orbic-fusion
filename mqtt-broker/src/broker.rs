use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::codec::{self, CONNACK_ACCEPTED, Packet};

#[derive(Debug, Clone, PartialEq)]
pub struct PublishedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

pub struct Broker {
    listener: TcpListener,
    tx: mpsc::Sender<PublishedMessage>,
}

impl Broker {
    pub async fn bind(addr: &str, tx: mpsc::Sender<PublishedMessage>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Broker { listener, tx })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Runs forever, accepting connections. A single misbehaving client
    /// (bad TCP peer, malformed stream) only tears down its own connection
    /// task, never the accept loop itself.
    pub async fn run(self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _peer)) => {
                    let tx = self.tx.clone();
                    tokio::spawn(handle_connection(stream, tx));
                }
                Err(_) => continue,
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, tx: mpsc::Sender<PublishedMessage>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 4096];

    loop {
        loop {
            match codec::try_parse(&buf) {
                Ok(Some((packet, consumed))) => {
                    let keep_going = handle_packet(packet, &mut stream, &tx).await;
                    buf.drain(..consumed);
                    if !keep_going {
                        return;
                    }
                }
                Ok(None) => break, // need more bytes from the socket
                Err(_) => return, // malformed stream — drop this connection only
            }
        }

        match stream.read(&mut read_buf).await {
            Ok(0) => return, // peer closed
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
            Err(_) => return,
        }
    }
}

/// Returns `false` when the connection should be torn down (DISCONNECT or a
/// write failure), `true` to keep reading.
async fn handle_packet(
    packet: Packet,
    stream: &mut TcpStream,
    tx: &mpsc::Sender<PublishedMessage>,
) -> bool {
    match packet {
        Packet::Connect { .. } => stream
            .write_all(&codec::encode_connack(false, CONNACK_ACCEPTED))
            .await
            .is_ok(),
        Packet::Publish { topic, payload } => {
            // Never block a producer's connection on the fusion-daemon's own
            // downstream backpressure — drop-newest if the internal channel
            // is full, matching the ESP32-side firmware's established
            // non-blocking queue convention.
            let _ = tx.try_send(PublishedMessage { topic, payload });
            true
        }
        Packet::Subscribe { packet_id, filters } => stream
            .write_all(&codec::encode_suback(packet_id, filters.len()))
            .await
            .is_ok(),
        Packet::PingReq => stream.write_all(&codec::encode_pingresp()).await.is_ok(),
        Packet::Disconnect => false,
        // ConnAck/SubAck/PingResp are broker->client only; a client sending
        // one of these is non-conformant. Ignore rather than tear the
        // connection down for it.
        Packet::ConnAck { .. } | Packet::SubAck { .. } | Packet::PingResp => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_publish;
    use std::time::Duration;

    async fn spawn_broker() -> (std::net::SocketAddr, mpsc::Receiver<PublishedMessage>) {
        let (tx, rx) = mpsc::channel(16);
        let broker = Broker::bind("127.0.0.1:0", tx).await.unwrap();
        let addr = broker.local_addr().unwrap();
        tokio::spawn(broker.run());
        (addr, rx)
    }

    fn build_connect(client_id: &str) -> Vec<u8> {
        let mut body = Vec::new();
        codec_encode_string("MQTT", &mut body);
        body.push(4);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        codec_encode_string(client_id, &mut body);
        let mut out = vec![0x10];
        codec::encode_varint(body.len(), &mut out);
        out.extend_from_slice(&body);
        out
    }

    // codec::encode_string is private to the codec module; duplicate the
    // trivial encoding here rather than widen that module's visibility just
    // for tests.
    fn codec_encode_string(s: &str, out: &mut Vec<u8>) {
        let bytes = s.as_bytes();
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(bytes);
    }

    #[tokio::test]
    async fn connect_gets_connack() {
        let (addr, _rx) = spawn_broker().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&build_connect("board-01")).await.unwrap();

        let mut buf = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, [0x20, 0x02, 0x00, 0x00]);
    }

    #[tokio::test]
    async fn publish_is_forwarded_to_internal_channel() {
        let (addr, mut rx) = spawn_broker().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&build_connect("board-01")).await.unwrap();

        let mut connack = [0u8; 4];
        stream.read_exact(&mut connack).await.unwrap();

        let publish = encode_publish("mockingbird-scrivener/board-01/sighting", b"{\"t\":\"ble\"}");
        stream.write_all(&publish).await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "mockingbird-scrivener/board-01/sighting");
        assert_eq!(msg.payload, b"{\"t\":\"ble\"}");
    }

    #[tokio::test]
    async fn pingreq_gets_pingresp() {
        let (addr, _rx) = spawn_broker().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&build_connect("board-01")).await.unwrap();
        let mut connack = [0u8; 4];
        stream.read_exact(&mut connack).await.unwrap();

        stream.write_all(&[0xC0, 0x00]).await.unwrap();
        let mut buf = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, [0xD0, 0x00]);
    }

    #[tokio::test]
    async fn a_malformed_client_does_not_take_down_the_broker() {
        let (addr, mut rx) = spawn_broker().await;

        // garbage connection: unsupported packet type
        let mut bad = TcpStream::connect(addr).await.unwrap();
        bad.write_all(&[0x60, 0x00]).await.unwrap(); // PUBREL, unsupported
        drop(bad);

        // a well-behaved second client still works
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut good = TcpStream::connect(addr).await.unwrap();
        good.write_all(&build_connect("board-02")).await.unwrap();
        let mut connack = [0u8; 4];
        good.read_exact(&mut connack).await.unwrap();
        assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

        let publish = encode_publish("deauth-detector/base-01/alert", b"{\"t\":\"alert\"}");
        good.write_all(&publish).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.topic, "deauth-detector/base-01/alert");
    }
}
