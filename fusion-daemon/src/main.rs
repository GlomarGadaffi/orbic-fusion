mod time;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::mpsc;

use fusion_core::sink::rsyslog::{RsyslogConfig, RsyslogSink};
use fusion_core::{FusionEnvelope, Sink};
use mqtt_broker::Broker;

#[derive(Default)]
struct Stats {
    started_at: Option<Instant>,
    total_messages: u64,
    by_source: HashMap<String, u64>,
}

type Shared = Arc<Mutex<Stats>>;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Positional args, matching this fleet's established convention
    // (mvp-daemon takes its device/data-dir/port the same way) rather than
    // pulling in an argument-parsing crate for four values.
    let mqtt_addr = args.get(1).cloned().unwrap_or_else(|| "0.0.0.0:1883".to_string());
    let http_addr = args.get(2).cloned().unwrap_or_else(|| "0.0.0.0:8090".to_string());
    let rsyslog_host = args.get(3).cloned().unwrap_or_else(|| "127.0.0.1".to_string());
    let rsyslog_port: u16 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(514);

    let (tx, mut rx) = mpsc::channel(256);
    let broker = Broker::bind(&mqtt_addr, tx)
        .await
        .unwrap_or_else(|e| panic!("failed to bind MQTT broker on {mqtt_addr}: {e}"));
    println!("orbic-fusion: mqtt broker listening on {mqtt_addr}");
    tokio::spawn(broker.run());

    let sink = Sink::Rsyslog(RsyslogSink::new(RsyslogConfig {
        host: rsyslog_host.clone(),
        port: rsyslog_port,
        app_name: "orbic-fusion".to_string(),
        hostname: "orbic-rc400l".to_string(),
        max_queue: 500,
    }));
    println!("orbic-fusion: forwarding to rsyslog at {rsyslog_host}:{rsyslog_port}");

    let stats: Shared = Arc::new(Mutex::new(Stats {
        started_at: Some(Instant::now()),
        ..Default::default()
    }));

    let ingest_stats = stats.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let envelope = FusionEnvelope::from_mqtt(&msg.topic, &msg.payload, time::now_rfc3339());
            {
                let mut s = ingest_stats.lock().unwrap();
                s.total_messages += 1;
                *s.by_source.entry(envelope.source.clone()).or_insert(0) += 1;
            }
            // Best-effort: RsyslogSink enqueues onto its own bounded retry
            // buffer and never blocks here, so a send "failure" (there
            // isn't one for Rsyslog today) wouldn't stall ingestion either
            // way — see Sink::send's doc comment.
            let _ = sink.send(&envelope).await;
        }
    });

    let app = Router::new()
        .route("/", get(ui_handler))
        .route("/api/status", get(status_handler))
        .with_state(stats);

    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind status UI on {http_addr}: {e}"));
    println!("orbic-fusion: status ui listening on {http_addr}");
    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| panic!("http server error: {e}"));
}

async fn ui_handler() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

async fn status_handler(State(stats): State<Shared>) -> Json<serde_json::Value> {
    let s = stats.lock().unwrap();
    Json(serde_json::json!({
        "uptime_secs": s.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0),
        "total_messages": s.total_messages,
        "by_source": s.by_source,
        "sink": "rsyslog",
    }))
}
