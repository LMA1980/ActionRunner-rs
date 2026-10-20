mod config;
mod webhook;

use axum::{
    response::Json,
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use crate::config::Config;
use crate::webhook::{handle_webhook, AppState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "action_runner=info,tower_http=info".into()),
        )
        .init();

    let config = Config::load();
    let bind_addr: SocketAddr = config.bind_addr.parse()?;

    info!("Starting ActionRunner-rs on {}", bind_addr);
    if config.webhook_secret.is_some() {
        info!("GitHub Webhook HMAC signature verification: ACTIVE");
    } else {
        info!("GitHub Webhook HMAC signature verification: DISABLED (set GITHUB_WEBHOOK_SECRET to enable)");
    }

    let state = Arc::new(AppState { config });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/webhook", post(handle_webhook).get(health_check))
        .route("/", get(health_check))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "ActionRunner-rs",
        "arch": "riscv64",
        "version": env!("CARGO_PKG_VERSION")
    }))
}
