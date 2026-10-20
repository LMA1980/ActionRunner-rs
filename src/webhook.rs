use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::Config;

type HmacSha256 = Hmac<Sha256>;

pub struct AppState {
    pub config: Config,
}

pub fn verify_signature(secret: &str, signature_header: &str, body: &[u8]) -> bool {
    let Some(hex_sig) = signature_header.strip_prefix("sha256=") else {
        return false;
    };

    let Ok(expected_mac) = hex::decode(hex_sig) else {
        return false;
    };

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };

    mac.update(body);
    mac.verify_slice(&expected_mac).is_ok()
}

pub async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let event = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let delivery = headers
        .get("x-github-delivery")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    // Verify HMAC signature if a secret is configured
    if let Some(ref secret) = state.config.webhook_secret {
        let sig_header = headers
            .get("x-hub-signature-256")
            .and_then(|h| h.to_str().ok());

        match sig_header {
            Some(sig) if verify_signature(secret, sig, &body) => {
                info!("Signature verified successfully for delivery: {}", delivery);
            }
            Some(_) => {
                warn!("Invalid HMAC signature for delivery: {}", delivery);
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "error": "Invalid HMAC signature",
                        "delivery": delivery
                    })),
                ));
            }
            None => {
                warn!("Missing X-Hub-Signature-256 header for delivery: {}", delivery);
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "error": "Missing X-Hub-Signature-256 header",
                        "delivery": delivery
                    })),
                ));
            }
        }
    } else {
        info!("No webhook secret configured; accepting unverified payload (dev mode).");
    }

    // Parse payload metadata (best effort)
    let repo_name = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("repository").and_then(|r| r.get("full_name")).and_then(|f| f.as_str()).map(String::from))
        .unwrap_or_else(|| "unknown".to_string());

    info!(
        event = %event,
        delivery = %delivery,
        repo = %repo_name,
        payload_bytes = body.len(),
        "GitHub Webhook received successfully"
    );

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "received",
            "event": event,
            "delivery": delivery,
            "repository": repo_name,
            "message": "ActionRunner-rs webhook accepted"
        })),
    ))
}
