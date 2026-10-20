use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Json,
};
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::job::{Job, JobStore};

type HmacSha256 = Hmac<Sha256>;

pub struct AppState {
    pub config: Config,
    pub job_store: JobStore,
    pub tx: mpsc::Sender<Job>,
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

    // 1. Verify HMAC Signature
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

    // 2. Parse Payload JSON
    let payload = serde_json::from_slice::<serde_json::Value>(&body)
        .map_err(|e| {
            warn!("Failed to parse webhook JSON payload: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid JSON payload",
                    "details": e.to_string()
                })),
            )
        })?;

    let repo_name = payload
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|f| f.as_str())
        .unwrap_or("unknown")
        .to_string();

    // 3. Verify Repository Allowlist
    if !state.config.is_repo_allowed(&repo_name) {
        warn!(repo = %repo_name, "Repository not allowed by security configuration");
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Repository not in allowlist",
                "repository": repo_name
            })),
        ));
    }

    // 4. Handle Specific GitHub Events
    if event == "ping" {
        info!(repo = %repo_name, "GitHub ping event received");
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "pong",
                "repository": repo_name,
                "message": "ActionRunner-rs webhook active"
            })),
        ));
    }

    if event == "push" {
        let commit_sha = payload
            .get("after")
            .and_then(|s| s.as_str())
            .or_else(|| {
                payload
                    .get("head_commit")
                    .and_then(|c| c.get("id"))
                    .and_then(|i| i.as_str())
            })
            .unwrap_or("")
            .to_string();

        let ref_name = payload
            .get("ref")
            .and_then(|r| r.as_str())
            .unwrap_or("refs/heads/main")
            .to_string();

        let clone_url = payload
            .get("repository")
            .and_then(|r| r.get("clone_url"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        let sender = payload
            .get("sender")
            .and_then(|s| s.get("login"))
            .and_then(|l| l.as_str())
            .unwrap_or("github")
            .to_string();

        if commit_sha.is_empty() || clone_url.is_empty() {
            warn!("Push event missing required commit SHA or clone URL");
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Missing commit SHA or clone URL in push event"
                })),
            ));
        }

        // Avoid triggering on deleted branches (0000000000000000000000000000000000000000)
        if commit_sha == "0000000000000000000000000000000000000000" {
            info!(ref_name = %ref_name, "Branch deletion push ignored");
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "ignored",
                    "reason": "branch deletion"
                })),
            ));
        }

        let sha_short = &commit_sha[..8.min(commit_sha.len())];
        let job_id = format!("job-{}-{}", Utc::now().timestamp(), sha_short);

        let job = Job {
            id: job_id.clone(),
            repo_name: repo_name.clone(),
            clone_url,
            commit_sha: commit_sha.clone(),
            ref_name: ref_name.clone(),
            sender,
            queued_at: Utc::now(),
        };

        let log_file = state.config.workspace_dir
            .join("logs")
            .join(&repo_name)
            .join(format!("{}.log", job.id));

        state.job_store.add_job(job.clone(), log_file).await;

        if let Err(e) = state.tx.send(job).await {
            error!(error = %e, "Failed to enqueue job onto executor channel");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to enqueue job",
                    "details": e.to_string()
                })),
            ));
        }

        info!(job_id = %job_id, repo = %repo_name, commit = %commit_sha, "Job enqueued successfully");

        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "status": "queued",
                "job_id": job_id,
                "repository": repo_name,
                "commit_sha": commit_sha,
                "ref": ref_name
            })),
        ));
    }

    info!(event = %event, repo = %repo_name, "Event accepted but no runner trigger configured");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "received",
            "event": event,
            "repository": repo_name,
            "message": "Event received; no execution required"
        })),
    ))
}
