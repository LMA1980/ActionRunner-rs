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
use tracing::{info, warn};

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
                warn!(
                    "Missing X-Hub-Signature-256 header for delivery: {}",
                    delivery
                );
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
    let payload = serde_json::from_slice::<serde_json::Value>(&body).map_err(|e| {
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

    let default_clone_url = payload
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

    let (commit_sha, ref_name, clone_url, event_type, should_run) = if event == "push" {
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

        if commit_sha == "0000000000000000000000000000000000000000" {
            info!(ref_name = %ref_name, "Branch deletion push ignored");
            (
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "push".to_string(),
                false,
            )
        } else {
            let evt = if ref_name.starts_with("refs/tags/") {
                "tag".to_string()
            } else {
                "push".to_string()
            };
            (commit_sha, ref_name, default_clone_url, evt, true)
        }
    } else if event == "pull_request" {
        let action = payload.get("action").and_then(|a| a.as_str()).unwrap_or("");

        let pr = payload.get("pull_request");
        let is_merged = pr
            .and_then(|p| p.get("merged"))
            .and_then(|m| m.as_bool())
            .unwrap_or(false);

        if action == "closed" && is_merged {
            let merge_sha = pr
                .and_then(|p| p.get("merge_commit_sha"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            let head_sha = pr
                .and_then(|p| p.get("head"))
                .and_then(|h| h.get("sha"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            let commit_sha = if !merge_sha.is_empty() {
                merge_sha.to_string()
            } else {
                head_sha.to_string()
            };

            let base_ref = pr
                .and_then(|p| p.get("base"))
                .and_then(|b| b.get("ref"))
                .and_then(|r| r.as_str())
                .unwrap_or("main")
                .to_string();

            (
                commit_sha,
                format!("refs/heads/{}", base_ref),
                default_clone_url,
                "merge".to_string(),
                true,
            )
        } else if matches!(action, "opened" | "synchronize" | "reopened") {
            let commit_sha = pr
                .and_then(|p| p.get("head"))
                .and_then(|h| h.get("sha"))
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string();

            let ref_name = pr
                .and_then(|p| p.get("head"))
                .and_then(|h| h.get("ref"))
                .and_then(|r| r.as_str())
                .unwrap_or("pr-branch")
                .to_string();

            let clone_url = pr
                .and_then(|p| p.get("head"))
                .and_then(|h| h.get("repo"))
                .and_then(|r| r.get("clone_url"))
                .and_then(|c| c.as_str())
                .unwrap_or(&default_clone_url)
                .to_string();

            (
                commit_sha,
                ref_name,
                clone_url,
                "pull_request".to_string(),
                true,
            )
        } else {
            info!(action = %action, repo = %repo_name, "Pull request action does not trigger CI");
            (
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "pull_request".to_string(),
                false,
            )
        }
    } else {
        // Generic event fallback
        let commit_sha = payload
            .get("after")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();

        let ref_name = payload
            .get("ref")
            .and_then(|r| r.as_str())
            .unwrap_or("refs/heads/main")
            .to_string();

        if commit_sha.is_empty() {
            (
                commit_sha,
                ref_name,
                default_clone_url,
                event.clone(),
                false,
            )
        } else {
            (commit_sha, ref_name, default_clone_url, event.clone(), true)
        }
    };

    if !should_run || commit_sha.is_empty() {
        info!(event = %event, repo = %repo_name, "Event accepted with no pending run");
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ignored",
                "event": event,
                "repository": repo_name
            })),
        ));
    }

    let sha_short = &commit_sha[..8.min(commit_sha.len())];
    let job_id = format!("job-{}-{}", Utc::now().timestamp(), sha_short);

    let job = Job {
        id: job_id.clone(),
        event_type: event_type.clone(),
        repo_name: repo_name.clone(),
        clone_url,
        commit_sha: commit_sha.clone(),
        ref_name: ref_name.clone(),
        sender,
        queued_at: Utc::now(),
    };

    let log_file = std::path::Path::new("logs")
        .join(&repo_name)
        .join(format!("{}.log", job.id));

    let enqueue_res = state.job_store.enqueue_job(job.clone(), log_file).await;
    let final_job_id = enqueue_res.job_id;
    let _ = state.tx.try_send(job);

    if !enqueue_res.superseded_jobs.is_empty() {
        info!(
            job_id = %final_job_id,
            superseded = ?enqueue_res.superseded_jobs,
            "Coalesced older queued jobs for same repository and ref"
        );
    }

    info!(
        job_id = %final_job_id,
        queue_position = enqueue_res.queue_position,
        event = %event_type,
        repo = %repo_name,
        commit = %commit_sha,
        "Job enqueued successfully"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "status": "queued",
            "job_id": final_job_id,
            "queue_position": enqueue_res.queue_position,
            "superseded_jobs": enqueue_res.superseded_jobs,
            "event": event_type,
            "repository": repo_name,
            "commit_sha": commit_sha,
            "ref": ref_name
        })),
    ))
}
