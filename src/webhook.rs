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

use crate::config::{is_safe_repo_name, Config};
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
    } else if state.config.allow_insecure_no_secret {
        warn!(
            "Running in INSECURE mode: No webhook secret configured; accepting unverified payload."
        );
    } else {
        error!("Webhook received but GITHUB_WEBHOOK_SECRET is not configured. Rejecting request.");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Server security configuration error: GITHUB_WEBHOOK_SECRET is not configured",
                "delivery": delivery
            })),
        ));
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

    // 3. Verify Repository Name Hygiene (Path Traversal Prevention)
    if !is_safe_repo_name(&repo_name) {
        warn!(repo = %repo_name, "Repository name contains unsafe characters or path traversal attempt");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Invalid repository name: directory traversal or invalid characters detected",
                "repository": repo_name
            })),
        ));
    }

    // 4. Verify Repository Allowlist
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

    // 5. Verify Sender Actor
    let sender = payload
        .get("sender")
        .and_then(|s| s.get("login"))
        .and_then(|l| l.as_str())
        .unwrap_or("github")
        .to_string();

    if !state.config.is_actor_allowed(&sender) {
        warn!(sender = %sender, repo = %repo_name, "Sender actor not permitted to trigger CI builds");
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Actor not authorized to trigger CI builds",
                "sender": sender
            })),
        ));
    }

    // 6. Handle Specific GitHub Events
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

        // Inspect PR fork origin and author association
        let head_repo = pr
            .and_then(|p| p.get("head"))
            .and_then(|h| h.get("repo"))
            .and_then(|r| r.get("full_name"))
            .and_then(|f| f.as_str())
            .unwrap_or("");

        let base_repo = pr
            .and_then(|p| p.get("base"))
            .and_then(|b| b.get("repo"))
            .and_then(|r| r.get("full_name"))
            .and_then(|f| f.as_str())
            .unwrap_or(&repo_name);

        let is_fork = !head_repo.is_empty() && !head_repo.eq_ignore_ascii_case(base_repo);

        let author_association = pr
            .and_then(|p| p.get("author_association"))
            .and_then(|a| a.as_str())
            .unwrap_or("NONE");

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
            // Anti-RCE Guard 1: Disallow untrusted pull requests from external forks
            if is_fork && !state.config.allow_fork_prs {
                info!(
                    repo = %repo_name,
                    head_repo = %head_repo,
                    author = %sender,
                    "Pull request from external fork ignored for security (untrusted fork execution disabled)"
                );
                return Ok((
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "status": "ignored",
                        "reason": "Pull requests from external forks are rejected for security",
                        "head_repo": head_repo,
                        "repository": repo_name
                    })),
                ));
            }

            // Anti-RCE Guard 2: Enforce author association (must be OWNER, MEMBER, or COLLABORATOR)
            if !state.config.is_pr_author_allowed(author_association) {
                info!(
                    repo = %repo_name,
                    author = %sender,
                    association = %author_association,
                    "Pull request author not authorized for automatic CI execution"
                );
                return Ok((
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "status": "ignored",
                        "reason": "Pull request author association requires manual approval",
                        "author_association": author_association,
                        "repository": repo_name
                    })),
                ));
            }

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

    // Validate commit SHA format (strictly hexadecimal)
    if should_run && !commit_sha.is_empty() && !commit_sha.chars().all(|c| c.is_ascii_hexdigit()) {
        warn!(commit = %commit_sha, "Invalid commit SHA format received");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Invalid commit SHA format: must be hexadecimal",
                "commit_sha": commit_sha
            })),
        ));
    }

    // Validate ref name does not contain path traversal or unsafe characters
    if should_run && (ref_name.contains("..") || ref_name.starts_with('-')) {
        warn!(ref_name = %ref_name, "Invalid ref name received");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Invalid ref name: contains traversal or option flags",
                "ref": ref_name
            })),
        ));
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_test_state(
        secret: Option<&str>,
        allow_insecure: bool,
        allow_forks: bool,
    ) -> (Arc<AppState>, mpsc::Receiver<Job>) {
        let (tx, rx) = mpsc::channel(16);
        let config = Config {
            bind_addr: "127.0.0.1:0".to_string(),
            webhook_secret: secret.map(|s| s.to_string()),
            allow_insecure_no_secret: allow_insecure,
            workspace_dir: PathBuf::from("/tmp"),
            allowed_repositories: vec!["LMA1980/*".to_string()],
            allowed_actors: vec![],
            allow_fork_prs: allow_forks,
            allowed_pr_author_associations: vec![
                "OWNER".to_string(),
                "MEMBER".to_string(),
                "COLLABORATOR".to_string(),
            ],
            job_timeout_seconds: 60,
            log_api_key: None,
            isolation_mode: crate::config::IsolationMode::None,
            cpu_quota: Some("1400%".to_string()),
        };
        let job_store = JobStore::new(10, None);
        (
            Arc::new(AppState {
                config,
                job_store,
                tx,
            }),
            rx,
        )
    }

    fn compute_sig(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let result = mac.finalize();
        format!("sha256={}", hex::encode(result.into_bytes()))
    }

    #[tokio::test]
    async fn test_webhook_secret_required_in_strict_mode() {
        let (state, _rx) = make_test_state(None, false, false);
        let headers = HeaderMap::new();
        let body = Bytes::from(r#"{"repository":{"full_name":"LMA1980/test"}}"#);
        let res = handle_webhook(State(state), headers, body).await;
        assert!(res.is_err());
        let (status, json) = res.unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(json
            .to_string()
            .contains("GITHUB_WEBHOOK_SECRET is not configured"));
    }

    #[tokio::test]
    async fn test_webhook_rejects_invalid_hmac() {
        let (state, _rx) = make_test_state(Some("mysecret"), false, false);
        let mut headers = HeaderMap::new();
        headers.insert("x-hub-signature-256", "sha256=badhex".parse().unwrap());
        let body = Bytes::from(r#"{"repository":{"full_name":"LMA1980/test"}}"#);
        let res = handle_webhook(State(state), headers, body).await;
        assert!(res.is_err());
        let (status, _) = res.unwrap_err();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_webhook_rejects_path_traversal_repo() {
        let secret = "testsecret";
        let (state, _rx) = make_test_state(Some(secret), false, false);
        let body_str = r#"{"repository":{"full_name":"LMA1980/../../etc/passwd"}}"#;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-hub-signature-256",
            compute_sig(secret, body_str.as_bytes()).parse().unwrap(),
        );
        let body = Bytes::from(body_str);
        let res = handle_webhook(State(state), headers, body).await;
        assert!(res.is_err());
        let (status, _) = res.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_webhook_ignores_fork_pull_request() {
        let secret = "testsecret";
        let (state, _rx) = make_test_state(Some(secret), false, false);
        let body_str = r#"{
            "repository": {"full_name": "LMA1980/ActionRunner-rs"},
            "action": "opened",
            "pull_request": {
                "head": {"repo": {"full_name": "attacker/ActionRunner-rs"}, "sha": "0123456789abcdef", "ref": "feature"},
                "base": {"repo": {"full_name": "LMA1980/ActionRunner-rs"}},
                "author_association": "NONE"
            }
        }"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", "pull_request".parse().unwrap());
        headers.insert(
            "x-hub-signature-256",
            compute_sig(secret, body_str.as_bytes()).parse().unwrap(),
        );
        let body = Bytes::from(body_str);
        let res = handle_webhook(State(state), headers, body).await;
        assert!(res.is_ok());
        let (status, json) = res.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ignored");
        assert!(json["reason"].as_str().unwrap().contains("external forks"));
    }

    #[tokio::test]
    async fn test_webhook_accepts_valid_internal_pr() {
        let secret = "testsecret";
        let (state, _rx) = make_test_state(Some(secret), false, false);
        let body_str = r#"{
            "repository": {"full_name": "LMA1980/ActionRunner-rs"},
            "action": "opened",
            "pull_request": {
                "head": {"repo": {"full_name": "LMA1980/ActionRunner-rs"}, "sha": "0123456789abcdef", "ref": "dev-feature"},
                "base": {"repo": {"full_name": "LMA1980/ActionRunner-rs"}},
                "author_association": "OWNER"
            }
        }"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", "pull_request".parse().unwrap());
        headers.insert(
            "x-hub-signature-256",
            compute_sig(secret, body_str.as_bytes()).parse().unwrap(),
        );
        let body = Bytes::from(body_str);
        let res = handle_webhook(State(state), headers, body).await;
        assert!(res.is_ok());
        let (status, json) = res.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(json["status"], "queued");
    }
}
