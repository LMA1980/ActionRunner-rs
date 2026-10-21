mod badge;
mod config;
mod executor;
mod job;
mod webhook;
mod workflow;

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::executor::Executor;
use crate::job::{JobStatus, JobStore};
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
    info!("Allowed repositories: {:?}", config.allowed_repositories);
    info!("Workspace directory:  {}", config.workspace_dir.display());

    if config.webhook_secret.is_some() {
        info!("GitHub Webhook HMAC signature verification: ACTIVE");
    } else if config.allow_insecure_no_secret {
        warn!("GitHub Webhook HMAC signature verification: INSECURE DEV MODE (unverified payloads accepted)");
    } else {
        warn!("GitHub Webhook HMAC signature verification: STRICT (incoming webhooks will be rejected until GITHUB_WEBHOOK_SECRET is set)");
    }

    let job_store = JobStore::new(100, Some(&config.workspace_dir));
    let (tx, rx) = mpsc::channel(32);

    let executor = Arc::new(Executor::new(config.clone(), job_store.clone()));
    tokio::spawn(executor.run_worker(rx));

    let state = Arc::new(AppState {
        config,
        job_store,
        tx,
    });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/health", get(health_check))
        .route("/webhook", post(handle_webhook))
        .route("/queue", get(get_queue))
        .route("/api/queue", get(get_queue))
        .route("/jobs", get(list_jobs))
        .route("/api/jobs", get(list_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/api/jobs/{id}", get(get_job))
        .route("/jobs/{id}/log", get(get_job_log))
        .route("/api/jobs/{id}/log", get(get_job_log))
        // Dynamic Status Badges & Cards
        .route("/badges/{owner}/{repo}/status.svg", get(get_repo_badge))
        .route(
            "/badges/{owner}/{repo}/{branch}/status.svg",
            get(get_branch_badge),
        )
        .route(
            "/badges/{owner}/{repo}/score.svg",
            get(get_repo_score_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/score.svg",
            get(get_branch_score_badge),
        )
        .route(
            "/badges/{owner}/{repo}/coverage.svg",
            get(get_repo_coverage_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/coverage.svg",
            get(get_branch_coverage_badge),
        )
        .route(
            "/badges/{owner}/{repo}/benchmark.svg",
            get(get_repo_benchmark_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/benchmark.svg",
            get(get_branch_benchmark_badge),
        )
        .route("/badges/{owner}/{repo}/shield.json", get(get_repo_shield))
        .route(
            "/badges/{owner}/{repo}/{branch}/shield.json",
            get(get_branch_shield),
        )
        .route("/cards/{owner}/{repo}/card.svg", get(get_repo_card))
        .route(
            "/cards/{owner}/{repo}/{branch}/card.svg",
            get(get_branch_card),
        )
        // Submodule Badges & Cards
        .route(
            "/badges/{owner}/{repo}/submodules/{submodule}/status.svg",
            get(get_submodule_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/submodules/{submodule}/status.svg",
            get(get_branch_submodule_badge),
        )
        .route(
            "/badges/{owner}/{repo}/submodules/{submodule}/score.svg",
            get(get_submodule_score_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/submodules/{submodule}/score.svg",
            get(get_branch_submodule_score_badge),
        )
        .route(
            "/badges/{owner}/{repo}/submodules/{submodule}/coverage.svg",
            get(get_submodule_coverage_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/submodules/{submodule}/coverage.svg",
            get(get_branch_submodule_coverage_badge),
        )
        .route(
            "/badges/{owner}/{repo}/submodules/{submodule}/benchmark.svg",
            get(get_submodule_benchmark_badge),
        )
        .route(
            "/badges/{owner}/{repo}/{branch}/submodules/{submodule}/benchmark.svg",
            get(get_branch_submodule_benchmark_badge),
        )
        .route(
            "/cards/{owner}/{repo}/submodules/{submodule}/card.svg",
            get(get_submodule_card),
        )
        .route(
            "/cards/{owner}/{repo}/{branch}/submodules/{submodule}/card.svg",
            get(get_branch_submodule_card),
        )
        .route("/", get(health_check))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn json_response(status: StatusCode, val: serde_json::Value) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        Json(val),
    )
        .into_response()
}

async fn health_check(State(state): State<Arc<AppState>>) -> Response {
    let snap = state.job_store.queue_snapshot().await;
    json_response(
        StatusCode::OK,
        serde_json::json!({
            "status": "ok",
            "service": "ActionRunner-rs",
            "arch": "riscv64",
            "version": env!("CARGO_PKG_VERSION"),
            "queue_status": snap.status,
            "queue_depth": snap.queue_depth,
            "concurrency": snap.concurrency
        }),
    )
}

async fn get_queue(State(state): State<Arc<AppState>>) -> Response {
    let snap = state.job_store.queue_snapshot().await;
    json_response(StatusCode::OK, serde_json::to_value(snap).unwrap())
}

#[derive(Debug, Default, Deserialize)]
struct JobsQuery {
    repo: Option<String>,
    status: Option<String>,
    limit: Option<usize>,
}

async fn list_jobs(State(state): State<Arc<AppState>>, Query(query): Query<JobsQuery>) -> Response {
    let snap = state.job_store.queue_snapshot().await;
    let all_jobs = state.job_store.list_jobs().await;

    let mut jobs: Vec<_> = all_jobs
        .into_iter()
        .filter(|rec| {
            if let Some(ref repo_filter) = query.repo {
                if !repo_filter.is_empty()
                    && !rec
                        .job
                        .repo_name
                        .to_ascii_lowercase()
                        .contains(&repo_filter.to_ascii_lowercase())
                {
                    return false;
                }
            }
            if let Some(ref status_filter) = query.status {
                let s = status_filter.to_ascii_lowercase();
                if !s.is_empty() && s != "all" {
                    let matches_status = match &rec.status {
                        JobStatus::Queued => s == "queued",
                        JobStatus::Running { .. } => s == "running",
                        JobStatus::Completed { exit_code: 0, .. } => {
                            s == "completed" || s == "passing" || s == "passed"
                        }
                        JobStatus::Completed { .. } | JobStatus::Failed { .. } => {
                            s == "failed" || s == "failing"
                        }
                        JobStatus::Skipped { .. } => s == "skipped" || s == "superseded",
                    };
                    if !matches_status {
                        return false;
                    }
                }
            }
            true
        })
        .collect();

    if let Some(limit) = query.limit {
        jobs.truncate(limit);
    }

    let job_ids: Vec<String> = jobs.iter().map(|j| j.job.id.clone()).collect();
    let active_job_id = snap.active_job.as_ref().map(|a| a.record.job.id.clone());

    json_response(
        StatusCode::OK,
        serde_json::json!({
            "job_ids": job_ids,
            "total": jobs.len(),
            "queue_depth": snap.queue_depth,
            "active_job_id": active_job_id,
            "stats": snap.stats,
            "jobs": jobs
        }),
    )
}

async fn get_job(State(state): State<Arc<AppState>>, Path(job_id): Path<String>) -> Response {
    match state.job_store.get_job(&job_id).await {
        Some(record) => {
            let mut val = serde_json::to_value(&record).unwrap();
            if let Some(obj) = val.as_object_mut() {
                if let Some(pos) = state.job_store.job_queue_position(&job_id).await {
                    obj.insert("queue_position".to_string(), serde_json::json!(pos));
                }
                if let JobStatus::Running { started_at } = record.status {
                    let elapsed = (Utc::now() - started_at).num_seconds().max(0) as u64;
                    obj.insert("elapsed_secs".to_string(), serde_json::json!(elapsed));
                }
            }
            json_response(StatusCode::OK, val)
        }
        None => json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "Job not found", "job_id": job_id }),
        ),
    }
}

async fn get_job_log(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Response {
    // If a log API key is configured, authenticate the caller
    if let Some(ref required_key) = state.config.log_api_key {
        let auth_hdr = headers
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        let api_key_hdr = headers.get("x-api-key").and_then(|h| h.to_str().ok());

        let authed =
            auth_hdr == Some(required_key.as_str()) || api_key_hdr == Some(required_key.as_str());
        if !authed {
            return (
                StatusCode::UNAUTHORIZED,
                [
                    (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                ],
                serde_json::to_string(&serde_json::json!({
                    "error": "Unauthorized: valid X-Api-Key or Authorization Bearer token required to access execution logs"
                })).unwrap(),
            )
                .into_response();
        }
    }

    let Some(record) = state.job_store.get_job(&job_id).await else {
        return (
            StatusCode::NOT_FOUND,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            ],
            "Job not found".to_string(),
        )
            .into_response();
    };

    let full_path = state.config.workspace_dir.join(&record.log_file);
    if !full_path.starts_with(&state.config.workspace_dir) {
        return (
            StatusCode::FORBIDDEN,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            ],
            "Forbidden: log path escapes workspace boundary".to_string(),
        )
            .into_response();
    }
    match tokio::fs::read_to_string(&full_path).await {
        Ok(content) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            ],
            content,
        )
            .into_response(),
        Err(_) => match &record.status {
            JobStatus::Queued => {
                let pos = state
                    .job_store
                    .job_queue_position(&job_id)
                    .await
                    .unwrap_or(1);
                let msg = format!(
                    "[ActionRunner-rs] Job '{}' is currently QUEUED (position #{}) waiting for an available worker.\nRepository: {}\nRef:        {}\nCommit SHA: {}\nQueued At:  {}\n",
                    record.job.id,
                    pos,
                    record.job.repo_name,
                    record.job.ref_name,
                    record.job.commit_sha,
                    record.job.queued_at.to_rfc3339()
                );
                (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                        (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
                        (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                    ],
                    msg,
                )
                    .into_response()
            }
            JobStatus::Skipped { reason } => {
                let msg = format!(
                    "[ActionRunner-rs] Job '{}' was SKIPPED before execution.\nReason:     {}\nRepository: {}\nRef:        {}\nCommit SHA: {}\nQueued At:  {}\n",
                    record.job.id,
                    reason,
                    record.job.repo_name,
                    record.job.ref_name,
                    record.job.commit_sha,
                    record.job.queued_at.to_rfc3339()
                );
                (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                        (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
                        (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                    ],
                    msg,
                )
                    .into_response()
            }
            _ => (
                StatusCode::NOT_FOUND,
                [
                    (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                ],
                "Log file not found or not yet generated".to_string(),
            )
                .into_response(),
        },
    }
}

async fn get_repo_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let svg = badge::render_status_badge("k3-ci", record.as_ref());
    svg_response(svg)
}

async fn get_branch_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let svg = badge::render_status_badge("k3-ci", record.as_ref());
    svg_response(svg)
}

async fn get_repo_shield(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let val = badge::render_shield_json("k3-ci", record.as_ref());
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        Json(val),
    )
        .into_response()
}

async fn get_branch_shield(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let val = badge::render_shield_json("k3-ci", record.as_ref());
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        Json(val),
    )
        .into_response()
}

async fn get_repo_card(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let branch = record
        .as_ref()
        .map(|r| r.job.ref_name.trim_start_matches("refs/heads/").to_string())
        .unwrap_or_else(|| "main".to_string());
    let svg = badge::render_card_svg(&repo_full, &branch, record.as_ref());
    svg_response(svg)
}

async fn get_branch_card(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let svg = badge::render_card_svg(&repo_full, &branch, record.as_ref());
    svg_response(svg)
}

async fn get_repo_score_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let svg = badge::render_score_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_score_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let svg = badge::render_score_badge(record.as_ref());
    svg_response(svg)
}

async fn get_repo_coverage_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let svg = badge::render_coverage_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_coverage_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let svg = badge::render_coverage_badge(record.as_ref());
    svg_response(svg)
}

async fn get_repo_benchmark_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let svg = badge::render_benchmark_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_benchmark_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch)): Path<(String, String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_latest_job(&repo_full, Some(&branch))
        .await;
    let svg = badge::render_benchmark_badge(record.as_ref());
    svg_response(svg)
}

fn svg_response(svg: String) -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600, s-maxage=3600"),
        ],
        svg,
    )
        .into_response()
}

async fn get_submodule_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, submodule)): Path<(String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, None)
        .await;
    let svg = badge::render_status_badge("k3-ci", record.as_ref());
    svg_response(svg)
}

async fn get_branch_submodule_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch, submodule)): Path<(String, String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, Some(&branch))
        .await;
    let svg = badge::render_status_badge("k3-ci", record.as_ref());
    svg_response(svg)
}

async fn get_submodule_score_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, submodule)): Path<(String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, None)
        .await;
    let svg = badge::render_score_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_submodule_score_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch, submodule)): Path<(String, String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, Some(&branch))
        .await;
    let svg = badge::render_score_badge(record.as_ref());
    svg_response(svg)
}

async fn get_submodule_coverage_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, submodule)): Path<(String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, None)
        .await;
    let svg = badge::render_coverage_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_submodule_coverage_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch, submodule)): Path<(String, String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, Some(&branch))
        .await;
    let svg = badge::render_coverage_badge(record.as_ref());
    svg_response(svg)
}

async fn get_submodule_benchmark_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, submodule)): Path<(String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, None)
        .await;
    let svg = badge::render_benchmark_badge(record.as_ref());
    svg_response(svg)
}

async fn get_branch_submodule_benchmark_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch, submodule)): Path<(String, String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, Some(&branch))
        .await;
    let svg = badge::render_benchmark_badge(record.as_ref());
    svg_response(svg)
}

async fn get_submodule_card(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, submodule)): Path<(String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, None)
        .await;
    let sub_name = format!("{}/{}", owner, submodule);
    let branch = record
        .as_ref()
        .map(|r| r.job.ref_name.trim_start_matches("refs/heads/").to_string())
        .unwrap_or_else(|| "main".to_string());
    let svg = badge::render_card_svg(&sub_name, &branch, record.as_ref());
    svg_response(svg)
}

async fn get_branch_submodule_card(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, branch, submodule)): Path<(String, String, String, String)>,
) -> Response {
    let parent_full = format!("{}/{}", owner, repo);
    let record = state
        .job_store
        .find_submodule_job(&parent_full, &submodule, Some(&branch))
        .await;
    let sub_name = format!("{}/{}", owner, submodule);
    let svg = badge::render_card_svg(&sub_name, &branch, record.as_ref());
    svg_response(svg)
}
