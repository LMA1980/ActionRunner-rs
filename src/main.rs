mod badge;
mod config;
mod executor;
mod job;
mod webhook;
mod workflow;

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::Config;
use crate::executor::Executor;
use crate::job::JobStore;
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
    } else {
        info!("GitHub Webhook HMAC signature verification: DISABLED (set GITHUB_WEBHOOK_SECRET to enable)");
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
        .route("/webhook", post(handle_webhook))
        .route("/jobs", get(list_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/log", get(get_job_log))
        // Dynamic Status Badges & Cards
        .route("/badges/{owner}/{repo}/status.svg", get(get_repo_badge))
        .route(
            "/badges/{owner}/{repo}/{branch}/status.svg",
            get(get_branch_badge),
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

async fn list_jobs(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let jobs = state.job_store.list_jobs().await;
    let job_ids: Vec<String> = jobs.iter().map(|j| j.job.id.clone()).collect();
    Json(serde_json::json!({
        "job_ids": job_ids,
        "total": jobs.len(),
        "jobs": jobs
    }))
}

async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match state.job_store.get_job(&job_id).await {
        Some(record) => Ok(Json(serde_json::to_value(record).unwrap())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Job not found", "job_id": job_id })),
        )),
    }
}

async fn get_job_log(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<String, (StatusCode, &'static str)> {
    let record = state
        .job_store
        .get_job(&job_id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "Job not found"))?;

    let full_path = state.config.workspace_dir.join(&record.log_file);
    tokio::fs::read_to_string(&full_path).await.map_err(|_| {
        (
            StatusCode::NOT_FOUND,
            "Log file not found or not yet generated",
        )
    })
}

async fn get_repo_badge(
    State(state): State<Arc<AppState>>,
    Path((owner, repo)): Path<(String, String)>,
) -> Response {
    let repo_full = format!("{}/{}", owner, repo);
    let record = state.job_store.find_latest_job(&repo_full, None).await;
    let svg = badge::render_status_badge("k3-ci", record.as_ref());
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        svg,
    )
        .into_response()
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
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        svg,
    )
        .into_response()
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
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        svg,
    )
        .into_response()
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
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
        ],
        svg,
    )
        .into_response()
}
