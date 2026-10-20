use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub event_type: String,
    pub repo_name: String,
    pub clone_url: String,
    pub commit_sha: String,
    pub ref_name: String,
    pub sender: String,
    pub queued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", content = "details")]
pub enum JobStatus {
    Queued,
    Running { started_at: DateTime<Utc> },
    Completed { duration_secs: u64, exit_code: i32 },
    Failed { error: String },
    Skipped { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetrics {
    pub quality_score: u32,
    pub letter_grade: String,
    #[serde(default)]
    pub coverage_pct: Option<f64>,
    #[serde(default)]
    pub benchmark_summary: Option<String>,
    #[serde(default)]
    pub tests_passed: u32,
    #[serde(default)]
    pub tests_total: u32,
    #[serde(default)]
    pub advisories_count: u32,
    #[serde(default)]
    pub clippy_warnings: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub job: Job,
    pub status: JobStatus,
    pub log_file: PathBuf,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub metrics: Option<JobMetrics>,
}

#[derive(Clone)]
pub struct JobStore {
    inner: Arc<RwLock<VecDeque<JobRecord>>>,
    max_history: usize,
    index_file: Option<PathBuf>,
}

impl JobStore {
    pub fn new(max_history: usize, workspace_dir: Option<&Path>) -> Self {
        let index_file = workspace_dir.map(|w| w.join("logs").join("jobs_index.json"));
        let mut initial_jobs = VecDeque::with_capacity(max_history);

        if let Some(ref path) = index_file {
            if path.exists() {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(jobs) = serde_json::from_str::<VecDeque<JobRecord>>(&content) {
                        info!(
                            "Restored {} job records from {}",
                            jobs.len(),
                            path.display()
                        );
                        initial_jobs = jobs;
                        while initial_jobs.len() > max_history {
                            initial_jobs.pop_back();
                        }
                    }
                }
            }
        }

        Self {
            inner: Arc::new(RwLock::new(initial_jobs)),
            max_history,
            index_file,
        }
    }

    async fn persist(&self) {
        if let Some(ref path) = self.index_file {
            let store = self.inner.read().await;
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Ok(json_str) = serde_json::to_string_pretty(&*store) {
                if let Err(e) = tokio::fs::write(path, json_str).await {
                    warn!("Failed to persist jobs_index.json: {}", e);
                }
            }
        }
    }

    pub async fn add_job(&self, job: Job, log_file: PathBuf) {
        {
            let mut store = self.inner.write().await;
            if store.len() >= self.max_history {
                store.pop_back();
            }
            store.push_front(JobRecord {
                job,
                status: JobStatus::Queued,
                log_file,
                updated_at: Utc::now(),
                metrics: None,
            });
        }
        self.persist().await;
    }

    pub async fn update_metrics(&self, job_id: &str, metrics: JobMetrics) {
        {
            let mut store = self.inner.write().await;
            if let Some(record) = store.iter_mut().find(|r| r.job.id == job_id) {
                record.metrics = Some(metrics);
                record.updated_at = Utc::now();
            }
        }
        self.persist().await;
    }

    pub async fn update_status(&self, job_id: &str, status: JobStatus) {
        {
            let mut store = self.inner.write().await;
            if let Some(record) = store.iter_mut().find(|r| r.job.id == job_id) {
                record.status = status;
                record.updated_at = Utc::now();
            }
        }
        self.persist().await;
    }

    pub async fn get_job(&self, job_id: &str) -> Option<JobRecord> {
        let store = self.inner.read().await;
        store.iter().find(|r| r.job.id == job_id).cloned()
    }

    pub async fn list_jobs(&self) -> Vec<JobRecord> {
        let store = self.inner.read().await;
        store.iter().cloned().collect()
    }

    pub async fn find_latest_job(&self, repo: &str, branch: Option<&str>) -> Option<JobRecord> {
        let store = self.inner.read().await;
        for record in store.iter() {
            let matches_repo = record.job.repo_name.eq_ignore_ascii_case(repo)
                || record.job.repo_name.ends_with(&format!("/{}", repo));

            if !matches_repo {
                continue;
            }

            if let Some(b) = branch {
                let matches_branch = record.job.ref_name.eq_ignore_ascii_case(b)
                    || record.job.ref_name == format!("refs/heads/{}", b)
                    || record.job.ref_name.ends_with(&format!("/{}", b))
                    || record.job.ref_name == format!("refs/tags/{}", b);

                if matches_branch {
                    return Some(record.clone());
                }
            } else {
                return Some(record.clone());
            }
        }
        None
    }
}
