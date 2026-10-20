use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
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
pub struct JobRecord {
    pub job: Job,
    pub status: JobStatus,
    pub log_file: PathBuf,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct JobStore {
    inner: Arc<RwLock<VecDeque<JobRecord>>>,
    max_history: usize,
}

impl JobStore {
    pub fn new(max_history: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(VecDeque::with_capacity(max_history))),
            max_history,
        }
    }

    pub async fn add_job(&self, job: Job, log_file: PathBuf) {
        let mut store = self.inner.write().await;
        if store.len() >= self.max_history {
            store.pop_back();
        }
        store.push_front(JobRecord {
            job,
            status: JobStatus::Queued,
            log_file,
            updated_at: Utc::now(),
        });
    }

    pub async fn update_status(&self, job_id: &str, status: JobStatus) {
        let mut store = self.inner.write().await;
        if let Some(record) = store.iter_mut().find(|r| r.job.id == job_id) {
            record.status = status;
            record.updated_at = Utc::now();
        }
    }

    pub async fn get_job(&self, job_id: &str) -> Option<JobRecord> {
        let store = self.inner.read().await;
        store.iter().find(|r| r.job.id == job_id).cloned()
    }

    pub async fn list_jobs(&self) -> Vec<JobRecord> {
        let store = self.inner.read().await;
        store.iter().cloned().collect()
    }
}
