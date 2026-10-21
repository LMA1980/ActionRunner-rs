use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
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
pub struct SubmoduleMetrics {
    pub name: String,
    #[serde(default)]
    pub path: String,
    pub quality_score: u32,
    pub letter_grade: String,
    #[serde(default)]
    pub tests_passed: u32,
    #[serde(default)]
    pub tests_total: u32,
    #[serde(default)]
    pub coverage_pct: Option<f64>,
    #[serde(default)]
    pub benchmark_summary: Option<String>,
    #[serde(default)]
    pub clippy_warnings: u32,
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
    #[serde(default)]
    pub submodules: BTreeMap<String, SubmoduleMetrics>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueResult {
    pub job_id: String,
    pub queue_position: usize,
    pub superseded_jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItemView {
    pub position: usize,
    pub wait_secs: u64,
    pub elapsed_secs: Option<u64>,
    pub record: JobRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueStats {
    pub total_jobs: usize,
    pub running_jobs: usize,
    pub queued_jobs: usize,
    pub completed_jobs: usize,
    pub failed_jobs: usize,
    pub skipped_jobs: usize,
    pub superseded_jobs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub status: String,
    pub concurrency: usize,
    pub queue_depth: usize,
    pub active_job: Option<QueueItemView>,
    pub queued_jobs: Vec<QueueItemView>,
    pub stats: QueueStats,
    pub recent_jobs: Vec<JobRecord>,
}

#[derive(Clone)]
pub struct JobStore {
    inner: Arc<RwLock<VecDeque<JobRecord>>>,
    notify: Arc<Notify>,
    max_history: usize,
    index_file: Option<PathBuf>,
}

impl JobStore {
    pub fn new(max_history: usize, workspace_dir: Option<&Path>) -> Self {
        let index_file = workspace_dir.map(|w| w.join("logs").join("jobs_index.json"));
        let mut initial_jobs = VecDeque::with_capacity(max_history);
        let notify = Arc::new(Notify::new());

        if let Some(ref path) = index_file {
            if path.exists() {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(mut jobs) = serde_json::from_str::<VecDeque<JobRecord>>(&content) {
                        let mut has_queued = false;
                        for rec in jobs.iter_mut() {
                            if matches!(rec.status, JobStatus::Running { .. }) {
                                rec.status = JobStatus::Queued;
                                rec.updated_at = Utc::now();
                            }
                            if matches!(rec.status, JobStatus::Queued) {
                                has_queued = true;
                            }
                        }
                        if has_queued {
                            notify.notify_one();
                        }
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
            notify,
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

    /// Enqueues a job into the coalescing FIFO queue.
    /// If any older job for the same `(repo_name, ref_name, event_type)` is currently `Queued`
    /// waiting for execution, it is marked as `Skipped` (superseded by this newer commit) so
    /// limited host resources only build the latest commit on that ref.
    pub async fn enqueue_job(&self, mut job: Job, mut log_file: PathBuf) -> EnqueueResult {
        let now = Utc::now();
        let mut superseded_jobs = Vec::new();
        let queue_position;

        {
            let mut store = self.inner.write().await;

            // Ensure unique job.id even when multiple webhooks arrive within the same second
            let base_id = job.id.clone();
            let mut suffix = 2u32;
            while store.iter().any(|r| r.job.id == job.id) {
                job.id = format!("{}-{}", base_id, suffix);
                log_file = Path::new("logs")
                    .join(&job.repo_name)
                    .join(format!("{}.log", job.id));
                suffix += 1;
            }

            let sha_short = &job.commit_sha[..8.min(job.commit_sha.len())];

            // Coalesce any older still-Queued jobs for the same repo + ref + event_type
            for record in store.iter_mut() {
                if matches!(record.status, JobStatus::Queued)
                    && record.job.repo_name.eq_ignore_ascii_case(&job.repo_name)
                    && record.job.ref_name == job.ref_name
                    && record.job.event_type == job.event_type
                {
                    superseded_jobs.push(record.job.id.clone());
                    record.status = JobStatus::Skipped {
                        reason: format!(
                            "Superseded in queue by newer commit {} ({})",
                            sha_short, job.id
                        ),
                    };
                    record.updated_at = now;
                }
            }

            // Evict oldest terminal record if at max_history capacity (never evict Queued/Running)
            if store.len() >= self.max_history {
                if let Some(pos) = store.iter().rposition(|r| {
                    !matches!(r.status, JobStatus::Queued | JobStatus::Running { .. })
                }) {
                    store.remove(pos);
                }
            }

            store.push_front(JobRecord {
                job: job.clone(),
                status: JobStatus::Queued,
                log_file,
                updated_at: now,
                metrics: None,
            });

            // Compute 1-based FIFO queue position among all currently Queued jobs
            queue_position = store
                .iter()
                .filter(|r| matches!(r.status, JobStatus::Queued))
                .count();
        }

        self.persist().await;
        self.notify.notify_one();

        EnqueueResult {
            job_id: job.id,
            queue_position,
            superseded_jobs,
        }
    }

    #[cfg(test)]
    pub async fn add_job(&self, job: Job, log_file: PathBuf) {
        let _ = self.enqueue_job(job, log_file).await;
    }

    /// Atomically claims the oldest `Queued` job in FIFO order and transitions it to `Running`.
    pub async fn claim_next_queued_job(&self) -> Option<Job> {
        let claimed = {
            let mut store = self.inner.write().await;
            let now = Utc::now();
            let mut found = None;
            for record in store.iter_mut().rev() {
                if matches!(record.status, JobStatus::Queued) {
                    record.status = JobStatus::Running { started_at: now };
                    record.updated_at = now;
                    found = Some(record.job.clone());
                    break;
                }
            }
            found
        };

        if claimed.is_some() {
            self.persist().await;
        }
        claimed
    }

    pub async fn wait_for_job(&self) {
        self.notify.notified().await;
    }

    pub async fn queue_snapshot(&self) -> QueueSnapshot {
        let store = self.inner.read().await;
        let now = Utc::now();

        let mut stats = QueueStats {
            total_jobs: store.len(),
            ..Default::default()
        };

        let mut active_job: Option<QueueItemView> = None;
        let mut queued_records: Vec<JobRecord> = Vec::new();

        for record in store.iter() {
            match &record.status {
                JobStatus::Running { started_at } => {
                    stats.running_jobs += 1;
                    let elapsed = (now - *started_at).num_seconds().max(0) as u64;
                    let wait = (*started_at - record.job.queued_at).num_seconds().max(0) as u64;
                    if active_job.is_none() {
                        active_job = Some(QueueItemView {
                            position: 0,
                            wait_secs: wait,
                            elapsed_secs: Some(elapsed),
                            record: record.clone(),
                        });
                    }
                }
                JobStatus::Queued => {
                    stats.queued_jobs += 1;
                    queued_records.push(record.clone());
                }
                JobStatus::Completed { exit_code: 0, .. } => {
                    stats.completed_jobs += 1;
                }
                JobStatus::Completed { .. } | JobStatus::Failed { .. } => {
                    stats.failed_jobs += 1;
                }
                JobStatus::Skipped { reason } => {
                    stats.skipped_jobs += 1;
                    if reason.contains("Superseded in queue") {
                        stats.superseded_jobs += 1;
                    }
                }
            }
        }

        // Order queued jobs in FIFO execution order (oldest first = position 1)
        queued_records.reverse();
        let queued_jobs: Vec<QueueItemView> = queued_records
            .into_iter()
            .enumerate()
            .map(|(idx, rec)| {
                let wait = (now - rec.job.queued_at).num_seconds().max(0) as u64;
                QueueItemView {
                    position: idx + 1,
                    wait_secs: wait,
                    elapsed_secs: None,
                    record: rec,
                }
            })
            .collect();

        let status = if active_job.is_some() || !queued_jobs.is_empty() {
            "busy".to_string()
        } else {
            "idle".to_string()
        };

        let recent_jobs = store.iter().take(50).cloned().collect();

        QueueSnapshot {
            status,
            concurrency: 1,
            queue_depth: queued_jobs.len(),
            active_job,
            queued_jobs,
            stats,
            recent_jobs,
        }
    }

    pub async fn job_queue_position(&self, job_id: &str) -> Option<usize> {
        let store = self.inner.read().await;
        let mut pos = 0usize;
        for record in store.iter().rev() {
            if matches!(record.status, JobStatus::Queued) {
                pos += 1;
                if record.job.id == job_id {
                    return Some(pos);
                }
            }
        }
        None
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

        // 1. Direct match on parent repository name
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

        // 2. Submodule fallback: if repo targets a submodule (e.g. "LMA1980/k3-core" or "k3-core")
        let clean_target = repo.rsplit_once('/').map(|(_, r)| r).unwrap_or(repo);
        for record in store.iter() {
            if let Some(ref metrics) = record.metrics {
                for (sub_name, sub) in &metrics.submodules {
                    let sub_short = sub_name
                        .rsplit_once('/')
                        .map(|(_, r)| r)
                        .unwrap_or(sub_name);
                    let path_short = sub
                        .path
                        .rsplit_once('/')
                        .map(|(_, r)| r)
                        .unwrap_or(&sub.path);

                    let is_match = sub_name.eq_ignore_ascii_case(repo)
                        || sub_short.eq_ignore_ascii_case(clean_target)
                        || path_short.eq_ignore_ascii_case(clean_target)
                        || sub_name
                            .replace('-', "_")
                            .eq_ignore_ascii_case(&clean_target.replace('-', "_"));

                    if is_match {
                        let owner = repo.split_once('/').map(|(o, _)| o).unwrap_or("LMA1980");
                        let sub_job = Job {
                            id: format!("{}-{}", record.job.id, sub_short),
                            event_type: record.job.event_type.clone(),
                            repo_name: format!("{}/{}", owner, sub_short),
                            clone_url: String::new(),
                            commit_sha: record.job.commit_sha.clone(),
                            ref_name: record.job.ref_name.clone(),
                            sender: record.job.sender.clone(),
                            queued_at: record.job.queued_at,
                        };
                        return Some(JobRecord {
                            job: sub_job,
                            status: record.status.clone(),
                            log_file: record.log_file.clone(),
                            updated_at: record.updated_at,
                            metrics: Some(JobMetrics {
                                quality_score: sub.quality_score,
                                letter_grade: sub.letter_grade.clone(),
                                coverage_pct: sub.coverage_pct,
                                benchmark_summary: sub.benchmark_summary.clone(),
                                tests_passed: sub.tests_passed,
                                tests_total: sub.tests_total,
                                advisories_count: 0,
                                clippy_warnings: sub.clippy_warnings,
                                submodules: BTreeMap::new(),
                            }),
                        });
                    }
                }
            }
        }

        None
    }

    pub async fn find_submodule_job(
        &self,
        parent_repo: &str,
        submodule: &str,
        branch: Option<&str>,
    ) -> Option<JobRecord> {
        let parent_record = self.find_latest_job(parent_repo, branch).await?;
        let metrics = parent_record.metrics.as_ref()?;
        let clean_target = submodule
            .rsplit_once('/')
            .map(|(_, r)| r)
            .unwrap_or(submodule);

        for (sub_name, sub) in &metrics.submodules {
            let sub_short = sub_name
                .rsplit_once('/')
                .map(|(_, r)| r)
                .unwrap_or(sub_name);
            let path_short = sub
                .path
                .rsplit_once('/')
                .map(|(_, r)| r)
                .unwrap_or(&sub.path);

            let is_match = sub_name.eq_ignore_ascii_case(submodule)
                || sub_short.eq_ignore_ascii_case(clean_target)
                || path_short.eq_ignore_ascii_case(clean_target)
                || sub_name
                    .replace('-', "_")
                    .eq_ignore_ascii_case(&clean_target.replace('-', "_"));

            if is_match {
                let owner = parent_repo
                    .split_once('/')
                    .map(|(o, _)| o)
                    .unwrap_or("LMA1980");
                let sub_job = Job {
                    id: format!("{}-{}", parent_record.job.id, sub_short),
                    event_type: parent_record.job.event_type.clone(),
                    repo_name: format!("{}/{}", owner, sub_short),
                    clone_url: String::new(),
                    commit_sha: parent_record.job.commit_sha.clone(),
                    ref_name: parent_record.job.ref_name.clone(),
                    sender: parent_record.job.sender.clone(),
                    queued_at: parent_record.job.queued_at,
                };
                return Some(JobRecord {
                    job: sub_job,
                    status: parent_record.status.clone(),
                    log_file: parent_record.log_file.clone(),
                    updated_at: parent_record.updated_at,
                    metrics: Some(JobMetrics {
                        quality_score: sub.quality_score,
                        letter_grade: sub.letter_grade.clone(),
                        coverage_pct: sub.coverage_pct,
                        benchmark_summary: sub.benchmark_summary.clone(),
                        tests_passed: sub.tests_passed,
                        tests_total: sub.tests_total,
                        advisories_count: 0,
                        clippy_warnings: sub.clippy_warnings,
                        submodules: BTreeMap::new(),
                    }),
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_submodule_job_lookup() {
        let store = JobStore::new(10, None);
        let job = Job {
            id: "job-model-runner-1".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/model-runner".to_string(),
            clone_url: "https://github.com/LMA1980/model-runner.git".to_string(),
            commit_sha: "abcd1234efgh".to_string(),
            ref_name: "refs/heads/main".to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        };

        let mut submodules = BTreeMap::new();
        submodules.insert(
            "k3-core".to_string(),
            SubmoduleMetrics {
                name: "k3-core".to_string(),
                path: "code/core".to_string(),
                quality_score: 95,
                letter_grade: "A+".to_string(),
                tests_passed: 4,
                tests_total: 4,
                coverage_pct: Some(92.5),
                benchmark_summary: Some("1 benches (49.0 ns/iter)".to_string()),
                clippy_warnings: 0,
            },
        );

        store
            .add_job(job.clone(), PathBuf::from("logs/test.log"))
            .await;
        store
            .update_metrics(
                &job.id,
                JobMetrics {
                    quality_score: 98,
                    letter_grade: "A+".to_string(),
                    coverage_pct: Some(90.0),
                    benchmark_summary: None,
                    tests_passed: 10,
                    tests_total: 10,
                    advisories_count: 0,
                    clippy_warnings: 0,
                    submodules,
                },
            )
            .await;
        store
            .update_status(
                &job.id,
                JobStatus::Completed {
                    duration_secs: 5,
                    exit_code: 0,
                },
            )
            .await;

        // Test direct parent lookup
        let parent = store.find_latest_job("LMA1980/model-runner", None).await;
        assert!(parent.is_some());
        assert_eq!(parent.unwrap().metrics.unwrap().quality_score, 98);

        // Test fallback lookup using repo name "k3-core"
        let sub_fallback = store.find_latest_job("LMA1980/k3-core", None).await;
        assert!(sub_fallback.is_some());
        let sub_rec = sub_fallback.unwrap();
        assert_eq!(sub_rec.job.repo_name, "LMA1980/k3-core");
        let sub_m = sub_rec.metrics.unwrap();
        assert_eq!(sub_m.quality_score, 95);
        assert_eq!(sub_m.letter_grade, "A+");
        assert_eq!(sub_m.tests_passed, 4);
        assert_eq!(sub_m.coverage_pct, Some(92.5));

        // Test fallback lookup using short name "k3-core"
        let sub_short = store.find_latest_job("k3-core", None).await;
        assert!(sub_short.is_some());

        // Test find_submodule_job explicitly
        let explicit_sub = store
            .find_submodule_job("LMA1980/model-runner", "k3-core", None)
            .await;
        assert!(explicit_sub.is_some());
        assert_eq!(explicit_sub.unwrap().metrics.unwrap().quality_score, 95);

        // Test path match fallback e.g. "core"
        let path_sub = store
            .find_submodule_job("LMA1980/model-runner", "core", None)
            .await;
        assert!(path_sub.is_some());
        assert_eq!(path_sub.unwrap().metrics.unwrap().quality_score, 95);
    }

    #[tokio::test]
    async fn test_coalescing_fifo_queue_and_snapshot() {
        let store = JobStore::new(20, None);

        // 1. Enqueue job 1 for ActionRunner-rs and claim it so it is Running
        let job1 = Job {
            id: "job-100-aaaa1111".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/ActionRunner-rs".to_string(),
            clone_url: "https://github.com/LMA1980/ActionRunner-rs.git".to_string(),
            commit_sha: "aaaa111100000000".to_string(),
            ref_name: "refs/heads/main".to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        };
        let res1 = store
            .enqueue_job(job1.clone(), PathBuf::from("logs/job1.log"))
            .await;
        assert_eq!(res1.queue_position, 1);
        assert!(res1.superseded_jobs.is_empty());

        let claimed1 = store.claim_next_queued_job().await;
        assert!(claimed1.is_some());
        assert_eq!(claimed1.unwrap().id, "job-100-aaaa1111");

        // 2. While job 1 is running, enqueue two pushes for model-runner main and one for k3-core
        let job2_old = Job {
            id: "job-101-bbbb2222".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/model-runner".to_string(),
            clone_url: "https://github.com/LMA1980/model-runner.git".to_string(),
            commit_sha: "bbbb222200000000".to_string(),
            ref_name: "refs/heads/main".to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        };
        let res2 = store
            .enqueue_job(job2_old.clone(), PathBuf::from("logs/job2.log"))
            .await;
        assert_eq!(res2.queue_position, 1);
        assert!(res2.superseded_jobs.is_empty());

        let job3_other = Job {
            id: "job-102-cccc3333".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/k3-core".to_string(),
            clone_url: "https://github.com/LMA1980/k3-core.git".to_string(),
            commit_sha: "cccc333300000000".to_string(),
            ref_name: "refs/heads/main".to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        };
        let res3 = store
            .enqueue_job(job3_other.clone(), PathBuf::from("logs/job3.log"))
            .await;
        assert_eq!(res3.queue_position, 2);

        // Newer push for model-runner main arrives while job2_old is still Queued -> coalesces job2_old!
        let job4_new = Job {
            id: "job-103-dddd4444".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/model-runner".to_string(),
            clone_url: "https://github.com/LMA1980/model-runner.git".to_string(),
            commit_sha: "dddd444400000000".to_string(),
            ref_name: "refs/heads/main".to_string(),
            sender: "LMA1980".to_string(),
            queued_at: Utc::now(),
        };
        let res4 = store
            .enqueue_job(job4_new.clone(), PathBuf::from("logs/job4.log"))
            .await;
        assert_eq!(res4.superseded_jobs, vec!["job-101-bbbb2222".to_string()]);
        assert_eq!(res4.queue_position, 2);

        // Verify queue snapshot reflects 1 active job, 2 queued jobs, and 1 superseded job
        let snap = store.queue_snapshot().await;
        assert_eq!(snap.status, "busy");
        assert_eq!(snap.queue_depth, 2);
        assert_eq!(snap.stats.running_jobs, 1);
        assert_eq!(snap.stats.queued_jobs, 2);
        assert_eq!(snap.stats.superseded_jobs, 1);
        assert_eq!(snap.queued_jobs[0].record.job.id, "job-102-cccc3333");
        assert_eq!(snap.queued_jobs[0].position, 1);
        assert_eq!(snap.queued_jobs[1].record.job.id, "job-103-dddd4444");
        assert_eq!(snap.queued_jobs[1].position, 2);

        // Claim remaining jobs in FIFO order
        let next1 = store.claim_next_queued_job().await.unwrap();
        assert_eq!(next1.id, "job-102-cccc3333");
        let next2 = store.claim_next_queued_job().await.unwrap();
        assert_eq!(next2.id, "job-103-dddd4444");
        assert!(store.claim_next_queued_job().await.is_none());
    }
}
