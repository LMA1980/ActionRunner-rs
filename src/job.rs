use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
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
                    let sub_short = sub_name.rsplit_once('/').map(|(_, r)| r).unwrap_or(sub_name);
                    let path_short = sub.path.rsplit_once('/').map(|(_, r)| r).unwrap_or(&sub.path);

                    let is_match = sub_name.eq_ignore_ascii_case(repo)
                        || sub_short.eq_ignore_ascii_case(clean_target)
                        || path_short.eq_ignore_ascii_case(clean_target)
                        || sub_name.replace('-', "_").eq_ignore_ascii_case(&clean_target.replace('-', "_"));

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
        let clean_target = submodule.rsplit_once('/').map(|(_, r)| r).unwrap_or(submodule);

        for (sub_name, sub) in &metrics.submodules {
            let sub_short = sub_name.rsplit_once('/').map(|(_, r)| r).unwrap_or(sub_name);
            let path_short = sub.path.rsplit_once('/').map(|(_, r)| r).unwrap_or(&sub.path);

            let is_match = sub_name.eq_ignore_ascii_case(submodule)
                || sub_short.eq_ignore_ascii_case(clean_target)
                || path_short.eq_ignore_ascii_case(clean_target)
                || sub_name.replace('-', "_").eq_ignore_ascii_case(&clean_target.replace('-', "_"));

            if is_match {
                let owner = parent_repo.split_once('/').map(|(o, _)| o).unwrap_or("LMA1980");
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

        store.add_job(job.clone(), PathBuf::from("logs/test.log")).await;
        store.update_metrics(&job.id, JobMetrics {
            quality_score: 98,
            letter_grade: "A+".to_string(),
            coverage_pct: Some(90.0),
            benchmark_summary: None,
            tests_passed: 10,
            tests_total: 10,
            advisories_count: 0,
            clippy_warnings: 0,
            submodules,
        }).await;
        store.update_status(&job.id, JobStatus::Completed {
            duration_secs: 5,
            exit_code: 0,
        }).await;

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
}

