use chrono::Utc;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info};

use crate::config::Config;
use crate::job::{Job, JobStatus, JobStore};
use crate::workflow::WorkflowConfig;

pub struct Executor {
    config: Config,
    job_store: JobStore,
}

impl Executor {
    pub fn new(config: Config, job_store: JobStore) -> Self {
        Self { config, job_store }
    }

    pub async fn run_worker(self: Arc<Self>, mut rx: mpsc::Receiver<Job>) {
        info!("ActionRunner executor worker started (concurrency = 1)");

        while let Some(job) = rx.recv().await {
            info!(job_id = %job.id, repo = %job.repo_name, commit = %job.commit_sha, "Starting job execution");
            self.execute_job(job).await;
        }

        info!("ActionRunner executor worker terminated");
    }

    async fn execute_job(&self, job: Job) {
        let start_time = Utc::now();
        self.job_store.update_status(&job.id, JobStatus::Running { started_at: start_time }).await;

        let repo_workspace = self.config.workspace_dir.join("repos").join(&job.repo_name);
        let log_file_path = self.config.workspace_dir.join("logs").join(&job.repo_name).join(format!("{}.log", job.id));

        if let Some(parent) = log_file_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::create_dir_all(&repo_workspace).await;

        let mut log = match OpenOptions::new().create(true).append(true).open(&log_file_path) {
            Ok(f) => f,
            Err(e) => {
                error!(error = %e, "Failed to open job log file");
                self.job_store.update_status(&job.id, JobStatus::Failed { error: format!("Failed to open log file: {}", e) }).await;
                return;
            }
        };

        let _ = writeln!(log, "================================================================================");
        let _ = writeln!(log, "ActionRunner-rs Job Execution: {}", job.id);
        let _ = writeln!(log, "Repository: {}", job.repo_name);
        let _ = writeln!(log, "Commit SHA: {}", job.commit_sha);
        let _ = writeln!(log, "Ref:        {}", job.ref_name);
        let _ = writeln!(log, "Triggered:  {}", job.sender);
        let _ = writeln!(log, "Started At: {}", start_time.to_rfc3339());
        let _ = writeln!(log, "================================================================================\n");

        // 1. Sync Git Repository
        let git_status = self.sync_git_repo(&job, &repo_workspace, &mut log).await;
        if let Err(err_msg) = git_status {
            let _ = writeln!(log, "\n[ActionRunner-rs ERROR] Git sync failed: {}", err_msg);
            self.job_store.update_status(&job.id, JobStatus::Failed { error: err_msg }).await;
            return;
        }

        // 2. Discover YAML Workflow
        let workflow_file_yml = repo_workspace.join(".github/action-runner.yml");
        let workflow_file_yaml = repo_workspace.join(".github/action-runner.yaml");

        let workflow_path = if workflow_file_yml.exists() {
            Some(workflow_file_yml)
        } else if workflow_file_yaml.exists() {
            Some(workflow_file_yaml)
        } else {
            None
        };

        let workflow_path = match workflow_path {
            Some(p) => p,
            None => {
                let msg = "No .github/action-runner.yml (or .yaml) found in repository";
                info!(job_id = %job.id, "{}", msg);
                let _ = writeln!(log, "\n[ActionRunner-rs] {}. Skipping workflow.", msg);
                self.job_store.update_status(&job.id, JobStatus::Skipped { reason: msg.to_string() }).await;
                return;
            }
        };

        // 3. Parse & Validate YAML
        let content = match tokio::fs::read_to_string(&workflow_path).await {
            Ok(c) => c,
            Err(e) => {
                let err_msg = format!("Failed to read workflow file: {}", e);
                let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err_msg);
                self.job_store.update_status(&job.id, JobStatus::Failed { error: err_msg }).await;
                return;
            }
        };

        let workflow = match WorkflowConfig::load_from_str(&content, &repo_workspace, self.config.job_timeout_seconds) {
            Ok(w) => w,
            Err(e) => {
                let err_msg = format!("Workflow schema validation error: {}", e);
                let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err_msg);
                self.job_store.update_status(&job.id, JobStatus::Failed { error: err_msg }).await;
                return;
            }
        };

        let _ = writeln!(log, "Workflow Name: {}", workflow.name);
        let _ = writeln!(log, "Total Steps:   {}\n", workflow.steps.len());

        // 4. Execute Steps Sequentially
        for (idx, step) in workflow.steps.iter().enumerate() {
            let step_start = Utc::now();
            let _ = writeln!(log, "--------------------------------------------------------------------------------");
            let _ = writeln!(log, "Step [{}/{}]: {}", idx + 1, workflow.steps.len(), step.name);
            let _ = writeln!(log, "Command:    {}", step.run);
            let _ = writeln!(log, "Started At: {}", step_start.to_rfc3339());
            let _ = writeln!(log, "--------------------------------------------------------------------------------");
            let _ = log.flush();

            let step_dir = step.working_dir
                .as_ref()
                .map(|d| repo_workspace.join(d))
                .unwrap_or_else(|| repo_workspace.clone());

            let mut cmd = Command::new("bash");
            cmd.arg("-e").arg("-c").arg(&step.run);
            cmd.current_dir(&step_dir);

            // Sanitized standard environment
            cmd.env("PATH", "/home/lma1980/opt/llvm-24/bin:/home/lma1980/opt/gcc-17-stage3-spacemit-x100/bin:/home/lma1980/.cargo/bin:/usr/local/bin:/usr/bin:/bin");
            cmd.env("CI", "true");
            cmd.env("GITHUB_ACTIONS", "true");
            cmd.env("ACTION_RUNNER", "true");
            cmd.env("GITHUB_REPOSITORY", &job.repo_name);
            cmd.env("GITHUB_SHA", &job.commit_sha);
            cmd.env("GITHUB_REF", &job.ref_name);
            cmd.env("JOB_ID", &job.id);

            // Injected workflow-level env
            if let Some(ref env_map) = workflow.env {
                for (k, v) in env_map {
                    cmd.env(k, v);
                }
            }

            // Injected step-level env
            if let Some(ref env_map) = step.env {
                for (k, v) in env_map {
                    cmd.env(k, v);
                }
            }

            // Stream stdout & stderr directly into log file
            let out_file = match OpenOptions::new().append(true).open(&log_file_path) {
                Ok(f) => f,
                Err(e) => {
                    let err = format!("Failed to open log file for step output: {}", e);
                    self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                    return;
                }
            };
            let err_file = match OpenOptions::new().append(true).open(&log_file_path) {
                Ok(f) => f,
                Err(e) => {
                    let err = format!("Failed to open log file for step error: {}", e);
                    self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                    return;
                }
            };

            cmd.stdout(Stdio::from(out_file));
            cmd.stderr(Stdio::from(err_file));

            let step_timeout = step.timeout_seconds.unwrap_or(self.config.job_timeout_seconds);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let err = format!("Failed to spawn step process: {}", e);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                    return;
                }
            };

            let execution_result = timeout(Duration::from_secs(step_timeout), child.wait()).await;

            match execution_result {
                Ok(Ok(exit_status)) => {
                    if exit_status.success() {
                        let _ = writeln!(log, "\n[ActionRunner-rs] Step '{}' completed successfully.", step.name);
                    } else {
                        let code = exit_status.code().unwrap_or(-1);
                        let err = format!("Step '{}' failed with exit code: {}", step.name, code);
                        let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                        self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                        return;
                    }
                }
                Ok(Err(e)) => {
                    let err = format!("Step wait error: {}", e);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                    return;
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let err = format!("Step '{}' timed out after {}s", step.name, step_timeout);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store.update_status(&job.id, JobStatus::Failed { error: err }).await;
                    return;
                }
            }
        }

        let end_time = Utc::now();
        let duration = (end_time - start_time).num_seconds().max(0) as u64;

        let _ = writeln!(log, "\n================================================================================");
        let _ = writeln!(log, "All steps completed successfully in {}s", duration);
        let _ = writeln!(log, "Finished At: {}", end_time.to_rfc3339());
        let _ = writeln!(log, "================================================================================");

        info!(job_id = %job.id, duration_secs = duration, "Job completed successfully");
        self.job_store.update_status(&job.id, JobStatus::Completed { duration_secs: duration, exit_code: 0 }).await;
    }

    async fn sync_git_repo(&self, job: &Job, repo_dir: &Path, log: &mut std::fs::File) -> Result<(), String> {
        if !repo_dir.join(".git").exists() {
            let display_dir = repo_dir.strip_prefix(&self.config.workspace_dir).unwrap_or(repo_dir);
            let _ = writeln!(log, "[ActionRunner-rs] Cloning repository {} into {}", job.repo_name, display_dir.display());
            let mut clone_cmd = Command::new("git");
            let git_url = if std::path::Path::new("/home/lma1980/.ssh/id_k3_ci").exists() {
                format!("git@github.com:{}.git", job.repo_name)
            } else {
                job.clone_url.clone()
            };
            clone_cmd.arg("clone").arg(&git_url).arg(".");
            clone_cmd.current_dir(repo_dir);

            clone_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            clone_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = clone_cmd.status().await.map_err(|e| format!("git clone spawn error: {}", e))?;
            if !status.success() {
                return Err(format!("git clone failed with code: {:?}", status.code()));
            }
        } else {
            let _ = writeln!(log, "[ActionRunner-rs] Fetching latest refs from origin");
            let mut fetch_cmd = Command::new("git");
            fetch_cmd.arg("fetch").arg("--tags").arg("origin");
            fetch_cmd.current_dir(repo_dir);

            fetch_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            fetch_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = fetch_cmd.status().await.map_err(|e| format!("git fetch spawn error: {}", e))?;
            if !status.success() {
                return Err(format!("git fetch failed with code: {:?}", status.code()));
            }
        }

        let _ = writeln!(log, "[ActionRunner-rs] Checking out commit {}", job.commit_sha);
        let mut checkout_cmd = Command::new("git");
        checkout_cmd.arg("checkout").arg("-f").arg(&job.commit_sha);
        checkout_cmd.current_dir(repo_dir);

        checkout_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        checkout_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

        let status = checkout_cmd.status().await.map_err(|e| format!("git checkout spawn error: {}", e))?;
        if !status.success() {
            return Err(format!("git checkout failed with code: {:?}", status.code()));
        }

        Ok(())
    }
}
