use chrono::Utc;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info};

use crate::config::Config;
use crate::job::{Job, JobMetrics, JobStatus, JobStore};
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
            info!(job_id = %job.id, event = %job.event_type, repo = %job.repo_name, commit = %job.commit_sha, "Starting job execution");
            self.execute_job(job).await;
        }

        info!("ActionRunner executor worker terminated");
    }

    async fn execute_job(&self, job: Job) {
        let start_time = Utc::now();
        self.job_store
            .update_status(
                &job.id,
                JobStatus::Running {
                    started_at: start_time,
                },
            )
            .await;

        let repo_workspace = self.config.workspace_dir.join("repos").join(&job.repo_name);
        let log_file_path = self
            .config
            .workspace_dir
            .join("logs")
            .join(&job.repo_name)
            .join(format!("{}.log", job.id));

        if let Some(parent) = log_file_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::create_dir_all(&repo_workspace).await;

        let mut log = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!(error = %e, "Failed to open job log file");
                self.job_store
                    .update_status(
                        &job.id,
                        JobStatus::Failed {
                            error: format!("Failed to open log file: {}", e),
                        },
                    )
                    .await;
                return;
            }
        };

        let _ = writeln!(
            log,
            "================================================================================"
        );
        let _ = writeln!(log, "ActionRunner-rs Job Execution: {}", job.id);
        let _ = writeln!(log, "Event:      {}", job.event_type);
        let _ = writeln!(log, "Repository: {}", job.repo_name);
        let _ = writeln!(log, "Commit SHA: {}", job.commit_sha);
        let _ = writeln!(log, "Ref:        {}", job.ref_name);
        let _ = writeln!(log, "Triggered:  {}", job.sender);
        let _ = writeln!(log, "Started At: {}", start_time.to_rfc3339());
        let _ = writeln!(
            log,
            "================================================================================\n"
        );

        // 1. Sync Git Repository
        let git_status = self.sync_git_repo(&job, &repo_workspace, &mut log).await;
        if let Err(err_msg) = git_status {
            let _ = writeln!(
                log,
                "\n[ActionRunner-rs ERROR] Git sync failed: {}",
                err_msg
            );
            self.job_store
                .update_status(&job.id, JobStatus::Failed { error: err_msg })
                .await;
            return;
        }

        // 2. Discover Event-Driven YAML Workflow (.actionrunner/on-<event>.yml)
        let workflow_path = match self.resolve_workflow_file(&repo_workspace, &job.event_type) {
            Some(p) => p,
            None => {
                let msg = format!(
                    "No .actionrunner/on-{}.yml workflow found in repository",
                    job.event_type
                );
                info!(job_id = %job.id, "{}", msg);
                let _ = writeln!(log, "\n[ActionRunner-rs] {}. Skipping workflow.", msg);
                self.job_store
                    .update_status(&job.id, JobStatus::Skipped { reason: msg })
                    .await;
                return;
            }
        };

        let workflow_rel = workflow_path
            .strip_prefix(&repo_workspace)
            .unwrap_or(&workflow_path);
        let _ = writeln!(
            log,
            "[ActionRunner-rs] Loaded workflow file: {}\n",
            workflow_rel.display()
        );

        // 3. Parse & Validate YAML
        let content = match tokio::fs::read_to_string(&workflow_path).await {
            Ok(c) => c,
            Err(e) => {
                let err_msg = format!("Failed to read workflow file: {}", e);
                let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err_msg);
                self.job_store
                    .update_status(&job.id, JobStatus::Failed { error: err_msg })
                    .await;
                return;
            }
        };

        let workflow = match WorkflowConfig::load_from_str(
            &content,
            &repo_workspace,
            self.config.job_timeout_seconds,
        ) {
            Ok(w) => w,
            Err(e) => {
                let err_msg = format!("Workflow schema validation error: {}", e);
                let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err_msg);
                self.job_store
                    .update_status(&job.id, JobStatus::Failed { error: err_msg })
                    .await;
                return;
            }
        };

        let _ = writeln!(log, "Workflow Name: {}", workflow.name);
        let _ = writeln!(log, "Total Steps:   {}\n", workflow.steps.len());

        // 4. Execute Steps Sequentially
        for (idx, step) in workflow.steps.iter().enumerate() {
            let step_start = Utc::now();
            let _ = writeln!(
                log,
                "--------------------------------------------------------------------------------"
            );
            let _ = writeln!(
                log,
                "Step [{}/{}]: {}",
                idx + 1,
                workflow.steps.len(),
                step.name
            );
            let _ = writeln!(log, "Command:    {}", step.run);
            let _ = writeln!(log, "Started At: {}", step_start.to_rfc3339());
            let _ = writeln!(
                log,
                "--------------------------------------------------------------------------------"
            );
            let _ = log.flush();

            let step_dir = step
                .working_dir
                .as_ref()
                .map(|d| repo_workspace.join(d))
                .unwrap_or_else(|| repo_workspace.clone());

            let mut cmd = Command::new("bash");
            cmd.arg("-e").arg("-c").arg(&step.run);
            cmd.current_dir(&step_dir);

            // Sanitized standard environment defaulting to standard system toolchain
            let default_path = "/home/lma1980/.cargo/bin:/usr/local/bin:/usr/bin:/bin";
            let cargo_home = self.config.workspace_dir.join(".cargo");
            cmd.env("PATH", default_path);
            cmd.env("CARGO_HOME", cargo_home);
            cmd.env("CI", "true");
            cmd.env("ACTION_RUNNER", "true");
            cmd.env("GITHUB_EVENT_NAME", &job.event_type);
            cmd.env("GITHUB_REPOSITORY", &job.repo_name);
            cmd.env("GITHUB_SHA", &job.commit_sha);
            cmd.env("GITHUB_REF", &job.ref_name);
            cmd.env("JOB_ID", &job.id);

            if job.ref_name.starts_with("refs/tags/") {
                let tag = job.ref_name.trim_start_matches("refs/tags/");
                cmd.env("K3_TAG_NAME", tag);
                cmd.env("GITHUB_REF_NAME", tag);
            } else if job.ref_name.starts_with("refs/heads/") {
                let branch = job.ref_name.trim_start_matches("refs/heads/");
                cmd.env("GITHUB_REF_NAME", branch);
            } else {
                cmd.env("GITHUB_REF_NAME", &job.ref_name);
            }

            // Injected workflow-level env (with $PATH expansion support)
            if let Some(ref env_map) = workflow.env {
                for (k, v) in env_map {
                    let val = v.replace("$PATH", default_path);
                    cmd.env(k, val);
                }
            }

            // Injected step-level env (with $PATH expansion support)
            if let Some(ref env_map) = step.env {
                for (k, v) in env_map {
                    let val = v.replace("$PATH", default_path);
                    cmd.env(k, val);
                }
            }

            // Stream stdout & stderr directly into log file
            let out_file = match OpenOptions::new().append(true).open(&log_file_path) {
                Ok(f) => f,
                Err(e) => {
                    let err = format!("Failed to open log file for step output: {}", e);
                    self.job_store
                        .update_status(&job.id, JobStatus::Failed { error: err })
                        .await;
                    return;
                }
            };
            let err_file = match OpenOptions::new().append(true).open(&log_file_path) {
                Ok(f) => f,
                Err(e) => {
                    let err = format!("Failed to open log file for step error: {}", e);
                    self.job_store
                        .update_status(&job.id, JobStatus::Failed { error: err })
                        .await;
                    return;
                }
            };

            cmd.stdout(Stdio::from(out_file));
            cmd.stderr(Stdio::from(err_file));

            let step_timeout = step
                .timeout_seconds
                .unwrap_or(self.config.job_timeout_seconds);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let err = format!("Failed to spawn step process: {}", e);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store
                        .update_status(&job.id, JobStatus::Failed { error: err })
                        .await;
                    return;
                }
            };

            let execution_result = timeout(Duration::from_secs(step_timeout), child.wait()).await;

            match execution_result {
                Ok(Ok(exit_status)) => {
                    if exit_status.success() {
                        let _ = writeln!(
                            log,
                            "\n[ActionRunner-rs] Step '{}' completed successfully.",
                            step.name
                        );
                    } else {
                        let code = exit_status.code().unwrap_or(-1);
                        let err = format!("Step '{}' failed with exit code: {}", step.name, code);
                        let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                        let metrics = self
                            .extract_or_compute_metrics(&repo_workspace, &log_file_path, false)
                            .await;
                        self.job_store.update_metrics(&job.id, metrics).await;
                        self.job_store
                            .update_status(&job.id, JobStatus::Failed { error: err })
                            .await;
                        return;
                    }
                }
                Ok(Err(e)) => {
                    let err = format!("Step wait error: {}", e);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store
                        .update_status(&job.id, JobStatus::Failed { error: err })
                        .await;
                    return;
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let err = format!("Step '{}' timed out after {}s", step.name, step_timeout);
                    let _ = writeln!(log, "\n[ActionRunner-rs ERROR] {}", err);
                    self.job_store
                        .update_status(&job.id, JobStatus::Failed { error: err })
                        .await;
                    return;
                }
            }
        }

        let end_time = Utc::now();
        let duration = (end_time - start_time).num_seconds().max(0) as u64;

        let _ = writeln!(
            log,
            "\n================================================================================"
        );
        let _ = writeln!(log, "All steps completed successfully in {}s", duration);
        let _ = writeln!(log, "Finished At: {}", end_time.to_rfc3339());
        let _ = writeln!(
            log,
            "================================================================================"
        );

        info!(job_id = %job.id, duration_secs = duration, "Job completed successfully");
        let metrics = self
            .extract_or_compute_metrics(&repo_workspace, &log_file_path, true)
            .await;
        self.job_store.update_metrics(&job.id, metrics).await;
        self.job_store
            .update_status(
                &job.id,
                JobStatus::Completed {
                    duration_secs: duration,
                    exit_code: 0,
                },
            )
            .await;
    }

    fn resolve_workflow_file(&self, repo_workspace: &Path, event_type: &str) -> Option<PathBuf> {
        let actionrunner_dir = repo_workspace.join(".actionrunner");
        let event_lower = event_type.to_ascii_lowercase();

        let mut candidates = Vec::new();
        candidates.push(format!("on-{}.yml", event_lower));
        candidates.push(format!("on-{}.yaml", event_lower));

        if event_lower == "pull_request" || event_lower == "pullrequest" {
            candidates.push("on-pullrequest.yml".to_string());
            candidates.push("on-pullrequest.yaml".to_string());
            candidates.push("on-pull-request.yml".to_string());
            candidates.push("on-pull-request.yaml".to_string());
            candidates.push("on-pull_request.yml".to_string());
            candidates.push("on-pull_request.yaml".to_string());
        } else if event_lower == "tag" {
            candidates.push("on-tag.yml".to_string());
            candidates.push("on-tag.yaml".to_string());
            candidates.push("on-release.yml".to_string());
            candidates.push("on-release.yaml".to_string());
        } else if event_lower == "merge" {
            candidates.push("on-merge.yml".to_string());
            candidates.push("on-merge.yaml".to_string());
        }

        for candidate in candidates {
            let path = actionrunner_dir.join(&candidate);
            if path.exists() {
                return Some(path);
            }
        }

        // Backwards compatibility fallback
        let legacy_yml = repo_workspace.join(".github/action-runner.yml");
        if legacy_yml.exists() {
            return Some(legacy_yml);
        }
        let legacy_yaml = repo_workspace.join(".github/action-runner.yaml");
        if legacy_yaml.exists() {
            return Some(legacy_yaml);
        }

        None
    }

    async fn sync_git_repo(
        &self,
        job: &Job,
        repo_dir: &Path,
        log: &mut std::fs::File,
    ) -> Result<(), String> {
        if !repo_dir.join(".git").exists() {
            let display_dir = repo_dir
                .strip_prefix(&self.config.workspace_dir)
                .unwrap_or(repo_dir);
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Cloning repository {} into {}",
                job.repo_name,
                display_dir.display()
            );

            let mut clone_cmd = Command::new("git");
            let git_url = if Path::new("/home/lma1980/.ssh/id_k3_ci").exists() {
                format!("git@github.com:{}.git", job.repo_name)
            } else {
                job.clone_url.clone()
            };
            clone_cmd.arg("clone").arg(&git_url).arg(".");
            clone_cmd.current_dir(repo_dir);

            clone_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            clone_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = clone_cmd
                .status()
                .await
                .map_err(|e| format!("git clone spawn error: {}", e))?;
            if !status.success() {
                return Err(format!("git clone failed with code: {:?}", status.code()));
            }
        } else {
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Fetching latest refs and pull requests from origin"
            );
            let mut fetch_cmd = Command::new("git");
            fetch_cmd
                .arg("fetch")
                .arg("--prune")
                .arg("--tags")
                .arg("origin")
                .arg("+refs/heads/*:refs/remotes/origin/*")
                .arg("+refs/pull/*/head:refs/remotes/origin/pr/*");
            fetch_cmd.current_dir(repo_dir);

            fetch_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            fetch_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = fetch_cmd
                .status()
                .await
                .map_err(|e| format!("git fetch spawn error: {}", e))?;
            if !status.success() {
                return Err(format!("git fetch failed with code: {:?}", status.code()));
            }
        }

        if job.ref_name.starts_with("refs/heads/") {
            let branch = job.ref_name.trim_start_matches("refs/heads/");
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Checking out and updating branch '{}' at commit {}",
                branch, job.commit_sha
            );
            let mut checkout_cmd = Command::new("git");
            checkout_cmd
                .arg("checkout")
                .arg("-f")
                .arg("-B")
                .arg(branch)
                .arg(&job.commit_sha);
            checkout_cmd.current_dir(repo_dir);

            checkout_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            checkout_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = checkout_cmd
                .status()
                .await
                .map_err(|e| format!("git checkout spawn error: {}", e))?;
            if !status.success() {
                return Err(format!(
                    "git checkout -B {} failed with code: {:?}",
                    branch,
                    status.code()
                ));
            }

            // Set up upstream tracking so local branch tracks origin/<branch>
            let mut track_cmd = Command::new("git");
            track_cmd
                .arg("branch")
                .arg(format!("--set-upstream-to=origin/{}", branch))
                .arg(branch);
            track_cmd.current_dir(repo_dir);
            track_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            track_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            let _ = track_cmd.status().await;
        } else {
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Checking out ref {} at commit {}",
                job.ref_name, job.commit_sha
            );
            let mut checkout_cmd = Command::new("git");
            checkout_cmd.arg("checkout").arg("-f").arg(&job.commit_sha);
            checkout_cmd.current_dir(repo_dir);

            checkout_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
            checkout_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));

            let status = checkout_cmd
                .status()
                .await
                .map_err(|e| format!("git checkout spawn error: {}", e))?;
            if !status.success() {
                return Err(format!(
                    "git checkout failed with code: {:?}",
                    status.code()
                ));
            }
        }

        // Hard reset working tree to target commit to wipe any tracked modifications
        let _ = writeln!(
            log,
            "[ActionRunner-rs] Resetting working tree to {}",
            job.commit_sha
        );
        let mut reset_cmd = Command::new("git");
        reset_cmd.arg("reset").arg("--hard").arg(&job.commit_sha);
        reset_cmd.current_dir(repo_dir);
        reset_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        reset_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        let reset_status = reset_cmd
            .status()
            .await
            .map_err(|e| format!("git reset spawn error: {}", e))?;
        if !reset_status.success() {
            return Err(format!(
                "git reset --hard failed with code: {:?}",
                reset_status.code()
            ));
        }

        // Clean any untracked leftover files from previous workflow executions
        let _ = writeln!(
            log,
            "[ActionRunner-rs] Cleaning untracked files and directories"
        );
        let mut clean_cmd = Command::new("git");
        clean_cmd.arg("clean").arg("-fd");
        clean_cmd.current_dir(repo_dir);
        clean_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        clean_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        let clean_status = clean_cmd
            .status()
            .await
            .map_err(|e| format!("git clean -fd spawn error: {}", e))?;
        if !clean_status.success() {
            return Err(format!(
                "git clean -fd failed with code: {:?}",
                clean_status.code()
            ));
        }

        // Submodule sync (synchronizes .gitmodules remote URLs into .git/config)
        let _ = writeln!(log, "[ActionRunner-rs] Synchronizing submodules");
        let mut sync_cmd = Command::new("git");
        sync_cmd.arg("submodule").arg("sync").arg("--recursive");
        sync_cmd.current_dir(repo_dir);
        sync_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        sync_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        let sync_status = sync_cmd
            .status()
            .await
            .map_err(|e| format!("git submodule sync spawn error: {}", e))?;
        if !sync_status.success() {
            return Err(format!(
                "git submodule sync failed with code: {:?}",
                sync_status.code()
            ));
        }

        // Submodule update recursively with --force
        let _ = writeln!(log, "[ActionRunner-rs] Updating submodules recursively");
        let mut submod_cmd = Command::new("git");
        submod_cmd
            .arg("submodule")
            .arg("update")
            .arg("--init")
            .arg("--recursive")
            .arg("--force");
        submod_cmd.current_dir(repo_dir);
        submod_cmd.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        submod_cmd.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        let submod_status = submod_cmd
            .status()
            .await
            .map_err(|e| format!("git submodule spawn error: {}", e))?;
        if !submod_status.success() {
            return Err(format!(
                "git submodule update failed with code: {:?}",
                submod_status.code()
            ));
        }

        // Ensure submodules are reset and untracked files in submodules are cleaned
        let mut submod_clean = Command::new("git");
        submod_clean
            .arg("submodule")
            .arg("foreach")
            .arg("--recursive")
            .arg("git reset --hard && git clean -fd");
        submod_clean.current_dir(repo_dir);
        submod_clean.stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        submod_clean.stderr(Stdio::from(log.try_clone().map_err(|e| e.to_string())?));
        let _ = submod_clean.status().await;

        if job.ref_name.starts_with("refs/heads/") {
            let branch = job.ref_name.trim_start_matches("refs/heads/");
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Local repository is up to date on branch '{}' at commit {}\n",
                branch, job.commit_sha
            );
        } else {
            let _ = writeln!(
                log,
                "[ActionRunner-rs] Local repository is up to date at commit {}\n",
                job.commit_sha
            );
        }

        Ok(())
    }

    async fn extract_or_compute_metrics(
        &self,
        repo_workspace: &Path,
        log_file_path: &Path,
        is_success: bool,
    ) -> JobMetrics {
        let metrics_file = repo_workspace.join(".actionrunner/metrics.json");
        if metrics_file.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&metrics_file).await {
                if let Ok(metrics) = serde_json::from_str::<JobMetrics>(&content) {
                    return metrics;
                }
            }
        }

        // Auto-derive metrics from execution log file
        let log_content = tokio::fs::read_to_string(log_file_path)
            .await
            .unwrap_or_default();
        let mut tests_passed = 0;
        let mut tests_total = 0;
        let mut bench_count = 0;
        let mut bench_last_time = String::new();
        let mut coverage_pct: Option<f64> = None;
        let mut advisories_count = 0;
        let mut clippy_warnings = 0;

        for line in log_content.lines() {
            // Test results: "test result: ok. 4 passed; 0 failed; ..."
            if line.contains("test result: ok.") || line.contains("test result: FAILED.") {
                if let Some(passed_str) = line
                    .split("passed;")
                    .next()
                    .and_then(|s| s.split_whitespace().last())
                {
                    if let Ok(p) = passed_str.parse::<u32>() {
                        tests_passed += p;
                        tests_total += p;
                    }
                }
                if let Some(failed_part) = line.split("passed;").nth(1) {
                    if let Some(failed_str) = failed_part
                        .split("failed;")
                        .next()
                        .and_then(|s| s.split_whitespace().last())
                    {
                        if let Ok(f) = failed_str.parse::<u32>() {
                            tests_total += f;
                        }
                    }
                }
            }

            // Benchmark results from `cargo bench`: "test bench_name ... bench: 12,345 ns/iter"
            if line.contains("... bench:") {
                bench_count += 1;
                if let Some(time_part) = line.split("... bench:").nth(1) {
                    bench_last_time = time_part
                        .split("(+/-")
                        .next()
                        .unwrap_or(time_part)
                        .trim()
                        .to_string();
                }
            }

            // Code Coverage: "TOTAL ... 89.5%" or "Lines: 89.5%"
            if line.contains("Lines:") || (line.starts_with("TOTAL") && line.contains('%')) {
                for token in line.split_whitespace().rev() {
                    if token.ends_with('%') {
                        if let Ok(pct) = token.trim_end_matches('%').parse::<f64>() {
                            coverage_pct = Some(pct);
                            break;
                        }
                    }
                }
            }

            // Clippy warning count
            if line.contains("warning: ") && line.contains("[-D clippy::") {
                clippy_warnings += 1;
            }

            // Cargo audit security advisories
            if line.contains("vulnerabilities found") {
                if let Some(num_str) = line.split_whitespace().next() {
                    if let Ok(n) = num_str.parse::<u32>() {
                        advisories_count = n;
                    }
                }
            }
        }

        let benchmark_summary = if bench_count > 0 {
            if !bench_last_time.is_empty() {
                Some(format!("{} benches ({})", bench_count, bench_last_time))
            } else {
                Some(format!("{} benches passed", bench_count))
            }
        } else {
            None
        };

        // Score formula out of 100
        let quality_score = if is_success {
            let mut score = 100u32;
            if clippy_warnings > 0 {
                score = score.saturating_sub((clippy_warnings * 5).min(20));
            }
            if advisories_count > 0 {
                score = score.saturating_sub((advisories_count * 10).min(30));
            }
            if tests_total > 0 && tests_passed < tests_total {
                let fail_ratio = (tests_total - tests_passed) as f64 / tests_total as f64;
                score = score.saturating_sub((fail_ratio * 40.0) as u32);
            }
            if let Some(cov) = coverage_pct {
                if cov < 80.0 {
                    score = score.saturating_sub(((80.0 - cov) * 0.5) as u32);
                }
            }
            score.max(1)
        } else {
            40u32
        };

        let letter_grade = match quality_score {
            95..=100 => "A+".to_string(),
            90..=94 => "A".to_string(),
            80..=89 => "B".to_string(),
            70..=79 => "C".to_string(),
            _ => "F".to_string(),
        };

        JobMetrics {
            quality_score,
            letter_grade,
            coverage_pct,
            benchmark_summary,
            tests_passed,
            tests_total,
            advisories_count,
            clippy_warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::process::Command as StdCommand;

    #[tokio::test]
    async fn test_sync_git_repo_branch_push() {
        let temp_dir = std::env::temp_dir().join(format!(
            "actionrunner-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let origin_dir = temp_dir.join("origin.git");
        let work_dir = temp_dir.join("work");
        let runner_workspace = temp_dir.join("workspace");
        let repo_dir = runner_workspace.join("repos/LMA1980/test-repo");
        let log_path = temp_dir.join("test.log");

        let _ = std::fs::create_dir_all(&repo_dir);

        // 1. Initialize bare origin repo
        assert!(StdCommand::new("git")
            .arg("init")
            .arg("--bare")
            .arg("-b")
            .arg("main")
            .arg(&origin_dir)
            .status()
            .unwrap()
            .success());

        // 2. Clone to temporary work directory and create two commits
        assert!(StdCommand::new("git")
            .arg("clone")
            .arg(&origin_dir)
            .arg(&work_dir)
            .status()
            .unwrap()
            .success());

        // Configure dummy user for commits
        let _ = StdCommand::new("git")
            .arg("config")
            .arg("user.name")
            .arg("CI")
            .current_dir(&work_dir)
            .status();
        let _ = StdCommand::new("git")
            .arg("config")
            .arg("user.email")
            .arg("ci@test.com")
            .current_dir(&work_dir)
            .status();

        std::fs::write(work_dir.join("file.txt"), "first commit\n").unwrap();
        assert!(StdCommand::new("git")
            .arg("add")
            .arg("file.txt")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());
        assert!(StdCommand::new("git")
            .arg("commit")
            .arg("-m")
            .arg("first")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());
        assert!(StdCommand::new("git")
            .arg("push")
            .arg("origin")
            .arg("main")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());

        let c1_output = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&work_dir)
            .output()
            .unwrap();
        let c1 = String::from_utf8_lossy(&c1_output.stdout).trim().to_string();

        std::fs::write(work_dir.join("file.txt"), "second commit\n").unwrap();
        assert!(StdCommand::new("git")
            .arg("commit")
            .arg("-am")
            .arg("second")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());
        assert!(StdCommand::new("git")
            .arg("push")
            .arg("origin")
            .arg("main")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());

        let c2_output = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&work_dir)
            .output()
            .unwrap();
        let c2 = String::from_utf8_lossy(&c2_output.stdout).trim().to_string();

        // 3. Instantiate Executor
        let config = Config {
            bind_addr: "127.0.0.1:0".to_string(),
            webhook_secret: None,
            workspace_dir: runner_workspace.clone(),
            allowed_repositories: vec!["LMA1980/*".to_string()],
            job_timeout_seconds: 60,
        };
        let job_store = JobStore::new(10, None);
        let executor = Executor::new(config, job_store);

        let mut log = File::create(&log_path).unwrap();

        // 4. Test initial clone and checkout on push to main (commit C1)
        let job1 = Job {
            id: "job-1".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: origin_dir.to_str().unwrap().to_string(),
            commit_sha: c1.clone(),
            ref_name: "refs/heads/main".to_string(),
            sender: "test".to_string(),
            queued_at: Utc::now(),
        };

        let res1 = executor.sync_git_repo(&job1, &repo_dir, &mut log).await;
        assert!(res1.is_ok(), "sync_git_repo job1 failed: {:?}", res1);

        // Verify HEAD is attached to main, not detached!
        let branch_out1 = StdCommand::new("git")
            .arg("symbolic-ref")
            .arg("--short")
            .arg("HEAD")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&branch_out1.stdout).trim(), "main");

        let rev_out1 = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&rev_out1.stdout).trim(), c1);

        // 5. Simulate dirty files left over in repo_dir
        std::fs::write(repo_dir.join("dirty.txt"), "leftover garbage\n").unwrap();
        std::fs::write(repo_dir.join("file.txt"), "tampered content\n").unwrap();

        // 6. Test push event to main at commit C2
        let job2 = Job {
            id: "job-2".to_string(),
            event_type: "push".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: origin_dir.to_str().unwrap().to_string(),
            commit_sha: c2.clone(),
            ref_name: "refs/heads/main".to_string(),
            sender: "test".to_string(),
            queued_at: Utc::now(),
        };

        let res2 = executor.sync_git_repo(&job2, &repo_dir, &mut log).await;
        assert!(res2.is_ok(), "sync_git_repo job2 failed: {:?}", res2);

        // Verify HEAD is still on main (not detached!)
        let branch_out2 = StdCommand::new("git")
            .arg("symbolic-ref")
            .arg("--short")
            .arg("HEAD")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&branch_out2.stdout).trim(), "main");

        // Verify commit SHA is C2
        let rev_out2 = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&rev_out2.stdout).trim(), c2);

        // Verify dirty file was cleaned
        assert!(
            !repo_dir.join("dirty.txt").exists(),
            "dirty.txt was not cleaned!"
        );

        // Verify working tree is clean
        let status_out = StdCommand::new("git")
            .arg("status")
            .arg("--porcelain")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&status_out.stdout).trim().is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_sync_git_repo_detached_ref() {
        let temp_dir = std::env::temp_dir().join(format!(
            "actionrunner-test-pr-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let origin_dir = temp_dir.join("origin.git");
        let work_dir = temp_dir.join("work");
        let runner_workspace = temp_dir.join("workspace");
        let repo_dir = runner_workspace.join("repos/LMA1980/test-pr-repo");
        let log_path = temp_dir.join("test.log");

        let _ = std::fs::create_dir_all(&repo_dir);

        assert!(StdCommand::new("git")
            .arg("init")
            .arg("--bare")
            .arg("-b")
            .arg("main")
            .arg(&origin_dir)
            .status()
            .unwrap()
            .success());

        assert!(StdCommand::new("git")
            .arg("clone")
            .arg(&origin_dir)
            .arg(&work_dir)
            .status()
            .unwrap()
            .success());

        let _ = StdCommand::new("git")
            .arg("config")
            .arg("user.name")
            .arg("CI")
            .current_dir(&work_dir)
            .status();
        let _ = StdCommand::new("git")
            .arg("config")
            .arg("user.email")
            .arg("ci@test.com")
            .current_dir(&work_dir)
            .status();

        std::fs::write(work_dir.join("file.txt"), "pr commit\n").unwrap();
        assert!(StdCommand::new("git")
            .arg("add")
            .arg("file.txt")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());
        assert!(StdCommand::new("git")
            .arg("commit")
            .arg("-m")
            .arg("pr commit")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());
        assert!(StdCommand::new("git")
            .arg("push")
            .arg("origin")
            .arg("main")
            .current_dir(&work_dir)
            .status()
            .unwrap()
            .success());

        let c1_output = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&work_dir)
            .output()
            .unwrap();
        let c1 = String::from_utf8_lossy(&c1_output.stdout).trim().to_string();

        let config = Config {
            bind_addr: "127.0.0.1:0".to_string(),
            webhook_secret: None,
            workspace_dir: runner_workspace.clone(),
            allowed_repositories: vec!["LMA1980/*".to_string()],
            job_timeout_seconds: 60,
        };
        let job_store = JobStore::new(10, None);
        let executor = Executor::new(config, job_store);

        let mut log = File::create(&log_path).unwrap();

        // Non-branch ref (e.g., PR or tag)
        let job = Job {
            id: "job-pr-1".to_string(),
            event_type: "pull_request".to_string(),
            repo_name: "LMA1980/test-pr-repo".to_string(),
            clone_url: origin_dir.to_str().unwrap().to_string(),
            commit_sha: c1.clone(),
            ref_name: "refs/pull/42/head".to_string(),
            sender: "pr-user".to_string(),
            queued_at: Utc::now(),
        };

        let res = executor.sync_git_repo(&job, &repo_dir, &mut log).await;
        assert!(res.is_ok(), "sync_git_repo for PR failed: {:?}", res);

        let rev_out = StdCommand::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&rev_out.stdout).trim(), c1);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
