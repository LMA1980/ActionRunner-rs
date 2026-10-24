use action_runner_ipc::{encode_log_batch, LogEntry};
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info};

use crate::config::{IsolationMode, WorkerConfig};
use action_runner_ipc::StageDispatchTask;

pub struct StageRunner {
    config: WorkerConfig,
}

pub struct StageExecutionResult {
    pub exit_code: i32,
    pub duration_secs: u64,
    pub error_message: Option<String>,
}

impl StageRunner {
    pub fn new(config: WorkerConfig) -> Self {
        Self { config }
    }

    pub async fn run_stage(
        &self,
        task: &StageDispatchTask,
        log_tx: mpsc::Sender<Vec<u8>>,
    ) -> StageExecutionResult {
        let start_time = Utc::now();
        info!(
            job_id = %task.job_id,
            stage = %task.stage_id,
            toolchain = ?task.toolchain,
            threads = task.threads,
            "Executing stage on worker node"
        );

        let stage_dir = self.prepare_stage_workspace(task).await;

        let working_dir = match &task.working_dir {
            Some(rel) => stage_dir.join(rel),
            None => stage_dir.clone(),
        };

        // Construct Command
        let mut cmd = match self.config.isolation_mode {
            IsolationMode::SystemdRun => {
                let unit_name = format!(
                    "ar-{}-{}",
                    task.job_id
                        .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                    task.stage_id
                        .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
                );
                let mut scmd = Command::new("systemd-run");
                scmd.arg("--pipe")
                    .arg("--wait")
                    .arg("--collect")
                    .arg(format!("--unit={}", unit_name))
                    .arg("--property=PrivateTmp=yes")
                    .arg(format!("--property=ReadWritePaths={}", stage_dir.display()))
                    .arg("--")
                    .arg("bash")
                    .arg("-e")
                    .arg("-c")
                    .arg(&task.command);
                scmd
            }
            IsolationMode::None => {
                let mut bcmd = Command::new("bash");
                bcmd.arg("-e").arg("-c").arg(&task.command);
                bcmd
            }
        };

        cmd.current_dir(&working_dir);

        // Standard Environment Setup
        cmd.env("CI", "true");
        cmd.env("ACTION_RUNNER", "true");
        cmd.env("JOB_ID", &task.job_id);
        cmd.env("STAGE_ID", &task.stage_id);
        cmd.env("GITHUB_REPOSITORY", &task.repo_name);
        cmd.env("GITHUB_SHA", &task.commit_sha);
        cmd.env("CARGO_BUILD_JOBS", task.threads.to_string());
        cmd.env("CARGO_TERM_COLOR", "always");

        // Toolchain Injection (LLVM 24 / GCC 17 with SpacemiT X100 Target)
        self.apply_toolchain_env(&mut cmd, task);

        // Custom environment variables from workflow
        for (k, v) in &task.env_vars {
            cmd.env(k, v);
        }

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let err_msg = format!("Failed to spawn stage process: {}", e);
                error!("{}", err_msg);
                self.cleanup_stage_workspace(&stage_dir).await;
                return StageExecutionResult {
                    exit_code: -1,
                    duration_secs: 0,
                    error_message: Some(err_msg),
                };
            }
        };

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let line_counter = Arc::new(AtomicU32::new(1));

        // Spawn stdout streaming task
        let out_handle = {
            let log_tx = log_tx.clone();
            let node_id = self.config.node_id.clone();
            let job_id = task.job_id.clone();
            let stage_id = task.stage_id.clone();
            let line_counter = line_counter.clone();

            tokio::spawn(async move {
                if let Some(out) = stdout {
                    let mut reader = BufReader::new(out).lines();
                    let mut buffer = Vec::new();

                    while let Ok(Some(line)) = reader.next_line().await {
                        let l_num = line_counter.fetch_add(1, Ordering::Relaxed);
                        buffer.push(LogEntry {
                            timestamp_us: Utc::now().timestamp_micros(),
                            job_id: job_id.clone(),
                            stage_id: stage_id.clone(),
                            node_id: node_id.clone(),
                            stream: 1, // stdout
                            line_no: l_num,
                            content: line,
                        });

                        if buffer.len() >= 16 {
                            if let Ok(bytes) = encode_log_batch(&buffer) {
                                let _ = log_tx.send(bytes).await;
                            }
                            buffer.clear();
                        }
                    }

                    if !buffer.is_empty() {
                        if let Ok(bytes) = encode_log_batch(&buffer) {
                            let _ = log_tx.send(bytes).await;
                        }
                    }
                }
            })
        };

        // Spawn stderr streaming task
        let err_handle = {
            let log_tx = log_tx.clone();
            let node_id = self.config.node_id.clone();
            let job_id = task.job_id.clone();
            let stage_id = task.stage_id.clone();
            let line_counter = line_counter.clone();

            tokio::spawn(async move {
                if let Some(err) = stderr {
                    let mut reader = BufReader::new(err).lines();
                    let mut buffer = Vec::new();

                    while let Ok(Some(line)) = reader.next_line().await {
                        let l_num = line_counter.fetch_add(1, Ordering::Relaxed);
                        buffer.push(LogEntry {
                            timestamp_us: Utc::now().timestamp_micros(),
                            job_id: job_id.clone(),
                            stage_id: stage_id.clone(),
                            node_id: node_id.clone(),
                            stream: 2, // stderr
                            line_no: l_num,
                            content: line,
                        });

                        if buffer.len() >= 16 {
                            if let Ok(bytes) = encode_log_batch(&buffer) {
                                let _ = log_tx.send(bytes).await;
                            }
                            buffer.clear();
                        }
                    }

                    if !buffer.is_empty() {
                        if let Ok(bytes) = encode_log_batch(&buffer) {
                            let _ = log_tx.send(bytes).await;
                        }
                    }
                }
            })
        };

        let timeout_duration = Duration::from_secs(task.timeout_seconds);
        let execution_result = timeout(timeout_duration, child.wait()).await;

        let _ = out_handle.await;
        let _ = err_handle.await;

        self.cleanup_stage_workspace(&stage_dir).await;

        let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;

        match execution_result {
            Ok(Ok(status)) => StageExecutionResult {
                exit_code: status.code().unwrap_or(-1),
                duration_secs,
                error_message: if status.success() {
                    None
                } else {
                    Some(format!(
                        "Command exited with status code {:?}",
                        status.code()
                    ))
                },
            },
            Ok(Err(e)) => StageExecutionResult {
                exit_code: -1,
                duration_secs,
                error_message: Some(format!("I/O wait error: {}", e)),
            },
            Err(_) => {
                let _ = child.kill().await;
                StageExecutionResult {
                    exit_code: 124, // Standard timeout code
                    duration_secs,
                    error_message: Some(format!(
                        "Stage timed out after {} seconds",
                        task.timeout_seconds
                    )),
                }
            }
        }
    }

    fn apply_toolchain_env(&self, cmd: &mut Command, task: &StageDispatchTask) {
        let base_path =
            std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".to_string());

        match task.toolchain.as_deref() {
            Some("llvm-24") => {
                let llvm_bin = if Path::new("/home/lma1980/opt/llvm-24/bin").exists() {
                    "/home/lma1980/opt/llvm-24/bin"
                } else {
                    "/opt/cross-llvm-24/bin"
                };
                cmd.env("PATH", format!("{}:{}", llvm_bin, base_path));
                cmd.env("CC", "clang");
                cmd.env("CXX", "clang++");
                cmd.env(
                    "RUSTFLAGS",
                    "-C target-feature=+v -C target-cpu=spacemit-x100",
                );
                info!(
                    toolchain = "llvm-24",
                    "Injected LLVM 24 environment with RVV 1.0 flags"
                );
            }
            Some("gcc-17") | Some("gcc-17-spacemit-x100") => {
                let gcc_bin = if Path::new("/home/lma1980/opt/gcc-17-spacemit-x100/bin").exists() {
                    "/home/lma1980/opt/gcc-17-spacemit-x100/bin"
                } else if Path::new("/home/lma1980/opt/gcc-17/bin").exists() {
                    "/home/lma1980/opt/gcc-17/bin"
                } else {
                    "/opt/cross-gcc-17/bin"
                };
                cmd.env("PATH", format!("{}:{}", gcc_bin, base_path));
                cmd.env("CC", "gcc");
                cmd.env("CXX", "g++");
                cmd.env("CFLAGS", "-mcpu=spacemit-x100 -march=rv64gcv");
                cmd.env("CXXFLAGS", "-mcpu=spacemit-x100 -march=rv64gcv");
                info!(
                    toolchain = "gcc-17",
                    "Injected GCC 17 SpacemiT X100 environment"
                );
            }
            _ => {
                if self.config.arch == "riscv64" {
                    cmd.env("RUSTFLAGS", "-C target-feature=+v");
                }
            }
        }
    }

    async fn prepare_stage_workspace(&self, task: &StageDispatchTask) -> PathBuf {
        let repo_cache = self
            .config
            .workspace_dir
            .join("cache")
            .join(&task.repo_name);
        let stages_dir = self.config.workspace_dir.join("stages");
        let stage_dir = stages_dir.join(format!("{}_{}", task.job_id, task.stage_id));

        let _ = tokio::fs::create_dir_all(&stages_dir).await;
        let _ = tokio::fs::create_dir_all(&repo_cache).await;

        // Sync git cache mirror if needed
        self.sync_git_cache(task, &repo_cache).await;

        // Btrfs snapshot creation or directory copy
        if self.config.use_btrfs {
            let snap_res = Command::new("btrfs")
                .arg("subvolume")
                .arg("snapshot")
                .arg(&repo_cache)
                .arg(&stage_dir)
                .status()
                .await;

            if let Ok(s) = snap_res {
                if s.success() {
                    info!(
                        path = %stage_dir.display(),
                        "Created isolated Btrfs subvolume snapshot"
                    );
                    return stage_dir;
                }
            }
        }

        // Fallback: directory clone via git checkout
        let _ = tokio::fs::create_dir_all(&stage_dir).await;
        let _ = Command::new("git")
            .arg("clone")
            .arg("--reference")
            .arg(&repo_cache)
            .arg(&task.clone_url)
            .arg(&stage_dir)
            .status()
            .await;

        let _ = Command::new("git")
            .arg("checkout")
            .arg(&task.commit_sha)
            .current_dir(&stage_dir)
            .status()
            .await;

        stage_dir
    }

    async fn sync_git_cache(&self, task: &StageDispatchTask, repo_cache: &Path) {
        if !repo_cache.join(".git").exists() && !repo_cache.join("HEAD").exists() {
            let _ = Command::new("git")
                .arg("clone")
                .arg(&task.clone_url)
                .arg(repo_cache)
                .status()
                .await;
        } else {
            let _ = Command::new("git")
                .arg("fetch")
                .arg("origin")
                .arg(&task.commit_sha)
                .current_dir(repo_cache)
                .status()
                .await;
        }
    }

    async fn cleanup_stage_workspace(&self, stage_dir: &Path) {
        if self.config.use_btrfs {
            let _ = Command::new("btrfs")
                .arg("subvolume")
                .arg("delete")
                .arg(stage_dir)
                .status()
                .await;
        } else {
            let _ = tokio::fs::remove_dir_all(stage_dir).await;
        }
    }
}
