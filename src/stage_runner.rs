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

use crate::config::{is_safe_repo_name, IsolationMode, WorkerConfig};
use action_runner_ipc::StageDispatchTask;

/// Validates that an identifier (job_id, stage_id) consists strictly of safe characters
/// and contains no path traversal sequences or directory separators.
pub fn is_safe_identifier(id: &str) -> bool {
    let id = id.trim();
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.contains('\0')
    {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// Sanitizes an input string to be safely used as a single path component.
pub fn sanitize_path_segment(input: &str) -> String {
    let sanitized: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

/// Validates that `rel_working_dir` is a strictly relative sub-path within `stage_dir`,
/// containing no parent traversals ('..'), root components, or symlink escapes.
pub fn validate_and_resolve_working_dir(
    stage_dir: &Path,
    rel_working_dir: Option<&str>,
) -> Result<PathBuf, String> {
    let rel = match rel_working_dir {
        None => return Ok(stage_dir.to_path_buf()),
        Some(s) => s.trim(),
    };

    if rel.is_empty() || rel == "." || rel == "./" {
        return Ok(stage_dir.to_path_buf());
    }

    if rel.contains('\0') {
        return Err("Invalid working_dir: contains null byte".to_string());
    }

    let rel_path = Path::new(rel);

    // Reject absolute paths
    if rel_path.is_absolute() || rel.starts_with('/') || rel.starts_with('\\') {
        return Err(format!(
            "Invalid working_dir '{}': Absolute paths are forbidden",
            rel
        ));
    }

    // Reject parent traversal (..) and root components
    for comp in rel_path.components() {
        match comp {
            std::path::Component::Normal(_) => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err(format!(
                    "Invalid working_dir '{}': Path traversal ('..') / parent directory is forbidden",
                    rel
                ));
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(format!(
                    "Invalid working_dir '{}': root or prefix components are forbidden",
                    rel
                ));
            }
        }
    }

    let resolved = stage_dir.join(rel_path);

    // Lexical boundary check
    if !resolved.starts_with(stage_dir) {
        return Err(format!(
            "Invalid working_dir '{}': resolved path '{}' escapes stage directory '{}'",
            rel,
            resolved.display(),
            stage_dir.display()
        ));
    }

    // Verify canonical path against symlink escape if both exist
    if stage_dir.exists() && resolved.exists() {
        if let (Ok(real_stage), Ok(real_resolved)) =
            (stage_dir.canonicalize(), resolved.canonicalize())
        {
            if !real_resolved.starts_with(&real_stage) {
                return Err(format!(
                    "Invalid working_dir '{}': canonical path '{}' escapes canonical stage directory '{}'",
                    rel,
                    real_resolved.display(),
                    real_stage.display()
                ));
            }
        }
    }

    Ok(resolved)
}

pub struct StageRunner {
    config: WorkerConfig,
}

pub struct StageExecutionResult {
    pub exit_code: i32,
    pub duration_secs: u64,
    #[allow(dead_code)]
    pub pid: u32,
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
        started_tx: Option<tokio::sync::oneshot::Sender<u32>>,
    ) -> StageExecutionResult {
        let start_time = Utc::now();
        let node_thread_limit = self.config.max_threads();
        let build_jobs = std::env::var("ACTION_RUNNER_THREADS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                std::env::var("CARGO_BUILD_JOBS")
                    .ok()
                    .and_then(|s| s.parse::<u32>().ok())
            })
            .unwrap_or_else(|| {
                if task.threads > 0 {
                    task.threads.min(node_thread_limit)
                } else {
                    node_thread_limit
                }
            })
            .min(node_thread_limit)
            .max(1);

        info!(
            job_id = %task.job_id,
            stage = %task.stage_id,
            toolchain = ?task.toolchain,
            threads = build_jobs,
            node_max_threads = node_thread_limit,
            "Executing stage on worker node"
        );

        // Validate repo_name against path traversal / invalid format
        if !is_safe_repo_name(&task.repo_name) {
            let err_msg = format!(
                "Stage execution rejected: invalid or unsafe repository name '{}'",
                task.repo_name
            );
            error!("{}", err_msg);
            return StageExecutionResult {
                exit_code: -1,
                duration_secs: 0,
                pid: 0,
                error_message: Some(err_msg),
            };
        }

        // Validate job_id and stage_id against path traversal
        if !is_safe_identifier(&task.job_id) || !is_safe_identifier(&task.stage_id) {
            let err_msg = format!(
                "Stage execution rejected: unsafe job_id '{}' or stage_id '{}'",
                task.job_id, task.stage_id
            );
            error!("{}", err_msg);
            return StageExecutionResult {
                exit_code: -1,
                duration_secs: 0,
                pid: 0,
                error_message: Some(err_msg),
            };
        }

        // Validate working_dir upfront (before workspace creation)
        if let Some(rel) = task.working_dir.as_deref() {
            let dummy = Path::new("/tmp/stage_check");
            if let Err(err_msg) = validate_and_resolve_working_dir(dummy, Some(rel)) {
                error!("{}", err_msg);
                return StageExecutionResult {
                    exit_code: -1,
                    duration_secs: 0,
                    pid: 0,
                    error_message: Some(err_msg),
                };
            }
        }

        let stage_dir = self.prepare_stage_workspace(task, &log_tx).await;

        let working_dir =
            match validate_and_resolve_working_dir(&stage_dir, task.working_dir.as_deref()) {
                Ok(p) => p,
                Err(err_msg) => {
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(&stage_dir).await;
                    return StageExecutionResult {
                        exit_code: -1,
                        duration_secs: 0,
                        pid: 0,
                        error_message: Some(err_msg),
                    };
                }
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
                #[cfg(unix)]
                bcmd.process_group(0);
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
        cmd.env("CARGO_BUILD_JOBS", build_jobs.to_string());
        cmd.env("RUST_TEST_THREADS", build_jobs.to_string());
        cmd.env("RAYON_NUM_THREADS", build_jobs.to_string());
        cmd.env("CMAKE_BUILD_PARALLEL_LEVEL", build_jobs.to_string());
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
                    pid: 0,
                    error_message: Some(err_msg),
                };
            }
        };

        let pid = child.id().unwrap_or(0);
        if let Some(tx) = started_tx {
            let _ = tx.send(pid);
        }

        // Emit initial process spawned log entry so zero-output commands still have lifecycle visibility
        let spawn_entry = LogEntry {
            timestamp_us: Utc::now().timestamp_micros(),
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            node_id: self.config.node_id.clone(),
            stream: 1,
            line_no: 0,
            content: format!(
                "[ActionRunner-rs] Process spawned with PID {} ({})",
                pid, task.command
            ),
        };
        if let Ok(bytes) = encode_log_batch(&[spawn_entry]) {
            let _ = log_tx.send(bytes).await;
        }

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

        let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;

        let res = match execution_result {
            Ok(Ok(status)) => StageExecutionResult {
                exit_code: status.code().unwrap_or(-1),
                duration_secs,
                pid,
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
                pid,
                error_message: Some(format!("I/O wait error: {}", e)),
            },
            Err(_) => {
                let _ = child.kill().await;
                StageExecutionResult {
                    exit_code: 124, // Standard timeout code
                    duration_secs,
                    pid,
                    error_message: Some(format!(
                        "Stage timed out after {} seconds",
                        task.timeout_seconds
                    )),
                }
            }
        };

        // Emit final process completion log entry
        let exit_entry = LogEntry {
            timestamp_us: Utc::now().timestamp_micros(),
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            node_id: self.config.node_id.clone(),
            stream: 1,
            line_no: line_counter.load(Ordering::Relaxed),
            content: format!(
                "[ActionRunner-rs] Process exited with status code {} (PID {}, elapsed {}s)",
                res.exit_code, pid, res.duration_secs
            ),
        };
        if let Ok(bytes) = encode_log_batch(&[exit_entry]) {
            let _ = log_tx.send(bytes).await;
        }

        self.cleanup_stage_workspace(&stage_dir).await;
        res
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

    async fn prepare_stage_workspace(
        &self,
        task: &StageDispatchTask,
        log_tx: &mpsc::Sender<Vec<u8>>,
    ) -> PathBuf {
        let safe_job = sanitize_path_segment(&task.job_id);
        let safe_stage = sanitize_path_segment(&task.stage_id);
        let stage_folder_name = format!("{}_{}", safe_job, safe_stage);

        let stages_dir = self.config.workspace_dir.join("stages");
        let stage_dir = stages_dir.join(&stage_folder_name);

        let repo_cache = self
            .config
            .workspace_dir
            .join("cache")
            .join(&task.repo_name);

        let emit_git_log = |content: String| {
            let entry = LogEntry {
                timestamp_us: Utc::now().timestamp_micros(),
                job_id: task.job_id.clone(),
                stage_id: "step-0".to_string(),
                node_id: self.config.node_id.clone(),
                stream: 1,
                line_no: 0,
                content,
            };
            if let Ok(bytes) = encode_log_batch(&[entry]) {
                let _ = log_tx.try_send(bytes);
            }
        };

        emit_git_log(format!(
            "[ActionRunner-rs] Synchronizing workspace cache for '{}'...",
            task.repo_name
        ));

        let _ = tokio::fs::create_dir_all(&stages_dir).await;

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

                    let _ = Command::new("git")
                        .arg("checkout")
                        .arg("-f")
                        .arg(&task.commit_sha)
                        .current_dir(&stage_dir)
                        .status()
                        .await;

                    emit_git_log(format!(
                        "[ActionRunner-rs] Checked out target commit '{}' into stage workspace",
                        task.commit_sha
                    ));

                    let _ = Command::new("git")
                        .arg("submodule")
                        .arg("update")
                        .arg("--init")
                        .arg("--recursive")
                        .current_dir(&stage_dir)
                        .status()
                        .await;

                    emit_git_log(
                        "[ActionRunner-rs] Git submodules synchronized successfully".to_string(),
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

        emit_git_log(format!(
            "[ActionRunner-rs] Checked out target commit '{}' into stage workspace",
            task.commit_sha
        ));

        let _ = Command::new("git")
            .arg("submodule")
            .arg("update")
            .arg("--init")
            .arg("--recursive")
            .current_dir(&stage_dir)
            .status()
            .await;

        emit_git_log("[ActionRunner-rs] Git submodules synchronized successfully".to_string());

        stage_dir
    }

    async fn sync_git_cache(&self, task: &StageDispatchTask, repo_cache: &Path) {
        if !repo_cache.join(".git").exists() && !repo_cache.join("HEAD").exists() {
            if let Some(parent) = repo_cache.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
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
        let stages_dir = self.config.workspace_dir.join("stages");
        if !stage_dir.starts_with(&stages_dir) || stage_dir == stages_dir {
            error!(
                "Refusing to clean up stage workspace at '{}': path is outside stages directory '{}'",
                stage_dir.display(),
                stages_dir.display()
            );
            return;
        }

        if self.config.use_btrfs {
            let res = Command::new("btrfs")
                .arg("subvolume")
                .arg("delete")
                .arg(stage_dir)
                .status()
                .await;
            if res.is_ok() && !stage_dir.exists() {
                return;
            }
        }
        let _ = tokio::fs::remove_dir_all(stage_dir).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_safe_identifier() {
        assert!(is_safe_identifier("job-123"));
        assert!(is_safe_identifier("stage_build"));
        assert!(is_safe_identifier("bench.run-1"));
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier("../job-123"));
        assert!(!is_safe_identifier("job/123"));
        assert!(!is_safe_identifier("job\\123"));
        assert!(!is_safe_identifier("job..123"));
    }

    #[test]
    fn test_sanitize_path_segment() {
        assert_eq!(sanitize_path_segment("job-123_abc"), "job-123_abc");
        assert_eq!(
            sanitize_path_segment("../../etc/passwd"),
            "______etc_passwd"
        );
        assert_eq!(sanitize_path_segment(""), "unknown");
    }

    #[test]
    fn test_validate_and_resolve_working_dir_valid() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stage_dir = temp_dir.path();
        let sub_dir = stage_dir.join("src");
        std::fs::create_dir_all(&sub_dir).unwrap();

        // None -> stage_dir
        assert_eq!(
            validate_and_resolve_working_dir(stage_dir, None).unwrap(),
            stage_dir.to_path_buf()
        );

        // Empty or "." -> stage_dir
        assert_eq!(
            validate_and_resolve_working_dir(stage_dir, Some("")).unwrap(),
            stage_dir.to_path_buf()
        );
        assert_eq!(
            validate_and_resolve_working_dir(stage_dir, Some(".")).unwrap(),
            stage_dir.to_path_buf()
        );

        // Valid relative subpath
        assert_eq!(
            validate_and_resolve_working_dir(stage_dir, Some("src")).unwrap(),
            sub_dir
        );
    }

    #[test]
    fn test_validate_and_resolve_working_dir_reject_traversal() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stage_dir = temp_dir.path();

        // Parent traversal
        assert!(validate_and_resolve_working_dir(stage_dir, Some("..")).is_err());
        assert!(validate_and_resolve_working_dir(stage_dir, Some("../../etc")).is_err());
        assert!(validate_and_resolve_working_dir(stage_dir, Some("src/../../etc")).is_err());

        // Absolute path
        assert!(validate_and_resolve_working_dir(stage_dir, Some("/etc/passwd")).is_err());
        assert!(validate_and_resolve_working_dir(stage_dir, Some("/tmp")).is_err());
    }

    #[tokio::test]
    async fn test_run_stage_rejects_path_traversal() {
        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000".to_string(),
            cluster_token: "test".to_string(),
            node_id: "test".to_string(),
            arch: "x86_64".to_string(),
            tags: vec![],
            max_slots: 1,
            workspace_dir: tempfile::tempdir().unwrap().path().to_path_buf(),
            toolchains: vec![],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let runner = StageRunner::new(config);
        let (tx, _rx) = mpsc::channel(10);

        // Malicious repo_name
        let bad_repo_task = StageDispatchTask {
            job_id: "j1".into(),
            stage_id: "s1".into(),
            stage_name: "test".into(),
            repo_name: "../../etc/passwd".into(),
            clone_url: "".into(),
            commit_sha: "head".into(),
            command: "echo test".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };
        let res = runner.run_stage(&bad_repo_task, tx.clone(), None).await;
        assert_eq!(res.exit_code, -1);
        assert!(res
            .error_message
            .unwrap()
            .contains("invalid or unsafe repository name"));

        // Malicious working_dir
        let bad_working_dir_task = StageDispatchTask {
            job_id: "j1".into(),
            stage_id: "s1".into(),
            stage_name: "test".into(),
            repo_name: "LMA1980/test-repo".into(),
            clone_url: "".into(),
            commit_sha: "head".into(),
            command: "echo test".into(),
            working_dir: Some("/etc".into()),
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };
        let res2 = runner
            .run_stage(&bad_working_dir_task, tx.clone(), None)
            .await;
        assert_eq!(res2.exit_code, -1);
        assert!(res2
            .error_message
            .unwrap()
            .contains("Absolute paths are forbidden"));
    }
}
