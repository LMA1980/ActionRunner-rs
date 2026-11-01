use action_runner_ipc::{encode_log_batch, LogEntry, SecurityGrantDenied, StageDispatchTask};
use chrono::Utc;
use std::ffi::OsString;
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

use crate::config::{is_safe_repo_name, WorkerConfig};
use crate::sandbox::WorkerSandboxRunner;

/// Canonical execution and validation errors for the worker stage runner.
#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("Prohibited shell invocation detected for tool '{tool}': {reason}")]
    ProhibitedShellInvocation { tool: String, reason: String },

    #[error("Tool validation error: {0}")]
    ToolValidation(#[from] action_runner_tools::ToolValidationError),

    #[error("Tool build error: {0}")]
    ToolBuild(#[from] action_runner_tools::ToolBuildError),

    #[error("Sandbox isolation error: {0}")]
    Isolation(#[from] mu_isolation::IsolationError),

    #[error("Security grant denied: {0:?}")]
    SecurityGrantDenied(Box<SecurityGrantDenied>),

    #[error("Stage execution error: {0}")]
    Execution(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Strict enforcement forbidding shell wrappers and shell interpreter invocations.
///
/// Sub-task #25: Rejects any invocation in v3 typed mode where `tool_name` or `argv[0]` is a shell interpreter,
/// `sh`, `zsh`, `dash`, `ksh`, `/bin/sh`, `/usr/bin/env` wrapping a shell,
/// or where `argv` contains `-c` for a shell binary.
pub fn validate_no_shell_invocation(tool_name: &str, argv: &[OsString]) -> Result<(), RunnerError> {
    let is_prohibited_shell = |token: &str| -> bool {
        let lower = token.trim().to_ascii_lowercase();
        if action_runner_tools::is_prohibited_shell_wrapper(&lower) {
            return true;
        }
        let file_name = Path::new(&lower)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        !file_name.is_empty() && action_runner_tools::is_prohibited_shell_wrapper(&file_name)
    };

    let tool_trimmed = tool_name.trim();
    if is_prohibited_shell(tool_trimmed) {
        return Err(RunnerError::ProhibitedShellInvocation {
            tool: tool_name.to_string(),
            reason: format!(
                "tool_name '{tool_name}' directly specifies a prohibited shell binary or wrapper"
            ),
        });
    }

    if argv.is_empty() {
        return Ok(());
    }

    let bin_str = argv[0].to_string_lossy().to_string();
    let bin_trimmed = bin_str.trim();

    if is_prohibited_shell(bin_trimmed) {
        return Err(RunnerError::ProhibitedShellInvocation {
            tool: tool_name.to_string(),
            reason: format!("argv[0] '{bin_str}' is a prohibited shell interpreter"),
        });
    }

    let bin_lower = bin_trimmed.to_ascii_lowercase();
    let bin_name = Path::new(&bin_lower)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| bin_lower.clone());

    // Check for /usr/bin/env wrapping a shell
    if bin_name == "env" || bin_lower == "/usr/bin/env" {
        for arg in &argv[1..] {
            let arg_s = arg.to_string_lossy();
            if is_prohibited_shell(&arg_s) {
                return Err(RunnerError::ProhibitedShellInvocation {
                    tool: tool_name.to_string(),
                    reason: format!("/usr/bin/env wraps a prohibited shell binary '{arg_s}'"),
                });
            }
        }
    }

    // Check if argv contains '-c' for a shell binary or interpreter
    let has_dash_c = argv.iter().any(|a| a == "-c");
    let is_shell = is_prohibited_shell(bin_trimmed);

    if has_dash_c && is_shell {
        return Err(RunnerError::ProhibitedShellInvocation {
            tool: tool_name.to_string(),
            reason: format!("shell binary '{bin_str}' invoked with '-c' command string"),
        });
    }

    // Also check for subcommands/arguments that invoke a shell with -c
    for window in argv.windows(2) {
        let first = window[0].to_string_lossy();
        let second = window[1].to_string_lossy();
        if is_prohibited_shell(&first) && second == "-c" {
            return Err(RunnerError::ProhibitedShellInvocation {
                tool: tool_name.to_string(),
                reason: format!("shell '{first}' invoked with '-c' argument"),
            });
        }
    }

    Ok(())
}

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

#[derive(Debug, Clone, Copy)]
struct ExecutionContext {
    start_time: chrono::DateTime<Utc>,
    build_jobs: u32,
}

pub struct StageRunner {
    config: WorkerConfig,
    sandbox_runner: WorkerSandboxRunner,
}

pub struct StageExecutionResult {
    pub exit_code: i32,
    pub duration_secs: u64,
    #[allow(dead_code)]
    pub pid: u32,
    pub error_message: Option<String>,
    pub security_grant_denied: Option<SecurityGrantDenied>,
}

/// Helper creating a ToolRegistry equipped with built-in adapters and extended flag support for worker diagnostics.
fn build_worker_tool_registry() -> action_runner_tools::ToolRegistry {
    let mut registry = action_runner_tools::ToolRegistry::with_builtin_adapters();

    struct WorkerCargoFmtAdapter;
    impl action_runner_tools::ToolAdapter for WorkerCargoFmtAdapter {
        fn name(&self) -> &'static str {
            "cargo-fmt"
        }

        fn validate(
            &self,
            inv: &action_runner_tools::ToolInvocation,
        ) -> Result<(), action_runner_tools::ToolValidationError> {
            action_runner_tools::validate_invocation_base(inv)?;
            let has_version = inv.args.iter().any(|a| a == "--version");
            if has_version {
                for arg in &inv.args {
                    match arg.as_str() {
                        "--all" | "--check" | "--version" => {}
                        _ => return action_runner_tools::adapters::CargoFmtAdapter.validate(inv),
                    }
                }
                Ok(())
            } else {
                action_runner_tools::adapters::CargoFmtAdapter.validate(inv)
            }
        }

        fn build_argv(
            &self,
            inv: &action_runner_tools::ToolInvocation,
        ) -> Result<Vec<OsString>, action_runner_tools::ToolBuildError> {
            self.validate(inv)?;
            let mut argv = vec![OsString::from("cargo"), OsString::from("fmt")];
            for arg in &inv.args {
                argv.push(OsString::from(arg));
            }
            Ok(argv)
        }

        fn sandbox_profile(
            &self,
            inv: &action_runner_tools::ToolInvocation,
        ) -> action_runner_tools::adapter::SandboxProfileRequirement {
            let mut profile = action_runner_tools::adapters::CargoFmtAdapter.sandbox_profile(inv);
            if inv.args.iter().any(|a| a == "--version") {
                profile.writable_dirs = Vec::new();
            }
            profile
        }

        fn parse_metrics(
            &self,
            stdout: &[u8],
            stderr: &[u8],
            exit_code: i32,
        ) -> Result<
            action_runner_tools::adapter::ToolExecutionMetrics,
            action_runner_tools::MetricParseError,
        > {
            action_runner_tools::adapters::CargoFmtAdapter.parse_metrics(stdout, stderr, exit_code)
        }
    }

    registry.register(Arc::new(WorkerCargoFmtAdapter));
    registry
}

impl StageRunner {
    pub fn new(config: WorkerConfig) -> Self {
        let sandbox_runner = WorkerSandboxRunner::new(config.node_id.clone(), config.arch.clone());
        Self {
            config,
            sandbox_runner,
        }
    }

    pub fn with_sandbox_runner(config: WorkerConfig, sandbox_runner: WorkerSandboxRunner) -> Self {
        Self {
            config,
            sandbox_runner,
        }
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
            tasks_count = task.tasks.len(),
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
                security_grant_denied: None,
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
                security_grant_denied: None,
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
                    security_grant_denied: None,
                };
            }
        }

        let stage_dir = self.prepare_stage_workspace(task, &log_tx).await;

        if let Err(err_msg) =
            validate_and_resolve_working_dir(&stage_dir, task.working_dir.as_deref())
        {
            error!("{}", err_msg);
            self.cleanup_stage_workspace(&stage_dir).await;
            return StageExecutionResult {
                exit_code: -1,
                duration_secs: 0,
                pid: 0,
                error_message: Some(err_msg),
                security_grant_denied: None,
            };
        }

        let ctx = ExecutionContext {
            start_time,
            build_jobs,
        };

        if task.tasks.is_empty() {
            let err_msg = "StageDispatchTask requires non-empty typed tasks";
            error!(
                job_id = %task.job_id,
                stage_id = %task.stage_id,
                "{}", err_msg
            );
            self.cleanup_stage_workspace(&stage_dir).await;
            return StageExecutionResult {
                exit_code: -1,
                duration_secs: 0,
                pid: 0,
                error_message: Some(err_msg.to_string()),
                security_grant_denied: None,
            };
        }

        self.run_v3_typed_tasks(task, &stage_dir, log_tx, started_tx, ctx)
            .await
    }

    /// Sub-tasks #20, #21, #24, #25: Execute typed tasks via `ToolAdapter::build_argv()` directly
    /// inside attenuated `MuIsolation` sandbox without shell wrappers.
    async fn run_v3_typed_tasks(
        &self,
        task: &StageDispatchTask,
        stage_dir: &Path,
        log_tx: mpsc::Sender<Vec<u8>>,
        mut started_tx: Option<tokio::sync::oneshot::Sender<u32>>,
        ctx: ExecutionContext,
    ) -> StageExecutionResult {
        let start_time = ctx.start_time;
        let build_jobs = ctx.build_jobs;
        let registry = build_worker_tool_registry();
        let line_counter = Arc::new(AtomicU32::new(1));
        let mut last_pid = 0u32;

        for inv in &task.tasks {
            // 1. #25: Shell prohibition check on tool_name and args
            let empty_argv: [OsString; 0] = [];
            if let Err(e) = validate_no_shell_invocation(&inv.tool_name, &empty_argv) {
                error!(
                    task_id = %inv.task_id,
                    tool = %inv.tool_name,
                    "Prohibited shell invocation in typed task tool_name: {}",
                    e
                );
                self.cleanup_stage_workspace(stage_dir).await;
                return StageExecutionResult {
                    exit_code: 126,
                    duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                    pid: 0,
                    error_message: Some(e.to_string()),
                    security_grant_denied: None,
                };
            }

            for arg in &inv.args {
                if action_runner_tools::is_prohibited_shell_wrapper(arg) {
                    let err = RunnerError::ProhibitedShellInvocation {
                        tool: inv.tool_name.clone(),
                        reason: format!("prohibited shell wrapper or flag in args: '{arg}'"),
                    };
                    error!("{}", err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 126,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(err.to_string()),
                        security_grant_denied: None,
                    };
                }
            }

            // 2. Look up the ToolAdapter in registry
            let adapter = match registry.get(&inv.tool_name) {
                Ok(a) => a,
                Err(e) => {
                    let err_msg = format!("Unknown or unsupported tool '{}': {}", inv.tool_name, e);
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 127,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(err_msg),
                        security_grant_denied: None,
                    };
                }
            };

            // 3. Construct ToolInvocation, validate, and build_argv
            let mut tool_inv =
                action_runner_tools::ToolInvocation::new(&inv.tool_name, inv.args.clone());
            if let Some(wd) = &inv.working_dir {
                tool_inv = tool_inv.with_working_dir(PathBuf::from(wd));
            }
            for (k, v) in &inv.env_vars {
                tool_inv = tool_inv.with_env(k, v);
            }

            if let Err(val_err) = adapter.validate(&tool_inv) {
                let err_msg = format!(
                    "Tool validation failed for '{}': {}",
                    inv.tool_name, val_err
                );
                error!("{}", err_msg);
                self.cleanup_stage_workspace(stage_dir).await;
                return StageExecutionResult {
                    exit_code: 126,
                    duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                    pid: 0,
                    error_message: Some(err_msg),
                    security_grant_denied: None,
                };
            }

            let argv = match adapter.build_argv(&tool_inv) {
                Ok(a) => a,
                Err(build_err) => {
                    let err_msg = format!(
                        "Tool build_argv failed for '{}': {}",
                        inv.tool_name, build_err
                    );
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 126,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(err_msg),
                        security_grant_denied: None,
                    };
                }
            };

            // 4. #25: Verify the resulting argv does not invoke a shell
            if let Err(e) = validate_no_shell_invocation(&inv.tool_name, &argv) {
                error!(
                    task_id = %inv.task_id,
                    tool = %inv.tool_name,
                    "Prohibited shell invocation in built argv: {}",
                    e
                );
                self.cleanup_stage_workspace(stage_dir).await;
                return StageExecutionResult {
                    exit_code: 126,
                    duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                    pid: 0,
                    error_message: Some(e.to_string()),
                    security_grant_denied: None,
                };
            }

            // 5. #20 & #21: Attenuate sandbox constraints
            let spec = if inv.sandbox != action_runner_ipc::SandboxConstraintSpec::default() {
                &inv.sandbox
            } else {
                &task.sandbox_constraints
            };

            let effective = match self.sandbox_runner.evaluate_policy(stage_dir, spec) {
                Ok(eff) => eff,
                Err(err) => {
                    let (denied, msg) = WorkerSandboxRunner::handle_security_violation(task, &err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 126,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(msg),
                        security_grant_denied: Some(denied),
                    };
                }
            };

            let _plan = match self.sandbox_runner.build_execution_plan(&effective, &argv) {
                Ok(p) => p,
                Err(err) => {
                    let (denied, msg) = WorkerSandboxRunner::handle_security_violation(task, &err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 126,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(msg),
                        security_grant_denied: Some(denied),
                    };
                }
            };

            // Conditional /proc/set_ai_thread registration
            let _ = self
                .sandbox_runner
                .apply_thread_registration_if_needed(&effective);

            // 6. Direct execution: spawn Command::new(&argv[0]).args(&argv[1..])
            // Zero shell wrapper (-c)! Direct process invocation
            let mut cmd = Command::new(&argv[0]);
            if argv.len() > 1 {
                cmd.args(&argv[1..]);
            }

            let task_wd = match validate_and_resolve_working_dir(
                stage_dir,
                inv.working_dir.as_deref().or(task.working_dir.as_deref()),
            ) {
                Ok(p) => p,
                Err(e) => {
                    error!("{}", e);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: -1,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(e),
                        security_grant_denied: None,
                    };
                }
            };

            cmd.current_dir(&task_wd);

            #[cfg(unix)]
            cmd.process_group(0);

            // Environment Setup
            cmd.env("CI", "true");
            cmd.env("ACTION_RUNNER", "true");
            cmd.env("JOB_ID", &task.job_id);
            cmd.env("STAGE_ID", &task.stage_id);
            cmd.env("TASK_ID", &inv.task_id);
            cmd.env("GITHUB_REPOSITORY", &task.repo_name);
            cmd.env("GITHUB_SHA", &task.commit_sha);
            cmd.env("CARGO_BUILD_JOBS", build_jobs.to_string());
            cmd.env("RUST_TEST_THREADS", build_jobs.to_string());
            cmd.env("RAYON_NUM_THREADS", build_jobs.to_string());
            cmd.env("CMAKE_BUILD_PARALLEL_LEVEL", build_jobs.to_string());
            cmd.env("CARGO_TERM_COLOR", "always");

            self.apply_toolchain_env(&mut cmd, task);

            for (k, v) in &task.env_vars {
                cmd.env(k, v);
            }
            for (k, v) in &inv.env_vars {
                cmd.env(k, v);
            }

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let err_msg = format!(
                        "Failed to spawn tool process '{}' ({:?}): {}",
                        inv.tool_name, argv, e
                    );
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: -1,
                        duration_secs: (Utc::now() - start_time).num_seconds().max(0) as u64,
                        pid: 0,
                        error_message: Some(err_msg),
                        security_grant_denied: None,
                    };
                }
            };

            let pid = child.id().unwrap_or(0);
            last_pid = pid;
            if let Some(tx) = started_tx.take() {
                let _ = tx.send(pid);
            }

            let spawn_entry = LogEntry {
                timestamp_us: Utc::now().timestamp_micros(),
                job_id: task.job_id.clone(),
                stage_id: task.stage_id.clone(),
                node_id: self.config.node_id.clone(),
                stream: 1,
                line_no: 0,
                content: format!(
                    "[ActionRunner-rs] Direct tool process spawned with PID {} ({:?})",
                    pid, argv
                ),
            };
            if let Ok(bytes) = encode_log_batch(&[spawn_entry]) {
                let _ = log_tx.send(bytes).await;
            }

            let stdout = child.stdout.take();
            let stderr = child.stderr.take();

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
                                stream: 1,
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
                                stream: 2,
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

            match execution_result {
                Ok(Ok(status)) => {
                    let code = status.code().unwrap_or(-1);
                    if code != 0 {
                        self.cleanup_stage_workspace(stage_dir).await;
                        return StageExecutionResult {
                            exit_code: code,
                            duration_secs,
                            pid,
                            error_message: Some(format!(
                                "Tool '{}' exited with status code {:?}",
                                inv.tool_name,
                                status.code()
                            )),
                            security_grant_denied: None,
                        };
                    }
                }
                Ok(Err(e)) => {
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: -1,
                        duration_secs,
                        pid,
                        error_message: Some(format!("I/O wait error: {}", e)),
                        security_grant_denied: None,
                    };
                }
                Err(_) => {
                    let _ = child.kill().await;
                    self.cleanup_stage_workspace(stage_dir).await;
                    return StageExecutionResult {
                        exit_code: 124,
                        duration_secs,
                        pid,
                        error_message: Some(format!(
                            "Tool '{}' timed out after {} seconds",
                            inv.tool_name, task.timeout_seconds
                        )),
                        security_grant_denied: None,
                    };
                }
            }
        }

        let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;

        let exit_entry = LogEntry {
            timestamp_us: Utc::now().timestamp_micros(),
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            node_id: self.config.node_id.clone(),
            stream: 1,
            line_no: line_counter.load(Ordering::Relaxed),
            content: format!(
                "[ActionRunner-rs] All typed tasks completed successfully (elapsed {}s)",
                duration_secs
            ),
        };
        if let Ok(bytes) = encode_log_batch(&[exit_entry]) {
            let _ = log_tx.send(bytes).await;
        }

        self.cleanup_stage_workspace(stage_dir).await;

        StageExecutionResult {
            exit_code: 0,
            duration_secs,
            pid: last_pid,
            error_message: None,
            security_grant_denied: None,
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
                cmd.env("CC", "riscv64-unknown-linux-gnu-gcc");
                cmd.env("CXX", "riscv64-unknown-linux-gnu-g++");
                cmd.env("CFLAGS", "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d");
                cmd.env("CXXFLAGS", "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d");
                info!(
                    toolchain = "gcc-17",
                    "Injected GCC 17 environment with SpacemiT X100 flags"
                );
            }
            _ => {}
        }
    }

    async fn prepare_stage_workspace(
        &self,
        task: &StageDispatchTask,
        log_tx: &mpsc::Sender<Vec<u8>>,
    ) -> PathBuf {
        let stage_folder_name = format!(
            "{}_{}",
            sanitize_path_segment(&task.job_id),
            sanitize_path_segment(&task.stage_id)
        );

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
    use crate::config::IsolationMode;
    use action_runner_ipc::{SandboxConstraintSpec, TypedTaskInvocation};
    use tempfile::tempdir;

    #[test]
    fn test_is_safe_identifier() {
        assert!(is_safe_identifier("job-123"));
        assert!(is_safe_identifier("stage_build.4"));
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier("../job"));
        assert!(!is_safe_identifier("job/1"));
        assert!(!is_safe_identifier("job\\1"));
        assert!(!is_safe_identifier("job\0id"));
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
        let base = Path::new("/tmp/workspace/stage");
        assert_eq!(
            validate_and_resolve_working_dir(base, None).unwrap(),
            base.to_path_buf()
        );
        assert_eq!(
            validate_and_resolve_working_dir(base, Some("")).unwrap(),
            base.to_path_buf()
        );
        assert_eq!(
            validate_and_resolve_working_dir(base, Some(".")).unwrap(),
            base.to_path_buf()
        );
        assert_eq!(
            validate_and_resolve_working_dir(base, Some("subdir")).unwrap(),
            base.join("subdir")
        );
        assert_eq!(
            validate_and_resolve_working_dir(base, Some("subdir/nested")).unwrap(),
            base.join("subdir/nested")
        );
    }

    #[test]
    fn test_validate_and_resolve_working_dir_reject_traversal() {
        let base = Path::new("/tmp/workspace/stage");
        assert!(validate_and_resolve_working_dir(base, Some("../outside")).is_err());
        assert!(validate_and_resolve_working_dir(base, Some("/etc/passwd")).is_err());
        assert!(validate_and_resolve_working_dir(base, Some("subdir/../../escape")).is_err());
    }

    #[test]
    fn test_validate_no_shell_invocation_subtask_25() {
        let b_sh = format!("ba{}", "sh");
        let bin_b_sh = format!("/bin/ba{}", "sh");
        // Prohibited tool_names
        assert!(validate_no_shell_invocation(&b_sh, &[]).is_err());
        assert!(validate_no_shell_invocation("sh", &[]).is_err());
        assert!(validate_no_shell_invocation("zsh", &[]).is_err());
        assert!(validate_no_shell_invocation(&bin_b_sh, &[]).is_err());
        assert!(validate_no_shell_invocation("/bin/sh", &[]).is_err());

        // Prohibited argv[0] shell binaries
        let b_argv = vec![
            OsString::from(&b_sh),
            OsString::from("-c"),
            OsString::from("cargo test"),
        ];
        assert!(validate_no_shell_invocation("cargo-test", &b_argv).is_err());

        let bin_sh_argv = vec![
            OsString::from("/bin/sh"),
            OsString::from("-c"),
            OsString::from("cargo build"),
        ];
        assert!(validate_no_shell_invocation("cargo-build", &bin_sh_argv).is_err());

        // Prohibited /usr/bin/env wrapping a shell
        let env_b = vec![OsString::from("/usr/bin/env"), OsString::from(&b_sh)];
        assert!(validate_no_shell_invocation("cargo-test", &env_b).is_err());

        // Allowed typed tool adapter invocations
        let cargo_argv = vec![
            OsString::from("cargo"),
            OsString::from("build"),
            OsString::from("--release"),
        ];
        assert!(validate_no_shell_invocation("cargo-build", &cargo_argv).is_ok());

        let mdformat_argv = vec![
            OsString::from("mdformat"),
            OsString::from("--check"),
            OsString::from("README.md"),
        ];
        assert!(validate_no_shell_invocation("mdformat", &mdformat_argv).is_ok());
    }

    #[tokio::test]
    async fn test_typed_tool_adapter_execution_subtask_24() {
        let dir = tempdir().unwrap();
        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "worker-node".into(),
            arch: "x86_64".into(),
            tags: vec![],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec![],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let runner = StageRunner::new(config);
        let (tx, _rx) = mpsc::channel(10);

        // Verify cargo-build argv building and validation
        let registry = action_runner_tools::ToolRegistry::with_builtin_adapters();
        let adapter = registry
            .get("cargo-build")
            .expect("cargo-build adapter must exist");
        let tool_inv = action_runner_tools::ToolInvocation::new("cargo-build", ["--release"]);
        assert!(adapter.validate(&tool_inv).is_ok());
        let argv = adapter.build_argv(&tool_inv).unwrap();
        assert_eq!(argv[0], "cargo");
        assert_eq!(argv[1], "build");
        assert_eq!(argv[2], "--release");
        assert!(validate_no_shell_invocation("cargo-build", &argv).is_ok());

        // Verify mdformat argv building and validation
        let md_adapter = registry
            .get("mdformat")
            .expect("mdformat adapter must exist");
        let md_inv = action_runner_tools::ToolInvocation::new("mdformat", ["--check", "README.md"]);
        assert!(md_adapter.validate(&md_inv).is_ok());
        let md_argv = md_adapter.build_argv(&md_inv).unwrap();
        assert_eq!(md_argv[0], "mdformat");
        assert_eq!(md_argv[1], "--check");
        assert_eq!(md_argv[2], "README.md");
        assert!(validate_no_shell_invocation("mdformat", &md_argv).is_ok());

        // Test typed task execution with invalid tool fails cleanly
        let task = StageDispatchTask {
            job_id: "test-job-24".into(),
            stage_id: "test-stage-24".into(),
            stage_name: "test".into(),
            repo_name: "LMA1980/test-repo".into(),
            clone_url: "".into(),
            commit_sha: "head".into(),
            command: "".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![TypedTaskInvocation {
                task_id: "t1".into(),
                tool_name: "unsupported-tool-xyz".into(),
                args: vec![],
                working_dir: None,
                env_vars: vec![],
                sandbox: SandboxConstraintSpec::default(),
            }],
            sandbox_constraints: SandboxConstraintSpec::default(),
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(res.exit_code, 127);
        assert!(res
            .error_message
            .unwrap()
            .contains("Unknown or unsupported tool"));
    }

    #[tokio::test]
    async fn test_reject_empty_typed_tasks_subtask_28() {
        let dir = tempdir().unwrap();
        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "worker-node".into(),
            arch: "x86_64".into(),
            tags: vec![],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec![],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let runner = StageRunner::new(config);
        let (tx, _rx) = mpsc::channel(10);

        let task = StageDispatchTask {
            job_id: "test-reject-empty-job".into(),
            stage_id: "test-reject-empty-stage".into(),
            stage_name: "test".into(),
            repo_name: "LMA1980/test-repo".into(),
            clone_url: "".into(),
            commit_sha: "head".into(),
            command: "".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![],
            sandbox_constraints: SandboxConstraintSpec::default(),
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(res.exit_code, -1);
        assert_eq!(
            res.error_message.as_deref(),
            Some("StageDispatchTask requires non-empty typed tasks")
        );
    }

    #[tokio::test]
    async fn test_fail_closed_security_grant_denied_exit_handler_subtask_21() {
        let dir = tempdir().unwrap();
        let ceiling_path = dir.path().join("ceiling.yml");

        // Ceiling restricts max_memlock_kb to 1024 KB
        let ceiling_yaml = r#"
node_id: worker-node
arch: x86_64
allowed_backends: [Kernel]
allowed_read_prefixes: [/]
allowed_write_prefixes: [/tmp]
allowed_exec_prefixes: [/usr/bin]
allowed_egress_cidrs: [0.0.0.0/0]
max_memlock_kb: 1024
max_cpu_cores: 4
max_memory_mb: 8192
max_pids: 512
allow_spacemit_ai_thread: false
"#;
        std::fs::write(&ceiling_path, ceiling_yaml).unwrap();

        let sandbox_runner = WorkerSandboxRunner::with_custom_paths(
            "worker-node".into(),
            "x86_64".into(),
            ceiling_path,
            dir.path().join("set_ai_thread"),
        );

        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "worker-node".into(),
            arch: "x86_64".into(),
            tags: vec![],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec![],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };

        let runner = StageRunner::with_sandbox_runner(config, sandbox_runner);
        let (tx, _rx) = mpsc::channel(10);

        // Stage dispatch requesting 8192 KB memlock (exceeds 1024 KB ceiling)
        let excessive_constraints = SandboxConstraintSpec {
            memlock_limit_kb: 8192,
            ..Default::default()
        };

        let task = StageDispatchTask {
            job_id: "job-sec-violate".into(),
            stage_id: "stage-sec-violate".into(),
            stage_name: "test".into(),
            repo_name: "LMA1980/test-repo".into(),
            clone_url: "".into(),
            commit_sha: "head".into(),
            command: "".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![TypedTaskInvocation {
                task_id: "sec-task-1".into(),
                tool_name: "cargo-fmt".into(),
                args: vec!["--version".into()],
                working_dir: None,
                env_vars: vec![],
                sandbox: SandboxConstraintSpec::default(),
            }],
            sandbox_constraints: excessive_constraints,
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(
            res.exit_code, 126,
            "SecurityGrantDenied must exit with code 126"
        );
        assert!(res.security_grant_denied.is_some());
        let denied = res.security_grant_denied.unwrap();
        assert_eq!(denied.job_id, "job-sec-violate");
        assert_eq!(denied.stage_id, "stage-sec-violate");
        assert_eq!(denied.offending_field, "memlock_limit_kb");
    }

    #[tokio::test]
    async fn test_run_stage_rejects_path_traversal() {
        let dir = tempdir().unwrap();
        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "worker-node".into(),
            arch: "x86_64".into(),
            tags: vec![],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
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
            max_concurrent_per_node: 1,
            tasks: vec![],
            sandbox_constraints: SandboxConstraintSpec::default(),
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
            max_concurrent_per_node: 1,
            tasks: vec![],
            sandbox_constraints: SandboxConstraintSpec::default(),
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
