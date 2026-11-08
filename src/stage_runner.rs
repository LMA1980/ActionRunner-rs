use chrono::Utc;
use mu_ci_ipc::{
    encode_log_batch, LogEntry, SccacheMode, SecurityGrantDenied, StageDispatchTask,
    TypedTaskInvocation,
};
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
use tracing::{error, info, warn};

use crate::config::{is_safe_repo_name, WorkerConfig};
use crate::sandbox::WorkerSandboxRunner;
use crate::telemetry_emitter::{
    convert_metrics_to_benchmark_entries, convert_metrics_to_test_entries,
    extract_crate_name_from_invocation, BenchmarkRunEntry, HardwareProfile, OtelSpanEntry,
    StageTraceSession, TelemetryEmitter, TestCaseRunEntry,
};

/// Canonical execution and validation errors for the worker stage runner.
#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error("Prohibited shell invocation detected for tool '{tool}': {reason}")]
    ProhibitedShellInvocation { tool: String, reason: String },

    #[error("Tool validation error: {0}")]
    ToolValidation(#[from] mu_ci_tools::ToolValidationError),

    #[error("Tool build error: {0}")]
    ToolBuild(#[from] mu_ci_tools::ToolBuildError),

    #[error("Sandbox isolation error: {0}")]
    Isolation(#[from] mu_ci_isolation::IsolationError),

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
        if mu_ci_tools::is_prohibited_shell_wrapper(&lower) {
            return true;
        }
        let file_name = Path::new(&lower)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        !file_name.is_empty() && mu_ci_tools::is_prohibited_shell_wrapper(&file_name)
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
    telemetry_emitter: TelemetryEmitter,
}

#[derive(Debug, Clone)]
pub struct StageExecutionResult {
    pub exit_code: i32,
    pub duration_secs: u64,
    #[allow(dead_code)]
    pub pid: u32,
    pub error_message: Option<String>,
    pub security_grant_denied: Option<SecurityGrantDenied>,
    pub traceparent: Option<String>,
    pub child_span: Option<OtelSpanEntry>,
    pub test_case_runs: Vec<TestCaseRunEntry>,
    pub benchmark_runs: Vec<BenchmarkRunEntry>,
}

impl StageExecutionResult {
    fn failed_early(
        session: &StageTraceSession,
        exit_code: i32,
        duration_secs: u64,
        pid: u32,
        error_message: String,
        security_grant_denied: Option<SecurityGrantDenied>,
    ) -> Self {
        let child_span = session.finish_span(exit_code, Some(&error_message));
        Self {
            exit_code,
            duration_secs,
            pid,
            error_message: Some(error_message),
            security_grant_denied,
            traceparent: Some(session.echo_traceparent.clone()),
            child_span: Some(child_span),
            test_case_runs: Vec::new(),
            benchmark_runs: Vec::new(),
        }
    }
}

/// Helper creating a ToolRegistry equipped with built-in adapters and extended flag support for worker diagnostics.
fn build_worker_tool_registry() -> mu_ci_tools::ToolRegistry {
    let mut registry = mu_ci_tools::ToolRegistry::with_builtin_adapters();

    struct WorkerCargoFmtAdapter;
    impl mu_ci_tools::ToolAdapter for WorkerCargoFmtAdapter {
        fn name(&self) -> &'static str {
            "cargo-fmt"
        }

        fn validate(
            &self,
            inv: &mu_ci_tools::ToolInvocation,
        ) -> Result<(), mu_ci_tools::ToolValidationError> {
            mu_ci_tools::validate_invocation_base(inv)?;
            let has_version = inv.args.iter().any(|a| a == "--version");
            if has_version {
                for arg in &inv.args {
                    match arg.as_str() {
                        "--all" | "--check" | "--version" => {}
                        _ => return mu_ci_tools::adapters::CargoFmtAdapter.validate(inv),
                    }
                }
                Ok(())
            } else {
                mu_ci_tools::adapters::CargoFmtAdapter.validate(inv)
            }
        }

        fn build_argv(
            &self,
            inv: &mu_ci_tools::ToolInvocation,
        ) -> Result<Vec<OsString>, mu_ci_tools::ToolBuildError> {
            self.validate(inv)?;
            let mut argv = vec![OsString::from("cargo"), OsString::from("fmt")];
            for arg in &inv.args {
                argv.push(OsString::from(arg));
            }
            Ok(argv)
        }

        fn sandbox_profile(
            &self,
            inv: &mu_ci_tools::ToolInvocation,
        ) -> mu_ci_tools::adapter::SandboxProfileRequirement {
            let mut profile = mu_ci_tools::adapters::CargoFmtAdapter.sandbox_profile(inv);
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
        ) -> Result<mu_ci_tools::adapter::ToolExecutionMetrics, mu_ci_tools::MetricParseError>
        {
            mu_ci_tools::adapters::CargoFmtAdapter.parse_metrics(stdout, stderr, exit_code)
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
            telemetry_emitter: TelemetryEmitter::from_env(),
        }
    }

    pub fn with_sandbox_runner(config: WorkerConfig, sandbox_runner: WorkerSandboxRunner) -> Self {
        Self {
            config,
            sandbox_runner,
            telemetry_emitter: TelemetryEmitter::from_env(),
        }
    }

    #[must_use]
    pub fn with_telemetry_emitter(mut self, telemetry_emitter: TelemetryEmitter) -> Self {
        self.telemetry_emitter = telemetry_emitter;
        self
    }

    pub async fn run_stage(
        &self,
        task: &StageDispatchTask,
        log_tx: mpsc::Sender<Vec<u8>>,
        started_tx: Option<tokio::sync::oneshot::Sender<u32>>,
    ) -> StageExecutionResult {
        let start_time = Utc::now();
        let stage_hardware = HardwareProfile::detect(&self.config, task, task.tasks.first());
        let mut trace_session =
            StageTraceSession::from_dispatch_task(task, &self.config.node_id, &stage_hardware);
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
            trace_id = %trace_session.trace_id_hex(),
            span_id = %trace_session.span_id_hex(),
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
            let res = StageExecutionResult::failed_early(&trace_session, -1, 0, 0, err_msg, None);
            if let Some(span) = &res.child_span {
                let _ = self
                    .telemetry_emitter
                    .emit_stage_bundle(std::slice::from_ref(span), &[], &[], &[])
                    .await;
            }
            return res;
        }

        // Validate job_id and stage_id against path traversal
        if !is_safe_identifier(&task.job_id) || !is_safe_identifier(&task.stage_id) {
            let err_msg = format!(
                "Stage execution rejected: unsafe job_id '{}' or stage_id '{}'",
                task.job_id, task.stage_id
            );
            error!("{}", err_msg);
            let res = StageExecutionResult::failed_early(&trace_session, -1, 0, 0, err_msg, None);
            if let Some(span) = &res.child_span {
                let _ = self
                    .telemetry_emitter
                    .emit_stage_bundle(std::slice::from_ref(span), &[], &[], &[])
                    .await;
            }
            return res;
        }

        // Validate working_dir upfront (before workspace creation)
        if let Some(rel) = task.working_dir.as_deref() {
            let dummy = Path::new("/tmp/stage_check");
            if let Err(err_msg) = validate_and_resolve_working_dir(dummy, Some(rel)) {
                error!("{}", err_msg);
                let res =
                    StageExecutionResult::failed_early(&trace_session, -1, 0, 0, err_msg, None);
                if let Some(span) = &res.child_span {
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(std::slice::from_ref(span), &[], &[], &[])
                        .await;
                }
                return res;
            }
        }

        let setup_start = std::time::Instant::now();
        let stage_dir = self.prepare_stage_workspace(task, &log_tx).await;
        trace_session.record_sandbox_setup_ns(setup_start.elapsed().as_nanos() as u64);

        if let Err(err_msg) =
            validate_and_resolve_working_dir(&stage_dir, task.working_dir.as_deref())
        {
            error!("{}", err_msg);
            self.cleanup_stage_workspace(&stage_dir).await;
            let res = StageExecutionResult::failed_early(&trace_session, -1, 0, 0, err_msg, None);
            if let Some(span) = &res.child_span {
                let _ = self
                    .telemetry_emitter
                    .emit_stage_bundle(std::slice::from_ref(span), &[], &[], &[])
                    .await;
            }
            return res;
        }

        let ctx = ExecutionContext {
            start_time,
            build_jobs,
        };

        if task.tasks.is_empty() {
            let err_msg = "StageDispatchTask requires non-empty typed tasks".to_string();
            error!(
                job_id = %task.job_id,
                stage_id = %task.stage_id,
                "{}", err_msg
            );
            self.cleanup_stage_workspace(&stage_dir).await;
            let res = StageExecutionResult::failed_early(&trace_session, -1, 0, 0, err_msg, None);
            if let Some(span) = &res.child_span {
                let _ = self
                    .telemetry_emitter
                    .emit_stage_bundle(std::slice::from_ref(span), &[], &[], &[])
                    .await;
            }
            return res;
        }

        self.run_v3_typed_tasks(task, &stage_dir, log_tx, started_tx, ctx, trace_session)
            .await
    }

    /// Sub-tasks #20, #21, #24, #25, #33, #34: Execute typed tasks via `ToolAdapter::build_argv()`
    /// directly inside attenuated `MuIsolation` sandbox without shell wrappers, correlating logs
    /// and metrics into W3C-linked Arrow telemetry batches.
    async fn run_v3_typed_tasks(
        &self,
        task: &StageDispatchTask,
        stage_dir: &Path,
        log_tx: mpsc::Sender<Vec<u8>>,
        mut started_tx: Option<tokio::sync::oneshot::Sender<u32>>,
        ctx: ExecutionContext,
        mut trace_session: StageTraceSession,
    ) -> StageExecutionResult {
        let start_time = ctx.start_time;
        let build_jobs = ctx.build_jobs;
        let registry = build_worker_tool_registry();
        let line_counter = Arc::new(AtomicU32::new(1));
        let mut last_pid = 0u32;
        let mut all_test_runs: Vec<TestCaseRunEntry> = Vec::new();
        let mut all_bench_runs: Vec<BenchmarkRunEntry> = Vec::new();

        for inv in &task.tasks {
            let sandbox_step_start = std::time::Instant::now();
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
                let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                let res = StageExecutionResult::failed_early(
                    &trace_session,
                    126,
                    duration_secs,
                    0,
                    e.to_string(),
                    None,
                );
                if let Some(span) = &res.child_span {
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                }
                return res;
            }

            for arg in &inv.args {
                if mu_ci_tools::is_prohibited_shell_wrapper(arg) {
                    let err = RunnerError::ProhibitedShellInvocation {
                        tool: inv.tool_name.clone(),
                        reason: format!("prohibited shell wrapper or flag in args: '{arg}'"),
                    };
                    error!("{}", err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        126,
                        duration_secs,
                        0,
                        err.to_string(),
                        None,
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
                }
            }

            // 2. Look up the ToolAdapter in registry
            let adapter = match registry.get(&inv.tool_name) {
                Ok(a) => a,
                Err(e) => {
                    let err_msg = format!("Unknown or unsupported tool '{}': {}", inv.tool_name, e);
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(stage_dir).await;
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        127,
                        duration_secs,
                        0,
                        err_msg,
                        None,
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
                }
            };

            // 3. Construct ToolInvocation, validate, and build_argv
            let mut tool_inv = mu_ci_tools::ToolInvocation::new(&inv.tool_name, inv.args.clone());
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
                let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                let res = StageExecutionResult::failed_early(
                    &trace_session,
                    126,
                    duration_secs,
                    0,
                    err_msg,
                    None,
                );
                if let Some(span) = &res.child_span {
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                }
                return res;
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
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        126,
                        duration_secs,
                        0,
                        err_msg,
                        None,
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
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
                let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                let res = StageExecutionResult::failed_early(
                    &trace_session,
                    126,
                    duration_secs,
                    0,
                    e.to_string(),
                    None,
                );
                if let Some(span) = &res.child_span {
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                }
                return res;
            }

            // 5. #20 & #21: Attenuate sandbox constraints
            let spec = if inv.sandbox != mu_ci_ipc::SandboxConstraintSpec::default() {
                &inv.sandbox
            } else {
                &task.sandbox_constraints
            };

            let effective = match self.sandbox_runner.evaluate_policy(stage_dir, spec) {
                Ok(eff) => eff,
                Err(err) => {
                    let (denied, msg) = WorkerSandboxRunner::handle_security_violation(task, &err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        126,
                        duration_secs,
                        0,
                        msg,
                        Some(denied),
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
                }
            };

            let _plan = match self.sandbox_runner.build_execution_plan(&effective, &argv) {
                Ok(p) => p,
                Err(err) => {
                    let (denied, msg) = WorkerSandboxRunner::handle_security_violation(task, &err);
                    self.cleanup_stage_workspace(stage_dir).await;
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        126,
                        duration_secs,
                        0,
                        msg,
                        Some(denied),
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
                }
            };

            // Conditional /proc/set_ai_thread registration
            let _ = self
                .sandbox_runner
                .apply_thread_registration_if_needed(&effective);

            trace_session.record_sandbox_setup_ns(sandbox_step_start.elapsed().as_nanos() as u64);

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
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        -1,
                        duration_secs,
                        0,
                        e,
                        None,
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
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
            cmd.env("TRACEPARENT", trace_session.child_traceparent());
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

            if let Err(sccache_err) = self.apply_sccache_env(&mut cmd, task, inv) {
                error!(
                    job_id = %task.job_id,
                    stage_id = %task.stage_id,
                    task_id = %inv.task_id,
                    "{}", sccache_err
                );
                self.cleanup_stage_workspace(stage_dir).await;
                let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                let res = StageExecutionResult::failed_early(
                    &trace_session,
                    126,
                    duration_secs,
                    0,
                    sccache_err,
                    None,
                );
                if let Some(span) = &res.child_span {
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                }
                return res;
            }

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let tool_exec_start = std::time::Instant::now();
            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let err_msg = format!(
                        "Failed to spawn tool process '{}' ({:?}): {}",
                        inv.tool_name, argv, e
                    );
                    error!("{}", err_msg);
                    self.cleanup_stage_workspace(stage_dir).await;
                    let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;
                    let res = StageExecutionResult::failed_early(
                        &trace_session,
                        -1,
                        duration_secs,
                        0,
                        err_msg,
                        None,
                    );
                    if let Some(span) = &res.child_span {
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                    }
                    return res;
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
                    let mut collected_bytes = Vec::new();
                    if let Some(out) = stdout {
                        let mut reader = BufReader::new(out).lines();
                        let mut buffer = Vec::new();

                        while let Ok(Some(line)) = reader.next_line().await {
                            collected_bytes.extend_from_slice(line.as_bytes());
                            collected_bytes.push(b'\n');
                            let l_num = line_counter.fetch_add(1, Ordering::Relaxed);
                            let entry = LogEntry {
                                timestamp_us: Utc::now().timestamp_micros(),
                                job_id: job_id.clone(),
                                stage_id: stage_id.clone(),
                                node_id: node_id.clone(),
                                stream: 1,
                                line_no: l_num,
                                content: line,
                            };
                            buffer.push(entry);

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
                    collected_bytes
                })
            };

            let err_handle = {
                let log_tx = log_tx.clone();
                let node_id = self.config.node_id.clone();
                let job_id = task.job_id.clone();
                let stage_id = task.stage_id.clone();
                let line_counter = line_counter.clone();

                tokio::spawn(async move {
                    let mut collected_bytes = Vec::new();
                    if let Some(err) = stderr {
                        let mut reader = BufReader::new(err).lines();
                        let mut buffer = Vec::new();

                        while let Ok(Some(line)) = reader.next_line().await {
                            collected_bytes.extend_from_slice(line.as_bytes());
                            collected_bytes.push(b'\n');
                            let l_num = line_counter.fetch_add(1, Ordering::Relaxed);
                            let entry = LogEntry {
                                timestamp_us: Utc::now().timestamp_micros(),
                                job_id: job_id.clone(),
                                stage_id: stage_id.clone(),
                                node_id: node_id.clone(),
                                stream: 2,
                                line_no: l_num,
                                content: line,
                            };
                            buffer.push(entry);

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
                    collected_bytes
                })
            };

            let timeout_duration = Duration::from_secs(task.timeout_seconds);
            let execution_result = timeout(timeout_duration, child.wait()).await;

            let stdout_bytes = out_handle.await.unwrap_or_default();
            let stderr_bytes = err_handle.await.unwrap_or_default();
            trace_session.record_tool_execution_ns(tool_exec_start.elapsed().as_nanos() as u64);

            let duration_secs = (Utc::now() - start_time).num_seconds().max(0) as u64;

            match execution_result {
                Ok(Ok(status)) => {
                    let code = status.code().unwrap_or(-1);
                    let parse_start = std::time::Instant::now();
                    if let Ok(metrics) = adapter.parse_metrics(&stdout_bytes, &stderr_bytes, code) {
                        let now_ns = Utc::now()
                            .timestamp_nanos_opt()
                            .unwrap_or(trace_session.start_time_unix_nano);
                        let inv_hw = HardwareProfile::detect(&self.config, task, Some(inv));
                        let crate_name = extract_crate_name_from_invocation(inv, &task.repo_name);
                        all_test_runs.extend(convert_metrics_to_test_entries(
                            &metrics,
                            task,
                            &crate_name,
                            &inv.tool_name,
                            &self.config.node_id,
                            &inv_hw,
                            now_ns,
                        ));
                        all_bench_runs.extend(convert_metrics_to_benchmark_entries(
                            &metrics,
                            task,
                            &crate_name,
                            &self.config.node_id,
                            &inv_hw,
                            now_ns,
                        ));
                    }
                    trace_session.record_metric_parse_ns(parse_start.elapsed().as_nanos() as u64);

                    if code != 0 {
                        self.cleanup_stage_workspace(stage_dir).await;
                        let err_msg = format!(
                            "Tool '{}' exited with status code {:?}",
                            inv.tool_name,
                            status.code()
                        );
                        let child_span = trace_session.finish_span(code, Some(&err_msg));
                        let _ = self
                            .telemetry_emitter
                            .emit_stage_bundle(
                                std::slice::from_ref(&child_span),
                                &[],
                                &all_test_runs,
                                &all_bench_runs,
                            )
                            .await;
                        return StageExecutionResult {
                            exit_code: code,
                            duration_secs,
                            pid,
                            error_message: Some(err_msg),
                            security_grant_denied: None,
                            traceparent: Some(trace_session.echo_traceparent),
                            child_span: Some(child_span),
                            test_case_runs: all_test_runs,
                            benchmark_runs: all_bench_runs,
                        };
                    }
                }
                Ok(Err(e)) => {
                    self.cleanup_stage_workspace(stage_dir).await;
                    let err_msg = format!("I/O wait error: {}", e);
                    let child_span = trace_session.finish_span(-1, Some(&err_msg));
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(&child_span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                    return StageExecutionResult {
                        exit_code: -1,
                        duration_secs,
                        pid,
                        error_message: Some(err_msg),
                        security_grant_denied: None,
                        traceparent: Some(trace_session.echo_traceparent),
                        child_span: Some(child_span),
                        test_case_runs: all_test_runs,
                        benchmark_runs: all_bench_runs,
                    };
                }
                Err(_) => {
                    let _ = child.kill().await;
                    self.cleanup_stage_workspace(stage_dir).await;
                    let err_msg = format!(
                        "Tool '{}' timed out after {} seconds",
                        inv.tool_name, task.timeout_seconds
                    );
                    let child_span = trace_session.finish_span(124, Some(&err_msg));
                    let _ = self
                        .telemetry_emitter
                        .emit_stage_bundle(
                            std::slice::from_ref(&child_span),
                            &[],
                            &all_test_runs,
                            &all_bench_runs,
                        )
                        .await;
                    return StageExecutionResult {
                        exit_code: 124,
                        duration_secs,
                        pid,
                        error_message: Some(err_msg),
                        security_grant_denied: None,
                        traceparent: Some(trace_session.echo_traceparent),
                        child_span: Some(child_span),
                        test_case_runs: all_test_runs,
                        benchmark_runs: all_bench_runs,
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

        let child_span = trace_session.finish_span(0, None);
        let _ = self
            .telemetry_emitter
            .emit_stage_bundle(
                std::slice::from_ref(&child_span),
                &[],
                &all_test_runs,
                &all_bench_runs,
            )
            .await;

        StageExecutionResult {
            exit_code: 0,
            duration_secs,
            pid: last_pid,
            error_message: None,
            security_grant_denied: None,
            traceparent: Some(trace_session.echo_traceparent),
            child_span: Some(child_span),
            test_case_runs: all_test_runs,
            benchmark_runs: all_bench_runs,
        }
    }

    fn compute_toolchain_env(
        &self,
        task: &StageDispatchTask,
        base_path: &str,
    ) -> Vec<(&'static str, String)> {
        let mut envs = Vec::new();
        let is_cross = self.config.arch != "riscv64";

        match task.toolchain.as_deref() {
            Some("llvm-24") => {
                let llvm_bin = if Path::new("/opt/llvm-24-spacemit-x100/bin").exists() {
                    "/opt/llvm-24-spacemit-x100/bin"
                } else if Path::new("/home/lma1980/opt/llvm-24/bin").exists() {
                    "/home/lma1980/opt/llvm-24/bin"
                } else {
                    "/opt/cross-llvm-24/bin"
                };
                let gcc_bin = if Path::new("/opt/gcc-17-spacemit-x100/bin").exists() {
                    Some("/opt/gcc-17-spacemit-x100/bin")
                } else if Path::new("/home/lma1980/opt/gcc-17-spacemit-x100/bin").exists() {
                    Some("/home/lma1980/opt/gcc-17-spacemit-x100/bin")
                } else if Path::new("/opt/cross-gcc-17/bin").exists() {
                    Some("/opt/cross-gcc-17/bin")
                } else {
                    None
                };
                let path_val = if let Some(gcc) = gcc_bin {
                    format!("{}:{}:{}", llvm_bin, gcc, base_path)
                } else {
                    format!("{}:{}", llvm_bin, base_path)
                };
                envs.push(("PATH", path_val));
                if is_cross {
                    envs.push((
                        "CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_RUSTFLAGS",
                        "-C target-feature=+v -C target-cpu=spacemit-x100 -C link-arg=--target=riscv64-linux-gnu -C link-arg=--sysroot=/ -C link-arg=-fuse-ld=lld".to_string(),
                    ));
                    envs.push((
                        "CARGO_BUILD_TARGET",
                        "riscv64gc-unknown-linux-gnu".to_string(),
                    ));
                    envs.push((
                        "CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER",
                        "clang".to_string(),
                    ));
                    envs.push(("CC_riscv64gc_unknown_linux_gnu", "clang".to_string()));
                    envs.push(("CXX_riscv64gc_unknown_linux_gnu", "clang++".to_string()));
                    envs.push((
                        "CFLAGS_riscv64gc_unknown_linux_gnu",
                        "--target=riscv64-linux-gnu --sysroot=/ -mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                    envs.push((
                        "CXXFLAGS_riscv64gc_unknown_linux_gnu",
                        "--target=riscv64-linux-gnu --sysroot=/ -mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                    info!(
                        toolchain = "llvm-24",
                        arch = %self.config.arch,
                        "Injected cross-compile LLVM 24 environment targeting riscv64gc-unknown-linux-gnu (RVV 1.0 + spacemit-x100)"
                    );
                } else {
                    envs.push((
                        "CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_RUSTFLAGS",
                        "-C target-feature=+v -C target-cpu=spacemit-x100".to_string(),
                    ));
                    envs.push(("CC", "clang".to_string()));
                    envs.push(("CXX", "clang++".to_string()));
                    envs.push((
                        "RUSTFLAGS",
                        "-C target-feature=+v -C target-cpu=spacemit-x100".to_string(),
                    ));
                    info!(
                        toolchain = "llvm-24",
                        "Injected native LLVM 24 environment with RVV 1.0 flags"
                    );
                }
            }
            Some("gcc-17") | Some("gcc-17-spacemit-x100") => {
                let gcc_bin = if Path::new("/opt/gcc-17-spacemit-x100/bin").exists() {
                    "/opt/gcc-17-spacemit-x100/bin"
                } else if Path::new("/home/lma1980/opt/gcc-17-spacemit-x100/bin").exists() {
                    "/home/lma1980/opt/gcc-17-spacemit-x100/bin"
                } else if Path::new("/home/lma1980/opt/gcc-17/bin").exists() {
                    "/home/lma1980/opt/gcc-17/bin"
                } else {
                    "/opt/cross-gcc-17/bin"
                };
                envs.push(("PATH", format!("{}:{}", gcc_bin, base_path)));
                envs.push((
                    "CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_RUSTFLAGS",
                    "-C target-feature=+v -C target-cpu=spacemit-x100".to_string(),
                ));
                if is_cross {
                    envs.push((
                        "CARGO_BUILD_TARGET",
                        "riscv64gc-unknown-linux-gnu".to_string(),
                    ));
                    envs.push((
                        "CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER",
                        "riscv64-unknown-linux-gnu-gcc".to_string(),
                    ));
                    envs.push((
                        "CC_riscv64gc_unknown_linux_gnu",
                        "riscv64-unknown-linux-gnu-gcc".to_string(),
                    ));
                    envs.push((
                        "CXX_riscv64gc_unknown_linux_gnu",
                        "riscv64-unknown-linux-gnu-g++".to_string(),
                    ));
                    envs.push((
                        "CFLAGS_riscv64gc_unknown_linux_gnu",
                        "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                    envs.push((
                        "CXXFLAGS_riscv64gc_unknown_linux_gnu",
                        "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                } else {
                    envs.push(("CC", "riscv64-unknown-linux-gnu-gcc".to_string()));
                    envs.push(("CXX", "riscv64-unknown-linux-gnu-g++".to_string()));
                    envs.push((
                        "CFLAGS",
                        "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                    envs.push((
                        "CXXFLAGS",
                        "-mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d".to_string(),
                    ));
                }
                info!(
                    toolchain = "gcc-17",
                    arch = %self.config.arch,
                    "Injected GCC 17 environment with SpacemiT X100 flags"
                );
            }
            _ => {}
        }
        envs
    }

    fn apply_toolchain_env(&self, cmd: &mut Command, task: &StageDispatchTask) {
        let base_path =
            std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".to_string());
        for (k, v) in self.compute_toolchain_env(task, &base_path) {
            cmd.env(k, v);
        }
    }

    pub fn is_sccache_eligible_tool(tool_name: &str) -> bool {
        matches!(
            tool_name.trim(),
            "cargo-build" | "cargo-check" | "cargo-test" | "cargo-clippy" | "cargo-bench" | "trunk"
        )
    }

    pub fn resolve_effective_sccache_mode(
        task: &StageDispatchTask,
        inv: &TypedTaskInvocation,
    ) -> SccacheMode {
        if inv.sccache_mode != SccacheMode::Try {
            return inv.sccache_mode;
        }
        let no_per_task_overrides = task
            .tasks
            .iter()
            .all(|t| t.sccache_mode == SccacheMode::Try);
        if no_per_task_overrides && task.sccache_mode != SccacheMode::Try {
            return task.sccache_mode;
        }
        inv.sccache_mode
    }

    pub fn compute_sccache_env(
        &self,
        task: &StageDispatchTask,
        inv: &TypedTaskInvocation,
        sccache_bin: Option<&Path>,
        sccache_extra_env: &[(String, String)],
    ) -> Result<Vec<(String, String)>, String> {
        let mode = Self::resolve_effective_sccache_mode(task, inv);
        let build_wrapper_envs = |bin: &Path| -> Vec<(String, String)> {
            let mut envs = vec![
                ("RUSTC_WRAPPER".to_string(), bin.display().to_string()),
                ("CARGO_INCREMENTAL".to_string(), "0".to_string()),
            ];
            if !sccache_extra_env
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("SCCACHE_IDLE_TIMEOUT"))
            {
                envs.push(("SCCACHE_IDLE_TIMEOUT".to_string(), "0".to_string()));
            }
            envs.extend(sccache_extra_env.iter().cloned());
            envs
        };

        match mode {
            SccacheMode::Off => Ok(Vec::new()),
            SccacheMode::Try => {
                if !Self::is_sccache_eligible_tool(&inv.tool_name) {
                    return Ok(Vec::new());
                }
                if let Some(bin) = sccache_bin {
                    Ok(build_wrapper_envs(bin))
                } else {
                    warn!(
                        job_id = %task.job_id,
                        stage_id = %task.stage_id,
                        task_id = %inv.task_id,
                        "sccache mode is 'try' but sccache binary is not available on host; proceeding without sccache"
                    );
                    Ok(Vec::new())
                }
            }
            SccacheMode::Mandatory => {
                let Some(bin) = sccache_bin else {
                    return Err(format!(
                        "Stage '{}' (task '{}') requires sccache: mandatory, but sccache binary is not installed or available on worker '{}'",
                        task.stage_id, inv.task_id, self.config.node_id
                    ));
                };
                if !Self::is_sccache_eligible_tool(&inv.tool_name) {
                    return Ok(Vec::new());
                }
                Ok(build_wrapper_envs(bin))
            }
        }
    }

    fn run_sccache_control_cmd(
        bin: &Path,
        arg: &str,
        sccache_extra_env: &[(String, String)],
        timeout: std::time::Duration,
    ) -> Result<std::process::Output, String> {
        let mut cmd = std::process::Command::new(bin);
        cmd.arg(arg)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !sccache_extra_env
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("SCCACHE_IDLE_TIMEOUT"))
        {
            cmd.env("SCCACHE_IDLE_TIMEOUT", "0");
        }
        for (k, v) in sccache_extra_env {
            cmd.env(k, v);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to execute '{} {}': {}", bin.display(), arg, e))?;

        let deadline = std::time::Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    return child.wait_with_output().map_err(|e| {
                        format!(
                            "failed to collect output from '{} {}': {}",
                            bin.display(),
                            arg,
                            e
                        )
                    });
                }
                Ok(None) => {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(format!(
                            "timed out after {}s waiting for '{} {}'",
                            timeout.as_secs(),
                            bin.display(),
                            arg
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(e) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "error waiting for '{} {}': {}",
                        bin.display(),
                        arg,
                        e
                    ));
                }
            }
        }
    }

    pub fn ensure_sccache_server_healthy(
        bin: &Path,
        sccache_extra_env: &[(String, String)],
    ) -> Result<(), String> {
        Self::ensure_sccache_server_healthy_with(sccache_extra_env, |arg, timeout| {
            let out = Self::run_sccache_control_cmd(bin, arg, sccache_extra_env, timeout)?;
            Ok((
                out.status.code(),
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ))
        })
    }

    pub fn ensure_sccache_server_healthy_with<F>(
        sccache_extra_env: &[(String, String)],
        mut run_control: F,
    ) -> Result<(), String>
    where
        F: FnMut(&str, std::time::Duration) -> Result<(Option<i32>, String, String), String>,
    {
        let timeout = std::time::Duration::from_secs(5);
        let expects_s3_bucket = sccache_extra_env
            .iter()
            .any(|(k, v)| k == "SCCACHE_BUCKET" && !v.trim().is_empty())
            || std::env::var("SCCACHE_BUCKET")
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);

        let first = run_control("--show-stats", timeout);
        match first {
            Ok((Some(0), stdout, _)) => {
                if expects_s3_bucket && stdout.contains("Local disk") {
                    warn!(
                        "sccache server was running in Local disk mode despite SCCACHE_BUCKET configuration; restarting server with S3 environment"
                    );
                    let _ = run_control("--stop-server", std::time::Duration::from_secs(3));
                    let (code, _, stderr) = run_control("--show-stats", timeout)?;
                    if code != Some(0) {
                        return Err(format!(
                            "sccache S3 server restart failed (exit {:?}): {}",
                            code,
                            stderr.trim()
                        ));
                    }
                }
                Ok(())
            }
            Ok(_) => {
                let _ = run_control("--stop-server", std::time::Duration::from_secs(3));
                let (code, stdout, stderr) = run_control("--show-stats", timeout)?;
                if code == Some(0) {
                    Ok(())
                } else {
                    let detail = if stderr.trim().is_empty() {
                        stdout.trim().to_string()
                    } else {
                        stderr.trim().to_string()
                    };
                    Err(format!(
                        "sccache health check failed (exit {:?}): {}",
                        code, detail
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn apply_sccache_env(
        &self,
        cmd: &mut Command,
        task: &StageDispatchTask,
        inv: &TypedTaskInvocation,
    ) -> Result<(), String> {
        let mode = Self::resolve_effective_sccache_mode(task, inv);
        if mode == SccacheMode::Off {
            cmd.env_remove("RUSTC_WRAPPER");
            cmd.env_remove("CARGO_BUILD_RUSTC_WRAPPER");
            return Ok(());
        }

        let sccache_bin = WorkerConfig::detect_sccache_bin();
        let sccache_extra_env = WorkerConfig::load_sccache_env();

        let envs =
            self.compute_sccache_env(task, inv, sccache_bin.as_deref(), &sccache_extra_env)?;
        if envs.is_empty() {
            cmd.env_remove("RUSTC_WRAPPER");
            cmd.env_remove("CARGO_BUILD_RUSTC_WRAPPER");
            return Ok(());
        }

        if let Some(ref bin) = sccache_bin {
            if let Err(health_err) = Self::ensure_sccache_server_healthy(bin, &sccache_extra_env) {
                if mode == SccacheMode::Mandatory {
                    return Err(format!(
                        "Stage '{}' (task '{}') requires sccache: mandatory, but {}",
                        task.stage_id, inv.task_id, health_err
                    ));
                }
                warn!(
                    job_id = %task.job_id,
                    stage_id = %task.stage_id,
                    task_id = %inv.task_id,
                    error = %health_err,
                    "sccache mode is 'try' but sccache health check failed; proceeding without sccache"
                );
                cmd.env_remove("RUSTC_WRAPPER");
                cmd.env_remove("CARGO_BUILD_RUSTC_WRAPPER");
                return Ok(());
            }
        }

        for (k, v) in envs {
            cmd.env(k, v);
        }
        Ok(())
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
                .stdout(Stdio::null())
                .stderr(Stdio::null())
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
            let is_subvol = Command::new("btrfs")
                .arg("subvolume")
                .arg("show")
                .arg(stage_dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .is_ok_and(|s| s.success());
            if is_subvol {
                let res = Command::new("btrfs")
                    .arg("subvolume")
                    .arg("delete")
                    .arg(stage_dir)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
                if res.is_ok() && !stage_dir.exists() {
                    return;
                }
            }
        }
        let _ = tokio::fs::remove_dir_all(stage_dir).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IsolationMode;
    use mu_ci_ipc::{SandboxConstraintSpec, TypedTaskInvocation};
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
    #[cfg_attr(miri, ignore)]
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
        let registry = mu_ci_tools::ToolRegistry::with_builtin_adapters();
        let adapter = registry
            .get("cargo-build")
            .expect("cargo-build adapter must exist");
        let tool_inv = mu_ci_tools::ToolInvocation::new("cargo-build", ["--release"]);
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
        let md_inv = mu_ci_tools::ToolInvocation::new("mdformat", ["--check", "README.md"]);
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
                sccache_mode: Default::default(),
            }],
            sandbox_constraints: SandboxConstraintSpec::default(),
            traceparent: None,
            sccache_mode: Default::default(),
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(res.exit_code, 127);
        assert!(res
            .error_message
            .unwrap()
            .contains("Unknown or unsupported tool"));
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
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
            traceparent: None,
            sccache_mode: Default::default(),
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(res.exit_code, -1);
        assert_eq!(
            res.error_message.as_deref(),
            Some("StageDispatchTask requires non-empty typed tasks")
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
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
                sccache_mode: Default::default(),
            }],
            sandbox_constraints: excessive_constraints,
            traceparent: None,
            sccache_mode: Default::default(),
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
    #[cfg_attr(miri, ignore)]
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
            traceparent: None,
            sccache_mode: Default::default(),
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
            traceparent: None,
            sccache_mode: Default::default(),
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

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_stage_runner_w3c_traceparent_and_child_span_subtask_33() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock collector");
        let collector_addr = listener.local_addr().expect("collector local addr");
        let collector_handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept collector");
            let mut frames = Vec::new();
            while let Ok((hdr, payload)) = crate::telemetry_emitter::read_frame(&mut stream).await {
                frames.push((hdr, payload));
            }
            frames
        });

        let dir = tempdir().unwrap();
        let config = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "worker-riscv64".into(),
            arch: "riscv64".into(),
            tags: vec!["k3".into(), "x100".into()],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec![],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let runner = StageRunner::new(config)
            .with_telemetry_emitter(TelemetryEmitter::new(collector_addr.to_string()));
        let (tx, mut rx) = mpsc::channel(32);

        let parent_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let task = StageDispatchTask {
            job_id: "job-trace-33".into(),
            stage_id: "stage-fmt-33".into(),
            stage_name: "fmt".into(),
            repo_name: "LMA1980/mu-ci-worker".into(),
            clone_url: "".into(),
            commit_sha: "deadbeef".into(),
            command: "".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 10,
            threads: 1,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![TypedTaskInvocation {
                task_id: "fmt-ver".into(),
                tool_name: "cargo-fmt".into(),
                args: vec!["--version".into()],
                working_dir: None,
                env_vars: vec![],
                sandbox: SandboxConstraintSpec::default(),
                sccache_mode: Default::default(),
            }],
            sandbox_constraints: SandboxConstraintSpec::default(),
            traceparent: Some(parent_tp.to_string()),
            sccache_mode: Default::default(),
        };

        let res = runner.run_stage(&task, tx, None).await;
        assert_eq!(res.exit_code, 0);
        assert_eq!(res.traceparent.as_deref(), Some(parent_tp));
        let span = res.child_span.expect("child span must be produced");
        assert_eq!(span.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.parent_span_id.as_deref(), Some("00f067aa0ba902b7"));
        assert_ne!(span.span_id, "00f067aa0ba902b7");
        assert_eq!(span.status_code, "OK");
        assert_eq!(span.node_id, "worker-riscv64");
        assert!(span.attributes_json.contains("\"cpu_cluster\":\"X100\""));
        assert!(span.attributes_json.contains("\"vlen_bits\":256"));

        let mut streamed_logs = Vec::new();
        while let Ok(batch_bytes) = rx.try_recv() {
            let decoded = mu_ci_ipc::decode_log_batch(&batch_bytes).expect("decode log batch");
            for entry in decoded {
                assert!(!entry.content.contains('\n') && !entry.content.contains('\r'));
                streamed_logs.push(entry);
            }
        }
        assert!(
            streamed_logs.iter().any(|l| l.line_no >= 1),
            "expected tool output lines streamed once over log_tx"
        );

        let collector_frames = collector_handle.await.expect("join collector");
        assert_eq!(collector_frames.len(), 1);
        assert_eq!(
            collector_frames[0].0.msg_type,
            crate::telemetry_emitter::MSG_TYPE_ARROW_OTEL_SPANS
        );
    }

    #[test]
    fn test_cross_compile_factory_vs_native_toolchain_env() {
        let dir = tempdir().unwrap();
        let cross_cfg = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "node-b".into(),
            arch: "x86_64".into(),
            tags: vec![
                "x86_64".into(),
                "cross-compile".into(),
                "riscv64".into(),
                "build".into(),
            ],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec![
                "system".into(),
                "llvm-24".into(),
                "gcc-17-spacemit-x100".into(),
            ],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let cross_runner = StageRunner::new(cross_cfg);

        let mut task = StageDispatchTask {
            job_id: "job-cross-1".into(),
            stage_id: "build-release".into(),
            stage_name: "Build Release".into(),
            repo_name: "LMA1980/k3-core".into(),
            clone_url: "".into(),
            commit_sha: "deadbeef".into(),
            command: "".into(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 10,
            threads: 6,
            toolchain: Some("llvm-24".into()),
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![],
            sandbox_constraints: SandboxConstraintSpec::default(),
            traceparent: None,
            sccache_mode: Default::default(),
        };

        let cross_envs: std::collections::HashMap<&str, String> = cross_runner
            .compute_toolchain_env(&task, "/usr/bin:/bin")
            .into_iter()
            .collect();
        assert_eq!(
            cross_envs.get("CARGO_BUILD_TARGET").map(String::as_str),
            Some("riscv64gc-unknown-linux-gnu")
        );
        assert_eq!(
            cross_envs
                .get("CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER")
                .map(String::as_str),
            Some("clang")
        );
        assert_eq!(
            cross_envs
                .get("CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_RUSTFLAGS")
                .map(String::as_str),
            Some("-C target-feature=+v -C target-cpu=spacemit-x100 -C link-arg=--target=riscv64-linux-gnu -C link-arg=--sysroot=/ -C link-arg=-fuse-ld=lld")
        );
        // Host RUSTFLAGS must not be set on x86_64 cross-builds so host build.rs/proc-macros compile cleanly
        assert!(!cross_envs.contains_key("RUSTFLAGS"));

        // Verify native riscv64 worker behavior
        let native_cfg = WorkerConfig {
            node_id: "worker-riscv64".into(),
            arch: "riscv64".into(),
            ..cross_runner.config.clone()
        };
        let native_runner = StageRunner::new(native_cfg);
        let native_envs: std::collections::HashMap<&str, String> = native_runner
            .compute_toolchain_env(&task, "/usr/bin:/bin")
            .into_iter()
            .collect();
        assert!(!native_envs.contains_key("CARGO_BUILD_TARGET"));
        assert_eq!(
            native_envs.get("RUSTFLAGS").map(String::as_str),
            Some("-C target-feature=+v -C target-cpu=spacemit-x100")
        );

        // Verify gcc-17-spacemit-x100 cross-compile env
        task.toolchain = Some("gcc-17-spacemit-x100".into());
        let gcc_cross_envs: std::collections::HashMap<&str, String> = cross_runner
            .compute_toolchain_env(&task, "/usr/bin:/bin")
            .into_iter()
            .collect();
        assert_eq!(
            gcc_cross_envs.get("CARGO_BUILD_TARGET").map(String::as_str),
            Some("riscv64gc-unknown-linux-gnu")
        );
        assert_eq!(
            gcc_cross_envs
                .get("CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER")
                .map(String::as_str),
            Some("riscv64-unknown-linux-gnu-gcc")
        );
    }

    #[test]
    fn test_compute_sccache_env_modes_and_tool_exclusions() {
        let dir = tempdir().unwrap();
        let cfg = WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".into(),
            cluster_token: "test".into(),
            node_id: "node-a".into(),
            arch: "x86_64".into(),
            tags: vec!["x86_64".into(), "sccache".into()],
            max_slots: 1,
            workspace_dir: dir.path().to_path_buf(),
            toolchains: vec!["system".into()],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        };
        let runner = StageRunner::new(cfg);
        let sccache_path = Path::new("/usr/local/bin/sccache");
        let extra = vec![
            ("SCCACHE_BUCKET".to_string(), "sccache".to_string()),
            (
                "SCCACHE_ENDPOINT".to_string(),
                "http://127.0.0.1:3900".to_string(),
            ),
        ];

        // 1. SccacheMode::Try with eligible tool (cargo-clippy) and sccache present -> injects RUSTC_WRAPPER + CARGO_INCREMENTAL=0
        let task_try = StageDispatchTask {
            job_id: "j1".into(),
            stage_id: "s1".into(),
            sccache_mode: SccacheMode::Try,
            ..Default::default()
        };
        let inv_clippy = TypedTaskInvocation {
            task_id: "t-clippy".into(),
            tool_name: "cargo-clippy".into(),
            sccache_mode: SccacheMode::Try,
            ..Default::default()
        };
        let envs = runner
            .compute_sccache_env(&task_try, &inv_clippy, Some(sccache_path), &extra)
            .expect("try with binary should succeed");
        let map: std::collections::HashMap<String, String> = envs.into_iter().collect();
        assert_eq!(
            map.get("RUSTC_WRAPPER").map(String::as_str),
            Some("/usr/local/bin/sccache")
        );
        assert_eq!(map.get("CARGO_INCREMENTAL").map(String::as_str), Some("0"));
        assert_eq!(
            map.get("SCCACHE_IDLE_TIMEOUT").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            map.get("SCCACHE_BUCKET").map(String::as_str),
            Some("sccache")
        );

        // 1b. cargo-check is also eligible for sccache
        assert!(StageRunner::is_sccache_eligible_tool("cargo-check"));

        // 2. SccacheMode::Try when sccache binary is missing -> falls back gracefully (Ok(empty))
        let envs_missing = runner
            .compute_sccache_env(&task_try, &inv_clippy, None, &extra)
            .expect("try without binary must not fail");
        assert!(envs_missing.is_empty());

        // 3. SccacheMode::Mandatory when sccache binary is missing -> fails closed with Err
        let inv_mandatory = TypedTaskInvocation {
            task_id: "t-build".into(),
            tool_name: "cargo-build".into(),
            sccache_mode: SccacheMode::Mandatory,
            ..Default::default()
        };
        let err = runner
            .compute_sccache_env(&task_try, &inv_mandatory, None, &extra)
            .expect_err("mandatory without binary must fail closed");
        assert!(err.contains("requires sccache: mandatory"));

        // 4. SccacheMode::Off -> returns empty even when binary is present
        let inv_off = TypedTaskInvocation {
            task_id: "t-off".into(),
            tool_name: "cargo-build".into(),
            sccache_mode: SccacheMode::Off,
            ..Default::default()
        };
        let envs_off = runner
            .compute_sccache_env(&task_try, &inv_off, Some(sccache_path), &extra)
            .expect("off mode should succeed");
        assert!(envs_off.is_empty());

        // 5. Excluded tools (cargo-miri, cargo-llvm-cov, cargo-fmt) do not get RUSTC_WRAPPER clobbered
        for excluded in ["cargo-miri", "cargo-llvm-cov", "cargo-fmt", "cargo-audit"] {
            let inv_excl = TypedTaskInvocation {
                task_id: "t-excl".into(),
                tool_name: excluded.into(),
                sccache_mode: SccacheMode::Try,
                ..Default::default()
            };
            let envs_excl = runner
                .compute_sccache_env(&task_try, &inv_excl, Some(sccache_path), &extra)
                .expect("excluded tool should succeed");
            assert!(
                envs_excl.is_empty(),
                "Tool {excluded} must not inject RUSTC_WRAPPER"
            );
        }

        // 6. Mixed stage: stage-level sccache_mode is Mandatory (for worker tag routing),
        //    but individual tasks preserve their resolved Try / Off / Mandatory modes.
        let mixed_stage = StageDispatchTask {
            job_id: "j-mixed".into(),
            stage_id: "s-mixed".into(),
            sccache_mode: SccacheMode::Mandatory,
            tasks: vec![inv_clippy.clone(), inv_off.clone(), inv_mandatory.clone()],
            ..Default::default()
        };
        assert_eq!(
            StageRunner::resolve_effective_sccache_mode(&mixed_stage, &mixed_stage.tasks[0]),
            SccacheMode::Try
        );
        assert_eq!(
            StageRunner::resolve_effective_sccache_mode(&mixed_stage, &mixed_stage.tasks[1]),
            SccacheMode::Off
        );
        assert_eq!(
            StageRunner::resolve_effective_sccache_mode(&mixed_stage, &mixed_stage.tasks[2]),
            SccacheMode::Mandatory
        );
    }

    #[test]
    fn test_ensure_sccache_server_healthy_and_local_disk_hijack_recovery() {
        let extra = vec![("SCCACHE_BUCKET".to_string(), "sccache".to_string())];
        let mut daemon_state = "local_disk".to_string();

        // 1. Auto-recover when daemon started in Local disk mode despite SCCACHE_BUCKET
        StageRunner::ensure_sccache_server_healthy_with(&extra, |arg, _| match arg {
            "--stop-server" => {
                if daemon_state != "permanently_broken" {
                    daemon_state = "stopped".to_string();
                }
                Ok((Some(0), String::new(), String::new()))
            }
            "--show-stats" => match daemon_state.as_str() {
                "local_disk" => Ok((
                    Some(0),
                    "Cache location                  Local disk: /tmp/sccache".to_string(),
                    String::new(),
                )),
                "stopped" | "s3" => {
                    daemon_state = "s3".to_string();
                    Ok((
                        Some(0),
                        "Cache location                  S3, bucket: Bucket(name=sccache)"
                            .to_string(),
                        String::new(),
                    ))
                }
                _ => Ok((Some(1), String::new(), "sccache daemon error".to_string())),
            },
            _ => Err("unexpected arg".to_string()),
        })
        .expect("should auto-recover from Local disk hijack to S3");
        assert_eq!(
            daemon_state, "s3",
            "Daemon must have been stopped and restarted in S3 mode"
        );

        // 2. Transiently wedged daemon (healed by --stop-server + retry)
        daemon_state = "wedged".to_string();
        StageRunner::ensure_sccache_server_healthy_with(&extra, |arg, _| match arg {
            "--stop-server" => {
                daemon_state = "stopped".to_string();
                Ok((Some(0), String::new(), String::new()))
            }
            "--show-stats" => match daemon_state.as_str() {
                "stopped" | "s3" => {
                    daemon_state = "s3".to_string();
                    Ok((
                        Some(0),
                        "Cache location                  S3, bucket: Bucket(name=sccache)"
                            .to_string(),
                        String::new(),
                    ))
                }
                _ => Ok((Some(1), String::new(), "wedged socket".to_string())),
            },
            _ => Err("unexpected arg".to_string()),
        })
        .expect("should auto-recover from transiently wedged daemon");
        assert_eq!(daemon_state, "s3");

        // 3. Permanently broken daemon fails health check
        let err = StageRunner::ensure_sccache_server_healthy_with(&extra, |arg, _| match arg {
            "--stop-server" => Ok((Some(0), String::new(), String::new())),
            "--show-stats" => Ok((
                Some(1),
                String::new(),
                "S3 endpoint unreachable".to_string(),
            )),
            _ => Err("unexpected arg".to_string()),
        })
        .expect_err("broken daemon must return Err");
        assert!(err.contains("sccache health check failed"));
        assert!(err.contains("S3 endpoint unreachable"));
    }
}
