//! Distributed stage tracing (`OtelSpanEntry`, `OtelLogEntry`) and hardware-tagged
//! granular test/benchmark (`TestCaseRunEntry`, `BenchmarkRunEntry`) Arrow batch emitter
//! for local `MuObserve-collector` (`#30`–`#34`, `M3.048`–`M3.052`).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use action_runner_ipc::{
    BenchmarkRunRecord, LogEntry, StageDispatchTask, TestCaseRunRecord, TypedTaskInvocation,
};
use action_runner_tools::{ParsedBenchmark, TestCaseStatus, ToolExecutionMetrics};
use chrono::Utc;
pub use muobserve_ipc::TraceContext as W3cTraceContext;
pub use muobserve_ipc::{
    decode_benchmark_run_batch, decode_log_batch, decode_span_batch, decode_test_run_batch,
    encode_benchmark_run_batch, encode_log_batch, encode_span_batch, encode_test_run_batch,
    read_frame, write_frame, BenchmarkRunEntry, DeterministicSecretRedactor, IpcHeader,
    OtelLogEntry, OtelSpanEntry, SecretRedactor, TestCaseRunEntry, TraceContext,
    FLAG_HIGH_PRIORITY, MSG_TYPE_ARROW_BENCH_RUNS, MSG_TYPE_ARROW_OTEL_LOGS,
    MSG_TYPE_ARROW_OTEL_SPANS, MSG_TYPE_ARROW_TEST_RUNS, MSG_TYPE_WAL_ACK,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::debug;

use crate::config::WorkerConfig;

static EMITTER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Default loopback TCP endpoint for the local `MuObserve-collector` native `MUOB` Arrow listener.
pub const DEFAULT_COLLECTOR_MUOB_ADDR: &str = "127.0.0.1:4319";

/// Detected or configured hardware execution profile for a stage or benchmark run.
///
/// Follows `AGENTS.md §6` canonical cluster mapping on SpacemiT K3:
/// - Cluster 0 (`X100`, cores `0–7`, mask `0x00FF`): `VLEN = 256`
/// - Cluster 1 (`A100`, cores `8–15`, mask `0xFF00`): `VLEN = 1024`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareProfile {
    pub arch: String,
    pub cpu_cluster: String,
    pub vlen_bits: u32,
}

impl HardwareProfile {
    /// Resolves the hardware profile for a worker stage execution from [`WorkerConfig`],
    /// [`StageDispatchTask`], and an optional [`TypedTaskInvocation`].
    #[must_use]
    pub fn detect(
        config: &WorkerConfig,
        task: &StageDispatchTask,
        inv: Option<&TypedTaskInvocation>,
    ) -> Self {
        let arch = if config.arch.trim().is_empty() {
            std::env::consts::ARCH.to_string()
        } else {
            config.arch.trim().to_string()
        };

        let is_riscv = arch.starts_with("riscv64");

        // Check environment overrides in task or invocation first
        let env_cluster = inv
            .and_then(|i| find_env(&i.env_vars, "ACTION_RUNNER_CPU_CLUSTER"))
            .or_else(|| find_env(&task.env_vars, "ACTION_RUNNER_CPU_CLUSTER"))
            .or_else(|| std::env::var("ACTION_RUNNER_CPU_CLUSTER").ok());

        let env_vlen = inv
            .and_then(|i| find_env(&i.env_vars, "ACTION_RUNNER_VLEN_BITS"))
            .or_else(|| find_env(&task.env_vars, "ACTION_RUNNER_VLEN_BITS"))
            .or_else(|| std::env::var("ACTION_RUNNER_VLEN_BITS").ok())
            .and_then(|s| s.trim().parse::<u32>().ok());

        let require_ai_thread = inv
            .map(|i| i.sandbox.require_set_ai_thread)
            .unwrap_or(false)
            || task.sandbox_constraints.require_set_ai_thread;

        let has_a100_tag = config.tags.iter().any(|t| t.eq_ignore_ascii_case("a100"));

        let cpu_cluster = if let Some(cluster) = env_cluster {
            normalize_cluster(&cluster)
        } else if is_riscv {
            if require_ai_thread || has_a100_tag {
                "A100".to_string()
            } else {
                "X100".to_string()
            }
        } else {
            "generic".to_string()
        };

        let vlen_bits = if let Some(v) = env_vlen {
            v
        } else {
            default_vlen_for_cluster(&arch, &cpu_cluster)
        };

        Self {
            arch,
            cpu_cluster,
            vlen_bits,
        }
    }

    /// Merges per-benchmark hardware tags extracted by `ActionRunner-tools` (`CargoBenchAdapter`)
    /// with this stage default hardware profile.
    #[must_use]
    pub fn resolve_for_benchmark(&self, bench: &ParsedBenchmark) -> Self {
        let arch = bench
            .arch
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| self.arch.clone());

        let cpu_cluster = bench
            .cpu_cluster
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(normalize_cluster)
            .unwrap_or_else(|| self.cpu_cluster.clone());

        let vlen_bits = bench.vlen_bits.unwrap_or_else(|| {
            if cpu_cluster == self.cpu_cluster && arch == self.arch && self.vlen_bits > 0 {
                self.vlen_bits
            } else {
                default_vlen_for_cluster(&arch, &cpu_cluster)
            }
        });

        Self {
            arch,
            cpu_cluster,
            vlen_bits,
        }
    }
}

fn find_env(vars: &[(String, String)], key: &str) -> Option<String> {
    vars.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.clone())
}

fn normalize_cluster(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("a100") {
        "A100".to_string()
    } else if trimmed.eq_ignore_ascii_case("x100") {
        "X100".to_string()
    } else if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("generic") {
        "generic".to_string()
    } else {
        trimmed.to_string()
    }
}

fn default_vlen_for_cluster(arch: &str, cpu_cluster: &str) -> u32 {
    if cpu_cluster.eq_ignore_ascii_case("A100") {
        1024
    } else if cpu_cluster.eq_ignore_ascii_case("X100") || arch.starts_with("riscv64") {
        256
    } else {
        0
    }
}

/// Active W3C trace and OpenTelemetry child span session for a single worker stage execution.
#[derive(Debug, Clone)]
pub struct StageTraceSession {
    pub parent_ctx: Option<TraceContext>,
    pub child_ctx: TraceContext,
    pub echo_traceparent: String,
    pub job_id: String,
    pub stage_id: String,
    pub repo_name: String,
    pub commit_sha: String,
    pub node_id: String,
    pub hardware: HardwareProfile,
    pub start_time_unix_nano: i64,
    pub sandbox_setup_ns: u64,
    pub tool_execution_ns: u64,
    pub metric_parse_ns: u64,
}

impl StageTraceSession {
    /// Creates a new [`StageTraceSession`] from a [`StageDispatchTask`], parsing its W3C
    /// `traceparent` if present and deriving a linked child [`TraceContext`].
    #[must_use]
    pub fn from_dispatch_task(
        task: &StageDispatchTask,
        node_id: &str,
        hardware: &HardwareProfile,
    ) -> Self {
        let now_ns = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(1_700_000_000_000_000_000);
        let step_seed = hash_identifier(&task.job_id)
            ^ hash_identifier(&task.stage_id).rotate_left(13)
            ^ (now_ns as u64);

        let (parent_ctx, child_ctx, echo_traceparent) = match task.traceparent.as_deref() {
            Some(raw) => match TraceContext::parse_traceparent(raw) {
                Ok(parent) => {
                    let child = parent.next_deterministic_child(step_seed);
                    (Some(parent), child, raw.trim().to_string())
                }
                Err(_) => {
                    let root = TraceContext::from_seed(
                        hash_identifier(&task.job_id),
                        hash_identifier(&task.commit_sha),
                        step_seed,
                        true,
                    );
                    let tp = root.to_traceparent();
                    (None, root, tp)
                }
            },
            None => {
                let root = TraceContext::from_seed(
                    hash_identifier(&task.job_id),
                    hash_identifier(&task.stage_id),
                    step_seed,
                    true,
                );
                let tp = root.to_traceparent();
                (None, root, tp)
            }
        };

        Self {
            parent_ctx,
            child_ctx,
            echo_traceparent,
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            repo_name: task.repo_name.clone(),
            commit_sha: task.commit_sha.clone(),
            node_id: node_id.to_string(),
            hardware: hardware.clone(),
            start_time_unix_nano: now_ns,
            sandbox_setup_ns: 0,
            tool_execution_ns: 0,
            metric_parse_ns: 0,
        }
    }

    /// Returns the 32-character lowercase hex W3C `trace_id`.
    #[must_use]
    pub fn trace_id_hex(&self) -> String {
        self.child_ctx.trace_id_hex()
    }

    /// Returns the 16-character lowercase hex child `span_id`.
    #[must_use]
    pub fn span_id_hex(&self) -> String {
        self.child_ctx.span_id_hex()
    }

    /// Returns the 16-character lowercase hex parent `span_id`, if linked to an upstream parent trace.
    #[must_use]
    pub fn parent_span_id_hex(&self) -> Option<String> {
        self.parent_ctx.as_ref().map(TraceContext::span_id_hex)
    }

    /// Returns the canonical W3C `traceparent` string for this child span.
    #[must_use]
    pub fn child_traceparent(&self) -> String {
        self.child_ctx.to_traceparent()
    }

    /// Records the duration spent in sandbox policy evaluation and workspace preparation.
    pub fn record_sandbox_setup_ns(&mut self, elapsed_ns: u64) {
        self.sandbox_setup_ns = self.sandbox_setup_ns.saturating_add(elapsed_ns);
    }

    /// Records the duration spent executing typed tool adapters.
    pub fn record_tool_execution_ns(&mut self, elapsed_ns: u64) {
        self.tool_execution_ns = self.tool_execution_ns.saturating_add(elapsed_ns);
    }

    /// Records the duration spent parsing `ToolExecutionMetrics`.
    pub fn record_metric_parse_ns(&mut self, elapsed_ns: u64) {
        self.metric_parse_ns = self.metric_parse_ns.saturating_add(elapsed_ns);
    }

    /// Finalizes and returns the [`OtelSpanEntry`] covering sandbox setup, tool execution, and metric parsing.
    #[must_use]
    pub fn finish_span(&self, exit_code: i32, error_message: Option<&str>) -> OtelSpanEntry {
        let now_ns = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(self.start_time_unix_nano);
        let end_time_unix_nano = now_ns.max(self.start_time_unix_nano);
        let measured_ns =
            u64::try_from(end_time_unix_nano - self.start_time_unix_nano).unwrap_or(0);
        let phase_sum_ns = self
            .sandbox_setup_ns
            .saturating_add(self.tool_execution_ns)
            .saturating_add(self.metric_parse_ns);
        let duration_ns = measured_ns.max(phase_sum_ns);

        let status_code = if exit_code == 0 {
            "OK".to_string()
        } else {
            "ERROR".to_string()
        };

        let attrs = serde_json::json!({
            "job_id": self.job_id,
            "stage_id": self.stage_id,
            "repo_name": self.repo_name,
            "commit_sha": self.commit_sha,
            "exit_code": exit_code,
            "arch": self.hardware.arch,
            "cpu_cluster": self.hardware.cpu_cluster,
            "vlen_bits": self.hardware.vlen_bits,
            "sandbox_setup_ns": self.sandbox_setup_ns,
            "tool_execution_ns": self.tool_execution_ns,
            "metric_parse_ns": self.metric_parse_ns,
        });

        let redactor = DeterministicSecretRedactor::new();
        let mut span = OtelSpanEntry {
            trace_id: self.trace_id_hex(),
            span_id: self.span_id_hex(),
            parent_span_id: self.parent_span_id_hex(),
            service_name: "action-runner-worker".to_string(),
            node_id: self.node_id.clone(),
            span_name: format!("ci.stage.{}", self.stage_id),
            span_kind: "INTERNAL".to_string(),
            start_time_unix_nano: self.start_time_unix_nano,
            end_time_unix_nano: self
                .start_time_unix_nano
                .saturating_add(i64::try_from(duration_ns).unwrap_or(0)),
            duration_ns,
            status_code,
            status_message: error_message.map(ToString::to_string),
            attributes_json: attrs.to_string(),
        };
        span.redact_in_place(&redactor);
        span
    }

    /// Converts worker [`LogEntry`] records into structured [`OtelLogEntry`] records correlated
    /// with this stage's `trace_id` and `span_id`.
    #[must_use]
    pub fn correlate_logs(&self, entries: &[LogEntry]) -> Vec<OtelLogEntry> {
        let redactor = DeterministicSecretRedactor::new();
        let trace_id = self.trace_id_hex();
        let span_id = self.span_id_hex();

        entries
            .iter()
            .map(|e| {
                let (severity_number, severity_text, stream_label) = if e.stream == 2 {
                    (13, "WARN".to_string(), "stderr")
                } else {
                    (9, "INFO".to_string(), "stdout")
                };
                let attrs = serde_json::json!({
                    "stream": stream_label,
                    "line_no": e.line_no,
                });
                let mut log = OtelLogEntry {
                    timestamp_unix_nano: e.timestamp_us.saturating_mul(1_000),
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    severity_number,
                    severity_text,
                    service_name: "action-runner-worker".to_string(),
                    node_id: e.node_id.clone(),
                    body: e.content.clone(),
                    attributes_json: attrs.to_string(),
                    job_id: Some(e.job_id.clone()),
                    stage_id: Some(e.stage_id.clone()),
                };
                log.redact_in_place(&redactor);
                log
            })
            .collect()
    }
}

/// Maps `ToolExecutionMetrics::test_cases` from `ActionRunner-tools` into hardware-tagged
/// [`TestCaseRunEntry`] records for `MuObserve-collector`.
#[must_use]
pub fn convert_metrics_to_test_entries(
    metrics: &ToolExecutionMetrics,
    task: &StageDispatchTask,
    crate_name: &str,
    test_binary: &str,
    node_id: &str,
    hardware: &HardwareProfile,
    timestamp_unix_nano: i64,
) -> Vec<TestCaseRunEntry> {
    let repo_key = normalize_repo_key(&task.repo_name);
    let branch_key = resolve_branch_key(task);
    let resolved_crate = if crate_name.trim().is_empty() {
        default_crate_from_repo(&task.repo_name)
    } else {
        crate_name.trim().to_string()
    };
    let resolved_binary = if test_binary.trim().is_empty() {
        resolved_crate.clone()
    } else {
        test_binary.trim().to_string()
    };

    metrics
        .test_cases
        .iter()
        .map(|tc| {
            let status = match tc.status {
                TestCaseStatus::Passed => "ok",
                TestCaseStatus::Failed => "failed",
                TestCaseStatus::Ignored => "ignored",
            };

            TestCaseRunEntry {
                job_id: task.job_id.clone(),
                repo_key: repo_key.clone(),
                branch_key: branch_key.clone(),
                commit_sha: task.commit_sha.clone(),
                crate_name: resolved_crate.clone(),
                test_binary: resolved_binary.clone(),
                test_name: tc.test_name.clone(),
                status: status.to_string(),
                duration_ms: tc.duration_ms.map(|d| d as f64).unwrap_or(0.0),
                failure_summary: tc.failure_snippet.clone(),
                timestamp_unix_nano,
                node_id: node_id.to_string(),
                arch: hardware.arch.clone(),
            }
        })
        .collect()
}

/// Maps `ToolExecutionMetrics::benchmarks` from `ActionRunner-tools` into hardware-tagged
/// [`BenchmarkRunEntry`] records (`arch`, `cpu_cluster` `X100`/`A100`/`generic`, `vlen_bits` `256`/`1024`)
/// for `MuObserve-collector`.
#[must_use]
pub fn convert_metrics_to_benchmark_entries(
    metrics: &ToolExecutionMetrics,
    task: &StageDispatchTask,
    crate_name: &str,
    node_id: &str,
    default_hardware: &HardwareProfile,
    timestamp_unix_nano: i64,
) -> Vec<BenchmarkRunEntry> {
    let repo_key = normalize_repo_key(&task.repo_name);
    let branch_key = resolve_branch_key(task);
    let resolved_crate = if crate_name.trim().is_empty() {
        default_crate_from_repo(&task.repo_name)
    } else {
        crate_name.trim().to_string()
    };

    metrics
        .benchmarks
        .iter()
        .map(|b| {
            let hw = default_hardware.resolve_for_benchmark(b);
            let variance = b.variance_ns.max(0.0);
            BenchmarkRunEntry {
                job_id: task.job_id.clone(),
                repo_key: repo_key.clone(),
                branch_key: branch_key.clone(),
                commit_sha: task.commit_sha.clone(),
                crate_name: resolved_crate.clone(),
                bench_name: b.bench_name.clone(),
                ns_per_iter: b.ns_per_iter,
                std_dev_ns: variance,
                mad_ns: variance,
                p50_ns: b.ns_per_iter,
                p95_ns: b.ns_per_iter + variance,
                p99_ns: b.ns_per_iter + (2.0 * variance),
                throughput_value: b.throughput_mb_s,
                throughput_unit: b.throughput_mb_s.map(|_| "MB/s".to_string()),
                arch: hw.arch,
                cpu_cluster: hw.cpu_cluster,
                vlen_bits: hw.vlen_bits,
                timestamp_unix_nano,
                node_id: node_id.to_string(),
            }
        })
        .collect()
}

/// Converts [`TestCaseRunEntry`] records into `ActionRunner-ipc` [`TestCaseRunRecord`] items.
#[must_use]
pub fn to_ipc_test_records(entries: &[TestCaseRunEntry], stage_id: &str) -> Vec<TestCaseRunRecord> {
    entries
        .iter()
        .map(|e| TestCaseRunRecord {
            timestamp_us: e.timestamp_unix_nano / 1_000,
            job_id: e.job_id.clone(),
            stage_id: stage_id.to_string(),
            repo_key: e.repo_key.clone(),
            branch_key: e.branch_key.clone(),
            commit_sha: e.commit_sha.clone(),
            crate_name: e.crate_name.clone(),
            test_binary: e.test_binary.clone(),
            test_name: e.test_name.clone(),
            status: e.status.clone(),
            duration_ms: Some(e.duration_ms.max(0.0).round() as u64),
            failure_summary: e.failure_summary.clone(),
        })
        .collect()
}

/// Converts [`BenchmarkRunEntry`] records into `ActionRunner-ipc` [`BenchmarkRunRecord`] items.
#[must_use]
pub fn to_ipc_benchmark_records(
    entries: &[BenchmarkRunEntry],
    stage_id: &str,
) -> Vec<BenchmarkRunRecord> {
    entries
        .iter()
        .map(|e| BenchmarkRunRecord {
            timestamp_us: e.timestamp_unix_nano / 1_000,
            job_id: e.job_id.clone(),
            stage_id: stage_id.to_string(),
            repo_key: e.repo_key.clone(),
            branch_key: e.branch_key.clone(),
            commit_sha: e.commit_sha.clone(),
            crate_name: e.crate_name.clone(),
            bench_name: e.bench_name.clone(),
            ns_per_iter: e.ns_per_iter,
            variance_ns: e.mad_ns,
            throughput_mb_s: e.throughput_value,
            arch: e.arch.clone(),
            cpu_cluster: e.cpu_cluster.clone(),
            vlen_bits: e.vlen_bits,
        })
        .collect()
}

/// Extracts the target crate name from a [`TypedTaskInvocation`] (`-p <crate>` or `--package <crate>`),
/// falling back to the repository's crate name.
#[must_use]
pub fn extract_crate_name_from_invocation(inv: &TypedTaskInvocation, repo_name: &str) -> String {
    let mut iter = inv.args.iter();
    while let Some(arg) = iter.next() {
        if arg == "-p" || arg == "--package" {
            if let Some(pkg) = iter.next() {
                let trimmed = pkg.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        } else if let Some(pkg) = arg.strip_prefix("--package=") {
            let trimmed = pkg.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    default_crate_from_repo(repo_name)
}

fn normalize_repo_key(repo_name: &str) -> String {
    repo_name.trim().to_ascii_lowercase()
}

fn default_crate_from_repo(repo_name: &str) -> String {
    repo_name
        .split('/')
        .nth(1)
        .unwrap_or(repo_name)
        .trim()
        .to_ascii_lowercase()
}

fn resolve_branch_key(task: &StageDispatchTask) -> String {
    for key in ["GITHUB_REF_NAME", "GIT_BRANCH", "BRANCH_NAME", "CI_BRANCH"] {
        if let Some(val) = find_env(&task.env_vars, key) {
            let trimmed = val.trim().trim_start_matches("refs/heads/");
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    "main".to_string()
}

fn hash_identifier(input: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in input.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    if h == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        h
    }
}

/// Asynchronous client emitting `MUOB`-framed Apache Arrow telemetry batches to the local
/// `MuObserve-collector` daemon.
#[derive(Debug, Clone)]
pub struct TelemetryEmitter {
    collector_addr: String,
    connect_timeout: Duration,
}

impl Default for TelemetryEmitter {
    fn default() -> Self {
        Self::from_env()
    }
}

impl TelemetryEmitter {
    /// Creates a new [`TelemetryEmitter`] targeting `collector_addr` (e.g. `"127.0.0.1:4319"`).
    #[must_use]
    pub fn new(collector_addr: impl Into<String>) -> Self {
        Self {
            collector_addr: collector_addr.into(),
            connect_timeout: Duration::from_millis(250),
        }
    }

    /// Creates a [`TelemetryEmitter`] resolved from `MUOBSERVE_COLLECTOR_ADDR` or
    /// [`DEFAULT_COLLECTOR_MUOB_ADDR`].
    #[must_use]
    pub fn from_env() -> Self {
        let addr = std::env::var("MUOBSERVE_COLLECTOR_ADDR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_COLLECTOR_MUOB_ADDR.to_string());
        Self::new(addr)
    }

    /// Overrides the connection/write timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout_duration: Duration) -> Self {
        self.connect_timeout = timeout_duration;
        self
    }

    /// Returns the configured collector endpoint address.
    #[must_use]
    pub fn collector_addr(&self) -> &str {
        &self.collector_addr
    }

    /// Streams a batch of [`OtelSpanEntry`] records as a `MUOB`-framed Arrow IPC message.
    pub async fn emit_spans(&self, spans: &[OtelSpanEntry]) -> Result<u64, String> {
        if spans.is_empty() {
            return Ok(0);
        }
        let payload = encode_span_batch(spans).map_err(|e| e.to_string())?;
        self.send_muob_frames(&[(MSG_TYPE_ARROW_OTEL_SPANS, 0, payload)])
            .await
    }

    /// Streams a batch of [`OtelLogEntry`] records as a `MUOB`-framed Arrow IPC message.
    pub async fn emit_logs(&self, logs: &[OtelLogEntry]) -> Result<u64, String> {
        if logs.is_empty() {
            return Ok(0);
        }
        let high_prio = logs.iter().any(|l| l.severity_number >= 13);
        let flags = if high_prio { FLAG_HIGH_PRIORITY } else { 0 };
        let payload = encode_log_batch(logs).map_err(|e| e.to_string())?;
        self.send_muob_frames(&[(MSG_TYPE_ARROW_OTEL_LOGS, flags, payload)])
            .await
    }

    /// Streams a batch of [`TestCaseRunEntry`] records as a `MUOB`-framed Arrow IPC message.
    pub async fn emit_test_runs(&self, test_runs: &[TestCaseRunEntry]) -> Result<u64, String> {
        if test_runs.is_empty() {
            return Ok(0);
        }
        let payload = encode_test_run_batch(test_runs).map_err(|e| e.to_string())?;
        self.send_muob_frames(&[(MSG_TYPE_ARROW_TEST_RUNS, 0, payload)])
            .await
    }

    /// Streams a batch of [`BenchmarkRunEntry`] records as a `MUOB`-framed Arrow IPC message.
    pub async fn emit_benchmark_runs(
        &self,
        bench_runs: &[BenchmarkRunEntry],
    ) -> Result<u64, String> {
        if bench_runs.is_empty() {
            return Ok(0);
        }
        let payload = encode_benchmark_run_batch(bench_runs).map_err(|e| e.to_string())?;
        self.send_muob_frames(&[(MSG_TYPE_ARROW_BENCH_RUNS, 0, payload)])
            .await
    }

    /// Streams all collected stage telemetry (`spans`, `logs`, `test_runs`, `bench_runs`) over a
    /// single `MUOB` connection to the local `MuObserve-collector`.
    ///
    /// Gracefully logs at `debug` level if the local collector is unreachable so worker CI stages
    /// never fail when running standalone.
    pub async fn emit_stage_bundle(
        &self,
        spans: &[OtelSpanEntry],
        logs: &[OtelLogEntry],
        test_runs: &[TestCaseRunEntry],
        bench_runs: &[BenchmarkRunEntry],
    ) -> Result<u64, String> {
        let mut frames = Vec::new();
        if !spans.is_empty() {
            let payload = encode_span_batch(spans).map_err(|e| e.to_string())?;
            frames.push((MSG_TYPE_ARROW_OTEL_SPANS, 0u32, payload));
        }
        if !logs.is_empty() {
            let high_prio = logs.iter().any(|l| l.severity_number >= 13);
            let flags = if high_prio { FLAG_HIGH_PRIORITY } else { 0 };
            let payload = encode_log_batch(logs).map_err(|e| e.to_string())?;
            frames.push((MSG_TYPE_ARROW_OTEL_LOGS, flags, payload));
        }
        if !test_runs.is_empty() {
            let payload = encode_test_run_batch(test_runs).map_err(|e| e.to_string())?;
            frames.push((MSG_TYPE_ARROW_TEST_RUNS, 0u32, payload));
        }
        if !bench_runs.is_empty() {
            let payload = encode_benchmark_run_batch(bench_runs).map_err(|e| e.to_string())?;
            frames.push((MSG_TYPE_ARROW_BENCH_RUNS, 0u32, payload));
        }

        if frames.is_empty() {
            return Ok(0);
        }

        match self.send_muob_frames(&frames).await {
            Ok(sent) => Ok(sent),
            Err(e) => {
                debug!(
                    collector = %self.collector_addr,
                    error = %e,
                    "Local MuObserve-collector unreachable; continuing gracefully"
                );
                Err(e)
            }
        }
    }

    async fn send_muob_frames(&self, frames: &[(u16, u32, Vec<u8>)]) -> Result<u64, String> {
        let addr_str = self
            .collector_addr
            .trim()
            .trim_start_matches("tcp://")
            .trim_start_matches("http://");
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("Invalid collector socket address '{addr_str}': {e}"))?;

        let mut stream = timeout(self.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| format!("Connect timeout to MuObserve-collector at {addr}"))?
            .map_err(|e| format!("Failed to connect to MuObserve-collector at {addr}: {e}"))?;

        let mut sent_count = 0u64;
        for (msg_type, flags, payload) in frames {
            let seq = EMITTER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let len = u32::try_from(payload.len()).map_err(|_| "Frame payload exceeds u32::MAX")?;
            let header = IpcHeader::new(*msg_type, len, *flags, seq);

            timeout(
                self.connect_timeout,
                write_frame(&mut stream, &header, payload),
            )
            .await
            .map_err(|_| "Write timeout to MuObserve-collector".to_string())?
            .map_err(|e| format!("Write error to MuObserve-collector: {e}"))?;

            // Read optional WalBatchAck if collector sends one back immediately
            let _ = timeout(Duration::from_millis(50), read_frame(&mut stream)).await;
            sent_count += 1;
        }

        Ok(sent_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IsolationMode;
    use action_runner_ipc::SandboxConstraintSpec;
    use action_runner_tools::ParsedTestCase;
    use std::path::PathBuf;
    use tokio::net::TcpListener;

    fn sample_worker_config(arch: &str, tags: &[&str]) -> WorkerConfig {
        WorkerConfig {
            controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
            cluster_token: "token".to_string(),
            node_id: "worker-riscv64".to_string(),
            arch: arch.to_string(),
            tags: tags.iter().map(|s| (*s).to_string()).collect(),
            max_slots: 1,
            workspace_dir: PathBuf::from("/tmp/ar-test"),
            toolchains: vec!["system".to_string()],
            use_btrfs: false,
            isolation_mode: IsolationMode::None,
        }
    }

    fn sample_dispatch_task(
        traceparent: Option<&str>,
        require_ai_thread: bool,
    ) -> StageDispatchTask {
        StageDispatchTask {
            job_id: "job-wave3-33".to_string(),
            stage_id: "stage-test-rvv".to_string(),
            stage_name: "RVV Kernel Test & Bench".to_string(),
            repo_name: "LMA1980/k3-kernels-rvv".to_string(),
            clone_url: "https://github.com/LMA1980/k3-kernels-rvv.git".to_string(),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
            command: String::new(),
            working_dir: None,
            env_vars: vec![("GITHUB_REF_NAME".to_string(), "feat/rvv-opt".to_string())],
            timeout_seconds: 60,
            threads: 6,
            toolchain: None,
            exclusive: true,
            max_concurrent_per_node: 1,
            tasks: vec![],
            sandbox_constraints: SandboxConstraintSpec {
                require_set_ai_thread: require_ai_thread,
                ..SandboxConstraintSpec::default()
            },
            traceparent: traceparent.map(ToString::to_string),
        }
    }

    #[test]
    fn test_child_span_parent_linkage_and_log_correlation() {
        let parent_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let cfg = sample_worker_config("riscv64", &["riscv64", "k3", "rvv"]);
        let task = sample_dispatch_task(Some(parent_tp), false);
        let hw = HardwareProfile::detect(&cfg, &task, None);
        assert_eq!(hw.arch, "riscv64");
        assert_eq!(hw.cpu_cluster, "X100");
        assert_eq!(hw.vlen_bits, 256);

        let mut session = StageTraceSession::from_dispatch_task(&task, &cfg.node_id, &hw);
        assert_eq!(session.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(
            session.parent_span_id_hex().as_deref(),
            Some("00f067aa0ba902b7")
        );
        assert_ne!(session.span_id_hex(), "00f067aa0ba902b7");
        assert_eq!(session.echo_traceparent, parent_tp);

        session.record_sandbox_setup_ns(1_500_000);
        session.record_tool_execution_ns(25_000_000);
        session.record_metric_parse_ns(500_000);

        let span = session.finish_span(0, None);
        assert_eq!(span.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.parent_span_id.as_deref(), Some("00f067aa0ba902b7"));
        assert_eq!(span.span_id, session.span_id_hex());
        assert_eq!(span.status_code, "OK");
        assert!(span.duration_ns >= 27_000_000);
        assert!(span.attributes_json.contains("\"cpu_cluster\":\"X100\""));
        assert!(span.attributes_json.contains("\"vlen_bits\":256"));

        let raw_logs = vec![
            LogEntry {
                timestamp_us: 1_700_000_000_000_000,
                job_id: task.job_id.clone(),
                stage_id: task.stage_id.clone(),
                node_id: cfg.node_id.clone(),
                stream: 1,
                line_no: 1,
                content: "running 2 tests with token=ghp_superSecret123456789".to_string(),
            },
            LogEntry {
                timestamp_us: 1_700_000_000_001_000,
                job_id: task.job_id.clone(),
                stage_id: task.stage_id.clone(),
                node_id: cfg.node_id.clone(),
                stream: 2,
                line_no: 2,
                content: "warning: benchmark variance high".to_string(),
            },
        ];

        let correlated_logs = session.correlate_logs(&raw_logs);
        assert_eq!(correlated_logs.len(), 2);
        for log in &correlated_logs {
            assert_eq!(log.trace_id, span.trace_id);
            assert_eq!(log.span_id, span.span_id);
            assert_eq!(log.job_id.as_deref(), Some("job-wave3-33"));
            assert_eq!(log.stage_id.as_deref(), Some("stage-test-rvv"));
        }
        assert!(correlated_logs[0].body.contains("[REDACTED:MUOBSERVE]"));
        assert_eq!(correlated_logs[0].severity_text, "INFO");
        assert_eq!(correlated_logs[1].severity_text, "WARN");
    }

    #[test]
    fn test_hardware_profile_tagging_x100_vs_a100_and_metrics_conversion() {
        let cfg_x100 = sample_worker_config("riscv64", &["riscv64", "x100"]);
        let task_x100 = sample_dispatch_task(None, false);
        let hw_x100 = HardwareProfile::detect(&cfg_x100, &task_x100, None);
        assert_eq!(hw_x100.cpu_cluster, "X100");
        assert_eq!(hw_x100.vlen_bits, 256);

        let cfg_a100 = sample_worker_config("riscv64", &["riscv64", "k3", "ime"]);
        let task_a100 = sample_dispatch_task(None, true);
        let hw_a100 = HardwareProfile::detect(&cfg_a100, &task_a100, None);
        assert_eq!(hw_a100.cpu_cluster, "A100");
        assert_eq!(hw_a100.vlen_bits, 1024);

        let metrics = ToolExecutionMetrics {
            duration_ms: 150,
            exit_code: 0,
            tests_passed: Some(1),
            tests_failed: Some(1),
            tests_ignored: Some(0),
            warnings_count: Some(0),
            coverage_pct: None,
            bench_ns_per_iter: Some(41_200.0),
            bench_mb_per_sec: Some(812.0),
            artifacts: vec![],
            test_cases: vec![
                ParsedTestCase {
                    test_name: "gemm::tests::test_vmadot_a100".to_string(),
                    status: TestCaseStatus::Passed,
                    duration_ms: Some(14),
                    failure_snippet: None,
                },
                ParsedTestCase {
                    test_name: "gemm::tests::test_bounds".to_string(),
                    status: TestCaseStatus::Failed,
                    duration_ms: Some(3),
                    failure_snippet: Some("index out of bounds".to_string()),
                },
            ],
            benchmarks: vec![
                ParsedBenchmark {
                    bench_name: "gemm_q8_0_a100".to_string(),
                    ns_per_iter: 41_200.0,
                    variance_ns: 180.0,
                    throughput_mb_s: Some(812.0),
                    arch: None,
                    cpu_cluster: None,
                    vlen_bits: None,
                },
                ParsedBenchmark {
                    bench_name: "gemm_q8_0_x100_override".to_string(),
                    ns_per_iter: 118_500.0,
                    variance_ns: 420.0,
                    throughput_mb_s: None,
                    arch: Some("riscv64".to_string()),
                    cpu_cluster: Some("X100".to_string()),
                    vlen_bits: Some(256),
                },
            ],
        };

        let ts_ns = 1_700_000_050_000_000_000;
        let test_entries = convert_metrics_to_test_entries(
            &metrics,
            &task_a100,
            "k3-kernels-rvv",
            "cargo-test",
            "worker-riscv64",
            &hw_a100,
            ts_ns,
        );
        assert_eq!(test_entries.len(), 2);
        assert_eq!(test_entries[0].repo_key, "lma1980/k3-kernels-rvv");
        assert_eq!(test_entries[0].branch_key, "feat/rvv-opt");
        assert_eq!(test_entries[0].status, "ok");
        assert_eq!(test_entries[1].status, "failed");
        assert_eq!(
            test_entries[1].failure_summary.as_deref(),
            Some("index out of bounds")
        );

        let bench_entries = convert_metrics_to_benchmark_entries(
            &metrics,
            &task_a100,
            "k3-kernels-rvv",
            "worker-riscv64",
            &hw_a100,
            ts_ns,
        );
        assert_eq!(bench_entries.len(), 2);
        assert_eq!(bench_entries[0].cpu_cluster, "A100");
        assert_eq!(bench_entries[0].vlen_bits, 1024);
        assert_eq!(bench_entries[0].throughput_unit.as_deref(), Some("MB/s"));
        assert_eq!(bench_entries[1].cpu_cluster, "X100");
        assert_eq!(bench_entries[1].vlen_bits, 256);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_telemetry_emitter_streams_muob_batches_to_mock_collector() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("local addr");

        let server_handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut received = Vec::new();
            while let Ok((hdr, payload)) = read_frame(&mut stream).await {
                received.push((hdr, payload));
            }
            received
        });

        let cfg = sample_worker_config("riscv64", &["riscv64", "k3"]);
        let task = sample_dispatch_task(
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            true,
        );
        let hw = HardwareProfile::detect(&cfg, &task, None);
        let session = StageTraceSession::from_dispatch_task(&task, &cfg.node_id, &hw);
        let span = session.finish_span(0, None);
        let logs = session.correlate_logs(&[LogEntry {
            timestamp_us: 1_700_000_000_000_000,
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            node_id: cfg.node_id.clone(),
            stream: 1,
            line_no: 1,
            content: "testgemm ... ok".to_string(),
        }]);

        let metrics = ToolExecutionMetrics {
            test_cases: vec![ParsedTestCase {
                test_name: "testgemm".to_string(),
                status: TestCaseStatus::Passed,
                duration_ms: Some(5),
                failure_snippet: None,
            }],
            benchmarks: vec![ParsedBenchmark {
                bench_name: "bench_vmadot".to_string(),
                ns_per_iter: 40_000.0,
                variance_ns: 100.0,
                throughput_mb_s: Some(1024.0),
                arch: None,
                cpu_cluster: None,
                vlen_bits: None,
            }],
            ..ToolExecutionMetrics::default()
        };
        let test_runs = convert_metrics_to_test_entries(
            &metrics,
            &task,
            "k3-kernels-rvv",
            "cargo-test",
            &cfg.node_id,
            &hw,
            1_700_000_000_000_000_000,
        );
        let bench_runs = convert_metrics_to_benchmark_entries(
            &metrics,
            &task,
            "k3-kernels-rvv",
            &cfg.node_id,
            &hw,
            1_700_000_000_000_000_000,
        );

        let emitter = TelemetryEmitter::new(addr.to_string());
        let count = emitter
            .emit_stage_bundle(std::slice::from_ref(&span), &logs, &test_runs, &bench_runs)
            .await
            .expect("emit bundle");
        assert_eq!(count, 4);

        let frames = server_handle.await.expect("join server");
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].0.msg_type, MSG_TYPE_ARROW_OTEL_SPANS);
        assert_eq!(frames[1].0.msg_type, MSG_TYPE_ARROW_OTEL_LOGS);
        assert_eq!(frames[2].0.msg_type, MSG_TYPE_ARROW_TEST_RUNS);
        assert_eq!(frames[3].0.msg_type, MSG_TYPE_ARROW_BENCH_RUNS);

        let decoded_spans = decode_span_batch(&frames[0].1).expect("decode spans");
        assert_eq!(decoded_spans, vec![span]);

        let decoded_logs = decode_log_batch(&frames[1].1).expect("decode logs");
        assert_eq!(decoded_logs, logs);

        let decoded_tests = decode_test_run_batch(&frames[2].1).expect("decode tests");
        assert_eq!(decoded_tests, test_runs);

        let decoded_benches = decode_benchmark_run_batch(&frames[3].1).expect("decode benches");
        assert_eq!(decoded_benches, bench_runs);
    }
}
