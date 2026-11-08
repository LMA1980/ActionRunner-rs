# Spec: Single-Pass Real-Time Line Streaming & Correlated Log Splitting

## Location: `specs/single-pass-line-streaming.md`

### 1. Overview

`mu-ci-worker` streams stage execution stdout and stderr lines **once** in real time over the WebSocket cluster tunnel (`MSG_TYPE_ARROW_LOG`) without retaining an unbounded full-stage `Vec<LogEntry>` copy in memory or re-transmitting the entire stage log a second time at stage completion.

---

### 2. Single-Pass Streaming in `StageRunner::run_v3_typed_tasks` (`src/stage_runner.rs`)

1. **Real-Time 16-Line Arrow Batches (`log_tx`)**:
   - As each tool process produces stdout (`stream = 1`) and stderr (`stream = 2`) lines, `StageRunner` assigns a monotonically increasing `line_no: u32` (`line_counter.fetch_add(1, Ordering::Relaxed)`) and flushes `LogEntry` records in 16-line `MSG_TYPE_ARROW_LOG` batches over `log_tx` to `mu-ci-controller`.
2. **Per-Invocation Metric Parsing Buffer (`stdout_bytes` / `stderr_bytes`)**:
   - Raw byte buffers (`stdout_bytes`, `stderr_bytes`) are scoped strictly to a single tool invocation for `ToolAdapter::parse_metrics(&stdout_bytes, &stderr_bytes, exit_code)` (`test_case_runs` and `benchmark_runs`) and dropped immediately after each tool invocation completes.
3. **Zero Duplicate Stage-Completion Log Re-Emission**:
   - At stage completion, `telemetry_emitter.emit_stage_bundle(std::slice::from_ref(&child_span), &[], &all_test_runs, &all_bench_runs)` emits the W3C child span (`MSG_TYPE_ARROW_OTEL_SPANS`), test case runs (`MSG_TYPE_ARROW_TEST_RUNS`), and benchmark runs (`MSG_TYPE_ARROW_BENCH_RUNS`) without re-sending stage log lines (`&[]`).

---

### 3. Single-Line Normalization in `StageTraceSession::correlate_logs` (`src/telemetry_emitter.rs`)

When `StageTraceSession::correlate_logs` converts `LogEntry` slices into W3C-correlated `OtelLogEntry` records:
- Any `LogEntry.content` containing embedded newlines (`\n`) is split into individual single-line `OtelLogEntry` records (trimming trailing `\r` and dropping empty trailing newlines).
- Each split record receives a deterministic monotonically increasing `"line_no"` (`base_line_no + idx`) and `"stream"` (`"stdout"` or `"stderr"`) in `attributes_json` and is scrubbed in-place via `DeterministicSecretRedactor`.
