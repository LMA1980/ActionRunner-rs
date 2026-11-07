---
name: mu-ci-worker
description: "Configure and debug the MuCI distributed CI worker daemon (mu-ci-worker), single-slot stage execution, and isolated service-account toolchains."
---

# `mu-ci-worker` Worker Daemon & Execution Invariants

Use these invariants when developing or deploying the `mu-ci-worker` worker agent:

## 1. Single-Slot Execution vs. Thread Budget (`max_slots = 1`)

- Every `mu-ci-worker` worker node enforces `max_slots = 1` (at most one active CI stage process per node at a time).
- Bound intra-stage parallelism strictly via thread budget environment variables (`CARGO_BUILD_JOBS`, `RAYON_NUM_THREADS`, `RUST_TEST_THREADS`). Never increase `ACTION_RUNNER_SLOTS` or `MU_CI_SLOTS` when adjusting CPU/thread allocation.

## 2. Isolated Service Account Toolchains (`action-runner`)

- Verify tool availability directly under the `action-runner` service account (`ProtectHome=read-only`), including:
  - `RUSTUP_HOME` (`/var/lib/action-runner/.rustup` with `wasm32-unknown-unknown` target and `nightly` with `miri` and `rust-src`)
  - `CARGO_HOME` subcommands (`cargo-audit`, `cargo-llvm-cov`, and `advisory-db`)
  - Python user site-packages in `/var/lib/action-runner/.local` (`mdformat-gfm`)
- Ensure parent configuration directories (`/etc/action-runner`) are owned/traversable by `action-runner:action-runner` (`0700`), and install updated binaries atomically via `sudo install -m 755` to prevent `ETXTBSY`.

## 3. Shared `sccache` Compiler Cache Integration

- **Auto-Detection & Capability Tagging**: `WorkerConfig::detect_sccache_bin()` probes `ACTION_RUNNER_SCCACHE_BIN` (or `off`/`false`/`0`/`none` to disable), `/usr/local/bin/sccache`, `/usr/bin/sccache`, `$CARGO_HOME/bin/sccache`, and `$HOME/.cargo/bin/sccache`. When present, `"sccache"` is automatically appended to the worker's advertised `tags`.
- **S3 / Storage Environment (`/etc/action-runner/sccache.env`)**: `WorkerConfig::load_sccache_env()` reads `/etc/action-runner/sccache.env` (or `ACTION_RUNNER_SCCACHE_ENV_FILE`) for `SCCACHE_*` and `AWS_*` settings while filtering out `RUSTC_WRAPPER` / `CARGO_BUILD_RUSTC_WRAPPER`.
- **Selective Wrapper Injection & Self-Healing (`StageRunner`)**:
  - Only eligible compiler tools (`cargo-build`, `cargo-check`, `cargo-test`, `cargo-clippy`, `cargo-bench`, `trunk`) receive `RUSTC_WRAPPER=<sccache_bin>`, `CARGO_INCREMENTAL=0`, and `SCCACHE_IDLE_TIMEOUT=0`.
  - Incompatible/non-compiler tools (`cargo-fmt`, `cargo-audit`, `mdformat`, `cargo-llvm-cov`, `cargo-miri`) have `RUSTC_WRAPPER` stripped.
  - `StageRunner::ensure_sccache_server_healthy` enforces a 5s bounded `--show-stats` check, automatically restarts the local daemon if it was running in `Local disk` mode despite `SCCACHE_BUCKET` configuration, falls back to uncached compilation in `SccacheMode::Try`, and fails closed (`exit 126`) in `SccacheMode::Mandatory`.
