---
name: action-runner-worker
description: "Configure and debug the ActionRunner distributed CI worker daemon, single-slot stage execution, and isolated service-account toolchains."
---

# `action-runner` Worker Daemon & Execution Invariants

Use these invariants when developing or deploying the `action-runner` worker agent:

## 1. Single-Slot Execution vs. Thread Budget (`max_slots = 1`)

- Every `action-runner` worker node enforces `max_slots = 1` (at most one active CI stage process per node at a time).
- Bound intra-stage parallelism strictly via thread budget environment variables (`CARGO_BUILD_JOBS`, `RAYON_NUM_THREADS`, `RUST_TEST_THREADS`). Never increase `ACTION_RUNNER_SLOTS` when adjusting CPU/thread allocation.

## 2. Isolated Service Account Toolchains (`action-runner`)

- Verify tool availability directly under the `action-runner` service account (`ProtectHome=read-only`), including:
  - `RUSTUP_HOME` (`/var/lib/action-runner/.rustup` with `wasm32-unknown-unknown` target and `nightly` with `miri` and `rust-src`)
  - `CARGO_HOME` subcommands (`cargo-audit`, `cargo-llvm-cov`, and `advisory-db`)
  - Python user site-packages in `/var/lib/action-runner/.local` (`mdformat-gfm`)
- Ensure parent configuration directories (`/etc/action-runner`) are owned/traversable by `action-runner:action-runner` (`0700`), and install updated binaries atomically via `sudo install -m 755` to prevent `ETXTBSY`.
