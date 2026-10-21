# ActionRunner-rs

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/cards/LMA1980/ActionRunner-rs/card.svg">
    <img src="https://hook.lma1980.duckdns.org/cards/LMA1980/ActionRunner-rs/card.svg" alt="ActionRunner-rs CI Factory Scorecard" width="520"/>
  </a>
</p>

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/status.svg"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/status.svg" alt="CI Status"/></a>
  <a href="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/score.svg"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/score.svg" alt="Quality Score"/></a>
  <a href="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/coverage.svg"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/coverage.svg" alt="Coverage"/></a>
  <a href="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/benchmark.svg"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/ActionRunner-rs/benchmark.svg" alt="Benchmark"/></a>
</p>

---

Lightweight, high-performance GitHub Webhook & CI execution sidecar daemon written in Rust for native **SpacemiT Key Stone K3** (8× X100 RISC-V @ 1.6GHz) hardware.

`ActionRunner-rs` **eats its own dog food** — whenever code is pushed to this repository, the sidecar clones or syncs the local workspace, compiles natively, runs formatting/clippy/audit/coverage, computes its quality score, and updates its own badges and scorecard in real time.

---

## Key Capabilities

- **Native RISC-V CI Execution**: Runs directly on bare-metal RISC-V cores with zero virtualization overhead.
- **Coalescing FIFO Webhook Queue**: Serializes builds (`concurrency = 1`) for resource-constrained K3 hosts, persists queue state to `jobs_index.json`, and automatically supersedes older queued commits when newer pushes arrive for the same `(repository, ref, event)`.
- **HMAC-SHA256 Signature Verification**: Validates GitHub's `X-Hub-Signature-256` payload signatures in constant time.
- **Automated Local Workspace Sync**: Automatically updates branches, clones submodules (`git submodule update --init --recursive --force`), cleans untracked artifacts, and executes YAML workflows.
- **Factory Pipeline Quality Scoring**: Evaluates tests pass-ratio, line coverage (`cargo llvm-cov`), security advisories (`cargo audit`), and linter hygiene (`cargo clippy`) to calculate an overall quality score (0–100) and letter grade (`A+` to `F`).
- **Individual Submodule Scoring & Badges**: Automatically discovers Git submodules from `.gitmodules`, tracks individual test counts, coverage, benchmarks, and hygiene, and generates dedicated badges and multi-row scorecard pill rows.
- **High-DPI SVG Badges & Scorecards**: Dynamic badges (`score.svg`, `status.svg`, `coverage.svg`, `benchmark.svg`) and rich multi-metric cards (`card.svg`) rendered on-the-fly.
- **Ultra-Low Resource Footprint**: Consumes < 15 MB RAM under idle operation.

---

## Factory Pipeline Workflows (`.actionrunner/`)

Workflows are defined in YAML and triggered based on GitHub webhook events:

| Workflow File | Trigger Event | Description |
|---|---|---|
| `.actionrunner/on-push.yml` | `push` | Formatting, Clippy `-D warnings`, Security audit, Test suite & `llvm-cov`, and release compilation |
| `.actionrunner/on-pullrequest.yml` | `pull_request` | PR branch validation and regression verification |
| `.actionrunner/on-tag.yml` | `push` (tags) | Production release binary validation |
| `.actionrunner/on-merge.yml` | Post-merge events | Main branch post-merge validation |

---

## Endpoints Reference

### 1. Ingress, Queue & Inspection API
- `GET /health` (`/api/health`) — Service healthcheck, queue status (`idle` / `busy`), queue depth, and architecture details (`riscv64`).
- `POST /webhook` — GitHub webhook ingress endpoint (returns `202 Accepted` with `job_id`, `queue_position`, and `superseded_jobs`).
- `GET /queue` (`/api/queue`) — Live queue monitor snapshot (`active_job`, `queued_jobs` with FIFO position & wait time, `stats`, and `recent_jobs`).
- `GET /jobs` (`/api/jobs`) — List queued and historical jobs with metrics (supports `?repo=`, `?status=`, and `?limit=` filters).
- `GET /jobs/{id}` (`/api/jobs/{id}`) — Query specific job status, queue position, elapsed/duration time, and metrics (including submodules).
- `GET /jobs/{id}/log` (`/api/jobs/{id}/log`) — View full stdout/stderr build and test logs.

### 2. Badges & Factory Scorecards
- `GET /badges/{owner}/{repo}/status.svg` — Build status badge (`k3-ci | passing`).
- `GET /badges/{owner}/{repo}/score.svg` — Quality score badge (`k3 score | 100% A+`).
- `GET /badges/{owner}/{repo}/coverage.svg` — Code coverage badge (`coverage | 87.9%`).
- `GET /badges/{owner}/{repo}/benchmark.svg` — Benchmark badge (`cargo bench | 49.0 ns/iter`).
- `GET /cards/{owner}/{repo}/card.svg` — Full factory pipeline scorecard SVG.

### 3. Submodule Badges & Cards
- `GET /badges/{owner}/{submodule}/score.svg` — Standalone submodule quality score badge.
- `GET /badges/{owner}/{repo}/submodules/{submodule}/score.svg` — Hierarchically scoped submodule score badge.
- `GET /cards/{owner}/{submodule}/card.svg` — Dedicated submodule scorecard SVG.

---

## Configuration

Environment variables configured in `/etc/systemd/system/action-runner.service`:

| Variable | Default | Purpose |
|---|---|---|
| `ACTION_RUNNER_BIND` | `127.0.0.1:8000` | Local listen socket address behind Nginx reverse proxy |
| `ACTION_RUNNER_WORKSPACE` | `/home/lma1980/repository/ci-workspace` | Base directory for repositories and build logs |
| `ACTION_RUNNER_ALLOWED_REPOS` | `LMA1980/*` | Whitelisted repositories permitted to execute CI steps |
| `GITHUB_WEBHOOK_SECRET` | `/home/lma1980/.github_webhook_secret` | HMAC-SHA256 webhook secret token |
| `ACTION_RUNNER_TIMEOUT_SECS` | `1800` | Maximum job execution timeout (seconds) |

---

## License

Licensed under the Apache License, Version 2.0.
