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

## Security Model & Sandboxing

`ActionRunner-rs` implements strict multi-layer defense against rogue code execution on public endpoints:

1. **Service Account Isolation**: Runs under a dedicated, unprivileged system account (`action-runner`). User `lma1980` can sudo as `action-runner` for diagnostics (`sudo -u action-runner -i`).
2. **Anti-RCE Pull Request Guards**:
   - Webhooks from external repository forks are automatically rejected (`allow_fork_prs = false`).
   - Pull request authors must hold trusted repository associations (`OWNER`, `MEMBER`, or `COLLABORATOR`).
3. **CGroup v2 & Namespace Sandboxing**:
   - Process sandboxing via `systemd-run` or systemd service security directives (`NoNewPrivileges=yes`, `ProtectSystem=strict`, `ProtectHome=read-only`, `PrivateTmp=yes`).
   - CGroup v2 resource envelopes enforce memory ceiling (`MemoryMax=6G`), CPU quota (`CPUQuota=600%`), and task limits (`TasksMax=1024`).
4. **Input Sanitization & Injection Defense**:
   - Strict repository name validation preventing directory traversal.
   - Hexadecimal-only commit SHA validation and git argument delimiters (`--`).
   - Blacklist filter for dangerous shell environment variables (`LD_PRELOAD`, `BASH_ENV`, etc.).
5. **Fail-Closed HMAC Authentication**:
   - Rejects unauthenticated webhooks on public endpoints; fails closed if secret token is unconfigured.

---

## Configuration

Environment variables configured in `/etc/action-runner/worker.env` (loaded by `action-runner-worker.service`):

| Variable | Default | Purpose |
|---|---|---|
| `ACTION_RUNNER_CONTROLLER_URL` | `ws://127.0.0.1:8000/api/cluster/tunnel` | WebSocket tunnel endpoint on `ActionRunner-controller` |
| `ACTION_RUNNER_CLUSTER_TOKEN` | `/etc/action-runner/cluster_token` | Shared cluster authentication token (file path or literal; fails closed if path unreadable) |
| `ACTION_RUNNER_NODE_ID` | Hostname | Unique worker node identifier (e.g. `node-a`, `worker-riscv64`) |
| `ACTION_RUNNER_TAGS` | Arch-dependent defaults | Comma-separated worker capability tags (`sccache` appended automatically when `sccache` binary is detected) |
| `ACTION_RUNNER_MAX_SLOTS` | `1` | Maximum concurrent stage slots per worker node (`1` recommended; scale intra-stage parallelism via `CARGO_BUILD_JOBS`) |
| `ACTION_RUNNER_WORKSPACE` | `/var/lib/action-runner/workspace` | Base directory for stage workspaces and git mirrors |
| `ACTION_RUNNER_SCCACHE_BIN` | Auto-detected | Optional explicit path to `sccache` binary, or `off`/`false`/`0`/`none` to disable `sccache` on the worker |
| `ACTION_RUNNER_SCCACHE_ENV_FILE` | `/etc/action-runner/sccache.env` | Path to optional S3/storage environment file injected into `sccache` |

---

## Operator Guide: Shared `sccache` Compiler Cache Integration

`action-runner` natively integrates with [`sccache`](https://github.com/mozilla/sccache) (local disk or S3-compatible object storage such as [Garage](https://garagehq.deuxfleurs.fr/), MinIO, or AWS S3) so distributed workers share compiled Rust and WebAssembly artifacts across CI runs.

### 1. Install the `sccache` Binary on Each Worker Node

Install `sccache` into one of the standard discovery paths probed by `WorkerConfig::detect_sccache_bin()`:
1. `ACTION_RUNNER_SCCACHE_BIN` (if set to a valid file path; set to `off`, `false`, `0`, or `none` to disable `sccache` on a specific host)
2. `/usr/local/bin/sccache` *(recommended system-wide location)*
3. `/usr/bin/sccache`
4. `$CARGO_HOME/bin/sccache` or `$HOME/.cargo/bin/sccache`

```bash
sudo install -m 755 sccache /usr/local/bin/sccache
/usr/local/bin/sccache --version
```

When `detect_sccache_bin()` locates a valid binary at startup, the worker **automatically appends `"sccache"`** to its advertised `tags` during controller registration (`WorkerRegistration`), making the node eligible for stages configured with `sccache: mandatory`.

### 2. Configure Shared S3 / Garage Credentials (`/etc/action-runner/sccache.env`)

To share cache artifacts across multiple worker nodes over an S3-compatible backend (or to customize local disk cache settings), create `/etc/action-runner/sccache.env` owned by the `action-runner` service account:

```bash
sudo install -d -m 700 -o action-runner -g action-runner /etc/action-runner
sudo tee /etc/action-runner/sccache.env >/dev/null <<'EOF'
SCCACHE_BUCKET=sccache
SCCACHE_REGION=us-east-1
SCCACHE_ENDPOINT=http://192.0.2.10:3900
SCCACHE_S3_KEY_PREFIX=v1/
SCCACHE_IDLE_TIMEOUT=0
AWS_ACCESS_KEY_ID=GK0123456789abcdef01234567
AWS_SECRET_ACCESS_KEY=replace-with-your-s3-secret-key
EOF
sudo chown action-runner:action-runner /etc/action-runner/sccache.env
sudo chmod 600 /etc/action-runner/sccache.env
```

`WorkerConfig::load_sccache_env()` reads `/etc/action-runner/sccache.env` (or the path in `ACTION_RUNNER_SCCACHE_ENV_FILE`) and merges any explicit environment overrides for:
- `SCCACHE_BUCKET`, `SCCACHE_REGION`, `SCCACHE_ENDPOINT`, `SCCACHE_S3_USE_SSL`, `SCCACHE_S3_KEY_PREFIX`
- `SCCACHE_DIR`, `SCCACHE_CACHE_SIZE`, `SCCACHE_SERVER_PORT`, `SCCACHE_SERVER_UDS`, `SCCACHE_IDLE_TIMEOUT`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

> **Note**: Do **not** hardcode `RUSTC_WRAPPER` inside `/etc/action-runner/sccache.env` or `/etc/action-runner/worker.env`; `StageRunner` automatically filters out `RUSTC_WRAPPER` from `sccache.env` and injects it selectively per task so incompatible tools (`cargo-miri`, `cargo-llvm-cov`) are never broken.

### 3. Ensure `MuIsolation` (`security.yml`) Permits `sccache` Execution

When repositories run under `MuIsolation` (`backend: Kernel` with Landlock LSM), ensure the directory containing `sccache` (`/usr/local/bin` or `/usr/bin`) is included in `exec_paths` and `/usr` in `read_paths` within `.actionrunner/security.yml`:

```yaml
default_profile: "default_ci"
profiles:
  default_ci:
    backend: Kernel
    read_paths: ["/usr", "/bin", "/lib", "/etc/ssl", "/var/lib/action-runner"]
    write_paths: ["/var/lib/action-runner", "/tmp"]
    exec_paths:
      - /usr/bin
      - /usr/local/bin
      - /bin
      - /var/lib/action-runner/.cargo/bin
      - /var/lib/action-runner/.rustup
      - /var/lib/action-runner/.local/bin
```

### 4. Runtime Behavior, Tool Filtering & Self-Healing

For each dispatched task in a stage, `StageRunner` resolves the effective `SccacheMode` (`Off`, `Try` [default], or `Mandatory`) and applies the following invariants:

- **Tool Eligibility Filtering**:
  - **Eligible compiler tools** (`cargo-build`, `cargo-check`, `cargo-test`, `cargo-clippy`, `cargo-bench`, `trunk`) receive `RUSTC_WRAPPER=<sccache_bin>`, `CARGO_INCREMENTAL=0`, `SCCACHE_IDLE_TIMEOUT=0`, and all S3/cache variables from `sccache.env`.
  - **Non-compiler & incompatible tools** (`cargo-fmt`, `cargo-audit`, `mdformat`, `cargo-llvm-cov`, `cargo-miri`) automatically have `RUSTC_WRAPPER` and `CARGO_BUILD_RUSTC_WRAPPER` removed from their process environment so Miri's own wrapper and LLVM coverage instrumentation operate cleanly.
- **Pre-Flight Health Check & Local-Disk Hijack Recovery (`ensure_sccache_server_healthy`)**:
  - Before spawning an eligible compiler tool, the worker runs `sccache --show-stats` under a bounded **5-second timeout** (auto-starting the local `sccache` daemon with `sccache.env` credentials if not yet running).
  - If `SCCACHE_BUCKET` is configured but the local `sccache` daemon reports `Cache location: Local disk` (e.g., if a user previously invoked `sccache` in a shell without `sccache.env`), `StageRunner` automatically stops the stale daemon (`sccache --stop-server`) and restarts it with the S3 environment.
  - If the daemon is transiently wedged, `StageRunner` executes a single `--stop-server` + `--show-stats` recovery cycle.
- **Failure Policy (`Try` vs. `Mandatory`)**:
  - **`SccacheMode::Try` (default)**: If `sccache` is not installed or fails the health check (e.g. temporary S3 outage), the worker logs a warning, strips `RUSTC_WRAPPER`, and **falls back cleanly to uncached `rustc` compilation** so CI pipelines remain operational.
  - **`SccacheMode::Mandatory`**: If `sccache` is missing or fails the health check after retry, the stage **fails closed immediately (`exit_code = 126`)** with a diagnostic error.

### 5. Verifying Worker `sccache` Status

Run the following command under the `action-runner` service account to verify S3 connectivity and cache hit rates on a worker node:

```bash
sudo -u action-runner bash -c 'set -a && source /etc/action-runner/sccache.env && set +a && /usr/local/bin/sccache --show-stats'
```

---

## Service Account Provisioning

Automated provisioning script in [`deploy/setup-service-account.sh`](deploy/setup-service-account.sh):
```bash
sudo ./deploy/setup-service-account.sh
```

Diagnostics for `lma1980`:
```bash
# Switch to action-runner service account for log or workspace inspection:
sudo -u action-runner -i
```

---

## License

Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See [LICENSE](LICENSE) for details.
