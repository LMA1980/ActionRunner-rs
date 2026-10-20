# ActionRunner-rs

Lightweight, high-performance GitHub Webhook & CI execution sidecar daemon written in Rust for the SpacemiT K3 RISC-V platform.

## Overview
`ActionRunner-rs` operates as an internal daemon behind an Nginx reverse proxy, providing:
- **HMAC-SHA256 Signature Authentication**: Validates GitHub's `X-Hub-Signature-256` in constant time before processing.
- **Ultra-Low Resource Footprint**: Consumes <15 MB RAM on the physical RISC-V board.
- **Native Architecture**: Compiled natively on `riscv64` with support for native toolchains (GCC 17, LLVM 24).

## Configuration
- `ACTION_RUNNER_BIND`: Listen address (defaults to `127.0.0.1:8000`).
- `GITHUB_WEBHOOK_SECRET`: Secret token configured in the GitHub repository webhook settings. Can also be stored in `/home/lma1980/.github_webhook_secret`.

## Endpoints
- `GET /health` or `GET /`: Healthcheck endpoint.
- `POST /webhook`: GitHub Webhook ingress endpoint.
