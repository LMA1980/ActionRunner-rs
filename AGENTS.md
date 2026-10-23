# ActionRunner-rs Agent Guidelines

## 1. Service Account Isolation & Daemon Deployment (`pey-wail-deu`)

- **Verify Toolchains Under Target Service Account**: When configuring or debugging `action-runner` running under its isolated service account (`User=action-runner` with `ProtectSystem=strict` and `ProtectHome=read-only`), always verify tool availability directly as that user (`sudo -u action-runner ...`), including:
  - `RUSTUP_HOME` (`/var/lib/action-runner/.rustup` with `toolchains/` and `wasm32-unknown-unknown` target)
  - `CARGO_HOME` subcommands (`cargo-audit`, `cargo-llvm-cov`, and `advisory-db`)
  - Python user site-packages in `/var/lib/action-runner/.local` (specifically `mdformat-gfm` for GFM table formatting)
  - SSH `known_hosts` and deploy key permissions in `/var/lib/action-runner/.ssh`
- **Directory Traversal Permissions**: Ensure parent configuration directories (such as `/etc/action-runner`) are owned by or traversable by the service account (`chown -R action-runner:action-runner /etc/action-runner && chmod 700 /etc/action-runner`), not `0700 root:root`.
- **Secret Path vs. Literal Value Resolution**: Environment variables that accept either a file path or a literal secret (e.g., `GITHUB_WEBHOOK_SECRET`) must read and trim the file when given a path, and fail closed (`None`) if the path cannot be read—never fall back to using the file path string itself as the secret token.
- **Atomic Binary Installation**: Use `sudo install -m 755 target/release/action-runner /usr/local/bin/action-runner` instead of `cp` when updating the running systemd service binary to avoid `Text file busy` (`ETXTBSY`) errors.
