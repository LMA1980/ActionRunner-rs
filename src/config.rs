use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub controller_url: String,
    pub cluster_token: String,
    pub node_id: String,
    pub arch: String,
    pub tags: Vec<String>,
    pub max_slots: u32,
    pub workspace_dir: PathBuf,
    pub toolchains: Vec<String>,
    pub use_btrfs: bool,
    pub isolation_mode: IsolationMode,
    pub rust_semver: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationMode {
    None,
    SystemdRun,
}

impl WorkerConfig {
    pub fn load() -> Self {
        let controller_url = std::env::var("ACTION_RUNNER_CONTROLLER_URL")
            .unwrap_or_else(|_| "ws://127.0.0.1:8000/api/cluster/tunnel".to_string());

        let cluster_token = Self::resolve_secret("ACTION_RUNNER_CLUSTER_TOKEN")
            .unwrap_or_else(|| "action-runner-default-cluster-token".to_string());

        let node_id = std::env::var("ACTION_RUNNER_NODE_ID").unwrap_or_else(|_| hostname());

        let arch = std::env::var("ACTION_RUNNER_ARCH")
            .unwrap_or_else(|_| std::env::consts::ARCH.to_string());

        let default_tags = match arch.as_str() {
            "riscv64" => "riscv64,native,k3,rvv,ime,bench",
            _ => "x86_64,fmt,clippy,audit,cross,heavy",
        };

        let mut tags: Vec<String> = std::env::var("ACTION_RUNNER_TAGS")
            .unwrap_or_else(|_| default_tags.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if Self::detect_sccache_bin().is_some() && !tags.iter().any(|t| t == "sccache") {
            tags.push("sccache".to_string());
        }

        let max_slots = std::env::var("ACTION_RUNNER_MAX_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(1)
            .max(1);

        let workspace_dir = std::env::var("ACTION_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/action-runner/workspace"));

        let mut toolchains = vec!["system".to_string()];
        if Path::new("/opt/llvm-24-spacemit-x100/bin/clang").exists()
            || Path::new("/home/lma1980/opt/llvm-24/bin/clang").exists()
            || Path::new("/opt/cross-llvm-24/bin/clang").exists()
        {
            toolchains.push("llvm-24".to_string());
        }
        if Path::new("/opt/gcc-17-spacemit-x100/bin/riscv64-unknown-linux-gnu-gcc").exists()
            || Path::new("/opt/gcc-17-spacemit-x100/bin/gcc").exists()
            || Path::new("/home/lma1980/opt/gcc-17-spacemit-x100/bin/riscv64-unknown-linux-gnu-gcc")
                .exists()
            || Path::new("/home/lma1980/opt/gcc-17-spacemit-x100/bin/gcc").exists()
            || Path::new("/opt/cross-gcc-17/bin/riscv64-unknown-linux-gnu-gcc").exists()
        {
            toolchains.push("gcc-17-spacemit-x100".to_string());
        } else if Path::new("/home/lma1980/opt/gcc-17/bin/gcc").exists() {
            toolchains.push("gcc-17".to_string());
        }

        let use_btrfs = std::env::var("ACTION_RUNNER_USE_BTRFS")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or_else(|_| Self::detect_btrfs(&workspace_dir));

        let isolation_mode = match std::env::var("ACTION_RUNNER_ISOLATION")
            .unwrap_or_else(|_| "none".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "systemd-run" => IsolationMode::SystemdRun,
            _ => IsolationMode::None,
        };

        let rust_semver = Self::probe_rustc_semver();

        let cfg = Self {
            controller_url,
            cluster_token,
            node_id,
            arch,
            tags,
            max_slots,
            workspace_dir,
            toolchains,
            use_btrfs,
            isolation_mode,
            rust_semver,
        };

        info!(
            node_id = %cfg.node_id,
            arch = %cfg.arch,
            max_slots = cfg.max_slots,
            max_threads = cfg.max_threads(),
            tags = ?cfg.tags,
            toolchains = ?cfg.toolchains,
            rust_semver = ?cfg.rust_semver,
            btrfs = cfg.use_btrfs,
            "Loaded ActionRunner worker configuration"
        );

        cfg
    }

    pub fn max_threads(&self) -> u32 {
        let default_threads = if self.node_id.eq_ignore_ascii_case("node-a")
            || self.tags.iter().any(|t| t.eq_ignore_ascii_case("n100"))
        {
            2
        } else {
            6
        };

        std::env::var("ACTION_RUNNER_THREADS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                std::env::var("CARGO_BUILD_JOBS")
                    .ok()
                    .and_then(|s| s.parse::<u32>().ok())
            })
            .unwrap_or(default_threads)
            .max(1)
    }

    fn detect_btrfs(path: &Path) -> bool {
        // Quick check via statvfs/df if path or parent is btrfs
        if let Ok(output) = std::process::Command::new("findmnt")
            .arg("-n")
            .arg("-o")
            .arg("FSTYPE")
            .arg("-T")
            .arg(path)
            .output()
        {
            if let Ok(fs) = std::str::from_utf8(&output.stdout) {
                return fs.trim().eq_ignore_ascii_case("btrfs");
            }
        }
        false
    }

    pub fn detect_sccache_bin() -> Option<PathBuf> {
        if let Ok(val) = std::env::var("ACTION_RUNNER_SCCACHE_BIN") {
            let trimmed = val.trim();
            if trimmed.eq_ignore_ascii_case("false")
                || trimmed.eq_ignore_ascii_case("off")
                || trimmed.eq_ignore_ascii_case("none")
                || trimmed == "0"
            {
                return None;
            }
            if !trimmed.is_empty() {
                let p = PathBuf::from(trimmed);
                if p.is_file() {
                    return Some(p);
                }
                return None;
            }
        }

        for candidate in ["/usr/local/bin/sccache", "/usr/bin/sccache"] {
            let p = Path::new(candidate);
            if p.is_file() {
                return Some(p.to_path_buf());
            }
        }

        if let Ok(home) = std::env::var("CARGO_HOME") {
            let p = Path::new(&home).join("bin/sccache");
            if p.is_file() {
                return Some(p);
            }
        }
        if let Ok(home) = std::env::var("HOME") {
            let p = Path::new(&home).join(".cargo/bin/sccache");
            if p.is_file() {
                return Some(p);
            }
        }
        None
    }

    pub fn load_sccache_env() -> Vec<(String, String)> {
        let mut envs = Vec::new();
        let env_file = std::env::var("ACTION_RUNNER_SCCACHE_ENV_FILE")
            .unwrap_or_else(|_| "/etc/action-runner/sccache.env".to_string());
        if let Ok(content) = std::fs::read_to_string(&env_file) {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = trimmed.split_once('=') {
                    let key = k.trim().trim_start_matches("export ").trim().to_string();
                    let val = v.trim().trim_matches('"').trim_matches('\'').to_string();
                    if !key.is_empty()
                        && !key.eq_ignore_ascii_case("RUSTC_WRAPPER")
                        && !key.eq_ignore_ascii_case("CARGO_BUILD_RUSTC_WRAPPER")
                    {
                        envs.push((key, val));
                    }
                }
            }
        }
        for key in [
            "SCCACHE_BUCKET",
            "SCCACHE_REGION",
            "SCCACHE_ENDPOINT",
            "SCCACHE_S3_USE_SSL",
            "SCCACHE_S3_KEY_PREFIX",
            "SCCACHE_DIR",
            "SCCACHE_CACHE_SIZE",
            "SCCACHE_SERVER_PORT",
            "SCCACHE_SERVER_UDS",
            "SCCACHE_IDLE_TIMEOUT",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ] {
            if let Ok(val) = std::env::var(key) {
                if !val.trim().is_empty() {
                    if let Some(pos) = envs.iter().position(|(k, _)| k == key) {
                        envs[pos].1 = val;
                    } else {
                        envs.push((key.to_string(), val));
                    }
                }
            }
        }
        envs
    }

    pub fn resolve_secret(env_var: &str) -> Option<String> {
        let val = std::env::var(env_var).ok()?;
        Self::resolve_secret_value(&val)
    }

    pub fn resolve_secret_value(val: &str) -> Option<String> {
        let val = val.trim();
        if val.is_empty() {
            return None;
        }

        let is_path = val.starts_with('/')
            || val.starts_with("./")
            || val.starts_with("../")
            || val.contains('/')
            || val.contains('\\');

        let path = Path::new(val);
        if path.is_file() {
            match std::fs::read_to_string(path) {
                Ok(content) => {
                    let secret = content.trim().to_string();
                    if secret.is_empty() {
                        warn!("Secret file at {} is empty", path.display());
                        None
                    } else {
                        info!("Loaded secret from {}", path.display());
                        Some(secret)
                    }
                }
                Err(err) => {
                    warn!("Failed to read secret file at {}: {}", path.display(), err);
                    None
                }
            }
        } else if is_path {
            warn!(
                "Secret path '{}' does not exist or is not a readable file; failing closed",
                val
            );
            None
        } else {
            Some(val.to_string())
        }
    }

    /// Resolve the `rustc` binary path while honoring `ACTION_RUNNER_RUSTC_BIN`,
    /// `CARGO_HOME`, `PATH`, `RUSTUP_HOME`, `HOME`, and the service-account fallback.
    pub fn resolve_rustc_bin_from(
        explicit_bin: Option<&Path>,
        cargo_home: Option<&Path>,
        rustup_home: Option<&Path>,
        path_env: Option<&std::ffi::OsStr>,
        home: Option<&Path>,
    ) -> PathBuf {
        if let Some(explicit) = explicit_bin.filter(|p| !p.as_os_str().is_empty()) {
            return explicit.to_path_buf();
        }

        if let Some(ch) = cargo_home.filter(|p| !p.as_os_str().is_empty()) {
            let candidate = ch.join("bin").join("rustc");
            if candidate.is_file() {
                return candidate;
            }
        }

        if let Some(pe) = path_env {
            for dir in std::env::split_paths(pe) {
                let candidate = dir.join("rustc");
                if candidate.is_file() {
                    return candidate;
                }
            }
        }

        if let Some(rh) = rustup_home.filter(|p| !p.as_os_str().is_empty()) {
            if let Some(parent) = rh.parent() {
                let candidate = parent.join(".cargo").join("bin").join("rustc");
                if candidate.is_file() {
                    return candidate;
                }
            }
        }

        if let Some(h) = home.filter(|p| !p.as_os_str().is_empty()) {
            let candidate = h.join(".cargo").join("bin").join("rustc");
            if candidate.is_file() {
                return candidate;
            }
        }

        let service_candidate = Path::new("/var/lib/action-runner/.cargo/bin/rustc");
        if service_candidate.is_file() {
            return service_candidate.to_path_buf();
        }

        PathBuf::from("rustc")
    }

    /// Detect the `rustc` binary using the current process environment.
    pub fn detect_rustc_bin() -> PathBuf {
        let explicit = std::env::var_os("ACTION_RUNNER_RUSTC_BIN").map(PathBuf::from);
        let cargo_home = std::env::var_os("CARGO_HOME").map(PathBuf::from);
        let rustup_home = std::env::var_os("RUSTUP_HOME").map(PathBuf::from);
        let path_env = std::env::var_os("PATH");
        let home = std::env::var_os("HOME").map(PathBuf::from);
        Self::resolve_rustc_bin_from(
            explicit.as_deref(),
            cargo_home.as_deref(),
            rustup_home.as_deref(),
            path_env.as_deref(),
            home.as_deref(),
        )
    }

    /// Construct the `rustc --version` probe command with optional `RUSTUP_HOME` and `CARGO_HOME`.
    pub fn build_rustc_probe_command(
        rustc_bin: &Path,
        rustup_home: Option<&Path>,
        cargo_home: Option<&Path>,
    ) -> std::process::Command {
        let mut cmd = std::process::Command::new(rustc_bin);
        cmd.arg("--version");
        if let Some(home) = rustup_home.filter(|p| !p.as_os_str().is_empty()) {
            cmd.env("RUSTUP_HOME", home);
        }
        if let Some(home) = cargo_home.filter(|p| !p.as_os_str().is_empty()) {
            cmd.env("CARGO_HOME", home);
        }
        cmd
    }

    /// Probe the active `rustc --version` under the current process or service-account environment.
    ///
    /// Fails soft (`None`) with a warning log if `rustc` cannot be executed or parsed.
    pub fn probe_rustc_semver() -> Option<String> {
        let explicit = std::env::var_os("ACTION_RUNNER_RUSTC_BIN").map(PathBuf::from);
        let cargo_home = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty());
        let rustup_home = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty());
        let path_env = std::env::var_os("PATH");
        let home = std::env::var_os("HOME").map(PathBuf::from);

        let rustc_bin = Self::resolve_rustc_bin_from(
            explicit.as_deref(),
            cargo_home.as_deref(),
            rustup_home.as_deref(),
            path_env.as_deref(),
            home.as_deref(),
        );

        let effective_rustup_home = rustup_home.or_else(|| {
            if rustc_bin.starts_with("/var/lib/action-runner")
                && Path::new("/var/lib/action-runner/.rustup").is_dir()
            {
                Some(PathBuf::from("/var/lib/action-runner/.rustup"))
            } else {
                None
            }
        });
        let effective_cargo_home = cargo_home.or_else(|| {
            if rustc_bin.starts_with("/var/lib/action-runner")
                && Path::new("/var/lib/action-runner/.cargo").is_dir()
            {
                Some(PathBuf::from("/var/lib/action-runner/.cargo"))
            } else {
                None
            }
        });

        Self::probe_rustc_semver_for(
            &rustc_bin,
            effective_rustup_home.as_deref(),
            effective_cargo_home.as_deref(),
        )
    }

    /// Probe `rustc --version` for a specific binary path and optional `RUSTUP_HOME` / `CARGO_HOME`.
    pub fn probe_rustc_semver_for(
        rustc_bin: &Path,
        rustup_home: Option<&Path>,
        cargo_home: Option<&Path>,
    ) -> Option<String> {
        let mut cmd = Self::build_rustc_probe_command(rustc_bin, rustup_home, cargo_home);
        Self::probe_rustc_semver_with(rustc_bin, || cmd.output())
    }

    fn probe_rustc_semver_with<F>(rustc_bin: &Path, run: F) -> Option<String>
    where
        F: FnOnce() -> std::io::Result<std::process::Output>,
    {
        match run() {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!(
                        bin = %rustc_bin.display(),
                        status = %output.status,
                        stderr = %stderr.trim(),
                        "rustc --version exited unsuccessfully during startup probe; continuing with rust_semver = None"
                    );
                    return None;
                }
                match std::str::from_utf8(&output.stdout) {
                    Ok(stdout) => match parse_rustc_semver(stdout) {
                        Some(ver) => Some(ver),
                        None => {
                            warn!(
                                bin = %rustc_bin.display(),
                                stdout = %stdout.trim(),
                                "Failed to parse semver from rustc --version output; continuing with rust_semver = None"
                            );
                            None
                        }
                    },
                    Err(err) => {
                        warn!(
                            bin = %rustc_bin.display(),
                            error = %err,
                            "rustc --version emitted non-UTF-8 stdout; continuing with rust_semver = None"
                        );
                        None
                    }
                }
            }
            Err(err) => {
                warn!(
                    bin = %rustc_bin.display(),
                    error = %err,
                    "Unable to invoke rustc --version during startup probe; continuing with rust_semver = None"
                );
                None
            }
        }
    }
}

/// Parses the semantic version token from `rustc --version` output.
///
/// Supports stable (`rustc 1.86.0 (...)`), beta (`rustc 1.87.0-beta.2 (...)`),
/// nightly (`rustc 1.88.0-nightly (...)`), and dev (`rustc 1.88.0-dev`) outputs.
/// Returns `None` for empty, non-`rustc`, or malformed version strings.
pub fn parse_rustc_semver(output: &str) -> Option<String> {
    let line = output.lines().map(str::trim).find(|l| !l.is_empty())?;
    let mut parts = line.split_whitespace();
    if parts.next()? != "rustc" {
        return None;
    }
    let version_token = parts.next()?;
    if !is_valid_semver(version_token) {
        return None;
    }
    Some(version_token.to_string())
}

fn is_valid_semver_ident(ident: &str, allow_leading_zero: bool) -> bool {
    if ident.is_empty() {
        return false;
    }
    if !allow_leading_zero
        && ident.len() > 1
        && ident.starts_with('0')
        && ident.bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    ident
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn is_valid_numeric_component(part: &str) -> bool {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if part.len() > 1 && part.starts_with('0') {
        return false;
    }
    part.parse::<u64>().is_ok()
}

fn is_valid_semver(version: &str) -> bool {
    let (core_and_pre, build_opt) = match version.split_once('+') {
        Some((before, build)) => (before, Some(build)),
        None => (version, None),
    };
    if let Some(build) = build_opt {
        if build.is_empty() || !build.split('.').all(|id| is_valid_semver_ident(id, true)) {
            return false;
        }
    }

    let (core, pre_opt) = match core_and_pre.split_once('-') {
        Some((before, pre)) => (before, Some(pre)),
        None => (core_and_pre, None),
    };
    if let Some(pre) = pre_opt {
        if pre.is_empty()
            || !pre.split('.').all(|id| {
                let numeric = id.bytes().all(|b| b.is_ascii_digit());
                is_valid_semver_ident(id, !numeric)
            })
        {
            return false;
        }
    }

    let mut core_parts = core.split('.');
    let (Some(major), Some(minor), Some(patch), None) = (
        core_parts.next(),
        core_parts.next(),
        core_parts.next(),
        core_parts.next(),
    ) else {
        return false;
    };
    is_valid_numeric_component(major)
        && is_valid_numeric_component(minor)
        && is_valid_numeric_component(patch)
}

pub fn is_safe_repo_name(repo: &str) -> bool {
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 {
        return false;
    }
    let owner = parts[0];
    let name = parts[1];
    if owner.is_empty() || name.is_empty() {
        return false;
    }
    if owner.contains("..") || name.contains("..") {
        return false;
    }
    let is_valid_char = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.';
    owner.chars().all(is_valid_char) && name.chars().all(is_valid_char)
}

fn hostname() -> String {
    if let Ok(name) = std::env::var("HOSTNAME") {
        if !name.is_empty() {
            return name;
        }
    }
    if let Ok(output) = std::process::Command::new("hostname").output() {
        if let Ok(name) = std::str::from_utf8(&output.stdout) {
            let trimmed = name.trim().to_string();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }
    }
    "worker-node".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_secret_value_literal() {
        let literal = "my_literal_token_12345";
        assert_eq!(
            WorkerConfig::resolve_secret_value(literal),
            Some(literal.to_string())
        );
    }

    #[test]
    fn test_resolve_secret_value_from_valid_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("secret.key");
        std::fs::write(&file_path, "  my-secret-token \n\t").unwrap();

        assert_eq!(
            WorkerConfig::resolve_secret_value(file_path.to_str().unwrap()),
            Some("my-secret-token".to_string())
        );
    }

    #[test]
    fn test_resolve_secret_value_missing_path_fail_closed() {
        assert_eq!(
            WorkerConfig::resolve_secret_value("/nonexistent/cluster.token"),
            None,
            "Missing absolute secret file must fail closed to None"
        );
        assert_eq!(
            WorkerConfig::resolve_secret_value("./missing_cluster.token"),
            None,
            "Missing relative secret file must fail closed to None"
        );
        assert_eq!(
            WorkerConfig::resolve_secret_value("../missing_cluster.token"),
            None,
            "Parent traversal secret path must fail closed to None"
        );
    }

    #[test]
    fn test_resolve_secret_value_empty_file_fail_closed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("empty.key");
        std::fs::write(&file_path, "   \n\t").unwrap();

        assert_eq!(
            WorkerConfig::resolve_secret_value(file_path.to_str().unwrap()),
            None,
            "Empty secret file must fail closed to None"
        );
    }

    #[test]
    fn test_resolve_secret_env_var_fail_closed() {
        let var_name = "TEST_WORKER_SECRET_FAIL_CLOSED_VAR";
        std::env::set_var(var_name, "/nonexistent/path/to/token.secret");
        assert_eq!(
            WorkerConfig::resolve_secret(var_name),
            None,
            "Environment variable pointing to nonexistent path must fail closed"
        );
        std::env::remove_var(var_name);
    }

    #[test]
    fn test_is_safe_repo_name() {
        assert!(is_safe_repo_name("LMA1980/model-runner"));
        assert!(is_safe_repo_name("owner-123/repo_456.test"));
        assert!(!is_safe_repo_name(""));
        assert!(!is_safe_repo_name("/etc/passwd"));
        assert!(!is_safe_repo_name("../etc/passwd"));
        assert!(!is_safe_repo_name("LMA1980/../passwd"));
        assert!(!is_safe_repo_name("LMA1980"));
        assert!(!is_safe_repo_name("LMA1980/repo/extra"));
        assert!(!is_safe_repo_name("LMA1980/repo;rm -rf /"));
    }

    #[test]
    fn test_parse_rustc_semver_stable() {
        assert_eq!(
            parse_rustc_semver("rustc 1.86.0 (05f9846f8 2025-03-31)\n"),
            Some("1.86.0".to_string())
        );
        assert_eq!(
            parse_rustc_semver("  rustc 1.99.1 (58cf8f9c3 2026-10-13)  \r\n"),
            Some("1.99.1".to_string())
        );
        assert_eq!(
            parse_rustc_semver("rustc 1.86.0"),
            Some("1.86.0".to_string())
        );
    }

    #[test]
    fn test_parse_rustc_semver_nightly_and_beta() {
        assert_eq!(
            parse_rustc_semver("rustc 1.88.0-nightly (123456789 2025-05-01)\n"),
            Some("1.88.0-nightly".to_string())
        );
        assert_eq!(
            parse_rustc_semver("rustc 1.87.0-beta.2 (abcdef123 2025-04-15)"),
            Some("1.87.0-beta.2".to_string())
        );
        assert_eq!(
            parse_rustc_semver("rustc 1.87.0-beta (abcdef123 2025-04-15)"),
            Some("1.87.0-beta".to_string())
        );
        assert_eq!(
            parse_rustc_semver("rustc 1.85.0-dev"),
            Some("1.85.0-dev".to_string())
        );
        assert_eq!(
            parse_rustc_semver("rustc 1.86.0+custom.1 (abcdef 2025-01-01)"),
            Some("1.86.0+custom.1".to_string())
        );
    }

    #[test]
    fn test_parse_rustc_semver_malformed() {
        for invalid in [
            "",
            "   \n\t ",
            "cargo 1.86.0 (05f9846f8 2025-03-31)",
            "rustc",
            "rustc 1.86",
            "rustc 1.86.0.1",
            "rustc v1.86.0 (05f9846f8 2025-03-31)",
            "rustc 1.86.x (05f9846f8 2025-03-31)",
            "rustc 01.86.0",
            "rustc 1.86.0-",
            "rustc 1.86.0-nightly.",
            "rustc 1.86.0+",
            "not a rustc banner",
        ] {
            assert_eq!(
                parse_rustc_semver(invalid),
                None,
                "Expected None for malformed output: {invalid:?}"
            );
        }
    }

    #[test]
    fn test_resolve_rustc_bin_and_probe_command_respect_service_account_env() {
        let dir = tempfile::tempdir().unwrap();
        let cargo_home = dir.path().join("cargo");
        let rustup_home = dir.path().join("rustup");
        let path_dir = dir.path().join("path-bin");
        let home_dir = dir.path().join("home");
        let sibling_cargo_bin = dir.path().join(".cargo/bin");

        std::fs::create_dir_all(cargo_home.join("bin")).unwrap();
        std::fs::create_dir_all(&rustup_home).unwrap();
        std::fs::create_dir_all(&path_dir).unwrap();
        std::fs::create_dir_all(home_dir.join(".cargo/bin")).unwrap();
        std::fs::create_dir_all(&sibling_cargo_bin).unwrap();

        let cargo_rustc = cargo_home.join("bin/rustc");
        let path_rustc = path_dir.join("rustc");
        let home_rustc = home_dir.join(".cargo/bin/rustc");
        let sibling_rustc = sibling_cargo_bin.join("rustc");
        std::fs::write(&cargo_rustc, b"").unwrap();
        std::fs::write(&path_rustc, b"").unwrap();
        std::fs::write(&home_rustc, b"").unwrap();
        std::fs::write(&sibling_rustc, b"").unwrap();

        let explicit = Path::new("/custom/bin/rustc");
        assert_eq!(
            WorkerConfig::resolve_rustc_bin_from(
                Some(explicit),
                Some(&cargo_home),
                Some(&rustup_home),
                Some(path_dir.as_os_str()),
                Some(&home_dir),
            ),
            explicit
        );

        assert_eq!(
            WorkerConfig::resolve_rustc_bin_from(
                None,
                Some(&cargo_home),
                Some(&rustup_home),
                Some(path_dir.as_os_str()),
                Some(&home_dir),
            ),
            cargo_rustc
        );

        assert_eq!(
            WorkerConfig::resolve_rustc_bin_from(
                None,
                Some(&dir.path().join("empty-cargo")),
                Some(&rustup_home),
                Some(path_dir.as_os_str()),
                Some(&home_dir),
            ),
            path_rustc
        );

        assert_eq!(
            WorkerConfig::resolve_rustc_bin_from(
                None,
                None,
                Some(&rustup_home),
                None,
                Some(&home_dir),
            ),
            sibling_rustc
        );

        let empty_rustup = dir.path().join("isolated/rustup");
        std::fs::create_dir_all(&empty_rustup).unwrap();
        assert_eq!(
            WorkerConfig::resolve_rustc_bin_from(
                None,
                None,
                Some(&empty_rustup),
                None,
                Some(&home_dir),
            ),
            home_rustc
        );

        let cmd = WorkerConfig::build_rustc_probe_command(
            &cargo_rustc,
            Some(&rustup_home),
            Some(&cargo_home),
        );
        assert_eq!(cmd.get_program(), cargo_rustc.as_os_str());
        let args: Vec<_> = cmd.get_args().collect();
        assert_eq!(args, vec![std::ffi::OsStr::new("--version")]);
        let envs: std::collections::HashMap<_, _> = cmd.get_envs().collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("RUSTUP_HOME"))
                .copied()
                .flatten(),
            Some(rustup_home.as_os_str())
        );
        assert_eq!(
            envs.get(std::ffi::OsStr::new("CARGO_HOME"))
                .copied()
                .flatten(),
            Some(cargo_home.as_os_str())
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_probe_rustc_semver_with_mock_outputs_and_fail_soft() {
        use std::os::unix::process::ExitStatusExt;

        let bin = Path::new("/var/lib/action-runner/.cargo/bin/rustc");

        let stable = WorkerConfig::probe_rustc_semver_with(bin, || {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b"rustc 1.86.0 (05f9846f8 2025-03-31)\n".to_vec(),
                stderr: Vec::new(),
            })
        });
        assert_eq!(stable, Some("1.86.0".to_string()));

        let nightly = WorkerConfig::probe_rustc_semver_with(bin, || {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b"rustc 1.88.0-nightly (123456789 2025-05-01)\n".to_vec(),
                stderr: Vec::new(),
            })
        });
        assert_eq!(nightly, Some("1.88.0-nightly".to_string()));

        let failed_exit = WorkerConfig::probe_rustc_semver_with(bin, || {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"error: no default toolchain configured".to_vec(),
            })
        });
        assert_eq!(failed_exit, None);

        let malformed_stdout = WorkerConfig::probe_rustc_semver_with(bin, || {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b"garbage output\n".to_vec(),
                stderr: Vec::new(),
            })
        });
        assert_eq!(malformed_stdout, None);

        let invalid_utf8 = WorkerConfig::probe_rustc_semver_with(bin, || {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![0xff, 0xfe],
                stderr: Vec::new(),
            })
        });
        assert_eq!(invalid_utf8, None);

        let spawn_err = WorkerConfig::probe_rustc_semver_with(bin, || {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "rustc missing",
            ))
        });
        assert_eq!(spawn_err, None);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_probe_rustc_semver_live_and_isolated_fail_soft() {
        // Missing binary fails soft cleanly without panicking.
        assert_eq!(
            WorkerConfig::probe_rustc_semver_for(
                Path::new("/nonexistent/action-runner-missing-rustc"),
                None,
                None,
            ),
            None
        );

        if let Some(semver) = WorkerConfig::probe_rustc_semver() {
            assert!(
                is_valid_semver(&semver),
                "Detected rustc semver must be valid semver: {semver}"
            );
        }
    }
}
