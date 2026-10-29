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

        let tags: Vec<String> = std::env::var("ACTION_RUNNER_TAGS")
            .unwrap_or_else(|_| default_tags.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let max_slots = std::env::var("ACTION_RUNNER_MAX_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(1)
            .max(1);

        let workspace_dir = std::env::var("ACTION_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/action-runner/workspace"));

        let mut toolchains = vec!["system".to_string()];
        if Path::new("/home/lma1980/opt/llvm-24").exists()
            || Path::new("/opt/cross-llvm-24").exists()
        {
            toolchains.push("llvm-24".to_string());
        }
        if Path::new("/home/lma1980/opt/gcc-17-spacemit-x100").exists()
            || Path::new("/opt/cross-gcc-17").exists()
        {
            toolchains.push("gcc-17-spacemit-x100".to_string());
        } else if Path::new("/home/lma1980/opt/gcc-17").exists() {
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
        };

        info!(
            node_id = %cfg.node_id,
            arch = %cfg.arch,
            max_slots = cfg.max_slots,
            max_threads = cfg.max_threads(),
            tags = ?cfg.tags,
            toolchains = ?cfg.toolchains,
            btrfs = cfg.use_btrfs,
            "Loaded ActionRunner worker configuration"
        );

        cfg
    }

    pub fn max_threads(&self) -> u32 {
        let default_threads = if self.node_id.eq_ignore_ascii_case("entrance")
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
}
