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

        let node_id = std::env::var("ACTION_RUNNER_NODE_ID").unwrap_or_else(|_| {
            hostname()
        });

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

        let default_slots = match arch.as_str() {
            "riscv64" => 1,
            _ => 4,
        };

        let max_slots = std::env::var("ACTION_RUNNER_SLOTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_slots);

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

        info!(
            node_id = %node_id,
            arch = %arch,
            max_slots = max_slots,
            tags = ?tags,
            toolchains = ?toolchains,
            btrfs = use_btrfs,
            "Loaded ActionRunner worker configuration"
        );

        Self {
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
        }
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

    fn resolve_secret(env_var: &str) -> Option<String> {
        let val = std::env::var(env_var).ok()?.trim().to_string();
        if val.is_empty() {
            return None;
        }

        let path = Path::new(&val);
        if path.is_file() {
            match std::fs::read_to_string(path) {
                Ok(content) => {
                    let secret = content.trim().to_string();
                    if !secret.is_empty() {
                        return Some(secret);
                    }
                }
                Err(err) => {
                    warn!("Failed to read secret file at {}: {}", path.display(), err);
                }
            }
        }
        Some(val)
    }
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
