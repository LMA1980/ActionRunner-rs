use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub webhook_secret: Option<String>,
    pub workspace_dir: PathBuf,
    pub allowed_repositories: Vec<String>,
    pub job_timeout_seconds: u64,
}

impl Config {
    pub fn load() -> Self {
        let bind_addr =
            env::var("ACTION_RUNNER_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

        let webhook_secret = env::var("GITHUB_WEBHOOK_SECRET").ok().or_else(|| {
            let secret_path = Path::new("/home/lma1980/.github_webhook_secret");
            if secret_path.exists() {
                fs::read_to_string(secret_path)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        });

        let workspace_dir = env::var("ACTION_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/home/lma1980/repository/ci-workspace"));

        let allowed_repositories = env::var("ACTION_RUNNER_ALLOWED_REPOS")
            .map(|val| {
                val.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|_| vec!["LMA1980/*".to_string()]);

        let job_timeout_seconds = env::var("ACTION_RUNNER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1800);

        Self {
            bind_addr,
            webhook_secret,
            workspace_dir,
            allowed_repositories,
            job_timeout_seconds,
        }
    }

    pub fn is_repo_allowed(&self, repo: &str) -> bool {
        for pattern in &self.allowed_repositories {
            if pattern == "*" {
                return true;
            }
            if let Some(prefix) = pattern.strip_suffix("/*") {
                if let Some((owner, _)) = repo.split_once('/') {
                    if owner.eq_ignore_ascii_case(prefix) {
                        return true;
                    }
                }
            } else if pattern.eq_ignore_ascii_case(repo) {
                return true;
            }
        }
        false
    }
}
