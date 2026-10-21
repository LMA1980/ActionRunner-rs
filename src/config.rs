use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Validates that a repository name adheres to strict owner/name format with no directory traversal
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationMode {
    None,
    SystemdRun,
    Bubblewrap,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub webhook_secret: Option<String>,
    pub allow_insecure_no_secret: bool,
    pub workspace_dir: PathBuf,
    pub allowed_repositories: Vec<String>,
    pub allowed_actors: Vec<String>,
    pub allow_fork_prs: bool,
    pub allowed_pr_author_associations: Vec<String>,
    pub job_timeout_seconds: u64,
    pub log_api_key: Option<String>,
    pub isolation_mode: IsolationMode,
}

impl Config {
    pub fn load() -> Self {
        let bind_addr =
            env::var("ACTION_RUNNER_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

        let webhook_secret = env::var("GITHUB_WEBHOOK_SECRET").ok().or_else(|| {
            let candidate_paths = [
                Path::new("/etc/action-runner/github_webhook_secret"),
                Path::new("/home/action-runner/.github_webhook_secret"),
                Path::new("/var/lib/action-runner/.github_webhook_secret"),
                Path::new("/home/lma1980/.github_webhook_secret"),
            ];
            for p in &candidate_paths {
                if p.exists() {
                    if let Ok(s) = fs::read_to_string(p) {
                        let trimmed = s.trim().to_string();
                        if !trimmed.is_empty() {
                            return Some(trimmed);
                        }
                    }
                }
            }
            None
        });

        let allow_insecure_no_secret = env::var("ACTION_RUNNER_ALLOW_INSECURE_NO_SECRET")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let workspace_dir = env::var("ACTION_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                if Path::new("/var/lib/action-runner/workspace").exists() {
                    PathBuf::from("/var/lib/action-runner/workspace")
                } else if Path::new("/home/action-runner/ci-workspace").exists() {
                    PathBuf::from("/home/action-runner/ci-workspace")
                } else {
                    PathBuf::from("/home/lma1980/repository/ci-workspace")
                }
            });

        let allowed_repositories = env::var("ACTION_RUNNER_ALLOWED_REPOS")
            .map(|val| {
                val.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|_| vec!["LMA1980/*".to_string()]);

        let allowed_actors = env::var("ACTION_RUNNER_ALLOWED_ACTORS")
            .map(|val| {
                val.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let allow_fork_prs = env::var("ACTION_RUNNER_ALLOW_FORK_PRS")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let allowed_pr_author_associations = env::var("ACTION_RUNNER_ALLOWED_PR_AUTHORS")
            .map(|val| {
                val.split(',')
                    .map(|s| s.trim().to_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|_| {
                vec![
                    "OWNER".to_string(),
                    "MEMBER".to_string(),
                    "COLLABORATOR".to_string(),
                ]
            });

        let job_timeout_seconds = env::var("ACTION_RUNNER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);

        let log_api_key = env::var("ACTION_RUNNER_LOG_API_KEY")
            .ok()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());

        let isolation_mode = match env::var("ACTION_RUNNER_ISOLATION")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "systemd-run" | "systemd" => IsolationMode::SystemdRun,
            "bwrap" | "bubblewrap" => IsolationMode::Bubblewrap,
            _ => IsolationMode::None,
        };

        Self {
            bind_addr,
            webhook_secret,
            allow_insecure_no_secret,
            workspace_dir,
            allowed_repositories,
            allowed_actors,
            allow_fork_prs,
            allowed_pr_author_associations,
            job_timeout_seconds,
            log_api_key,
            isolation_mode,
        }
    }

    pub fn is_repo_allowed(&self, repo: &str) -> bool {
        if !is_safe_repo_name(repo) {
            return false;
        }

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

    pub fn is_actor_allowed(&self, actor: &str) -> bool {
        if self.allowed_actors.is_empty() {
            return true; // No restrictive actor list configured
        }
        self.allowed_actors
            .iter()
            .any(|a| a == "*" || a.eq_ignore_ascii_case(actor))
    }

    pub fn is_pr_author_allowed(&self, association: &str) -> bool {
        self.allowed_pr_author_associations
            .iter()
            .any(|a| a.eq_ignore_ascii_case(association))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_safe_repo_name() {
        assert!(is_safe_repo_name("LMA1980/ActionRunner-rs"));
        assert!(is_safe_repo_name("LMA1980/model-runner"));
        assert!(is_safe_repo_name("user-name/repo.name_1"));

        // Path traversal attempts
        assert!(!is_safe_repo_name("LMA1980/../../etc"));
        assert!(!is_safe_repo_name("../LMA1980/repo"));
        assert!(!is_safe_repo_name("LMA1980/.."));
        assert!(!is_safe_repo_name(".."));
        assert!(!is_safe_repo_name("/etc/passwd"));
        assert!(!is_safe_repo_name("repo_only"));
        assert!(!is_safe_repo_name("a/b/c"));
        assert!(!is_safe_repo_name("owner/repo;rm -rf"));
        assert!(!is_safe_repo_name("owner/repo\n"));
    }

    #[test]
    fn test_repo_allowlist() {
        let config = Config {
            bind_addr: "127.0.0.1:8000".to_string(),
            webhook_secret: Some("secret".to_string()),
            allow_insecure_no_secret: false,
            workspace_dir: PathBuf::from("/tmp"),
            allowed_repositories: vec!["LMA1980/*".to_string(), "org/specific-repo".to_string()],
            allowed_actors: vec![],
            allow_fork_prs: false,
            allowed_pr_author_associations: vec!["OWNER".to_string(), "MEMBER".to_string()],
            job_timeout_seconds: 600,
            log_api_key: None,
            isolation_mode: IsolationMode::None,
        };

        assert!(config.is_repo_allowed("LMA1980/ActionRunner-rs"));
        assert!(config.is_repo_allowed("LMA1980/model-runner"));
        assert!(config.is_repo_allowed("lma1980/k3-core"));
        assert!(config.is_repo_allowed("org/specific-repo"));

        assert!(!config.is_repo_allowed("other-org/other-repo"));
        assert!(!config.is_repo_allowed("LMA1980/../../etc/passwd"));
    }

    #[test]
    fn test_pr_author_association() {
        let config = Config {
            bind_addr: "127.0.0.1:8000".to_string(),
            webhook_secret: Some("secret".to_string()),
            allow_insecure_no_secret: false,
            workspace_dir: PathBuf::from("/tmp"),
            allowed_repositories: vec!["*".to_string()],
            allowed_actors: vec![],
            allow_fork_prs: false,
            allowed_pr_author_associations: vec![
                "OWNER".to_string(),
                "MEMBER".to_string(),
                "COLLABORATOR".to_string(),
            ],
            job_timeout_seconds: 600,
            log_api_key: None,
            isolation_mode: IsolationMode::None,
        };

        assert!(config.is_pr_author_allowed("OWNER"));
        assert!(config.is_pr_author_allowed("owner"));
        assert!(config.is_pr_author_allowed("COLLABORATOR"));
        assert!(config.is_pr_author_allowed("MEMBER"));

        assert!(!config.is_pr_author_allowed("NONE"));
        assert!(!config.is_pr_author_allowed("FIRST_TIME_CONTRIBUTOR"));
        assert!(!config.is_pr_author_allowed("CONTRIBUTOR"));
    }
}
