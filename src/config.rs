use std::env;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub webhook_secret: Option<String>,
}

impl Config {
    pub fn load() -> Self {
        let bind_addr = env::var("ACTION_RUNNER_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

        let webhook_secret = env::var("GITHUB_WEBHOOK_SECRET").ok().or_else(|| {
            let secret_path = Path::new("/home/lma1980/.github_webhook_secret");
            if secret_path.exists() {
                fs::read_to_string(secret_path).ok().map(|s| s.trim().to_string())
            } else {
                None
            }
        });

        Self {
            bind_addr,
            webhook_secret,
        }
    }
}
