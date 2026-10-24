mod config;
mod stage_runner;
mod worker;

use config::WorkerConfig;
use worker::WorkerAgent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "action_runner=info".into()),
        )
        .init();

    let config = WorkerConfig::load();
    let agent = WorkerAgent::new(config);
    agent.run_forever().await;

    Ok(())
}
