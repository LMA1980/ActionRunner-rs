use action_runner::config::{IsolationMode, WorkerConfig};
use action_runner::stage_runner::StageRunner;
use action_runner_ipc::{decode_log_batch, StageDispatchTask};
use std::path::PathBuf;
use tokio::sync::mpsc;

#[test]
fn test_worker_config_defaults() {
    let config = WorkerConfig::load();
    assert!(!config.node_id.is_empty());
    assert!(!config.arch.is_empty());
    assert_eq!(config.max_slots, 1);
    assert!(config.max_threads() >= 1);
    assert!(!config.tags.is_empty());
}

#[tokio::test]
async fn test_stage_runner_execution_and_log_batching() {
    let config = WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-worker".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: PathBuf::from("/tmp/action-runner-test-workspace"),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };

    let runner = StageRunner::new(config);
    let (log_tx, mut log_rx) = mpsc::channel(32);

    let task = StageDispatchTask {
        job_id: "job-test-1".to_string(),
        stage_id: "echo-stage".to_string(),
        stage_name: "Echo Test".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo 'Hello from StageRunner'; echo 'Second line output'".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 10,
        threads: 2,
        toolchain: None,
    };

    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<u32>();
    let res = runner.run_stage(&task, log_tx, Some(started_tx)).await;
    assert_eq!(res.exit_code, 0);
    let pid = started_rx.await.expect("received pid");
    assert!(pid > 0);
    assert_eq!(res.pid, pid);

    let mut all_logs = Vec::new();
    while let Ok(batch_bytes) = log_rx.try_recv() {
        if let Ok(entries) = decode_log_batch(&batch_bytes) {
            all_logs.extend(entries);
        }
    }

    assert!(!all_logs.is_empty());
    assert!(all_logs
        .iter()
        .any(|l| l.content.contains("Hello from StageRunner")));
    assert!(all_logs
        .iter()
        .any(|l| l.content.contains("Second line output")));
}
