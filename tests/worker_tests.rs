use action_runner::config::{IsolationMode, WorkerConfig};
use action_runner::stage_runner::StageRunner;
use action_runner::worker::WorkerAgent;
use action_runner_ipc::{decode_log_batch, StageDispatchTask};
use std::time::Duration;
use tokio::sync::mpsc;

#[test]
fn test_worker_config_defaults() {
    let config = WorkerConfig::load();
    assert!(!config.node_id.is_empty());
    assert!(!config.arch.is_empty());
    assert!(config.max_slots >= 1);
    assert!(config.max_threads() >= 1);
    assert!(!config.tags.is_empty());
}

#[tokio::test]
async fn test_stage_runner_execution_and_log_batching() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-worker".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: temp_workspace.path().to_path_buf(),
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
        exclusive: false,
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

#[test]
fn test_worker_config_max_slots_from_env() {
    let env_var = "ACTION_RUNNER_MAX_SLOTS";
    let original = std::env::var(env_var).ok();

    std::env::set_var(env_var, "4");
    let config = WorkerConfig::load();
    assert_eq!(config.max_slots, 4);

    // Clamped to at least 1 when 0 or invalid
    std::env::set_var(env_var, "0");
    let config_zero = WorkerConfig::load();
    assert_eq!(config_zero.max_slots, 1);

    std::env::set_var(env_var, "invalid");
    let config_invalid = WorkerConfig::load();
    assert_eq!(config_invalid.max_slots, 1);

    match original {
        Some(v) => std::env::set_var(env_var, v),
        None => std::env::remove_var(env_var),
    }
}

#[tokio::test]
async fn test_worker_gating_shared_permits() {
    let config = WorkerConfig {
        max_slots: 3,
        ..WorkerConfig::load()
    };
    let worker = WorkerAgent::new(config);
    assert_eq!(worker.available_permits(), 3);

    let p1 = worker.acquire_permit(false).await.unwrap();
    assert_eq!(worker.available_permits(), 2);

    let p2 = worker.acquire_permit(false).await.unwrap();
    assert_eq!(worker.available_permits(), 1);

    let p3 = worker.acquire_permit(false).await.unwrap();
    assert_eq!(worker.available_permits(), 0);

    // 4th permit attempt fails immediately
    assert!(worker.try_acquire_permit(false).is_err());

    drop(p1);
    assert_eq!(worker.available_permits(), 1);
    drop(p2);
    drop(p3);
    assert_eq!(worker.available_permits(), 3);
}

#[tokio::test]
async fn test_worker_gating_exclusive_all_permits() {
    let config = WorkerConfig {
        max_slots: 4,
        ..WorkerConfig::load()
    };
    let worker = WorkerAgent::new(config);
    assert_eq!(worker.available_permits(), 4);

    // Exclusive task acquires all 4 permits
    let exc_permit = worker.acquire_permit(true).await.unwrap();
    assert_eq!(worker.available_permits(), 0);

    // Any other acquisition is blocked
    assert!(worker.try_acquire_permit(false).is_err());
    assert!(worker.try_acquire_permit(true).is_err());

    // Drop exclusive permit -> all 4 permits immediately restored
    drop(exc_permit);
    assert_eq!(worker.available_permits(), 4);
}

#[tokio::test]
async fn test_worker_exclusive_waits_for_shared_drain() {
    let config = WorkerConfig {
        max_slots: 2,
        ..WorkerConfig::load()
    };
    let worker = std::sync::Arc::new(WorkerAgent::new(config));

    let shared_permit = worker.acquire_permit(false).await.unwrap();
    assert_eq!(worker.available_permits(), 1);

    // Spawn exclusive waiter requesting all 2 permits
    let worker_clone = worker.clone();
    let exc_handle = tokio::spawn(async move { worker_clone.acquire_permit(true).await.unwrap() });

    // Yield to allow exc_handle to queue
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!exc_handle.is_finished());

    // Release shared permit
    drop(shared_permit);

    // Exclusive waiter unblocks and holds all permits
    let exc_permit = tokio::time::timeout(Duration::from_secs(2), exc_handle)
        .await
        .expect("exclusive task did not time out")
        .expect("exclusive task join succeeded");
    assert_eq!(worker.available_permits(), 0);

    drop(exc_permit);
    assert_eq!(worker.available_permits(), 2);
}

#[tokio::test]
async fn test_worker_deterministic_permit_release_on_abort() {
    let config = WorkerConfig {
        max_slots: 2,
        ..WorkerConfig::load()
    };
    let worker = std::sync::Arc::new(WorkerAgent::new(config));

    let worker_clone = worker.clone();
    let handle = tokio::spawn(async move {
        let _permit = worker_clone.acquire_permit(true).await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(worker.available_permits(), 0);

    // Abort task simulating worker disconnect, panic, or cancellation
    handle.abort();
    let _ = handle.await;

    // Permit must be restored immediately
    assert_eq!(worker.available_permits(), 2);
}

#[tokio::test]
async fn test_path_traversal_rejection_absolute_working_dir() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-worker".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: temp_workspace.path().to_path_buf(),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    let task = StageDispatchTask {
        job_id: "job-sec-1".to_string(),
        stage_id: "stage-sec-1".to_string(),
        stage_name: "Security Test".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo test".to_string(),
        working_dir: Some("/etc".to_string()),
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };

    let res = runner.run_stage(&task, log_tx, None).await;
    assert_eq!(res.exit_code, -1);
    let err = res.error_message.expect("expected error message");
    assert!(
        err.to_lowercase().contains("absolute"),
        "Expected error message mentioning absolute paths, got: {}",
        err
    );
}

#[tokio::test]
async fn test_path_traversal_rejection_parent_dir() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-worker".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: temp_workspace.path().to_path_buf(),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    let task = StageDispatchTask {
        job_id: "job-sec-2".to_string(),
        stage_id: "stage-sec-2".to_string(),
        stage_name: "Security Test Traversal".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo test".to_string(),
        working_dir: Some("../../etc/shadow".to_string()),
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };

    let res = runner.run_stage(&task, log_tx, None).await;
    assert_eq!(res.exit_code, -1);
    let err = res.error_message.expect("expected error message");
    assert!(
        err.to_lowercase().contains("traversal") || err.to_lowercase().contains("parent"),
        "Expected error message mentioning traversal/parent directory, got: {}",
        err
    );
}

#[tokio::test]
async fn test_path_traversal_rejection_malicious_repo_and_ids() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-worker".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: temp_workspace.path().to_path_buf(),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    // Malicious repo_name
    let bad_repo_task = StageDispatchTask {
        job_id: "job-valid".to_string(),
        stage_id: "stage-valid".to_string(),
        stage_name: "Bad Repo".to_string(),
        repo_name: "../../etc/passwd".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo test".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };
    let res1 = runner.run_stage(&bad_repo_task, log_tx.clone(), None).await;
    assert_eq!(res1.exit_code, -1);
    assert!(res1
        .error_message
        .unwrap()
        .contains("invalid or unsafe repository name"));

    // Malicious job_id
    let bad_job_task = StageDispatchTask {
        job_id: "../job-escape".to_string(),
        stage_id: "stage-valid".to_string(),
        stage_name: "Bad Job".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo test".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };
    let res2 = runner.run_stage(&bad_job_task, log_tx.clone(), None).await;
    assert_eq!(res2.exit_code, -1);
    assert!(res2.error_message.unwrap().contains("unsafe job_id"));

    // Malicious stage_id
    let bad_stage_task = StageDispatchTask {
        job_id: "job-valid".to_string(),
        stage_id: "stage/escape".to_string(),
        stage_name: "Bad Stage".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo test".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };
    let res3 = runner.run_stage(&bad_stage_task, log_tx, None).await;
    assert_eq!(res3.exit_code, -1);
    assert!(res3.error_message.unwrap().contains("unsafe job_id"));
}

#[test]
fn test_fail_closed_secret_resolution() {
    // 1. Literal secret
    let literal = "my_super_secret_token_abc123";
    assert_eq!(
        WorkerConfig::resolve_secret_value(literal),
        Some(literal.to_string())
    );

    // 2. Valid file in tempdir
    let temp_dir = tempfile::tempdir().unwrap();
    let secret_file = temp_dir.path().join("cluster.token");
    std::fs::write(&secret_file, "  valid-file-secret-token \n").unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(secret_file.to_str().unwrap()),
        Some("valid-file-secret-token".to_string())
    );

    // 3. Nonexistent path fails closed to None
    assert_eq!(
        WorkerConfig::resolve_secret_value("/nonexistent/path/cluster.token"),
        None,
        "Nonexistent absolute secret path must fail closed to None"
    );
    assert_eq!(
        WorkerConfig::resolve_secret_value("./missing_cluster.token"),
        None,
        "Nonexistent relative secret path must fail closed to None"
    );
    assert_eq!(
        WorkerConfig::resolve_secret_value("../missing_cluster.token"),
        None,
        "Nonexistent parent traversal secret path must fail closed to None"
    );

    // 4. Empty file fails closed to None
    let empty_file = temp_dir.path().join("empty.token");
    std::fs::write(&empty_file, "   \n\t").unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(empty_file.to_str().unwrap()),
        None,
        "Empty secret file must fail closed to None"
    );
}
