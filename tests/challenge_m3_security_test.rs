use action_runner::config::{is_safe_repo_name, IsolationMode, WorkerConfig};
use action_runner::stage_runner::{
    is_safe_identifier, sanitize_path_segment, validate_and_resolve_working_dir, StageRunner,
};
use action_runner_ipc::StageDispatchTask;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

#[test]
fn test_challenge_3b_sanitize_path_segment() {
    assert_eq!(sanitize_path_segment("job-123_abc"), "job-123_abc");
    assert_eq!(
        sanitize_path_segment("../../etc/passwd"),
        "______etc_passwd"
    );
    assert_eq!(sanitize_path_segment(""), "unknown");
    assert_eq!(sanitize_path_segment("foo\0bar"), "foo_bar");
    assert_eq!(sanitize_path_segment("foo/bar\\baz"), "foo_bar_baz");
}

fn make_test_config(workspace_dir: PathBuf) -> WorkerConfig {
    WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: "test-node".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 2,
        workspace_dir,
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    }
}

/// Challenge 1: Path Traversal attacks against `working_dir` in `run_stage`
#[tokio::test]
async fn test_challenge_1_working_dir_path_traversal() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = make_test_config(temp_workspace.path().to_path_buf());
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    let canary_file = temp_workspace.path().join("canary_should_never_exist");
    let touch_canary_cmd = format!("touch {}", canary_file.display());

    let adversarial_working_dirs = vec![
        ("/etc", "absolute path"),
        ("/etc/shadow", "absolute file path"),
        ("/tmp", "root tmp directory"),
        ("/", "filesystem root"),
        ("../../etc/shadow", "parent traversal"),
        (
            "../../../../../../../../etc/passwd",
            "deep parent traversal",
        ),
        ("subdir/../../etc", "nested parent traversal"),
        ("..", "bare parent traversal"),
        ("\\windows\\system32", "backslash path prefix"),
        ("dir\0null", "null byte injection"),
    ];

    for (malicious_dir, desc) in adversarial_working_dirs {
        let task = StageDispatchTask {
            job_id: "sec-job-1".to_string(),
            stage_id: "sec-stage-1".to_string(),
            stage_name: "Adversarial WorkingDir".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: touch_canary_cmd.clone(),
            working_dir: Some(malicious_dir.to_string()),
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };

        let res = runner.run_stage(&task, log_tx.clone(), None).await;

        assert_eq!(
            res.exit_code, -1,
            "Failed for case '{}' ({}): expected exit_code -1, got {}",
            malicious_dir, desc, res.exit_code
        );
        assert!(
            res.error_message.is_some(),
            "Failed for case '{}' ({}): expected error message, got None",
            malicious_dir,
            desc
        );
        let err = res.error_message.unwrap();
        assert!(
            err.contains("Invalid working_dir"),
            "Failed for case '{}' ({}): error message should mention 'Invalid working_dir', got '{}'",
            malicious_dir, desc, err
        );

        // Verify canary file was NEVER created (command was not executed)
        assert!(
            !canary_file.exists(),
            "Security breach: command was executed for malicious working_dir '{}'",
            malicious_dir
        );
    }
}

/// Challenge 1b: Symlink escape attacks against `working_dir`
#[test]
fn test_challenge_1b_symlink_escape_working_dir() {
    let temp_stage = tempfile::tempdir().unwrap();
    let stage_dir = temp_stage.path();

    let target_outside = tempfile::tempdir().unwrap();
    let symlink_path = stage_dir.join("link_to_outside");

    // Create a symlink pointing outside the stage directory
    #[cfg(unix)]
    std::os::unix::fs::symlink(target_outside.path(), &symlink_path).unwrap();

    let res = validate_and_resolve_working_dir(stage_dir, Some("link_to_outside"));
    assert!(
        res.is_err(),
        "Expected symlink pointing outside stage directory to be rejected, got: {:?}",
        res
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("escapes canonical stage directory"),
        "Expected error mentioning symlink escape, got: {}",
        err
    );
}

/// Challenge 2: `repo_name` Path Traversal & Injection Attacks against `run_stage`
#[tokio::test]
async fn test_challenge_2_repo_name_validation_and_traversal() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = make_test_config(temp_workspace.path().to_path_buf());
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    let canary_file = temp_workspace.path().join("canary_repo_test");
    let touch_cmd = format!("touch {}", canary_file.display());

    let adversarial_repos = vec![
        ("../../etc/passwd", "parent traversal path"),
        ("/etc/passwd", "absolute path"),
        ("LMA1980/../../passwd", "embedded traversal"),
        ("LMA1980", "single component"),
        ("LMA1980/repo/extra", "three components"),
        ("LMA1980/repo;rm -rf /", "semicolon command injection"),
        ("LMA1980/repo | id", "pipe character"),
        ("LMA1980/repo && id", "logical AND injection"),
        ("", "empty repo"),
        ("   ", "whitespace only"),
        ("owner/repo\0evil", "null byte injection"),
        ("owner/repo\nevil", "newline injection"),
        ("../etc", "parent dir owner"),
        ("owner/..", "parent dir name"),
    ];

    for (malicious_repo, desc) in adversarial_repos {
        // First verify is_safe_repo_name rejects it
        assert!(
            !is_safe_repo_name(malicious_repo),
            "is_safe_repo_name must return false for '{}' ({})",
            malicious_repo,
            desc
        );

        let task = StageDispatchTask {
            job_id: "sec-job-2".to_string(),
            stage_id: "sec-stage-2".to_string(),
            stage_name: "Adversarial RepoName".to_string(),
            repo_name: malicious_repo.to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: touch_cmd.clone(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };

        let res = runner.run_stage(&task, log_tx.clone(), None).await;

        assert_eq!(
            res.exit_code, -1,
            "Failed for repo '{}' ({}): expected exit_code -1, got {}",
            malicious_repo, desc, res.exit_code
        );
        let err = res.error_message.expect("expected error message");
        assert!(
            err.contains("invalid or unsafe repository name"),
            "Failed for repo '{}' ({}): expected unsafe repo message, got '{}'",
            malicious_repo,
            desc,
            err
        );

        assert!(
            !canary_file.exists(),
            "Security breach: command was executed for malicious repo_name '{}'",
            malicious_repo
        );
    }
}

/// Challenge 3: `job_id` and `stage_id` Traversal & Escape Attacks
#[tokio::test]
async fn test_challenge_3_job_and_stage_id_traversal() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = make_test_config(temp_workspace.path().to_path_buf());
    let runner = StageRunner::new(config);
    let (log_tx, _log_rx) = mpsc::channel(32);

    let canary_file = temp_workspace.path().join("canary_ids_test");
    let touch_cmd = format!("touch {}", canary_file.display());

    let adversarial_pairs = vec![
        ("../../../../tmp", "s1", "job_id parent traversal to tmp"),
        ("j1", "s1/../../escape", "stage_id parent traversal"),
        ("j1/escape", "s1", "job_id slash separator"),
        ("j1", "/root", "stage_id absolute path root"),
        ("..", "s1", "bare parent traversal job_id"),
        ("j1", "..", "bare parent traversal stage_id"),
        ("j1\\escape", "s1", "job_id backslash"),
        ("j1", "s1\\escape", "stage_id backslash"),
        ("j1\0evil", "s1", "job_id null byte"),
        ("j1", "s1\0evil", "stage_id null byte"),
        ("", "s1", "empty job_id"),
        ("j1", "", "empty stage_id"),
        ("   ", "s1", "whitespace job_id"),
        ("j1", "   ", "whitespace stage_id"),
        ("j1..2", "s1", "job_id double dot"),
        ("j1", "s1..2", "stage_id double dot"),
    ];

    for (job_id, stage_id, desc) in adversarial_pairs {
        assert!(
            !is_safe_identifier(job_id) || !is_safe_identifier(stage_id),
            "is_safe_identifier must reject pair ({}, {}) ({})",
            job_id,
            stage_id,
            desc
        );

        let task = StageDispatchTask {
            job_id: job_id.to_string(),
            stage_id: stage_id.to_string(),
            stage_name: "Adversarial ID".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: touch_cmd.clone(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };

        let res = runner.run_stage(&task, log_tx.clone(), None).await;

        assert_eq!(
            res.exit_code, -1,
            "Failed for pair ('{}', '{}') ({}): expected exit_code -1, got {}",
            job_id, stage_id, desc, res.exit_code
        );
        let err = res.error_message.expect("expected error message");
        assert!(
            err.contains("unsafe job_id") || err.contains("unsafe repository name"),
            "Failed for pair ('{}', '{}') ({}): expected unsafe error message, got '{}'",
            job_id,
            stage_id,
            desc,
            err
        );

        assert!(
            !canary_file.exists(),
            "Security breach: command was executed for malicious IDs ('{}', '{}')",
            job_id,
            stage_id
        );
    }
}

/// Challenge 4: Fail-Closed Secret Resolution (`resolve_secret` and `resolve_secret_value`)
#[test]
fn test_challenge_4_fail_closed_secret_resolution() {
    let temp_dir = tempfile::tempdir().unwrap();

    // 1. Nonexistent paths MUST return None and NEVER return the path string
    let nonexistent_paths = vec![
        "/nonexistent/path/cluster.token",
        "/etc/nonexistent_secret_key.token",
        "./missing_cluster.token",
        "../missing_cluster.token",
        "secrets/cluster.token",
        "nested/path/to/token.key",
        "C:\\windows\\system32\\secret.token",
    ];
    for path_str in nonexistent_paths {
        let res = WorkerConfig::resolve_secret_value(path_str);
        assert_eq!(
            res, None,
            "Nonexistent path '{}' must fail closed to None, but returned {:?}",
            path_str, res
        );
    }

    // 2. Unreadable / Empty files MUST return None
    let empty_file = temp_dir.path().join("empty.token");
    fs::write(&empty_file, "").unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(empty_file.to_str().unwrap()),
        None,
        "Empty secret file must return None"
    );

    let whitespace_file = temp_dir.path().join("whitespace.token");
    fs::write(&whitespace_file, "   \t\r\n   ").unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(whitespace_file.to_str().unwrap()),
        None,
        "Whitespace-only secret file must return None"
    );

    // Unreadable file (chmod 000 on Unix)
    let unreadable_file = temp_dir.path().join("unreadable.token");
    fs::write(&unreadable_file, "valid-secret-content").unwrap();
    fs::set_permissions(&unreadable_file, fs::Permissions::from_mode(0o000)).unwrap();
    let unreadable_res = WorkerConfig::resolve_secret_value(unreadable_file.to_str().unwrap());
    assert_eq!(
        unreadable_res, None,
        "Unreadable (chmod 000) secret file must fail closed to None, got {:?}",
        unreadable_res
    );
    // Restore permissions for cleanup
    fs::set_permissions(&unreadable_file, fs::Permissions::from_mode(0o600)).unwrap();

    // Directory path MUST return None
    let dir_path = temp_dir.path().join("secret_dir");
    fs::create_dir(&dir_path).unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(dir_path.to_str().unwrap()),
        None,
        "Directory path must return None"
    );

    // 3. Valid file MUST return Some(trimmed_secret)
    let valid_file = temp_dir.path().join("valid.token");
    fs::write(&valid_file, "  super-secure-token-12345 \n\t").unwrap();
    assert_eq!(
        WorkerConfig::resolve_secret_value(valid_file.to_str().unwrap()),
        Some("super-secure-token-12345".to_string())
    );

    // 4. Non-path literal string MUST return Some(val)
    let literal_tokens = vec![
        "super-secret-literal-token-xyz",
        "ghp_1234567890abcdefABCDEF",
        "CLUSTER_TOKEN_VALUE_42",
    ];
    for lit in literal_tokens {
        assert_eq!(
            WorkerConfig::resolve_secret_value(lit),
            Some(lit.to_string()),
            "Literal string without path separators must return Some(val)"
        );
    }

    // 5. Empty / whitespace literal strings return None
    assert_eq!(WorkerConfig::resolve_secret_value(""), None);
    assert_eq!(WorkerConfig::resolve_secret_value("   "), None);

    // 6. Environment variable resolution
    let env_key = "ACTION_RUNNER_TEST_SECRET_FAIL_CLOSED_ENV";

    // Nonexistent env var -> None
    std::env::remove_var(env_key);
    assert_eq!(WorkerConfig::resolve_secret(env_key), None);

    // Env var pointing to missing file -> None
    std::env::set_var(env_key, "/nonexistent/path/for/env.secret");
    assert_eq!(WorkerConfig::resolve_secret(env_key), None);

    // Env var pointing to valid file -> Some(secret)
    std::env::set_var(env_key, valid_file.to_str().unwrap());
    assert_eq!(
        WorkerConfig::resolve_secret(env_key),
        Some("super-secure-token-12345".to_string())
    );

    // Env var with literal secret -> Some(secret)
    std::env::set_var(env_key, "env-literal-token");
    assert_eq!(
        WorkerConfig::resolve_secret(env_key),
        Some("env-literal-token".to_string())
    );

    std::env::remove_var(env_key);
}

/// Challenge 5: Workspace isolation, cleanup boundary invariant verification
#[tokio::test]
async fn test_challenge_5_workspace_isolation_and_cleanup_boundary() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = make_test_config(temp_workspace.path().to_path_buf());
    let runner = StageRunner::new(config.clone());
    let (log_tx, _log_rx) = mpsc::channel(32);

    let stages_dir = config.workspace_dir.join("stages");
    fs::create_dir_all(&stages_dir).unwrap();

    // Verify boundary validation logic:
    // Any cleanup target MUST satisfy: `stage_dir.starts_with(&stages_dir) && stage_dir != stages_dir`
    let is_safe_cleanup =
        |path: &Path| -> bool { path.starts_with(&stages_dir) && path != stages_dir };

    // Outside paths must be refused:
    assert!(
        !is_safe_cleanup(Path::new("/tmp")),
        "Cleanup of /tmp must be refused"
    );
    assert!(
        !is_safe_cleanup(Path::new("/")),
        "Cleanup of / must be refused"
    );
    assert!(
        !is_safe_cleanup(Path::new("/etc")),
        "Cleanup of /etc must be refused"
    );
    assert!(
        !is_safe_cleanup(&stages_dir),
        "Cleanup of stages_dir itself must be refused"
    );
    assert!(
        !is_safe_cleanup(&config.workspace_dir),
        "Cleanup of workspace root must be refused"
    );

    // Legitimate stage workspace must be allowed:
    let legitimate_stage = stages_dir.join("job-1_stage-1");
    assert!(
        is_safe_cleanup(&legitimate_stage),
        "Legitimate stage dir should be allowed"
    );

    // Execute a legitimate stage and verify post-cleanup state:
    let task = StageDispatchTask {
        job_id: "legit-job".to_string(),
        stage_id: "legit-stage".to_string(),
        stage_name: "Clean Run".to_string(),
        repo_name: "LMA1980/test-repo".to_string(),
        clone_url: "".to_string(),
        commit_sha: "head".to_string(),
        command: "echo 'execution clean'".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 5,
        threads: 1,
        toolchain: None,
        exclusive: false,
    };

    let res = runner.run_stage(&task, log_tx, None).await;
    assert_eq!(res.exit_code, 0);

    // stages_dir must still exist!
    assert!(
        stages_dir.exists(),
        "stages_dir must not be deleted during stage cleanup"
    );
    assert!(
        temp_workspace.path().exists(),
        "workspace root must not be deleted"
    );

    // The individual stage directory should have been cleaned up
    let created_stage_dir = stages_dir.join("legit-job_legit-stage");
    assert!(
        !created_stage_dir.exists(),
        "Stage directory must be cleaned up after execution"
    );
}

/// Challenge 6: Concurrent adversarial attack fuzzing
#[tokio::test]
async fn test_challenge_6_concurrent_adversarial_fuzzing() {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = make_test_config(temp_workspace.path().to_path_buf());
    let runner = std::sync::Arc::new(StageRunner::new(config));

    let malicious_payloads = vec![
        (
            "../../../../tmp",
            "s1",
            "LMA1980/test-repo",
            Some("../../etc"),
        ),
        ("j1", "s1/../../escape", "LMA1980/test-repo", None),
        ("j1", "s1", "../../etc/passwd", None),
        ("j1", "s1", "LMA1980/test-repo", Some("/etc")),
        ("j1", "s1", "LMA1980/test-repo", Some("/etc/shadow")),
        ("j1/escape", "s1", "LMA1980/test-repo", None),
        ("j1", "s1", "LMA1980/bad;rm", None),
        ("j1", "s1", "LMA1980/repo", Some("..")),
        ("j1", "s1", "LMA1980/repo", Some("dir\0null")),
        ("j1\0evil", "s1", "LMA1980/repo", None),
    ];

    let mut handles = Vec::new();
    for (job_id, stage_id, repo, working_dir) in malicious_payloads {
        let runner_clone = runner.clone();
        let (tx, _rx) = mpsc::channel(16);
        let task = StageDispatchTask {
            job_id: job_id.to_string(),
            stage_id: stage_id.to_string(),
            stage_name: "Fuzz Task".to_string(),
            repo_name: repo.to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: "echo should_never_run".to_string(),
            working_dir: working_dir.map(String::from),
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };

        handles.push(tokio::spawn(async move {
            runner_clone.run_stage(&task, tx, None).await
        }));
    }

    for handle in handles {
        let res = handle.await.expect("join handle ok");
        assert_eq!(
            res.exit_code, -1,
            "All adversarial tasks in fuzz test must exit with -1"
        );
        assert!(res.error_message.is_some());
    }
}
