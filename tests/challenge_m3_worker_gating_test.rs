use action_runner::config::{IsolationMode, WorkerConfig};
use action_runner::worker::WorkerAgent;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::TryAcquireError;

fn make_test_config(max_slots: u32) -> WorkerConfig {
    WorkerConfig {
        controller_url: "ws://127.0.0.1:8000/api/cluster/tunnel".to_string(),
        cluster_token: "test-token".to_string(),
        node_id: format!("test-worker-slots-{}", max_slots),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots,
        workspace_dir: std::path::PathBuf::from("/tmp/test-workspace"),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    }
}

/// CHALLENGE 1: Saturated multi-slot worker (max_slots = 4, max_slots = 8)
/// Rapid alternating bursts of shared and exclusive permit acquisitions.
#[tokio::test]
async fn test_challenge_1_rapid_alternating_bursts_multi_slot() {
    for &slots in &[4u32, 8u32] {
        let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));
        let total_slots = slots as usize;

        assert_eq!(
            worker.available_permits(),
            total_slots,
            "Initial permits must equal max_slots"
        );

        // 100 alternating burst cycles
        for cycle in 0..100 {
            // --- Burst 1: Full saturation with shared permits ---
            let mut shared_permits = Vec::with_capacity(total_slots);
            for i in 0..total_slots {
                let p = worker.acquire_permit(false).await.unwrap_or_else(|e| {
                    panic!("Cycle {cycle}: failed to acquire shared permit {i}: {e}")
                });
                shared_permits.push(p);
            }

            // Assert fully saturated
            assert_eq!(
                worker.available_permits(),
                0,
                "Cycle {cycle}: available permits must be 0 after saturating shared permits"
            );
            assert!(
                worker.try_acquire_permit(false).is_err(),
                "Cycle {cycle}: 1-permit acquisition must fail when saturated"
            );
            assert!(
                worker.try_acquire_permit(true).is_err(),
                "Cycle {cycle}: exclusive acquisition must fail when saturated"
            );

            // Release all shared permits
            drop(shared_permits);

            assert_eq!(
                worker.available_permits(),
                total_slots,
                "Cycle {cycle}: available permits must be restored after dropping shared permits"
            );

            // --- Burst 2: Full-node exclusive lock ---
            let exc_permit = worker.acquire_permit(true).await.unwrap_or_else(|e| {
                panic!("Cycle {cycle}: failed to acquire exclusive permit: {e}")
            });

            assert_eq!(
                worker.available_permits(),
                0,
                "Cycle {cycle}: available permits must be 0 while holding exclusive permit"
            );
            assert!(
                worker.try_acquire_permit(false).is_err(),
                "Cycle {cycle}: shared acquisition must fail while exclusive is held"
            );
            assert!(
                worker.try_acquire_permit(true).is_err(),
                "Cycle {cycle}: exclusive acquisition must fail while exclusive is held"
            );

            // Release exclusive permit
            drop(exc_permit);

            assert_eq!(
                worker.available_permits(),
                total_slots,
                "Cycle {cycle}: available permits must be restored after dropping exclusive permit"
            );
        }
    }
}

/// CHALLENGE 2: Total lockout during exclusive permit holding
/// Verify that while an exclusive permit is held, ALL other acquisitions
/// (`try_acquire_owned`, `try_acquire_many_owned`) return error.
#[tokio::test]
async fn test_challenge_2_exclusive_permit_total_lockout() {
    for &slots in &[1u32, 4u32, 8u32] {
        let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));
        let total_slots = slots as usize;

        // Acquire exclusive permit
        let exc_permit = worker
            .acquire_permit(true)
            .await
            .expect("acquired exclusive");
        assert_eq!(worker.available_permits(), 0);

        // 1. Worker helper try_acquire_permit(false) -> Err(NoPermits)
        match worker.try_acquire_permit(false) {
            Err(TryAcquireError::NoPermits) => {}
            other => panic!(
                "Slots {slots}: Expected TryAcquireError::NoPermits for shared permit, got {other:?}"
            ),
        }

        // 2. Worker helper try_acquire_permit(true) -> Err(NoPermits)
        match worker.try_acquire_permit(true) {
            Err(TryAcquireError::NoPermits) => {}
            other => panic!(
                "Slots {slots}: Expected TryAcquireError::NoPermits for exclusive permit, got {other:?}"
            ),
        }

        // 3. Raw execution_gate().try_acquire_owned() -> Err(NoPermits)
        match worker.execution_gate().try_acquire_owned() {
            Err(TryAcquireError::NoPermits) => {}
            other => panic!(
                "Slots {slots}: Expected TryAcquireError::NoPermits for raw gate try_acquire_owned, got {other:?}"
            ),
        }

        // 4. Raw execution_gate().try_acquire_many_owned(k) for all k in 1..=slots -> Err(NoPermits)
        for k in 1..=slots {
            match worker.execution_gate().try_acquire_many_owned(k) {
                Err(TryAcquireError::NoPermits) => {}
                other => panic!(
                    "Slots {slots}: Expected TryAcquireError::NoPermits for try_acquire_many_owned({k}), got {other:?}"
                ),
            }
        }

        // 5. Asynchronous acquisition with timeout must time out
        let worker_clone1 = worker.clone();
        let timeout_shared = tokio::time::timeout(
            Duration::from_millis(50),
            worker_clone1.acquire_permit(false),
        )
        .await;
        assert!(
            timeout_shared.is_err(),
            "Slots {slots}: Shared async acquire must time out while exclusive permit is held"
        );

        let worker_clone2 = worker.clone();
        let timeout_exclusive = tokio::time::timeout(
            Duration::from_millis(50),
            worker_clone2.acquire_permit(true),
        )
        .await;
        assert!(
            timeout_exclusive.is_err(),
            "Slots {slots}: Exclusive async acquire must time out while exclusive permit is held"
        );

        // Release exclusive permit
        drop(exc_permit);

        // Immediately verify all permits restored
        assert_eq!(worker.available_permits(), total_slots);
        assert!(
            worker.try_acquire_permit(true).is_ok(),
            "Slots {slots}: Exclusive permit must be immediately acquirable after release"
        );
    }
}

/// CHALLENGE 3: Exclusive tasks wait for all shared tasks to release before acquiring
/// Partial occupancy must block exclusive acquisition until the very last shared permit is released.
#[tokio::test]
async fn test_challenge_3_exclusive_waits_for_all_shared_drain() {
    for &slots in &[4u32, 8u32] {
        let total_slots = slots as usize;

        // Test every possible partial occupancy level from 1 to max_slots
        for partial_fill in 1..=total_slots {
            let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));

            // Acquire partial_fill shared permits
            let mut shared_permits = Vec::with_capacity(partial_fill);
            for _ in 0..partial_fill {
                shared_permits.push(worker.acquire_permit(false).await.unwrap());
            }

            assert_eq!(worker.available_permits(), total_slots - partial_fill);

            // Spawn an exclusive task waiting for all permits
            let worker_clone = worker.clone();
            let exclusive_acquired = Arc::new(AtomicBool::new(false));
            let exclusive_acquired_clone = exclusive_acquired.clone();

            let exc_handle = tokio::spawn(async move {
                let _p = worker_clone.acquire_permit(true).await.unwrap();
                exclusive_acquired_clone.store(true, Ordering::SeqCst);
                // Hold briefly
                tokio::time::sleep(Duration::from_millis(20)).await;
            });

            // Allow the exclusive task to queue
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(30)).await;

            assert!(
                !exclusive_acquired.load(Ordering::SeqCst),
                "Slots {slots}, partial_fill {partial_fill}: Exclusive task MUST NOT acquire while shared permits are held"
            );

            // Release shared permits one by one, verifying exclusive task remains blocked until the LAST permit
            for (idx, permit) in shared_permits.drain(..).enumerate() {
                drop(permit);
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(10)).await;

                if idx < partial_fill - 1 {
                    assert!(
                        !exclusive_acquired.load(Ordering::SeqCst),
                        "Slots {slots}: Exclusive task must remain blocked after dropping {idx} of {partial_fill} shared permits"
                    );
                }
            }

            // Now that all shared permits are dropped, exclusive task MUST acquire and complete
            let join_res = tokio::time::timeout(Duration::from_secs(2), exc_handle).await;
            assert!(
                join_res.is_ok(),
                "Slots {slots}: Exclusive task did not complete after shared permits drained"
            );
            assert!(
                exclusive_acquired.load(Ordering::SeqCst),
                "Slots {slots}: Exclusive task must have acquired all permits after full drain"
            );

            // Once exclusive task completes, all permits are restored
            assert_eq!(worker.available_permits(), total_slots);
        }
    }
}

/// CHALLENGE 4: Abrupt task abort / panic simulation
/// Hard cancellation (`handle.abort()`) during exclusive and shared execution
/// must deterministically restore all permits without leakage or poisoning.
#[tokio::test]
async fn test_challenge_4_abrupt_abort_and_panic_determinism() {
    for &slots in &[4u32, 8u32] {
        let total_slots = slots as usize;

        // --- 4A: Hard cancellation of exclusive task ---
        {
            let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));
            let worker_clone = worker.clone();
            let started = Arc::new(AtomicBool::new(false));
            let started_clone = started.clone();

            let handle = tokio::spawn(async move {
                let _permit = worker_clone.acquire_permit(true).await.unwrap();
                started_clone.store(true, Ordering::SeqCst);
                // Hang indefinitely until aborted
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            });

            // Wait until exclusive permit is acquired
            while !started.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(worker.available_permits(), 0);

            // Hard abort
            handle.abort();
            let join_res = handle.await;
            assert!(join_res.unwrap_err().is_cancelled());

            // Permits must be deterministically restored immediately
            assert_eq!(
                worker.available_permits(),
                total_slots,
                "Slots {slots}: All permits must be restored after aborting exclusive task"
            );

            // Verify clean immediate re-acquisition
            let p = worker.try_acquire_permit(true);
            assert!(
                p.is_ok(),
                "Slots {slots}: Exclusive permit must be re-acquirable"
            );
            drop(p);
        }

        // --- 4B: Incremental hard cancellation of individual shared tasks ---
        {
            let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));
            let mut handles = Vec::with_capacity(total_slots);
            let active_count = Arc::new(AtomicU32::new(0));

            for _ in 0..total_slots {
                let worker_clone = worker.clone();
                let active = active_count.clone();
                let h = tokio::spawn(async move {
                    let _permit = worker_clone.acquire_permit(false).await.unwrap();
                    active.fetch_add(1, Ordering::SeqCst);
                    loop {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    }
                });
                handles.push(h);
            }

            while active_count.load(Ordering::SeqCst) < slots {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(worker.available_permits(), 0);

            // Abort handles one by one, checking available_permits increments exactly by 1
            for (i, h) in handles.into_iter().enumerate() {
                h.abort();
                let _ = h.await;
                let expected_available = i + 1;
                assert_eq!(
                    worker.available_permits(),
                    expected_available,
                    "Slots {slots}: Expected {expected_available} permits available after aborting {i} shared tasks"
                );
            }
            assert_eq!(worker.available_permits(), total_slots);
        }

        // --- 4C: Hard cancellation while WAITING in queue for permit ---
        {
            let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));

            // Hold 1 shared permit so exclusive waiter will block
            let held_shared = worker.acquire_permit(false).await.unwrap();
            assert_eq!(worker.available_permits(), total_slots - 1);

            let worker_clone = worker.clone();
            let exc_handle = tokio::spawn(async move {
                // This blocks because held_shared is not released
                let _p = worker_clone.acquire_permit(true).await.unwrap();
            });

            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(30)).await;

            // Abort the waiting exclusive task while queued
            exc_handle.abort();
            let _ = exc_handle.await;

            // Now release the held shared permit
            drop(held_shared);

            // Semaphore must be completely healthy with all permits intact
            assert_eq!(
                worker.available_permits(),
                total_slots,
                "Slots {slots}: Aborting queued waiter must not leak or corrupt permits"
            );
            assert!(worker.try_acquire_permit(true).is_ok());
        }

        // --- 4D: Panic inside task holding exclusive permit ---
        {
            let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));
            let worker_clone = worker.clone();

            let panic_handle = tokio::spawn(async move {
                let _permit = worker_clone.acquire_permit(true).await.unwrap();
                panic!("Simulated intentional panic in exclusive stage task");
            });

            let join_res = panic_handle.await;
            assert!(
                join_res.unwrap_err().is_panic(),
                "Slots {slots}: Task must have panicked"
            );

            // Permitted must be fully restored by unwind Drop
            assert_eq!(
                worker.available_permits(),
                total_slots,
                "Slots {slots}: All permits must be restored after exclusive task panics"
            );
            assert!(worker.try_acquire_permit(true).is_ok());
        }
    }
}

/// CHALLENGE 5: High-concurrency randomized stress oracle
/// 40 concurrent workers randomly requesting shared vs exclusive permits under continuous invariant checking.
#[tokio::test]
async fn test_challenge_5_high_concurrency_stress_oracle() {
    let slots = 8u32;
    let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));

    let active_shared = Arc::new(AtomicU32::new(0));
    let active_exclusive = Arc::new(AtomicU32::new(0));
    let invariant_violation = Arc::new(AtomicBool::new(false));

    let num_tasks = 40;
    let iterations_per_task = 50;
    let mut handles = Vec::with_capacity(num_tasks);

    for task_idx in 0..num_tasks {
        let worker = worker.clone();
        let active_shared = active_shared.clone();
        let active_exclusive = active_exclusive.clone();
        let invariant_violation = invariant_violation.clone();

        let h = tokio::spawn(async move {
            for iter in 0..iterations_per_task {
                // 25% exclusive, 75% shared
                let is_exclusive = (task_idx + iter) % 4 == 0;

                let permit = worker
                    .acquire_permit(is_exclusive)
                    .await
                    .expect("acquired permit");

                if is_exclusive {
                    let prev_exc = active_exclusive.fetch_add(1, Ordering::SeqCst);
                    let curr_shared = active_shared.load(Ordering::SeqCst);

                    if prev_exc != 0 || curr_shared != 0 {
                        eprintln!(
                            "INVARIANT VIOLATION: Exclusive acquired while active_exclusive={} active_shared={}",
                            prev_exc + 1,
                            curr_shared
                        );
                        invariant_violation.store(true, Ordering::SeqCst);
                    }

                    // Simulate micro-work
                    tokio::task::yield_now().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;

                    active_exclusive.fetch_sub(1, Ordering::SeqCst);
                } else {
                    let curr_shared = active_shared.fetch_add(1, Ordering::SeqCst) + 1;
                    let curr_exc = active_exclusive.load(Ordering::SeqCst);

                    if curr_exc != 0 || curr_shared > slots {
                        eprintln!(
                            "INVARIANT VIOLATION: Shared acquired while active_exclusive={} active_shared={}",
                            curr_exc,
                            curr_shared
                        );
                        invariant_violation.store(true, Ordering::SeqCst);
                    }

                    // Simulate micro-work
                    tokio::task::yield_now().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;

                    active_shared.fetch_sub(1, Ordering::SeqCst);
                }

                drop(permit);
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.await.expect("task join succeeded");
    }

    assert!(
        !invariant_violation.load(Ordering::SeqCst),
        "Invariant violation detected during high-concurrency stress test!"
    );
    assert_eq!(
        active_shared.load(Ordering::SeqCst),
        0,
        "Active shared must be 0 after stress test"
    );
    assert_eq!(
        active_exclusive.load(Ordering::SeqCst),
        0,
        "Active exclusive must be 0 after stress test"
    );
    assert_eq!(
        worker.available_permits(),
        slots as usize,
        "All permits must be restored after stress test"
    );
}

/// CHALLENGE 6: Multiple queued exclusive tasks serialize deterministically
/// When multiple exclusive tasks wait behind shared tasks, they must execute one-by-one.
#[tokio::test]
async fn test_challenge_6_multiple_queued_exclusive_tasks_serialize() {
    let slots = 4u32;
    let worker = Arc::new(WorkerAgent::new(make_test_config(slots)));

    // Hold shared permit
    let shared_permit = worker.acquire_permit(false).await.unwrap();
    assert_eq!(worker.available_permits(), 3);

    let active_exclusive = Arc::new(AtomicU32::new(0));
    let execution_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut exc_handles = Vec::new();
    for id in 1..=3 {
        let worker = worker.clone();
        let active_exclusive = active_exclusive.clone();
        let execution_order = execution_order.clone();

        let h = tokio::spawn(async move {
            let _permit = worker.acquire_permit(true).await.unwrap();
            let current = active_exclusive.fetch_add(1, Ordering::SeqCst) + 1;
            assert_eq!(
                current, 1,
                "Never more than 1 exclusive task running concurrently!"
            );
            execution_order.lock().await.push(id);
            tokio::time::sleep(Duration::from_millis(20)).await;
            active_exclusive.fetch_sub(1, Ordering::SeqCst);
        });
        exc_handles.push(h);
    }

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        active_exclusive.load(Ordering::SeqCst),
        0,
        "No exclusive tasks should run while shared permit is held"
    );

    // Release shared permit -> exclusive tasks unblock one by one
    drop(shared_permit);

    for h in exc_handles {
        h.await.unwrap();
    }

    let order = execution_order.lock().await.clone();
    assert_eq!(order.len(), 3, "All 3 exclusive tasks must have completed");
    assert_eq!(
        worker.available_permits(),
        4,
        "All 4 permits restored after all exclusive tasks finish"
    );
}

/// CHALLENGE 7: End-to-end WebSocket tunnel dispatch and exclusivity gating
/// Spin up a mock controller WebSocket server, connect `WorkerAgent::connect_and_process`,
/// dispatch concurrent shared tasks and an exclusive task, and verify gating on the wire.
#[tokio::test]
async fn test_challenge_7_e2e_worker_tunnel_dispatch_and_exclusivity() {
    use action_runner_ipc::{
        create_fory_runtime, IpcHeader, StageDispatchTask, WorkerRegistration,
        MSG_TYPE_STAGE_COMPLETION, MSG_TYPE_STAGE_DISPATCH, MSG_TYPE_STAGE_STARTED,
        MSG_TYPE_WORKER_HEARTBEAT, MSG_TYPE_WORKER_REGISTRATION,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/cluster/tunnel", port);

    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: ws_url,
        cluster_token: "mock-cluster-token".to_string(),
        node_id: "test-node-m3".to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 2,
        workspace_dir: temp_workspace.path().to_path_buf(),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };

    let worker = Arc::new(WorkerAgent::new(config));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Mock controller background task
    let mock_controller = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let fory = create_fory_runtime().unwrap();

        // 1. Receive Registration
        let msg = ws.next().await.unwrap().unwrap();
        let bin = match msg {
            Message::Binary(b) => b,
            _ => panic!("Expected binary message for registration"),
        };
        let (hdr, payload) = IpcHeader::parse(&bin).unwrap();
        assert_eq!(hdr.msg_type, MSG_TYPE_WORKER_REGISTRATION);
        let reg = fory.deserialize::<WorkerRegistration>(payload).unwrap();
        assert_eq!(reg.node_id, "test-node-m3");
        assert_eq!(reg.max_slots, 2);

        // 2. Dispatch a long-running shared task (sleep 0.3s)
        let shared_task = StageDispatchTask {
            job_id: "job-shared-1".to_string(),
            stage_id: "stage-shared-1".to_string(),
            stage_name: "Shared Sleep".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: "sleep 0.3".to_string(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: false,
        };
        let s_payload = fory.serialize(&shared_task).unwrap();
        let s_hdr = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, s_payload.len() as u32, 0, 101);
        let mut s_frame = Vec::new();
        s_frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&s_hdr));
        s_frame.extend_from_slice(&s_payload);
        ws.send(Message::Binary(s_frame.into())).await.unwrap();

        // 3. Immediately dispatch an exclusive task (command: echo 'exclusive done')
        let exc_task = StageDispatchTask {
            job_id: "job-exc-1".to_string(),
            stage_id: "stage-exc-1".to_string(),
            stage_name: "Exclusive Stage".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: "".to_string(),
            commit_sha: "head".to_string(),
            command: "echo 'exclusive done'".to_string(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 5,
            threads: 1,
            toolchain: None,
            exclusive: true,
        };
        let e_payload = fory.serialize(&exc_task).unwrap();
        let e_hdr = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, e_payload.len() as u32, 0, 102);
        let mut e_frame = Vec::new();
        e_frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&e_hdr));
        e_frame.extend_from_slice(&e_payload);
        ws.send(Message::Binary(e_frame.into())).await.unwrap();

        // 4. Collect inbound messages from worker
        let mut events = Vec::new();
        let start_time = tokio::time::Instant::now();
        let mut shared_started_at = None;
        let mut shared_completed_at = None;
        let mut exc_started_at = None;

        while let Some(Ok(m)) = ws.next().await {
            if let Message::Binary(b) = m {
                let (hdr, _) = IpcHeader::parse(&b).unwrap();
                if hdr.msg_type == MSG_TYPE_STAGE_STARTED {
                    let now = start_time.elapsed();
                    if shared_started_at.is_none() {
                        shared_started_at = Some(now);
                        events.push("shared_started");
                    } else if exc_started_at.is_none() {
                        exc_started_at = Some(now);
                        events.push("exclusive_started");
                    }
                } else if hdr.msg_type == MSG_TYPE_STAGE_COMPLETION {
                    let now = start_time.elapsed();
                    if shared_completed_at.is_none() {
                        shared_completed_at = Some(now);
                        events.push("shared_completed");
                    } else {
                        events.push("exclusive_completed");
                        break;
                    }
                } else if hdr.msg_type == MSG_TYPE_WORKER_HEARTBEAT {
                    // Ignore heartbeats in event sequence
                }
            }
        }

        (
            events,
            shared_started_at.unwrap(),
            shared_completed_at.unwrap(),
            exc_started_at.unwrap(),
        )
    });

    // Run worker tunnel
    let worker_clone = worker.clone();
    let worker_task = tokio::spawn(async move {
        let _ = worker_clone.connect_and_process(&mut shutdown_rx).await;
    });

    let (events, _shared_started, shared_completed, exc_started) =
        tokio::time::timeout(Duration::from_secs(5), mock_controller)
            .await
            .expect("mock controller completed within timeout")
            .expect("mock controller joined");

    // Invariant check: events must occur in exact order:
    // shared_started -> shared_completed -> exclusive_started -> exclusive_completed
    assert_eq!(
        events,
        vec![
            "shared_started",
            "shared_completed",
            "exclusive_started",
            "exclusive_completed"
        ],
        "Exclusive task must NOT start until shared task completes!"
    );

    assert!(
        exc_started >= shared_completed,
        "Exclusive stage started at {:?} which is before shared completed at {:?}",
        exc_started,
        shared_completed
    );

    // Shutdown worker
    let _ = shutdown_tx.send(true);
    let _ = worker_task.await;

    // Verify permits restored
    assert_eq!(worker.available_permits(), 2);
}
