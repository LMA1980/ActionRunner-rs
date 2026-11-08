use futures_util::{SinkExt, StreamExt};
use mu_ci_ipc::{IpcHeader, MSG_TYPE_WORKER_REGISTRATION};
use mu_ci_worker::config::{IsolationMode, WorkerConfig};
use mu_ci_worker::worker::WorkerAgent;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

// Mutex to serialize tests that modify process environment variables
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn make_test_config(ws_url: String, node_id: &str) -> (WorkerConfig, tempfile::TempDir) {
    let temp_workspace = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        controller_url: ws_url,
        cluster_token: "mock-cluster-token".to_string(),
        node_id: node_id.to_string(),
        arch: "x86_64".to_string(),
        tags: vec!["test".to_string()],
        max_slots: 1,
        workspace_dir: temp_workspace.path().to_path_buf(),
        toolchains: vec!["system".to_string()],
        use_btrfs: false,
        isolation_mode: IsolationMode::None,
    };
    (config, temp_workspace)
}

/// Scenario A1: Read liveness timeout enforcement under coordinator gray-failure.
/// When the coordinator stops transmitting frames after handshake, the worker must
/// abort connect_and_process within the configured liveness timeout and return io::ErrorKind::TimedOut.
#[tokio::test]
async fn test_scenario_a_read_liveness_timeout_aborts_hung_coordinator() {
    let _lock = ENV_LOCK.lock().await;
    let prev_val = std::env::var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS").ok();
    std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", "2");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/tunnel/ws", port);

    let (config, _temp_ws) = make_test_config(ws_url, "worker-liveness-short");
    let worker = WorkerAgent::new(config);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Mock coordinator: accepts WS connection, receives registration, then goes completely silent
    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Receive Registration frame
        let msg = ws.next().await.unwrap().unwrap();
        match msg {
            Message::Binary(bin) => {
                let (hdr, _) = IpcHeader::parse(&bin).unwrap();
                assert_eq!(hdr.msg_type, MSG_TYPE_WORKER_REGISTRATION);
            }
            other => panic!("Expected Binary frame for registration, got {:?}", other),
        }

        // 2. Intentionally hang and NEVER send any Pong, Ping, or data
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    let start = Instant::now();
    let res = worker.connect_and_process(&mut shutdown_rx).await;
    let elapsed = start.elapsed();

    // Restore environment variable
    match prev_val {
        Some(v) => std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", v),
        None => std::env::remove_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS"),
    }

    // Worker must return Err with TimedOut
    assert!(
        res.is_err(),
        "Worker connect_and_process must return Err on liveness timeout, but returned: {:?}",
        res
    );
    let err = res.unwrap_err();
    let err_str = err.to_string();
    let is_timed_out = err
        .downcast_ref::<std::io::Error>()
        .map(|e| e.kind() == std::io::ErrorKind::TimedOut)
        .unwrap_or(false)
        || err_str.to_lowercase().contains("timed out")
        || err_str.to_lowercase().contains("deadline");

    assert!(
        is_timed_out,
        "Expected std::io::ErrorKind::TimedOut, got: {}",
        err_str
    );
    assert!(
        elapsed >= Duration::from_millis(1900),
        "Timeout triggered too fast: {:?}",
        elapsed
    );
    assert!(
        elapsed <= Duration::from_millis(4500),
        "Timeout took too long (should be ~2s, max 4.5s): {:?}",
        elapsed
    );

    server_handle.abort();
}

/// Scenario A2: Inbound Pong frames refresh read-liveness deadline.
/// When the coordinator sends Pong frames at intervals shorter than liveness timeout,
/// the worker must remain connected without timing out.
#[tokio::test]
async fn test_scenario_a_inbound_pong_refreshes_liveness() {
    let _lock = ENV_LOCK.lock().await;
    let prev_val = std::env::var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS").ok();
    std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", "3");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/tunnel/ws", port);

    let (config, _temp_ws) = make_test_config(ws_url, "worker-liveness-refresh");
    let worker = WorkerAgent::new(config);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Mock coordinator: sends Pong every 1.5s to keep worker alive for 5.0s (> 3s timeout)
    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Receive Registration
        let _ = ws.next().await.unwrap().unwrap();

        // 2. Send 4 Pongs spaced by 1.5s
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            if ws.send(Message::Pong(vec![].into())).await.is_err() {
                break;
            }
        }

        // 3. Keep listening until shutdown
        while ws.next().await.is_some() {}
    });

    let worker_handle =
        tokio::spawn(async move { worker.connect_and_process(&mut shutdown_rx).await });

    // Wait 5.0s (well beyond the 3s timeout) - worker should still be running!
    tokio::time::sleep(Duration::from_millis(5000)).await;
    assert!(
        !worker_handle.is_finished(),
        "Worker disconnected prematurely despite receiving periodic Pong frames!"
    );

    // Trigger clean shutdown
    let _ = shutdown_tx.send(true);
    let res = tokio::time::timeout(Duration::from_secs(3), worker_handle)
        .await
        .expect("Worker stopped cleanly within timeout")
        .expect("Worker task completed");

    match prev_val {
        Some(v) => std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", v),
        None => std::env::remove_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS"),
    }

    assert!(res.is_ok(), "Expected clean shutdown, got {:?}", res);
    server_handle.abort();
}

/// Scenario B1: Worker periodically transmits Message::Ping.
/// The mock coordinator asserts incoming Message::Ping frames arrive periodically
/// and responds with Message::Pong to maintain session health.
#[tokio::test]
async fn test_scenario_b_periodic_ping_and_pong_exchange() {
    let _lock = ENV_LOCK.lock().await;
    let prev_ping = std::env::var("ACTION_RUNNER_PING_INTERVAL_SECS").ok();
    let prev_live = std::env::var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS").ok();
    std::env::set_var("ACTION_RUNNER_PING_INTERVAL_SECS", "2");
    std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", "15");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/tunnel/ws", port);

    let (config, _temp_ws) = make_test_config(ws_url, "worker-ping-pong");
    let worker = WorkerAgent::new(config);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let pings_received = Arc::new(AtomicU32::new(0));
    let pings_clone = pings_received.clone();

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Receive Registration
        let _ = ws.next().await.unwrap().unwrap();

        // 2. Loop and count incoming Message::Ping frames, replying with Pong
        while let Some(Ok(msg)) = ws.next().await {
            match msg {
                Message::Ping(payload) => {
                    pings_clone.fetch_add(1, Ordering::SeqCst);
                    let _ = ws.send(Message::Pong(payload)).await;
                }
                Message::Binary(bin) => {
                    let _ = IpcHeader::parse(&bin);
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    let worker_handle =
        tokio::spawn(async move { worker.connect_and_process(&mut shutdown_rx).await });

    // Wait for at least 2 ping frames (up to 12s)
    let start = Instant::now();
    while pings_received.load(Ordering::SeqCst) < 2 && start.elapsed() < Duration::from_secs(12) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let count = pings_received.load(Ordering::SeqCst);
    assert!(
        count >= 2,
        "Expected at least 2 Message::Ping frames from worker, but received {}",
        count
    );

    // Clean shutdown
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), worker_handle).await;

    match prev_ping {
        Some(v) => std::env::set_var("ACTION_RUNNER_PING_INTERVAL_SECS", v),
        None => std::env::remove_var("ACTION_RUNNER_PING_INTERVAL_SECS"),
    }
    match prev_live {
        Some(v) => std::env::set_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS", v),
        None => std::env::remove_var("ACTION_RUNNER_LIVENESS_TIMEOUT_SECS"),
    }
    server_handle.abort();
}

/// Scenario B2: Coordinator-initiated Message::Ping receives immediate Message::Pong reply.
#[tokio::test]
async fn test_scenario_b_coordinator_probe_ping_replied_with_pong() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/tunnel/ws", port);

    let (config, _temp_ws) = make_test_config(ws_url, "worker-probe-ping");
    let worker = WorkerAgent::new(config);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let probe_payload = b"k3-probe-ping-payload-42".to_vec();
    let probe_clone = probe_payload.clone();

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Receive Registration
        let _ = ws.next().await.unwrap().unwrap();

        // 2. Send Message::Ping from coordinator to worker
        ws.send(Message::Ping(probe_clone.clone().into()))
            .await
            .unwrap();

        // 3. Await Message::Pong with identical payload
        let mut got_pong = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if let Some(Ok(Message::Pong(data))) = ws.next().await {
                assert_eq!(
                    data.as_ref(),
                    probe_clone.as_slice(),
                    "Worker Pong payload must match coordinator Ping payload"
                );
                got_pong = true;
                break;
            }
        }
        assert!(
            got_pong,
            "Coordinator did not receive expected Pong response within 3s"
        );
    });

    let worker_handle =
        tokio::spawn(async move { worker.connect_and_process(&mut shutdown_rx).await });

    let server_res = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
    assert!(server_res.is_ok(), "Server task timed out");
    server_res.unwrap().unwrap();

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
}

/// Scenario C: Outbound write timeout enforcement on stalled TCP window.
/// When the mock coordinator's TCP receive window fills up and stalls,
/// outbound frame sends from the worker must time out within bounded time.
#[tokio::test]
async fn test_scenario_c_outbound_write_timeout_on_stalled_socket() {
    let _lock = ENV_LOCK.lock().await;
    let prev_write = std::env::var("ACTION_RUNNER_WRITE_TIMEOUT_SECS").ok();
    let prev_mu_write = std::env::var("MU_CI_WRITE_TIMEOUT_SECS").ok();
    std::env::set_var("ACTION_RUNNER_WRITE_TIMEOUT_SECS", "2");
    std::env::set_var("MU_CI_WRITE_TIMEOUT_SECS", "2");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ws_url = format!("ws://127.0.0.1:{}/api/tunnel/ws", port);

    let (mut config, _temp_ws) = make_test_config(ws_url, "worker-write-timeout");
    // Ensure outbound registration frame exceeds the loopback TCP socket buffer
    // so ws_sender.send blocks and times out within ACTION_RUNNER_WRITE_TIMEOUT_SECS.
    // 10 MB reliably exceeds Linux max tcp_wmem ceiling (typically ~4 MB).
    config.tags = vec!["x".repeat(10_000_000)];
    let worker = WorkerAgent::new(config);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        // Constrain receive buffer so socket buffer fills quickly on loopback
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = stream.as_raw_fd();
            let size: libc::c_int = 2048;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &size as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&size) as libc::socklen_t,
                );
            }
        }

        let _ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // Freeze and stop reading from the TCP stream so write blocks
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let start = Instant::now();
    let res = worker.connect_and_process(&mut shutdown_rx).await;
    let elapsed = start.elapsed();

    match prev_write {
        Some(v) => std::env::set_var("ACTION_RUNNER_WRITE_TIMEOUT_SECS", v),
        None => std::env::remove_var("ACTION_RUNNER_WRITE_TIMEOUT_SECS"),
    }
    match prev_mu_write {
        Some(v) => std::env::set_var("MU_CI_WRITE_TIMEOUT_SECS", v),
        None => std::env::remove_var("MU_CI_WRITE_TIMEOUT_SECS"),
    }

    assert!(
        res.is_err(),
        "Expected worker to fail with write timeout error, but got {:?}",
        res
    );
    let err_str = res.unwrap_err().to_string().to_lowercase();
    assert!(
        err_str.contains("timed out") || err_str.contains("timeout"),
        "Error message should mention timeout, got: {}",
        err_str
    );
    assert!(
        elapsed <= Duration::from_secs(12),
        "Outbound write timeout exceeded 12s ceiling: {:?}",
        elapsed
    );

    server_handle.abort();
}
