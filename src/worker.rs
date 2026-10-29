use action_runner_ipc::{
    create_fory_runtime, IpcHeader, StageCancelTask, StageCompletionReport, StageDispatchTask,
    StageStarted, WorkerHeartbeat, WorkerRegistration, WorkerShutdown, MSG_TYPE_ARROW_LOG,
    MSG_TYPE_STAGE_CANCEL, MSG_TYPE_STAGE_COMPLETION, MSG_TYPE_STAGE_DISPATCH,
    MSG_TYPE_STAGE_STARTED, MSG_TYPE_WORKER_HEARTBEAT, MSG_TYPE_WORKER_REGISTRATION,
    MSG_TYPE_WORKER_SHUTDOWN,
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::WorkerConfig;
use crate::stage_runner::StageRunner;

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received SIGINT shutdown signal");
                }
                _ = term.recv() => {
                    tracing::info!("Received SIGTERM shutdown signal from OS/systemd");
                }
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Received Ctrl-C shutdown signal");
}

pub struct WorkerAgent {
    config: WorkerConfig,
    execution_gate: Arc<Semaphore>,
    active_slots: Arc<AtomicU32>,
    active_stages: Arc<RwLock<Vec<String>>>,
    active_processes: Arc<RwLock<HashMap<String, u32>>>,
}

impl WorkerAgent {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            // Strictly 1 action (stage) may execute at a time on a worker node
            execution_gate: Arc::new(Semaphore::new(1)),
            active_slots: Arc::new(AtomicU32::new(0)),
            active_stages: Arc::new(RwLock::new(Vec::new())),
            active_processes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run_forever(self) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        });

        let mut backoff = Duration::from_secs(1);

        loop {
            if *shutdown_rx.borrow() {
                info!("Worker shutting down per received signal; terminating main loop");
                break;
            }

            info!(
                controller = %self.config.controller_url,
                "Attempting connection to ActionRunner-controller cluster tunnel..."
            );

            let mut shutdown_watch = shutdown_rx.clone();
            tokio::select! {
                res = self.connect_and_process(&mut shutdown_watch) => {
                    match res {
                        Ok(()) => {
                            if *shutdown_rx.borrow() {
                                info!("Worker shutting down cleanly; exiting");
                                break;
                            }
                            info!("Cluster session concluded cleanly; reconnecting...");
                            backoff = Duration::from_secs(1);
                        }
                        Err(e) => {
                            if *shutdown_rx.borrow() {
                                info!("Worker shutting down; exiting");
                                break;
                            }
                            warn!(
                                error = %e,
                                retry_in = ?backoff,
                                "Cluster connection error; backing off before retry"
                            );
                            tokio::select! {
                                _ = sleep(backoff) => {}
                                _ = shutdown_rx.changed() => {
                                    info!("Shutdown signaled during retry backoff; exiting");
                                    break;
                                }
                            }
                            backoff = (backoff * 2).min(Duration::from_secs(15));
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Shutdown signaled during tunnel operation; exiting");
                    break;
                }
            }
        }
    }

    pub async fn connect_and_process(
        &self,
        shutdown_watch: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut req = self.config.controller_url.clone().into_client_request()?;
        req.headers_mut()
            .insert("x-cluster-token", self.config.cluster_token.parse()?);

        let (ws_stream, _) = connect_async(req).await?;
        info!("Successfully connected to cluster tunnel!");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let fory = create_fory_runtime()?;

        let mut seq = 0u64;

        // 1. Send WorkerRegistration frame
        let reg = WorkerRegistration {
            node_id: self.config.node_id.clone(),
            arch: self.config.arch.clone(),
            tags: self.config.tags.clone(),
            max_slots: self.config.max_slots,
            ram_total_mb: 32768, // Detected or configured
            btrfs_enabled: self.config.use_btrfs,
            toolchains: self.config.toolchains.clone(),
        };

        let reg_payload = fory.serialize(&reg)?;
        seq += 1;
        let reg_hdr = IpcHeader::new(
            MSG_TYPE_WORKER_REGISTRATION,
            reg_payload.len() as u32,
            0,
            seq,
        );

        let mut reg_frame = Vec::with_capacity(action_runner_ipc::HEADER_SIZE + reg_payload.len());
        reg_frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&reg_hdr));
        reg_frame.extend_from_slice(&reg_payload);
        ws_sender.send(Message::Binary(reg_frame.into())).await?;
        info!("Registered with coordinator as '{}'", self.config.node_id);

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(128);

        // Heartbeat task loop
        let node_id = self.config.node_id.clone();
        let active_slots = self.active_slots.clone();
        let hb_tx = outbound_tx.clone();
        let hb_fory = create_fory_runtime()?;

        let hb_handle = tokio::spawn(async move {
            let mut hb_seq = 1000u64;
            loop {
                sleep(Duration::from_secs(5)).await;
                let hb = WorkerHeartbeat {
                    node_id: node_id.clone(),
                    active_slots: active_slots.load(Ordering::Relaxed),
                    cpu_load_pct: 10.0,
                    ram_used_mb: 1024,
                    timestamp_epoch_ms: Utc::now().timestamp_millis() as u64,
                };

                if let Ok(payload) = hb_fory.serialize(&hb) {
                    hb_seq += 1;
                    let hdr =
                        IpcHeader::new(MSG_TYPE_WORKER_HEARTBEAT, payload.len() as u32, 0, hb_seq);
                    let mut frame =
                        Vec::with_capacity(action_runner_ipc::HEADER_SIZE + payload.len());
                    frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&hdr));
                    frame.extend_from_slice(&payload);
                    if hb_tx.send(frame).await.is_err() {
                        break;
                    }
                }
            }
        });

        let stage_runner = Arc::new(StageRunner::new(self.config.clone()));

        loop {
            tokio::select! {
                _ = shutdown_watch.changed() => {
                    if *shutdown_watch.borrow() {
                        info!("Received shutdown signal; notifying coordinator before disconnecting");
                        for pid in self.active_processes.read().await.values() {
                            info!(pid = pid, "Terminating active process group before worker disconnect");
                            #[cfg(unix)]
                            unsafe {
                                let _ = libc::kill(-(*pid as i32), libc::SIGTERM);
                                let _ = libc::kill(*pid as i32, libc::SIGTERM);
                            }
                        }
                        let interrupted = self.active_stages.read().await.clone();
                        let shutdown_payload = WorkerShutdown {
                            node_id: self.config.node_id.clone(),
                            reason: "Worker daemon restarting / shutting down (SIGTERM/SIGINT)".to_string(),
                            interrupted_stages: interrupted.clone(),
                        };
                        if let Ok(payload) = fory.serialize(&shutdown_payload) {
                            seq += 1;
                            let hdr = IpcHeader::new(
                                MSG_TYPE_WORKER_SHUTDOWN,
                                payload.len() as u32,
                                0,
                                seq,
                            );
                            let mut frame = Vec::with_capacity(action_runner_ipc::HEADER_SIZE + payload.len());
                            frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&hdr));
                            frame.extend_from_slice(&payload);
                            let _ = ws_sender.send(Message::Binary(frame.into())).await;
                            let _ = ws_sender.flush().await;
                            info!(node = %self.config.node_id, stages = ?interrupted, "Dispatched WorkerShutdown frame to coordinator");
                        }
                        break;
                    }
                }

                // Outbound messages to send to coordinator
                Some(frame) = outbound_rx.recv() => {
                    ws_sender.send(Message::Binary(frame.into())).await?;
                }

                // Inbound messages received from coordinator
                inbound = ws_receiver.next() => {
                    let msg = match inbound {
                        Some(Ok(m)) => m,
                        _ => break, // Disconnected
                    };

                    if let Message::Binary(bin) = msg {
                        match IpcHeader::parse(&bin) {
                            Ok((hdr, payload)) => {
                                info!(msg_type = hdr.msg_type, payload_len = payload.len(), "Worker received binary frame");
                                if hdr.msg_type == MSG_TYPE_STAGE_DISPATCH {
                                    match fory.deserialize::<StageDispatchTask>(payload) {
                                        Ok(task) => {
                                            info!(stage = %task.stage_id, "Worker successfully deserialized StageDispatchTask");
                                            let runner = stage_runner.clone();
                                            let gate = self.execution_gate.clone();
                                            let out_tx = outbound_tx.clone();
                                            let active_counter = self.active_slots.clone();
                                            let active_stages = self.active_stages.clone();
                                            let active_processes = self.active_processes.clone();
                                            let comp_fory = create_fory_runtime()?;

                                            let (started_tx, started_rx) =
                                                tokio::sync::oneshot::channel::<u32>();
                                            let node_id_started = self.config.node_id.clone();
                                            let out_tx_started = outbound_tx.clone();
                                            let stage_id_started = task.stage_id.clone();
                                            let job_id_started = task.job_id.clone();
                                            let active_proc_reg = active_processes.clone();

                                            tokio::spawn(async move {
                                                if let Ok(pid) = started_rx.await {
                                                    active_proc_reg
                                                        .write()
                                                        .await
                                                        .insert(stage_id_started.clone(), pid);
                                                    let started_event = StageStarted {
                                                        job_id: job_id_started,
                                                        stage_id: stage_id_started,
                                                        node_id: node_id_started,
                                                        pid,
                                                        started_at_epoch_ms: Utc::now()
                                                            .timestamp_millis()
                                                            as u64,
                                                    };
                                                    if let Ok(sfory) = create_fory_runtime() {
                                                        if let Ok(spayload) =
                                                            sfory.serialize(&started_event)
                                                        {
                                                            let shdr = IpcHeader::new(
                                                                MSG_TYPE_STAGE_STARTED,
                                                                spayload.len() as u32,
                                                                0,
                                                                1001,
                                                            );
                                                            let mut sframe = Vec::with_capacity(
                                                                action_runner_ipc::HEADER_SIZE
                                                                    + spayload.len(),
                                                            );
                                                            sframe.extend_from_slice(
                                                                zerocopy::IntoBytes::as_bytes(
                                                                    &shdr,
                                                                ),
                                                            );
                                                            sframe.extend_from_slice(&spayload);
                                                            let _ = out_tx_started
                                                                .send(sframe)
                                                                .await;
                                                        }
                                                    }
                                                }
                                            });

                                            tokio::spawn(async move {
                                                // Enforce single-action execution per node at the worker level
                                                let _permit = gate.acquire_owned().await;
                                                active_counter.fetch_add(1, Ordering::Relaxed);
                                                active_stages
                                                    .write()
                                                    .await
                                                    .push(task.stage_id.clone());

                                                let (log_batch_tx, mut log_batch_rx) =
                                                    mpsc::channel::<Vec<u8>>(32);

                                                // Forward log batches to tunnel outbound channel
                                                let log_forwarder = {
                                                    let out_tx = out_tx.clone();
                                                    tokio::spawn(async move {
                                                        let mut l_seq = 2000u64;
                                                        while let Some(batch_bytes) =
                                                            log_batch_rx.recv().await
                                                        {
                                                            l_seq += 1;
                                                            let log_hdr = IpcHeader::new(
                                                                MSG_TYPE_ARROW_LOG,
                                                                batch_bytes.len() as u32,
                                                                0,
                                                                l_seq,
                                                            );
                                                            let mut frame = Vec::with_capacity(
                                                                action_runner_ipc::HEADER_SIZE
                                                                    + batch_bytes.len(),
                                                            );
                                                            frame.extend_from_slice(
                                                                zerocopy::IntoBytes::as_bytes(
                                                                    &log_hdr,
                                                                ),
                                                            );
                                                            frame.extend_from_slice(&batch_bytes);
                                                            let _ = out_tx.send(frame).await;
                                                        }
                                                    })
                                                };

                                                let res = runner
                                                    .run_stage(
                                                        &task,
                                                        log_batch_tx,
                                                        Some(started_tx),
                                                    )
                                                    .await;
                                                let _ = log_forwarder.await;

                                                active_counter.fetch_sub(1, Ordering::Relaxed);
                                                active_stages
                                                    .write()
                                                    .await
                                                    .retain(|s| s != &task.stage_id);
                                                active_processes
                                                    .write()
                                                    .await
                                                    .remove(&task.stage_id);

                                                let report = StageCompletionReport {
                                                    job_id: task.job_id.clone(),
                                                    stage_id: task.stage_id.clone(),
                                                    exit_code: res.exit_code,
                                                    duration_secs: res.duration_secs,
                                                    error_message: res.error_message,
                                                };

                                                if let Ok(comp_payload) =
                                                    comp_fory.serialize(&report)
                                                {
                                                    let comp_hdr = IpcHeader::new(
                                                        MSG_TYPE_STAGE_COMPLETION,
                                                        comp_payload.len() as u32,
                                                        0,
                                                        999,
                                                    );
                                                    let mut frame = Vec::with_capacity(
                                                        action_runner_ipc::HEADER_SIZE
                                                            + comp_payload.len(),
                                                    );
                                                    frame.extend_from_slice(
                                                        zerocopy::IntoBytes::as_bytes(&comp_hdr),
                                                    );
                                                    frame.extend_from_slice(&comp_payload);
                                                    let _ = out_tx.send(frame).await;
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                "Worker failed to deserialize StageDispatchTask with Fory"
                                            );
                                        }
                                    }
                                } else if hdr.msg_type == MSG_TYPE_STAGE_CANCEL {
                                    if let Ok(cancel_task) =
                                        fory.deserialize::<StageCancelTask>(payload)
                                    {
                                        info!(
                                            stage = %cancel_task.stage_id,
                                            reason = %cancel_task.reason,
                                            "Worker received StageCancelTask; checking for active process"
                                        );
                                        let maybe_pid = self
                                            .active_processes
                                            .write()
                                            .await
                                            .remove(&cancel_task.stage_id);
                                        if let Some(pid) = maybe_pid {
                                            info!(
                                                stage = %cancel_task.stage_id,
                                                pid = pid,
                                                "Terminating active process group via SIGTERM"
                                            );
                                            #[cfg(unix)]
                                            unsafe {
                                                let _ = libc::kill(-(pid as i32), libc::SIGTERM);
                                                let _ = libc::kill(pid as i32, libc::SIGTERM);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "Worker failed to parse IpcHeader");
                            }
                        }
                    }
                }
            }
        }

        hb_handle.abort();
        Ok(())
    }
}
