use action_runner_ipc::{
    create_fory_runtime, IpcHeader, StageCompletionReport, StageDispatchTask, WorkerHeartbeat,
    WorkerRegistration, MSG_TYPE_ARROW_LOG, MSG_TYPE_STAGE_COMPLETION, MSG_TYPE_STAGE_DISPATCH,
    MSG_TYPE_WORKER_HEARTBEAT, MSG_TYPE_WORKER_REGISTRATION,
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::WorkerConfig;
use crate::stage_runner::StageRunner;

pub struct WorkerAgent {
    config: WorkerConfig,
    active_slots: Arc<AtomicU32>,
}

impl WorkerAgent {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            active_slots: Arc::new(AtomicU32::new(0)),
        }
    }

    pub async fn run_forever(self) {
        let mut backoff = Duration::from_secs(1);

        loop {
            info!(
                controller = %self.config.controller_url,
                "Attempting connection to ActionRunner-controller cluster tunnel..."
            );

            match self.connect_and_process().await {
                Ok(()) => {
                    info!("Cluster session concluded cleanly; reconnecting...");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        retry_in = ?backoff,
                        "Cluster connection error; backing off before retry"
                    );
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(15));
                }
            }
        }
    }

    pub async fn connect_and_process(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut req = self.config.controller_url.clone().into_client_request()?;
        req.headers_mut().insert(
            "x-cluster-token",
            self.config.cluster_token.parse()?,
        );

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
                    let hdr = IpcHeader::new(
                        MSG_TYPE_WORKER_HEARTBEAT,
                        payload.len() as u32,
                        0,
                        hb_seq,
                    );
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
                                            let out_tx = outbound_tx.clone();
                                            let active_counter = self.active_slots.clone();
                                            let comp_fory = create_fory_runtime()?;

                                            active_counter.fetch_add(1, Ordering::Relaxed);

                                            tokio::spawn(async move {
                                                let (log_batch_tx, mut log_batch_rx) = mpsc::channel::<Vec<u8>>(32);

                                                // Forward log batches to tunnel outbound channel
                                                let log_forwarder = {
                                                    let out_tx = out_tx.clone();
                                                    tokio::spawn(async move {
                                                        let mut l_seq = 2000u64;
                                                        while let Some(batch_bytes) = log_batch_rx.recv().await {
                                                            l_seq += 1;
                                                            let log_hdr = IpcHeader::new(
                                                                MSG_TYPE_ARROW_LOG,
                                                                batch_bytes.len() as u32,
                                                                0,
                                                                l_seq,
                                                            );
                                                            let mut frame = Vec::with_capacity(action_runner_ipc::HEADER_SIZE + batch_bytes.len());
                                                            frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&log_hdr));
                                                            frame.extend_from_slice(&batch_bytes);
                                                            let _ = out_tx.send(frame).await;
                                                        }
                                                    })
                                                };

                                                let res = runner.run_stage(&task, log_batch_tx).await;
                                                let _ = log_forwarder.await;

                                                active_counter.fetch_sub(1, Ordering::Relaxed);

                                                let report = StageCompletionReport {
                                                    job_id: task.job_id.clone(),
                                                    stage_id: task.stage_id.clone(),
                                                    exit_code: res.exit_code,
                                                    duration_secs: res.duration_secs,
                                                    error_message: res.error_message,
                                                };

                                                if let Ok(comp_payload) = comp_fory.serialize(&report) {
                                                    let comp_hdr = IpcHeader::new(
                                                        MSG_TYPE_STAGE_COMPLETION,
                                                        comp_payload.len() as u32,
                                                        0,
                                                        999,
                                                    );
                                                    let mut frame = Vec::with_capacity(action_runner_ipc::HEADER_SIZE + comp_payload.len());
                                                    frame.extend_from_slice(zerocopy::IntoBytes::as_bytes(&comp_hdr));
                                                    frame.extend_from_slice(&comp_payload);
                                                    let _ = out_tx.send(frame).await;
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            tracing::error!(error = %e, "Worker failed to deserialize StageDispatchTask with Fory");
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
