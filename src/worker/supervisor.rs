use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::{
        Mutex, broadcast,
        mpsc::{self, Sender},
    },
    time,
};
use tracing::*;

use crate::{
    context::GlobalContext,
    integration::TelegramIntegration,
    worker::{
        WorkerHintsBuilder,
        worker::{ExitReason, Worker, WorkerConfig, WorkerHints, WorkerRequest},
    },
};

pub type WorkerId = String;

#[derive(Clone)]
pub struct SupervisorInner {
    workers: Arc<Mutex<HashMap<WorkerId, (Sender<WorkerRequest>, broadcast::Receiver<String>)>>>,
    global: Arc<GlobalContext>,
}

pub type Supervisor = Arc<SupervisorInner>;

pub fn new(global: Arc<GlobalContext>) -> anyhow::Result<Supervisor> {
    Ok(Arc::new(SupervisorInner {
        workers: Arc::new(Mutex::new(HashMap::new())),
        global,
    }))
}

impl SupervisorInner {
    pub async fn spawn_all(self: &Arc<Self>) -> anyhow::Result<()> {
        let integrations = self.global.account().list_all_integrations().await?;

        for integration in integrations {
            debug!(%integration.phone, "Automatically spawning worker");

            self.spawn_worker(&integration.phone, WorkerHintsBuilder::default().build()?)
                .await;
        }

        Ok(())
    }

    pub async fn restart_worker(&self, phone: &str) -> anyhow::Result<bool> {
        let locked = self.workers.lock().await;

        let id = phone.to_owned();

        if let Some((sender, _)) = locked.get(&id) {
            sender.send(WorkerRequest::Restart).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn spawn_worker(
        self: &Arc<Self>,
        phone: &str,
        hints: WorkerHints,
    ) -> Sender<WorkerRequest> {
        let mut locked = self.workers.lock().await;

        let id = phone.to_owned();
        let workers = self.workers.clone();

        if let Some((sender, _)) = locked.get(&id) {
            sender.clone()
        } else {
            let (sender, mut receiver) = mpsc::channel(16);

            let (shutdown_sender, shutdown_receiver) = broadcast::channel(4);

            locked.insert(id.clone(), (sender.clone(), shutdown_receiver));

            let config = WorkerConfig {
                id: id.clone(),
                phone: phone.to_owned(),

                // leak sender, so recever will never close
                _self_sender: sender.clone(),
                hints: hints.clone(),
            };

            trace!(%id, "Spawn worker");
            let task_name = format!("worker-{}", id);
            let global = self.global.clone();

            let task = async move {
                'outer: loop {
                    let id = id.clone();

                    debug!(%id, "Creating and running worker");

                    let delay = match Worker::new(global.clone(), config.clone()).await {
                        Ok(worker) => {
                            let reason = worker.run(&mut receiver).await;

                            match reason {
                                ExitReason::Shutdown => {
                                    debug!(%id, "Worker shutdown");
                                    None
                                }
                                ExitReason::Hibenate => {
                                    debug!(%id, "Worker hibernated");
                                    Some(Duration::from_secs(5))
                                }
                                ExitReason::Restart => {
                                    debug!(%id, "Worker restart");
                                    Some(Duration::ZERO)
                                }
                                ExitReason::NotAuthorized => {
                                    debug!(%id, "Worker not authorized");
                                    None
                                }
                                ExitReason::Error(error) => {
                                    debug!(%id, %error, "Worker terminated unexpectedly");
                                    Some(Duration::from_secs(5))
                                }
                            }
                        }

                        Err(error) => {
                            error!(%id, ?error, "Cannot create worker");
                            None
                        }
                    };

                    if let Some(delay) = delay {
                        trace!(%id, ?delay, "Worker sleeping for before re-spawn");

                        let until = time::Instant::now() + delay;

                        loop {
                            select! {
                                _ = time::sleep_until(until) =>{
                                    break;
                                }

                                message = receiver.recv() => {
                                    if matches!(message, Some(WorkerRequest::Shutdown(_))) {
                                        break 'outer;
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }

                workers.lock().await.remove(&id);

                let _ = shutdown_sender.send(id);
            };

            let _ = tokio::task::Builder::new().name(&task_name).spawn(task);

            sender
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn shutdown_all(&self) {
        let mut receivers = Vec::new();

        {
            let workers = self.workers.lock().await;

            for (id, (sender, receiver)) in workers.iter() {
                let receiver = receiver.resubscribe();

                trace!(%id, "Shutting down worker");
                match sender.send(WorkerRequest::Shutdown(false)).await {
                    Ok(()) => {
                        trace!(%id, "Sent shutdown message");

                        receivers.push(receiver);
                    }
                    Err(error) => {
                        error!(%id, %error, "Failed to send shutdown message");
                    }
                }
            }
        }

        for mut receiver in receivers {
            _ = receiver.recv().await;
        }
    }

    pub async fn shutdown(&self, phone: &str, sign_out: bool) -> anyhow::Result<bool> {
        let locked = self.workers.lock().await;

        if let Some((sender, _)) = locked.get(phone) {
            sender.send(WorkerRequest::Shutdown(sign_out)).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
