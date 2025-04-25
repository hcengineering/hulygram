use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::{
        Mutex, broadcast,
        mpsc::{self, Sender},
    },
    task, time,
};
use tracing::*;

use crate::{
    integration::TelegramIntegration,
    worker::{
        WorkerHintsBuilder,
        services::GlobalServices,
        worker::{ExitReason, Message, Worker, WorkerConfig, WorkerHints},
    },
};

pub type WorkerId = String;

#[derive(Clone)]
pub struct SupervisorInner {
    workers: Arc<Mutex<HashMap<WorkerId, (Sender<Message>, broadcast::Receiver<String>)>>>,
    global_services: GlobalServices,
}

pub type Supervisor = Arc<SupervisorInner>;

pub fn new(global_services: GlobalServices) -> anyhow::Result<Supervisor> {
    Ok(Arc::new(SupervisorInner {
        workers: Arc::new(Mutex::new(HashMap::new())),
        global_services,
    }))
}

impl SupervisorInner {
    pub async fn spawn_all(self: &Arc<Self>) -> anyhow::Result<()> {
        let integrations = self
            .global_services
            .account()
            .list_all_integrations()
            .await?;

        for integration in integrations {
            trace!(%integration.phone, "Automatically spawning worker");

            self.spawn_worker(&integration.phone, WorkerHintsBuilder::default().build()?)
                .await;
        }

        Ok(())
    }

    pub async fn spawn_worker(
        self: &Arc<Self>,
        phone: &str,
        hints: WorkerHints,
    ) -> Sender<Message> {
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
                global_services: self.global_services.clone(),
                hints: hints.clone(),
            };

            trace!(%id, "Spawn worker");

            task::spawn(async move {
                'outer: loop {
                    let id = id.clone();

                    trace!(%id, "Creating and running worker");

                    let delay = match Worker::new(config.clone()).await {
                        Ok(mut worker) => {
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
                                    if matches!(message, Some(Message::Shutdown)) {
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
            });

            sender
        }
    }

    pub async fn shutdown_all(&self) {
        let mut receivers = Vec::new();

        {
            let workers = self.workers.lock().await;

            for (id, (sender, receiver)) in workers.iter() {
                let receiver = receiver.resubscribe();

                match sender.send(Message::Shutdown).await {
                    Ok(()) => {
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
}
