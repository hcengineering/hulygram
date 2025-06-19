use std::sync::{Arc, atomic::AtomicU32};

use anyhow::Result;
use chrono::TimeDelta;
use grammers_client::types::Message;
use multimap::MultiMap;
use tokio::{
    self,
    sync::{Mutex, mpsc},
    task::{Builder as TaskBuilder, JoinHandle},
    time::{self, Duration},
};
use tracing::*;

use super::{
    super::context::WorkerContext,
    context::SyncContext,
    export::{CardInfo, Exporter},
    state::{Progress, SyncState},
    telegram::{ChatExt, MessageExt},
};
use crate::integration::TelegramIntegration;

struct SyncChat {
    sender_realtime: mpsc::Sender<Arc<ImporterEvent>>,
    sender_backfill: mpsc::Sender<Arc<ImporterEvent>>,

    context: Arc<SyncContext>,
}

enum ImporterEvent {
    BackfillMessage(Message),
    NewMessage(Message),
    MessageEdited(Message),
    MessageDeleted(Vec<i32>),
    BackfillComplete,
}

impl SyncChat {
    #[instrument(level = "trace", skip_all)]
    async fn spawn(context: Arc<SyncContext>) -> Result<(Self, JoinHandle<()>)> {
        let (sender_backfill, receiver_backfill) = mpsc::channel(1);
        let (sender_realtime, receiver_realtime) = mpsc::channel(16);

        let export = TaskBuilder::new()
            .name(&format!("export-{}", context.chat.id()))
            .spawn(Self::export_task(
                context.clone(),
                receiver_backfill,
                receiver_realtime,
            ))?;

        Ok((
            SyncChat {
                sender_realtime,
                sender_backfill,
                context,
            },
            export,
        ))
    }

    #[instrument(level = "debug", name="export", skip_all, fields(chat_id = %context.chat.id(), chat_name = %context.chat.card_title()))]
    async fn export_task(
        context: Arc<SyncContext>,
        mut receiver_backfill: mpsc::Receiver<Arc<ImporterEvent>>,
        mut receiver_realtime: mpsc::Receiver<Arc<ImporterEvent>>,
    ) {
        let mut exporter = Exporter::new(context.clone()).await.unwrap();

        let card = match exporter.ensure_card().await {
            Ok(Some(card)) => card,
            Ok(None) => {
                warn!("NoCard");
                return;
            }
            Err(error) => {
                error!(%error, "EnsureChannel");
                return;
            }
        };

        loop {
            async fn handle_event(
                event: Arc<ImporterEvent>,
                card: &CardInfo,
                state: &SyncState,
                exporter: &mut Exporter,
            ) -> Result<()> {
                match &*event {
                    ImporterEvent::BackfillMessage(message) => {
                        let telegram_id = message.id();

                        let span = span!(Level::TRACE, "Backfill", telegram_id);
                        let _enter = span.enter();

                        let person_id = exporter.ensure_person(message).await?;

                        match state.get_h_message(telegram_id).await? {
                            None => {
                                let huly_message =
                                    exporter.new_message(&card, &person_id, &message).await?;

                                state.set_t_message(telegram_id, huly_message).await?;
                            }

                            Some(huly_message) if message.last_date() > huly_message.date => {
                                exporter
                                    .edit(&card, &person_id, huly_message, message)
                                    .await?;
                            }

                            Some(_) => {
                                //
                            }
                        }

                        state.set_progress(Progress::Progress(telegram_id)).await?;
                    }

                    ImporterEvent::BackfillComplete => {
                        debug!("Backfill Complete");

                        if !crate::config::CONFIG.dry_run {
                            _ = state.set_progress(Progress::Complete).await;
                        }
                    }

                    ImporterEvent::NewMessage(message) => {
                        let telegram_id = message.id();

                        let span = span!(Level::TRACE, "NewMessage", telegram_id);
                        let _enter = span.enter();

                        let person_id = exporter.ensure_person(message).await?;
                        let huly_id = exporter.new_message(&card, &person_id, &message).await?;

                        state.set_t_message(telegram_id, huly_id).await?;
                    }

                    ImporterEvent::MessageEdited(message) => {
                        let telegram_id = message.id();

                        let span = span!(Level::TRACE, "MessageEdited", telegram_id);
                        let _enter = span.enter();

                        if let Some(huly_message) = state.get_h_message(message.id()).await? {
                            let person_id = exporter.ensure_person(message).await?;

                            let huly_message = exporter
                                .edit(&card, &person_id, huly_message, message)
                                .await?;

                            state.set_t_message(telegram_id, huly_message).await?;
                        }
                    }

                    ImporterEvent::MessageDeleted(messages) => {
                        let span = span!(Level::TRACE, "MessageDeleted", telegram_ids = ?messages);
                        let _enter = span.enter();

                        for message in messages {
                            if let Some(huly_message) = state.get_h_message(*message).await? {
                                exporter.delete(&card, &huly_message.id).await?;
                            }
                        }
                    }
                }

                Ok(())
            }

            let event = tokio::select! {
                biased;

                event = receiver_realtime.recv() => {
                    event
                }

                event = receiver_backfill.recv() => {
                    event
                }

                else => {
                    None
                }
            };

            if let Some(event) = event {
                if let Err(error) = handle_event(event, &card, &context.state, &mut exporter).await
                {
                    error!("Error while handling event: {:?}", error);
                }
            } else {
                panic!("Receivers closed")
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(id = _id,  telegram_id = %self.context.worker.me.id(), chat_id = %self.context.chat.id(), chat_name = %self.context.chat.card_title()))]
    async fn backfill(&self, _id: u32, progress: Progress) {
        assert_ne!(progress, Progress::Complete);

        debug!("Backfill begin");

        let mut messages = self
            .context
            .worker
            .telegram
            .iter_messages(self.context.chat.pack());

        if let Progress::Progress(offset) = progress {
            messages = messages.offset_id(offset);
        }

        let mut count = 0;

        loop {
            count += 1;

            if progress.is_complete() {
                if count >= 100 {
                    trace!("Limit reached");
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::BackfillComplete))
                        .await;
                    break;
                }
            }

            let next = time::timeout(Duration::from_secs(30), messages.next());

            self.context
                .worker
                .global
                .limiters()
                .get_history
                .until_key_ready(&self.context.worker.me.id())
                .await;

            match next.await {
                Ok(Ok(Some(message))) => {
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::BackfillMessage(message)))
                        .await;
                }
                Ok(Ok(_)) => {
                    trace!("No more messages");
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::BackfillComplete))
                        .await;
                    break;
                }

                Ok(Err(e)) => {
                    error!(error = %e);
                    break;
                }

                Err(error) => {
                    error!(error = %error, "Timeout");
                }
            }
        }

        debug!(count, "Backfill end");
    }
}

pub struct Sync {
    syncs: MultiMap<i64, Arc<SyncChat>>,
    cleanup: Arc<Mutex<Vec<JoinHandle<()>>>>,
    context: Arc<WorkerContext>,
}

impl Sync {
    pub fn new(context: Arc<WorkerContext>) -> Self {
        Self {
            context,
            syncs: MultiMap::new(),
            cleanup: Arc::default(),
        }
    }

    pub async fn spawn(&mut self) -> Result<()> {
        self.syncs.clear();

        let global_services = &self.context.global;

        let me = Arc::new(self.context.telegram.get_me().await?);
        let mut integrations = global_services
            .account()
            .find_workspace_integrations(me.id())
            .await?;

        let mut chats = Vec::new();
        let mut iter_dialogs = self.context.telegram.iter_dialogs();
        while let Some(dialog) = iter_dialogs.next().await? {
            if dialog
                .last_message
                .as_ref()
                .map(|m| chrono::Utc::now() - m.date() < TimeDelta::days(90))
                .unwrap_or(false)
                && !dialog.chat().is_deleted()
            {
                chats.push(Arc::new(dialog.chat().to_owned()));
            }
        }

        for chat in chats {
            let chat_id = chat.id();

            for integration in &mut integrations {
                match integration.find_channel_config(chat.id()) {
                    // channel is enabled
                    Some(channel) if !channel.enabled => {
                        continue;
                    }

                    // channel unknwon and sync all is disabled
                    None if !integration.data.config.sync_all => {
                        continue;
                    }

                    _ => {}
                }

                debug!(%chat_id, title=chat.card_title(),  "*** Found Chat");

                /*
                let card_id = if let Some(channel) = integration.find_channel_mapping(chat_id) {
                    channel.card_id.clone()
                } else {
                    let card_id = ksuid::Ksuid::generate().to_base62();
                    integration.add_channel_mapping(chat_id, card_id.clone());

                    card_id
                };
                */

                let context = Arc::new(
                    SyncContext::new(self.context.clone(), chat.clone(), integration).await?,
                );

                let sync = SyncChat::spawn(context).await;

                match sync {
                    Ok((sync, export)) => {
                        self.syncs.insert(chat_id, Arc::new(sync));
                        self.cleanup.lock().await.push(export);
                    }
                    Err(error) => {
                        error!(%error, "Cannot spawn sync for chat {}", chat_id);
                    }
                }
            }
        }

        for integration in &integrations {
            if integration.is_modified {
                global_services
                    .account()
                    .update_workspace_integration(integration)
                    .await?;
            }
        }

        let mut syncs = self.syncs.flat_iter().collect::<Vec<_>>();
        syncs.sort_by_key(|(channel_id, _)| *channel_id);
        syncs.reverse();

        let syncs = syncs
            .into_iter()
            .map(|(_, sync)| sync.clone())
            .collect::<Vec<_>>();

        let semaphore = self.context.global.limiters().sync_semaphore.clone();

        let cleanup = self.cleanup.clone();

        let task = async move {
            for sync in syncs {
                match sync.context.state.get_progress().await {
                    Ok(Progress::Complete) => {
                        continue;
                    }

                    Ok(progress) => {
                        static IDS: AtomicU32 = AtomicU32::new(0);

                        let id = IDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        trace!(
                            id,
                            permits = semaphore.available_permits(),
                            "Backfill permit acquired"
                        );

                        let sync = TaskBuilder::new().name(&format!("backfill-{}", id)).spawn(
                            async move {
                                sync.backfill(id, progress).await;
                                drop(permit);
                            },
                        );

                        match sync {
                            Ok(handle) => {
                                cleanup.lock().await.push(handle);
                            }
                            Err(error) => {
                                error!(%error, "Cannot spawn backfill task");
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "Cannot get progress");
                    }
                }
            }
        };

        let handle = TaskBuilder::new()
            .name(&format!("scheduler-{}", self.context.me.id()))
            .spawn(task)?;

        self.cleanup.lock().await.push(handle);

        Ok(())
    }

    pub async fn handle_update(&mut self, update: grammers_client::types::Update) -> Result<()> {
        use grammers_client::types::Update;

        match update {
            Update::NewMessage(message) => {
                let chat = message.chat().id();

                if let Some(syncs) = self.syncs.get_vec_mut(&chat) {
                    for sync in syncs {
                        let _ = sync
                            .sender_realtime
                            .send(Arc::new(ImporterEvent::NewMessage((*message).clone())))
                            .await;
                    }
                }
            }

            Update::MessageEdited(message) => {
                let chat = message.chat().id();

                if let Some(syncs) = self.syncs.get_vec_mut(&chat) {
                    for sync in syncs {
                        let _ = sync
                            .sender_realtime
                            .send(Arc::new(ImporterEvent::MessageEdited((*message).clone())))
                            .await;
                    }
                }
            }

            Update::MessageDeleted(message) => {
                if let Some(channel_id) = message.channel_id() {
                    if let Some(syncs) = self.syncs.get_vec_mut(&channel_id) {
                        for sync in syncs {
                            let _ = sync
                                .sender_realtime
                                .send(Arc::new(ImporterEvent::MessageDeleted(
                                    message.messages().to_vec(),
                                )))
                                .await;
                        }
                    }
                } else {
                    // have iterate over all syncs :(
                    for (_, sync) in self.syncs.flat_iter_mut() {
                        let _ = sync
                            .sender_realtime
                            .send(Arc::new(ImporterEvent::MessageDeleted(
                                message.messages().to_vec(),
                            )))
                            .await;
                    }
                }
            }

            _ => {}
        }

        Ok(())
    }

    pub async fn abort(self) {
        for handle in self.cleanup.lock().await.drain(..) {
            handle.abort();
        }
    }
}
