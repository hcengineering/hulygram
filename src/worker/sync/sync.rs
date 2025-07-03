use std::collections::HashSet;
use std::sync::{Arc, atomic::AtomicU32};

use anyhow::{Result, bail};
use chrono::TimeDelta;
use grammers_client::InputMessage;
use grammers_client::types::Chat;
use grammers_client::types::Message;
use multimap::MultiMap;
use tokio::{
    self,
    sync::{Mutex, Semaphore, mpsc},
    task::{Builder as TaskBuilder, JoinHandle},
    time::{self, Duration},
};
use tracing::*;

use super::{
    super::context::WorkerContext,
    context::SyncContext,
    context::SyncInfo,
    export::Exporter,
    state::{Progress, SyncState},
    telegram::{ChatExt, MessageExt},
};

use crate::worker::sync::state::HulyMessage;
use crate::{config::CONFIG, integration::TelegramIntegration};

struct SyncChat {
    sender_realtime: mpsc::Sender<Arc<ImporterEvent>>,
    sender_backfill: mpsc::Sender<Arc<ImporterEvent>>,

    context: Arc<SyncContext>,
}

#[derive(Debug)]
pub enum ReverseUpdate {
    MessageCreated {
        huly_message_id: String,
        content: String,
    },

    MessageUpdated {
        huly_message_id: String,
        content: String,
    },
}

impl ReverseUpdate {
    pub fn huly_message_id(&self) -> &String {
        match self {
            ReverseUpdate::MessageCreated {
                huly_message_id, ..
            } => huly_message_id,
            ReverseUpdate::MessageUpdated {
                huly_message_id, ..
            } => huly_message_id,
        }
    }
}

#[derive(strum::Display)]
enum ImporterEvent {
    BackfillMessage(Message),
    NewMessage(Message),
    MessageEdited(Message),
    MessageDeleted(Vec<i32>),
    BackfillComplete,
}

impl ImporterEvent {
    fn id(&self) -> i32 {
        match self {
            ImporterEvent::BackfillMessage(message) => message.id(),
            ImporterEvent::NewMessage(message) => message.id(),
            ImporterEvent::MessageEdited(message) => message.id(),
            ImporterEvent::MessageDeleted(_) => -1,
            ImporterEvent::BackfillComplete => -1,
        }
    }
}

impl SyncChat {
    #[instrument(level = "trace", skip_all)]
    async fn spawn(context: Arc<SyncContext>) -> (Self, JoinHandle<()>) {
        let (sender_backfill, receiver_backfill) = mpsc::channel(1);
        let (sender_realtime, receiver_realtime) = mpsc::channel(16);

        let export = TaskBuilder::new()
            .name(&format!("export-{}", context.chat.id()))
            .spawn(Self::export_task(
                context.clone(),
                receiver_backfill,
                receiver_realtime,
            ))
            .unwrap();

        (
            SyncChat {
                sender_realtime,
                sender_backfill,
                context,
            },
            export,
        )
    }

    #[instrument(level = "debug", name="export", skip_all, fields(chat_id = %context.chat.id(), chat_name = %context.chat.card_title()))]
    async fn export_task(
        context: Arc<SyncContext>,
        mut receiver_backfill: mpsc::Receiver<Arc<ImporterEvent>>,
        mut receiver_realtime: mpsc::Receiver<Arc<ImporterEvent>>,
    ) {
        let mut exporter = Exporter::new(context.clone()).await.unwrap();

        async fn ensure_card(exporter: &mut Exporter, context: &SyncContext) -> Result<()> {
            use super::export::CardState;

            match exporter.ensure_card(context.is_fresh).await? {
                CardState::Exists => {
                    //
                }

                CardState::Created => {
                    context.persist_info().await?;
                }

                CardState::NotExists => {
                    bail!("NoCard");
                }
            };

            Ok(())
        }

        let mut card_ensured = false;

        loop {
            #[instrument(level = "debug", skip_all, fields(event_type = %event.to_string(), event_id = %event.id()))]
            async fn handle_event(
                event: Arc<ImporterEvent>,
                state: &SyncState,
                exporter: &mut Exporter,
            ) -> Result<()> {
                match &*event {
                    ImporterEvent::BackfillMessage(message) => {
                        let telegram_id = message.id();

                        let person_id = exporter.ensure_person(message).await?;

                        match state.get_h_message(telegram_id).await? {
                            None => {
                                let huly_message =
                                    exporter.new_message(&person_id, &message, false).await?;

                                state.set_message(telegram_id, huly_message).await?;
                            }

                            Some(huly_message) if message.last_date() > huly_message.date => {
                                exporter.edit(&person_id, huly_message, message).await?;
                            }

                            Some(_) => {
                                //
                            }
                        }

                        state.set_progress(Progress::Progress(telegram_id)).await?;
                    }

                    ImporterEvent::BackfillComplete => {
                        if !crate::config::CONFIG.dry_run {
                            _ = state.set_progress(Progress::Complete).await;
                        }
                    }

                    ImporterEvent::NewMessage(message) => {
                        let telegram_id = message.id();

                        let person_id = exporter.ensure_person(message).await?;
                        let huly_id = exporter.new_message(&person_id, &message, true).await?;

                        state.set_message(telegram_id, huly_id).await?;
                    }

                    ImporterEvent::MessageEdited(message) => {
                        let telegram_id = message.id();

                        if let Some(huly_message) = state.get_h_message(message.id()).await? {
                            let person_id = exporter.ensure_person(message).await?;

                            let huly_message =
                                exporter.edit(&person_id, huly_message, message).await?;

                            state.set_message(telegram_id, huly_message).await?;
                        }
                    }

                    ImporterEvent::MessageDeleted(messages) => {
                        for message in messages {
                            if let Some(huly_message) = state.get_h_message(*message).await? {
                                exporter.delete(&huly_message.id).await?;
                            }
                        }
                    }
                }

                debug!("Processed");

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

            if !card_ensured {
                if let Err(error) = ensure_card(&mut exporter, &context).await {
                    warn!(%error, "EnsureCard");
                    return;
                }

                card_ensured = true;
            }

            if let Some(event) = event {
                if let Err(error) = handle_event(event, &context.state, &mut exporter).await {
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

        loop {
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

        debug!("Backfill complete");
    }

    pub async fn get_t_message(&self, huly_message_id: &String) -> Result<Option<i32>> {
        self.context.state.get_t_message(huly_message_id).await
    }

    pub async fn handle_reverse_update(
        &self,
        update: ReverseUpdate,
        telegram_id: Option<i32>,
    ) -> Result<Option<i32>> {
        let chat = self.context.chat.pack();

        let message_id = match update {
            ReverseUpdate::MessageCreated {
                content,
                huly_message_id,
                ..
            } => {
                if telegram_id.is_none() {
                    let message = InputMessage::markdown(&content);

                    let message = self
                        .context
                        .worker
                        .telegram
                        .send_message(chat, message)
                        .await?;

                    let huly_message = HulyMessage {
                        id: huly_message_id,
                        date: message.last_date(),
                    };

                    self.context
                        .state
                        .set_message(message.id(), huly_message)
                        .await?;

                    Some(message.id())
                } else {
                    None
                }
            }

            ReverseUpdate::MessageUpdated { content, .. } => {
                if let Some(telegram_message_id) = telegram_id {
                    let message = InputMessage::markdown(&content);

                    self.context
                        .worker
                        .telegram
                        .edit_message(chat, telegram_message_id, message)
                        .await?;

                    Some(telegram_message_id)
                } else {
                    None
                }
            }
        };

        Ok(message_id)
    }
}

pub struct Sync {
    syncs: MultiMap<String, Arc<SyncChat>>,
    cleanup: Arc<Mutex<Vec<JoinHandle<()>>>>,
    context: Arc<WorkerContext>,
    breaker: Mutex<HashSet<(String, i32)>>,
}

impl Sync {
    pub fn new(context: Arc<WorkerContext>) -> Self {
        Self {
            context,
            syncs: MultiMap::new(),
            cleanup: Arc::default(),
            breaker: Mutex::default(),
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

                let context = Arc::new(
                    SyncContext::new(self.context.clone(), chat.clone(), integration).await?,
                );

                let (sync, export) = SyncChat::spawn(context).await;

                self.syncs.insert(chat.global_id(), Arc::new(sync));
                self.cleanup.lock().await.push(export);
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

        let global_semaphore = self.context.global.limiters().sync_semaphore.clone();

        let cleanup = self.cleanup.clone();

        let task = async move {
            let local_semaphore = Arc::new(Semaphore::new(CONFIG.sync_process_limit_local));

            for sync in syncs {
                match sync.context.state.get_progress().await {
                    Ok(Progress::Complete) => {
                        continue;
                    }

                    Ok(progress) => {
                        static IDS: AtomicU32 = AtomicU32::new(0);

                        let id = IDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        let local_permit = local_semaphore.clone().acquire_owned().await.unwrap();
                        let global_permit = global_semaphore.clone().acquire_owned().await.unwrap();

                        trace!(
                            id,
                            permits = global_semaphore.available_permits(),
                            "Backfill permit acquired"
                        );

                        let sync = TaskBuilder::new().name(&format!("backfill-{}", id)).spawn(
                            async move {
                                sync.backfill(id, progress).await;

                                drop(local_permit);
                                drop(global_permit);
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

        let mut breaker = self.breaker.lock().await;

        match update {
            Update::NewMessage(message) => {
                let chat_id = message.chat().global_id();

                if !breaker.remove(&(chat_id.clone(), message.id())) {
                    if let Some(syncs) = self.syncs.get_vec_mut(&chat_id) {
                        for sync in syncs {
                            let _ = sync
                                .sender_realtime
                                .send(Arc::new(ImporterEvent::NewMessage((*message).clone())))
                                .await;
                        }
                    }
                }
            }

            Update::MessageEdited(message) => {
                let chat_id = message.chat().global_id();

                if !breaker.remove(&(chat_id.clone(), message.id())) {
                    if let Some(syncs) = self.syncs.get_vec_mut(&chat_id) {
                        for sync in syncs {
                            let _ = sync
                                .sender_realtime
                                .send(Arc::new(ImporterEvent::MessageEdited((*message).clone())))
                                .await;
                        }
                    }

                    breaker.insert((chat_id.clone(), message.id()));
                }
            }

            Update::MessageDeleted(message) => {
                if let Some(channel_id) = message.channel_id() {
                    if let Some(syncs) =
                        self.syncs.get_vec_mut(&Chat::channel_global_id(channel_id))
                    {
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

    pub async fn handle_reverse_update(
        &self,
        sync_info: SyncInfo,
        update: ReverseUpdate,
    ) -> Result<()> {
        let mut breaker = self.breaker.lock().await;
        let syncs = self.syncs.get_vec(&sync_info.telegram_chat_id);

        if let Some(syncs) = syncs
            && let Some(sync) = syncs
                .iter()
                .find(|sync| sync.context.info.huly_workspace_id == sync_info.huly_workspace_id)
        {
            let telegram_id = sync.get_t_message(update.huly_message_id()).await?;

            if telegram_id.is_none()
                || !breaker.remove(&(sync_info.telegram_chat_id.clone(), telegram_id.unwrap()))
            {
                if let Some(id) = sync.handle_reverse_update(update, telegram_id).await? {
                    breaker.insert((sync_info.telegram_chat_id.clone(), id));
                }
            }
        }
        // probably post to other workspaces

        Ok(())
    }

    pub async fn abort(self) {
        for handle in self.cleanup.lock().await.drain(..) {
            handle.abort();
        }
    }
}
