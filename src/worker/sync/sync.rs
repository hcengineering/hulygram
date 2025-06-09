use std::{
    collections::HashSet,
    sync::{Arc, atomic::AtomicU32},
};

use crate::{
    integration::{TelegramIntegration, WorkspaceIntegration},
    worker::{
        WorkerConfig,
        chat::{ChatExt, DialogType},
        services::{GlobalServices, WorkspaceServices},
    },
};
use anyhow::Result;
use chrono::TimeDelta;
use grammers_client::{
    Client as TelegramClient,
    types::{Chat, Message, User},
};
use hulyrs::services::types::{AccountUuid, SocialIdId, WorkspaceUuid};
use multimap::MultiMap;
use serde::{Deserialize, Serialize};
use tokio::{
    self,
    sync::{Mutex, mpsc},
    task::{Builder as TaskBuilder, JoinHandle},
    time::{self, Duration, Instant},
};
use tracing::*;

use super::{export::Exporter, state::SyncState};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncProgress {
    #[default]
    Unsynced,
    Progress(i32),
    Complete,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DialogInfo {
    // telegram user, chat type and chat id
    pub telegram_user: i64,
    pub telegram_type: DialogType,
    pub telegram_chat_id: i64,

    // huly workspace, account and chanel
    pub huly_workspace: WorkspaceUuid,
    pub huly_account: AccountUuid,
    pub huly_social_id: SocialIdId,
    pub huly_channel: String,
    pub huly_space: String,
    pub huly_title: String,

    #[serde(default)]
    pub progress: SyncProgress,
}

struct SyncProcess {
    chat: Chat,
    sender_realtime: mpsc::Sender<Arc<ImporterEvent>>,
    sender_backfill: mpsc::Sender<Arc<ImporterEvent>>,
    telegram: TelegramClient,
    global_services: GlobalServices,
    me: Arc<User>,
    progress: SyncProgress,
}

enum ImporterEvent {
    Message(Message),
    Delete(i32),
    BatchEnd(bool),
}

impl SyncProcess {
    #[instrument(level = "trace", skip_all)]
    async fn maybe_spawn(
        me: Arc<User>,
        chat: &Chat,
        ws: &WorkspaceIntegration,
        global_services: GlobalServices,
        telegram: TelegramClient,
    ) -> Result<(Self, JoinHandle<()>)> {
        let chat_key = format!("chat_{}_{}_{}", me.id(), chat.id(), ws.workspace);
        let state_key = format!("sta_{}_{}_{}", me.id(), chat.id(), ws.workspace);

        let workspace_services = WorkspaceServices::new(&ws.transactor_url, ws.workspace)?;

        let exporter = Exporter::new(
            chat_key,
            &me,
            chat,
            ws,
            telegram.clone(),
            global_services.clone(),
            workspace_services,
        )
        .await?;

        let (sender_backfill, receiver_backfill) = mpsc::channel(1);
        let (sender_realtime, receiver_realtime) = mpsc::channel(16);

        let state = SyncState::load(chat.id(), state_key, global_services.clone()).await?;
        let progress = exporter.progress();

        let export = TaskBuilder::new()
            .name(&format!("exporter-{}", chat.id()))
            .spawn(Self::export_task(
                state,
                exporter,
                chat.clone(),
                receiver_backfill,
                receiver_realtime,
            ))?;

        Ok((
            SyncProcess {
                chat: chat.clone(),
                sender_realtime,
                sender_backfill,
                telegram,
                global_services,
                me,
                progress,
            },
            export,
        ))
    }

    #[instrument(level = "debug", skip_all, fields(telegram_id = %self.me.id(), chat_id = %self.chat.id(), chat_name = %self.chat.card_title()))]
    pub fn sync_message(&self, message: &Message) -> Result<()> {
        let chat_id = self.chat.id();
        let message_id = message.id();

        trace!(chat = chat_id, message = message_id, "Sync message");

        if let Err(error) = self
            .sender_realtime
            .try_send(Arc::new(ImporterEvent::Message(message.clone())))
        {
            warn!(%error, "Cannot send message");
        }

        Ok(())
    }

    #[instrument(level = "debug", skip_all, fields(telegram_id = %self.me.id(), chat_id = %self.chat.id(), chat_name = %self.chat.card_title()))]
    pub fn delete_messages(&self, messages: &[i32]) -> Result<()> {
        for id in messages {
            if let Err(error) = self
                .sender_realtime
                .try_send(Arc::new(ImporterEvent::Delete(*id)))
            {
                warn!(%error, "Cannot send delete message");
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", name="export", skip_all, fields(chat_id = %chat.id(), chat_name = %chat.card_title()))]
    async fn export_task(
        mut state: SyncState,
        mut exporter: Exporter,
        chat: Chat,
        mut receiver_backfill: mpsc::Receiver<Arc<ImporterEvent>>,
        mut receiver_realtime: mpsc::Receiver<Arc<ImporterEvent>>,
    ) {
        let mut progress = exporter.progress();
        let mut seen = HashSet::new();
        let mut persist_state = time::interval(Duration::from_secs(10));
        persist_state.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut last_persist = time::Instant::now();

        loop {
            if Instant::now() - last_persist > Duration::from_secs(30) {
                last_persist = Instant::now();

                debug!(?progress, "Persisting state");

                if !crate::config::CONFIG.dry_run {
                    if let Err(error) = state.persist().await {
                        error!(%error, "Cannot persist state");
                    }

                    if let Err(error) = exporter.set_progress(progress).await {
                        error!(%error, "Cannot set progress");
                    }
                }
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
                match &*event {
                    ImporterEvent::Message(message) => {
                        let span = span!(Level::TRACE, "message", telegram_id = message.id());
                        let _enter = span.enter();

                        let date = message.edit_date().unwrap_or(message.date());
                        let id = message.id();

                        seen.insert(message.id());

                        let state_entry = state.lookup(id).map(ToOwned::to_owned);

                        let result = match state_entry {
                            None => {
                                // Unknown
                                trace!("New");
                                exporter.create(&message).await.map(Option::Some)
                            }
                            // known
                            Some(state_entry) if state_entry.date < date => {
                                // Known and updated
                                trace!("Updated");
                                exporter
                                    .update(&message, state_entry)
                                    .await
                                    .map(Option::Some)
                            }

                            Some(_) => Ok(None),
                        };

                        match result {
                            Ok(Some(entry)) => {
                                state.upsert(&entry);
                            }
                            Ok(None) => {
                                //progress = SyncProgress::Progress(message.id());
                            }

                            Err(e) => {
                                error!(error = %e, "Message");
                            }
                        }

                        progress = SyncProgress::Progress(message.id());
                    }

                    ImporterEvent::Delete(message) => {
                        if state.lookup(*message).is_some() {
                            if let Err(error) = exporter.delete(*message).await {
                                error!(%error);
                            }

                            state.delete(*message);
                        }
                    }

                    ImporterEvent::BatchEnd(_progress) => {
                        debug!("Batch end");

                        progress = SyncProgress::Complete;

                        let min = seen.iter().fold(i32::MAX, |acc, id| acc.min(*id));

                        for id in state.ids() {
                            if id >= min && !seen.contains(&id) {
                                let _ = exporter.delete(id).await;
                                state.delete(id);
                            }
                        }

                        if !crate::config::CONFIG.dry_run {
                            let _ = state.persist().await;
                            _ = exporter.set_progress(progress).await;
                        }
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(id = _id,  telegram_id = %self.me.id(), chat_id = %self.chat.id(), chat_name = %self.chat.card_title()))]
    async fn backfill(&self, _id: u32) {
        if matches!(self.progress, SyncProgress::Complete) {
            return;
        }

        debug!(progress = ?self.progress, "Backfill begin");

        let mut messages = self.telegram.iter_messages(self.chat.pack());
        if let SyncProgress::Progress(offset) = self.progress {
            debug!(offset, "Continue from offset");
            messages = messages.offset_id(offset);
        }

        let mut count = 0;

        /*
        self.global_services
            .limiters()
            .get_dialog
            .until_key_ready(&self.me.id())
            .await;*/

        loop {
            count += 1;

            // if complete - break after limit reached
            if self.progress == SyncProgress::Complete {
                if count >= 100 {
                    debug!("Limit reached");
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::BatchEnd(true)))
                        .await;
                    break;
                }
            }

            let next = time::timeout(Duration::from_secs(30), messages.next());

            self.global_services
                .limiters()
                .get_history
                .until_key_ready(&self.me.id())
                .await;

            match next.await {
                Ok(Ok(Some(message))) => {
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::Message(message)))
                        .await;
                }
                Ok(Ok(_)) => {
                    debug!("No more messages");
                    let _ = self
                        .sender_backfill
                        .send(Arc::new(ImporterEvent::BatchEnd(matches!(
                            self.progress,
                            SyncProgress::Complete
                        ))))
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
    telegram: TelegramClient,
    config: Arc<WorkerConfig>,
    syncs: MultiMap<i64, Arc<SyncProcess>>,
    cleanup: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Sync {
    pub fn new(telegram: TelegramClient, config: Arc<WorkerConfig>) -> Self {
        Self {
            telegram,
            config,
            syncs: MultiMap::new(),
            cleanup: Arc::default(),
        }
    }

    pub async fn spawn(&mut self) -> Result<()> {
        self.syncs.clear();

        let global_services = &self.config.global_services;

        let me = Arc::new(self.telegram.get_me().await?);
        let integrations = global_services
            .account()
            .find_workspace_integrations(me.id())
            .await?;

        let mut iter_dialogs = self.telegram.iter_dialogs();

        while let Some(dialog) = iter_dialogs.next().await? {
            let chat = dialog.chat();
            let chat_id = chat.id();

            let not_too_old = dialog
                .last_message
                .as_ref()
                .map(|m| chrono::Utc::now() - m.date() < TimeDelta::days(90))
                .unwrap_or(false);

            if not_too_old {
                for ws in &integrations {
                    let sync = SyncProcess::maybe_spawn(
                        me.clone(),
                        chat,
                        ws,
                        global_services.clone(),
                        self.telegram.clone(),
                    )
                    .await;

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
        }

        let mut syncs = self.syncs.flat_iter().collect::<Vec<_>>();
        syncs.sort_by_key(|(channel_id, _)| *channel_id);
        syncs.reverse();

        let syncs = syncs
            .into_iter()
            .map(|(_, sync)| sync.clone())
            .collect::<Vec<_>>();

        let semaphore = self
            .config
            .global_services
            .limiters()
            .sync_semaphore
            .clone();

        let cleanup = self.cleanup.clone();

        let task = async move {
            static IDS: AtomicU32 = AtomicU32::new(0);

            for sync in syncs {
                let id = IDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                trace!(
                    id,
                    permits = semaphore.available_permits(),
                    "Backfill permit acquired"
                );

                let sync = TaskBuilder::new()
                    .name(&format!("backfill-{}", id))
                    .spawn(async move {
                        let _ = sync.backfill(id).await;
                        drop(permit);
                    });

                match sync {
                    Ok(handle) => {
                        cleanup.lock().await.push(handle);
                    }
                    Err(error) => {
                        error!(%error, "Cannot spawn sync");
                    }
                }
            }
        };

        let handle = TaskBuilder::new()
            .name(&format!("scheduler-{}", self.config.id))
            .spawn(task)?;

        self.cleanup.lock().await.push(handle);

        Ok(())
    }

    pub async fn handle_update(&mut self, update: grammers_client::types::Update) -> Result<()> {
        use grammers_client::types::Update;

        match update {
            Update::NewMessage(message) | Update::MessageEdited(message) => {
                let chat = message.chat().id();

                if let Some(syncs) = self.syncs.get_vec_mut(&chat) {
                    for sync in syncs {
                        sync.sync_message(&message)?;
                    }
                }
            }

            Update::MessageDeleted(message) => {
                if let Some(channel_id) = message.channel_id() {
                    if let Some(syncs) = self.syncs.get_vec_mut(&channel_id) {
                        for sync in syncs {
                            sync.delete_messages(message.messages())?;
                        }
                    }
                } else {
                    // have iterate over all syncs :(
                    for (_, sync) in self.syncs.flat_iter_mut() {
                        sync.delete_messages(message.messages())?;
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
