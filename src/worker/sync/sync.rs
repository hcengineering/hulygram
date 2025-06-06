use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{
    integration::{TelegramIntegration, WorkspaceIntegration},
    worker::{
        WorkerConfig,
        chat::DialogType,
        services::{GlobalServices, WorkspaceServices},
    },
};
use anyhow::Result;
use chrono::TimeDelta;
use grammers_client::{
    Client as TelegramClient,
    types::{Chat, Message, PackedChat, User},
};
use hulyrs::services::types::{AccountUuid, SocialIdId, WorkspaceUuid};
use multimap::MultiMap;
use serde::{Deserialize, Serialize};
use tokio::{
    self,
    sync::mpsc,
    task::{self, JoinHandle},
    time,
};
use tracing::*;

use super::{export::Exporter, state::SyncState};

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

    #[serde(default = "bool::default")]
    pub is_complete: bool,
}

struct SyncProcess {
    chat: PackedChat,
    sender: mpsc::Sender<ImporterEvent>,
    export: JoinHandle<()>,
    generate: JoinHandle<()>,
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
    ) -> Result<Option<Self>> {
        let chat_key = format!("chat_{}_{}_{}", me.id(), chat.id(), ws.workspace);
        let state_key = format!("sta_{}_{}_{}", me.id(), chat.id(), ws.workspace);

        let workspace_services = WorkspaceServices::new(&ws.transactor_url, ws.workspace)?;

        let exporter = Exporter::maybe_create(
            chat_key,
            &me,
            chat,
            ws,
            telegram.clone(),
            global_services.clone(),
            workspace_services,
        )
        .await?;

        if let Some(exporter) = exporter {
            let (sender, receiver) = mpsc::channel(1);

            let state = SyncState::load(chat.id(), state_key, global_services.clone()).await?;
            let limit = exporter.limit();

            let export = task::spawn(Self::export_task(state, exporter, chat.pack(), receiver));
            let generate = task::spawn(Self::generate_task(
                telegram,
                global_services,
                me,
                limit,
                chat.pack(),
                sender.clone(),
            ));

            Ok(Some(SyncProcess {
                chat: chat.pack(),
                sender,
                export,
                generate,
            }))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "trace", skip_all, fields(chat = %self.chat.id))]
    pub fn abort(&self) {
        self.export.abort();
        self.generate.abort();
    }

    pub async fn sync_message(&mut self, message: &Message) -> Result<()> {
        let chat_id = self.chat.id;
        let message_id = message.id();

        self.sender
            .send(ImporterEvent::Message(message.clone()))
            .await?;

        trace!(chat = chat_id, message = message_id, "Sync message");

        Ok(())
    }

    pub async fn delete_messages(&mut self, messages: &[i32]) -> Result<()> {
        for id in messages {
            self.sender.send(ImporterEvent::Delete(*id)).await?;
        }

        Ok(())
    }

    #[instrument(level = "trace", name="export", skip(state, exporter, chat, receiver), fields(chat_id = %chat.id))]
    async fn export_task(
        mut state: SyncState,
        mut exporter: Exporter,
        chat: PackedChat,
        mut receiver: mpsc::Receiver<ImporterEvent>,
    ) {
        let mut seen = HashSet::new();
        let mut persist_state = time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = persist_state.tick() => {
                    if let Err(error) = state.persist().await {
                        error!(%error, "Cannot persist state");
                    }
                }

                event = receiver.recv() => {
                    match event {
                        Some(ImporterEvent::Message(message)) => {
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
                                    exporter.update(&message, state_entry).await.map(Option::Some)
                                }

                                Some(_) => {
                                    Ok(None)
                                }
                            };

                            match result {
                                Ok(Some(entry)) => {
                                    state.upsert(&entry);
                                }
                                Ok(None) => {}

                                Err(e) => {
                                    error!(error = %e, "Message");
                                }
                            }
                        }

                        Some(ImporterEvent::Delete(message)) => {
                            let span = span!(Level::TRACE, "Delete", telegram_id = message);
                            let _enter = span.enter();


                            if state.lookup(message).is_some() {
                                if let Err(error) = exporter.delete(message).await {
                                    error!(%error);
                                }

                                state.delete(message);
                            }
                        }

                        Some(ImporterEvent::BatchEnd(is_limit)) => {
                            trace!(is_limit, "Batch end");

                            let min = seen.iter().fold(i32::MAX, |acc, id| acc.min(*id));

                            for id in state.ids() {
                                if id >= min && !seen.contains(&id) {
                                    let _ = exporter.delete(id).await;
                                    state.delete(id);
                                }
                            }

                            let _ = state.persist().await;

                            if !is_limit {
                                _ = exporter.set_complete().await;
                            }
                        }

                        None => {
                            // ok for now, but should be fixed
                            //panic!("Receiver closed unexpectedly");
                        }
                    }
                }
            }
        }
    }

    #[instrument(level = "trace", name="generate", skip(telegram, sender, global_services, me), fields(chat = %chat.id))]
    async fn generate_task(
        telegram: TelegramClient,
        global_services: GlobalServices,
        me: Arc<User>,
        limit: Option<u32>,
        chat: PackedChat,
        sender: mpsc::Sender<ImporterEvent>,
    ) {
        let mut messages = telegram.iter_messages(chat);
        let mut count = 0;

        global_services
            .limiters()
            .get_dialog
            .until_key_ready(&me.id())
            .await;

        loop {
            count += 1;

            if let Some(limit) = limit {
                if count > limit {
                    trace!("Limit reached");
                    let _ = sender.send(ImporterEvent::BatchEnd(true)).await;
                    break;
                }
            }

            let next = time::timeout(Duration::from_secs(30), messages.next());

            global_services
                .limiters()
                .get_history
                .until_key_ready(&me.id())
                .await;

            match next.await {
                Ok(Ok(Some(message))) => {
                    let _ = sender.send(ImporterEvent::Message(message)).await;
                }
                Ok(Ok(_)) => {
                    trace!("No more messages");
                    let _ = sender.send(ImporterEvent::BatchEnd(false)).await;
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
    }
}

pub struct Sync {
    telegram: TelegramClient,
    config: Arc<WorkerConfig>,
    syncs: MultiMap<i64, SyncProcess>,
}

impl Sync {
    pub fn new(telegram: TelegramClient, config: Arc<WorkerConfig>) -> Self {
        Self {
            telegram,
            config,
            syncs: MultiMap::new(),
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
                        Ok(Some(sync)) => {
                            self.syncs.insert(chat_id, sync);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            error!(%error, "Cannot spawn sync for chat {}", chat_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn handle_update(&mut self, update: grammers_client::types::Update) -> Result<()> {
        use grammers_client::types::Update;

        match update {
            Update::NewMessage(message) | Update::MessageEdited(message) => {
                let chat = message.chat().id();

                if let Some(syncs) = self.syncs.get_vec_mut(&chat) {
                    for sync in syncs {
                        sync.sync_message(&message).await?;
                    }
                }
            }

            Update::MessageDeleted(message) => {
                if let Some(channel_id) = message.channel_id() {
                    if let Some(syncs) = self.syncs.get_vec_mut(&channel_id) {
                        for sync in syncs {
                            sync.delete_messages(message.messages()).await?;
                        }
                    }
                } else {
                    // have iterate over all syncs :(
                    for (_, sync) in self.syncs.flat_iter_mut() {
                        sync.delete_messages(message.messages()).await?;
                    }
                }
            }

            _ => {}
        }

        Ok(())
    }

    pub fn abort(&mut self) {
        for (_, sync) in self.syncs.flat_iter() {
            sync.abort();
        }
    }
}
