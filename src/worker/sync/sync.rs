use core::panic;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::AtomicU32};

use anyhow::{Context, Result, anyhow};
use grammers_client::InputMessage;
use grammers_client::types::Chat;
use grammers_client::types::Message;
use hulyrs::services::core::WorkspaceUuid;
use hulyrs::services::jwt::ClaimsBuilder;
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
};
use crate::integration::{Access, ChannelConfig, WorkspaceIntegration};
use crate::telegram::{ChatExt, MessageExt};
use crate::worker::sync::state::HulyMessage;
use crate::worker::sync::tx::TransactorExt;
use crate::{
    config::{CONFIG, hulyrs::SERVICES},
    integration::TelegramIntegration,
};

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

    MessageDeleted {
        huly_message_id: String,
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

            ReverseUpdate::MessageDeleted { huly_message_id } => huly_message_id,
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
    async fn spawn(context: SyncContext) -> (Self, JoinHandle<()>) {
        let context = Arc::new(context);

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
        let mut exporter = Exporter::new(context.clone());

        loop {
            #[instrument(level = "debug", skip_all, fields(event_type = %event.to_string(), event_id = %event.id()))]
            async fn process_event(
                event: Arc<ImporterEvent>,
                state: &SyncState,
                exporter: &mut Exporter,
            ) -> Result<()> {
                match &*event {
                    ImporterEvent::BackfillMessage(message) => {
                        let telegram_id = message.id();

                        let person_id = exporter
                            .ensure_person(message)
                            .await
                            .context("EnsurePerson")?;

                        match state
                            .get_h_message(telegram_id)
                            .await
                            .context("GetHMessage")?
                        {
                            None => {
                                let huly_message = exporter
                                    .new_message(&person_id, &message, false)
                                    .await
                                    .context("NewMessage")?;

                                state
                                    .set_message(telegram_id, huly_message)
                                    .await
                                    .context("SetMessage")?;
                            }

                            Some(huly_message) if message.last_date() > huly_message.date => {
                                exporter
                                    .edit(&person_id, huly_message, message)
                                    .await
                                    .context("Edit")?;
                            }

                            Some(_) => {
                                //
                            }
                        }

                        state
                            .set_progress(Progress::Progress(telegram_id))
                            .await
                            .context("SetProgress")?;
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
                                exporter.delete(&huly_message.id).await.context("Delete")?;
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

            if let Some(event) = event {
                if let Err(error) = process_event(event, &context.state, &mut exporter).await {
                    error!(?error, "Process event");
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
        update: &ReverseUpdate,
        telegram_id: Option<i32>,
    ) -> Result<Option<i32>> {
        let chat = self.context.chat.pack();
        let telegram = &self.context.worker.telegram;

        let message_id = match update {
            ReverseUpdate::MessageCreated {
                content,
                huly_message_id,
                ..
            } => {
                if telegram_id.is_none() {
                    let message = InputMessage::markdown(&content);

                    let message = telegram.send_message(chat, message).await?;

                    let huly_message = HulyMessage {
                        id: huly_message_id.to_owned(),
                        date: message.last_date(),
                    };

                    self.context
                        .state
                        .set_message(message.id(), huly_message)
                        .await?;

                    None
                } else {
                    None
                }
            }

            ReverseUpdate::MessageUpdated { content, .. } => {
                if let Some(telegram_message_id) = telegram_id {
                    let message = InputMessage::markdown(&content);

                    telegram
                        .edit_message(chat, telegram_message_id, message)
                        .await?;
                }

                telegram_id
            }

            ReverseUpdate::MessageDeleted { .. } => {
                if let Some(telegram_message_id) = telegram_id {
                    telegram
                        .delete_messages(chat, &[telegram_message_id])
                        .await?;
                }

                None
            }
        };

        Ok(message_id)
    }
}

#[derive(Clone, Copy, serde::Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    Sync,
    Disabled,
    Unknown,
}

use crate::context::GlobalContext;
use grammers_client::Client as TelegramClient;

pub struct Sync {
    syncs: MultiMap<String, Arc<SyncChat>>,
    chats: Vec<(WorkspaceUuid, Arc<Chat>, SyncMode)>,
    all_chats: Vec<Arc<Chat>>,
    integrations: Vec<WorkspaceIntegration>,
    cleanup: Arc<Mutex<Vec<JoinHandle<()>>>>,
    context: Arc<GlobalContext>,
    debouncer: Mutex<HashSet<(String, i32)>>,
}

impl Sync {
    pub fn new(global: Arc<GlobalContext>) -> Self {
        Self {
            context: global,
            syncs: MultiMap::new(),
            chats: Vec::default(),
            all_chats: Vec::default(),
            integrations: Vec::default(),
            cleanup: Arc::default(),
            debouncer: Mutex::default(),
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn spawn(&mut self, telegram: TelegramClient) -> Result<()> {
        self.syncs.clear();

        let context = Arc::new(WorkerContext::new(self.context.clone(), telegram).await?);

        let global_services = &context.global;

        let me = Arc::new(context.telegram.get_me().await?);

        self.integrations = global_services
            .account()
            .find_workspace_integrations(me.id())
            .await?;

        let mut iter_dialogs = context.telegram.iter_dialogs();

        while let Some(dialog) = iter_dialogs.next().await? {
            if !dialog.chat().is_deleted() {
                self.all_chats.push(Arc::new(dialog.chat().to_owned()));
            }
        }

        let all_chats = self.all_chats.len();
        let mut sync_active = 0;
        let mut sync_disabled = 0;
        let mut sync_unknown = 0;

        #[derive(Debug)]
        struct ChannelInfo {
            title: String,
            is_personal: bool,
        }

        #[derive(Debug, Default)]
        struct WSCache {
            inner: HashMap<WorkspaceUuid, HashMap<String, ChannelInfo>>,
        }

        impl WSCache {
            async fn get(
                &mut self,
                context: &SyncContext,
                workspace_id: WorkspaceUuid,
            ) -> Result<&HashMap<String, ChannelInfo>> {
                if !self.inner.contains_key(&workspace_id) {
                    let enumerated = context.transactor.enumerate_channels().await?;
                    let person_space = context
                        .transactor
                        .find_personal_space(context.worker.account_id)
                        .await?;

                    let mut channels = HashMap::default();

                    for ch in enumerated {
                        let is_personal = match &person_space {
                            Some(space) if space == &ch.space => true,
                            _ => false,
                        };

                        channels.insert(
                            ch.id,
                            ChannelInfo {
                                title: ch.title,
                                is_personal,
                            },
                        );
                    }

                    self.inner.insert(workspace_id, channels);
                }

                Ok(self.inner.get(&workspace_id).unwrap())
            }
        }

        let mut wschannels = WSCache::default();

        let mut spawn_sync = async |integration: &WorkspaceIntegration,
                                    chat: &Arc<Chat>,
                                    config: &ChannelConfig|
               -> Result<SyncMode> {
            let transactor = SERVICES.new_transactor_client(
                integration.endpoint.clone(),
                &ClaimsBuilder::default()
                    .system_account()
                    .workspace(integration.workspace_id)
                    .extra("service", &CONFIG.service_id)
                    .build()?,
            )?;

            let (info, is_fresh) = if let Some(mut info) = SyncContext::load_sync_info(
                &global_services,
                integration.workspace_id,
                context.me.id(),
                &chat.global_id(),
            )
            .await?
            {
                // temp migration fixture
                if info.huly_space_id.is_empty() {
                    let person_space = transactor
                        .find_personal_space(context.account_id)
                        .await?
                        .ok_or_else(|| {
                            warn!(account_id = %context.account_id, "Personal space not found");
                            anyhow!("NoPersonalSpace")
                        })?;

                    info.huly_space_id = config
                        .space
                        .as_ref()
                        .map(|s| s.to_owned())
                        .unwrap_or_else(|| {
                            if config.access == Some(Access::Public) {
                                String::from("card:space:Default")
                            } else {
                                person_space
                            }
                        });

                    SyncContext::store_sync_info(&global_services, &info).await?;
                }
                // end of temp migration fixture

                (info, false)
            } else {
                let personal_space = transactor
                    .find_personal_space(context.account_id)
                    .await?
                    .ok_or_else(|| {
                        warn!(account_id = %context.account_id, "Personal space not found");
                        anyhow!("NoPersonalSpace")
                    })?;

                let huly_space_id =
                    config
                        .space
                        .as_ref()
                        .map(|s| s.to_owned())
                        .unwrap_or_else(|| {
                            if config.access == Some(Access::Public) {
                                String::from("card:space:Default")
                            } else {
                                personal_space
                            }
                        });

                let info = SyncInfo {
                    telegram_user_id: context.me.id(),
                    telegram_chat_id: chat.global_id(),
                    telegram_phone_number: context.me.phone().unwrap().to_string(),

                    huly_workspace_id: integration.workspace_id,
                    huly_space_id,
                    huly_card_id: ksuid::Ksuid::generate().to_base62(),
                    huly_card_title: chat.card_title(),
                };

                SyncContext::store_sync_info(&global_services, &info).await?;

                debug!(huly_space_id = %info.huly_space_id, huly_card_id = %info.huly_card_id, "Initialize SyncInfo");

                (info, true)
            };

            let mut context =
                SyncContext::new(context.clone(), chat.clone(), integration, info).await?;

            let ws_channels = wschannels.get(&context, integration.workspace_id).await?;

            if ws_channels.get(&context.info.huly_card_id).is_none() {
                // huly channel does not exist

                if !is_fresh {
                    // reset context (and lazily re-create)

                    let huly_card_id_old = context.info.huly_card_id.clone();

                    SyncContext::cleanup(
                        &context.worker.global,
                        context.info.huly_workspace_id,
                        context.info.telegram_user_id,
                        &context.info.telegram_chat_id,
                    )
                    .await?;

                    context = context.set_huly_card_id(ksuid::Ksuid::generate().to_base62());

                    SyncContext::store_sync_info(&context.worker.global, &context.info).await?;

                    debug!(
                        huly_card_id_old = huly_card_id_old,
                        huly_card_id = context.info.huly_card_id,
                        "Huly channel does not exist, re-creating"
                    );
                }

                Exporter::create_card(&context).await?;
            }

            debug!(huly_card_id = context.info.huly_card_id, "Sync spawned");

            let (sync, handle) = SyncChat::spawn(context).await;

            self.cleanup.lock().await.push(handle);
            self.syncs.insert(chat.global_id(), Arc::new(sync));
            sync_active += 1;

            Ok(SyncMode::Sync)
        };

        for chat in &self.all_chats {
            for integration in &self.integrations {
                let mode = match integration.find_config(chat.id()) {
                    // sync enabled
                    Some(config) if config.enabled => {
                        let span = debug_span!("Spawn synchronisation",
                            huly_workspace = %integration.workspace_id,
                            huly_account = %context.account_id,
                            telegram_phone = %context.me.phone().unwrap_or_default(),
                            telegram_user = %context.me.id(),
                            telegram_chat = %chat.id(),
                            telegram_chat_title = %chat.card_title()
                        );

                        spawn_sync(integration, chat, config)
                            .instrument(span)
                            .await?
                    }

                    // sync disabled
                    Some(_channel) => {
                        sync_disabled += 1;
                        SyncMode::Disabled
                    }

                    // no sync configuration
                    None => {
                        if SyncContext::cleanup(
                            global_services,
                            integration.workspace_id,
                            context.me.id(),
                            &chat.global_id(),
                        )
                        .await?
                        {
                            debug!(workspace = %integration.workspace_id,
                                telegram_user = %context.me.id(),
                                telegram_chat = %chat.global_id(),
                                telegram_chat_title = %chat.card_title(),
                            "Sync cleanup");
                        }

                        sync_unknown += 1;
                        SyncMode::Unknown
                    }
                };

                self.chats
                    .push((integration.workspace_id, chat.clone(), mode))
            }
        }

        debug!(
            all_chats,
            sync_active, sync_disabled, sync_unknown, "Sync stats"
        );

        let mut syncs = self.syncs.flat_iter().collect::<Vec<_>>();
        // ???
        syncs.sort_by_key(|(channel_id, _)| *channel_id);
        syncs.reverse();

        let syncs = syncs
            .into_iter()
            .map(|(_, sync)| sync.clone())
            .collect::<Vec<_>>();

        let global_semaphore = context.global.limiters().sync_semaphore.clone();

        let cleanup = self.cleanup.clone();

        let task = async move {
            let local_semaphore = Arc::new(Semaphore::new(CONFIG.sync_process_limit_local));

            for sync in syncs.into_iter() {
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
            .name(&format!("scheduler-{}", context.me.id()))
            .spawn(task)?;

        self.cleanup.lock().await.push(handle);

        Ok(())
    }

    pub fn chats(&self, workspace: WorkspaceUuid) -> Vec<(Arc<Chat>, SyncMode)> {
        if self
            .integrations
            .iter()
            .any(|i| i.workspace_id == workspace)
        {
            self.chats
                .iter()
                .filter(|(w, _, _)| *w == workspace)
                .map(|(_, c, m)| (c.clone(), *m))
                .collect()
        } else {
            self.all_chats
                .iter()
                .map(|c| (c.clone(), SyncMode::Unknown))
                .collect()
        }
    }

    pub async fn handle_update(&mut self, update: grammers_client::types::Update) -> Result<()> {
        use grammers_client::types::Update;

        let mut debouncer = self.debouncer.lock().await;

        fn is_empty(message: &grammers_client::types::update::Message) -> bool {
            use grammers_tl_types::enums::{Message, Update};
            use grammers_tl_types::types::{UpdateEditMessage, UpdateNewMessage};

            matches!(
                message.raw,
                Update::NewMessage(UpdateNewMessage {
                    message: Message::Empty(_),
                    ..
                }) | Update::EditMessage(UpdateEditMessage {
                    message: Message::Empty(_),
                    ..
                })
            )
        }

        match update {
            Update::NewMessage(message) if !is_empty(&message) => {
                let chat_id = message.chat().global_id();

                if let Some(syncs) = self.syncs.get_vec_mut(&chat_id) {
                    for sync in syncs {
                        let _ = sync
                            .sender_realtime
                            .send(Arc::new(ImporterEvent::NewMessage((*message).clone())))
                            .await;
                    }
                }
            }

            Update::MessageEdited(message) => {
                let chat_id = message.chat().global_id();

                if !debouncer.remove(&(chat_id.clone(), message.id())) {
                    if let Some(syncs) = self.syncs.get_vec_mut(&chat_id) {
                        for sync in syncs {
                            let _ = sync
                                .sender_realtime
                                .send(Arc::new(ImporterEvent::MessageEdited((*message).clone())))
                                .await;
                        }
                    }

                    debouncer.insert((chat_id.clone(), message.id()));
                }
            }

            Update::MessageDeleted(message) => {
                if !message.messages().is_empty() {
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
            }

            _ => {}
        }

        Ok(())
    }

    pub async fn handle_reverse_update(
        &self,
        sync_info: &SyncInfo,
        update: &ReverseUpdate,
    ) -> Result<()> {
        let mut debouncer = self.debouncer.lock().await;
        let syncs = self.syncs.get_vec(&sync_info.telegram_chat_id);

        if let Some(syncs) = syncs
            && let Some(sync) = syncs
                .iter()
                .find(|sync| sync.context.info.huly_workspace_id == sync_info.huly_workspace_id)
        {
            let telegram_id = sync.get_t_message(update.huly_message_id()).await?;

            if telegram_id.is_none()
                || !debouncer.remove(&(sync_info.telegram_chat_id.clone(), telegram_id.unwrap()))
            {
                if let Some(id) = sync.handle_reverse_update(update, telegram_id).await? {
                    debouncer.insert((sync_info.telegram_chat_id.clone(), id));
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
