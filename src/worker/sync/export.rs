use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use grammers_client::{
    Client as TelegramClient,
    client::files::DownloadIter,
    types::{Chat, Downloadable, Media, Message, User},
};
use hulyrs::services::{
    transactor::{
        event::{
            CreateFileEventBuilder, CreateMessageEventBuilder, CreatePatchEventBuilder,
            FileDataBuilder, MessageRequestType, PatchData,
        },
        person::{EnsurePerson, EnsurePersonRequest},
    },
    types::PersonId,
};
use rand::Rng;
use tokio::time;
use tracing::*;
use uuid::Uuid;

use super::{
    blob::{BlobClient, Sender as BlobSender},
    state::{Entry as StateEntry, EntryBuilder as StateEntryBuilder},
    sync::DialogInfo,
    tx::TransactorExt,
};
use crate::{
    CONFIG,
    integration::WorkspaceIntegration,
    worker::{
        chat::ChatExt,
        limiters::TelegramLimiter,
        services::{GlobalServices, WorkspaceServices},
        sync::state::GroupRole,
    },
};

type MessageId = i64;

trait HulyMessageId {
    fn huly_message_id(&self) -> MessageId;
    fn huly_blob_id(&self) -> Uuid;
}

impl HulyMessageId for Message {
    fn huly_message_id(&self) -> MessageId {
        (self.chat().id(), self.id()).huly_message_id()
    }

    fn huly_blob_id(&self) -> Uuid {
        (self.chat().id(), self.id()).huly_blob_id()
    }
}

impl HulyMessageId for (i64, i32) {
    fn huly_message_id(&self) -> MessageId {
        //Uuid::new_v4()

        rand::rng().random()
    }

    fn huly_blob_id(&self) -> Uuid {
        Uuid::new_v4()
    }
}

trait DownloadIterExt {
    async fn next_timeout(&mut self) -> Result<Option<Vec<u8>>>;
}

impl DownloadIterExt for DownloadIter {
    async fn next_timeout(&mut self) -> Result<Option<Vec<u8>>> {
        match time::timeout(Duration::from_secs(5), self.next()).await {
            Ok(x) => x.map_err(Into::into),
            Err(_) => {
                anyhow::bail!("Timeout");
            }
        }
    }
}

trait TelegramExt {
    async fn download_all<D: Downloadable>(
        &mut self,
        d: &D,
        limiter: &TelegramLimiter,
    ) -> Result<Vec<u8>>;
    async fn download_in_chunks<D: Downloadable>(
        &mut self,
        d: &D,
        sender: BlobSender,
        limiter: &TelegramLimiter,
    ) -> Result<()>;
}

impl TelegramExt for TelegramClient {
    async fn download_all<D: Downloadable>(
        &mut self,
        d: &D,
        limiter: &TelegramLimiter,
    ) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut download = self.iter_download(d);

        let limiter_key = self.get_me().await?.id();

        while let Some(chunk) = download.next_timeout().await? {
            bytes.extend_from_slice(&chunk);
            limiter.until_key_ready(&limiter_key).await;
        }

        Ok(bytes)
    }

    #[instrument(level = "trace", skip_all)]
    async fn download_in_chunks<D: Downloadable>(
        &mut self,
        d: &D,
        sender: BlobSender,
        limiter: &TelegramLimiter,
    ) -> Result<()> {
        trace!("Download start");

        let mut download = self.iter_download(d);

        let limiter_key = self.get_me().await?.id();

        let mut nchunk = 0;
        loop {
            limiter.until_key_ready(&limiter_key).await;

            match download.next_timeout().await {
                Ok(Some(chunk)) => {
                    nchunk += 1;
                    trace!(nchunk, "Chunk");
                    sender.send(Ok(chunk)).await?
                }
                Ok(None) => {
                    break {
                        trace!("Download complete");
                        Ok(())
                    };
                }
                Err(error) => {
                    let message = error.to_string();

                    warn!(%error, "Chunk error");

                    sender
                        .send(Err(std::io::Error::new(std::io::ErrorKind::Other, error)))
                        .await?;

                    break Err(anyhow::anyhow!("{}", message));
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Exporter {
    info_key: String,
    pub info: DialogInfo,
    telegram: TelegramClient,
    global_services: GlobalServices,
    workspace_services: WorkspaceServices,
    blobs: BlobClient,
    groups: HashMap<i64, MessageId>,
    social_ids: HashMap<String, PersonId>,
}

impl Exporter {
    #[instrument(level = "trace", skip_all, fields(chat = %chat.id(), user = %me.id(), account = %ws.account, workspace = %ws.workspace))]
    pub(super) async fn maybe_create(
        info_key: String,
        me: &Arc<User>,
        chat: &Chat,
        ws: &WorkspaceIntegration,
        telegram: TelegramClient,
        global_services: GlobalServices,
        workspace_services: WorkspaceServices,
    ) -> Result<Option<Self>> {
        let info = global_services.kvs().get(&info_key).await?;

        let tx = workspace_services.transactor();

        let exporter = if let Some(info) = &info {
            let info = serde_json::from_slice::<DialogInfo>(info)?;
            let blobs = BlobClient::new(info.huly_workspace)?;

            trace!("Chat info found");

            let is_found = tx.find_channel(&info.huly_channel).await?;

            if is_found {
                Some(Self {
                    info_key,
                    info,
                    telegram,
                    global_services,
                    workspace_services,
                    blobs,
                    groups: HashMap::new(),
                    social_ids: HashMap::new(),
                })
            } else {
                trace!("Channel not found");
                None
            }
        } else {
            trace!("Chat info not found");

            let card_title = chat.card_title();
            let is_private = !CONFIG.allowed_dialog_ids.contains(&chat.id().to_string());

            let (space, channel) = if is_private {
                let person_id = tx.find_person(ws.account).await?.ok_or_else(|| {
                    warn!("Person not found");
                    anyhow!("NoPerson")
                })?;

                let space_id = tx.find_personal_space(&person_id).await?.ok_or_else(|| {
                    warn!(%person_id, "Personal space not found");
                    anyhow!("NoPersonSpace")
                })?;

                let channel = tx
                    .create_channel(&ws.social_id, &space_id, &card_title)
                    .await?;

                (space_id, channel)
            } else {
                let space = "card:space:Default".to_owned();

                let channel = tx
                    .create_channel(&ws.social_id, &space, &card_title)
                    .await?;

                (space, channel)
            };

            let info = DialogInfo {
                telegram_user: me.id(),
                telegram_type: chat.r#type(),
                telegram_chat_id: chat.id(),

                huly_workspace: ws.workspace,
                huly_account: ws.account,
                huly_social_id: ws.social_id.clone(),
                huly_channel: channel.clone(),
                huly_space: space,
                huly_title: card_title,

                is_complete: false,
            };

            trace!(channel = %info.huly_channel, "Channel created");

            global_services
                .kvs()
                .upsert(&info_key, &serde_json::to_vec(&info)?)
                .await?;

            let blobs = BlobClient::new(info.huly_workspace)?;

            Some(Self {
                info_key,
                info,
                telegram,
                global_services,
                workspace_services,
                blobs,
                groups: HashMap::new(),
                social_ids: HashMap::new(),
            })
        };

        Ok(exporter)
    }

    pub fn limit(&self) -> Option<u32> {
        if self.info.is_complete {
            Some(1000)
        } else {
            None
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn set_complete(&mut self) -> Result<()> {
        self.info.is_complete = true;
        self.global_services
            .kvs()
            .upsert(&self.info_key, &serde_json::to_vec(&self.info)?)
            .await?;

        Ok(())
    }

    #[instrument(level = "trace", skip_all, fields(social = %request.social_value))]
    async fn ensure_person(&mut self, request: EnsurePersonRequest) -> Result<PersonId> {
        if let Some(person_id) = self.social_ids.get(&request.social_value) {
            return Ok(person_id.clone());
        } else {
            trace!("Cache miss");
            let ensured = self
                .workspace_services
                .transactor()
                .ensure_person(&request)
                .await?;

            self.social_ids
                .insert(request.social_value, ensured.social_id.clone());

            return Ok(ensured.social_id);
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn create(&mut self, message: &Message) -> Result<StateEntry> {
        let dry_run = crate::config::CONFIG.dry_run;

        let person_request = message
            .sender()
            .unwrap_or(message.chat())
            .ensure_person_request();

        let social_id = self.ensure_person(person_request).await?;

        let huly_channel_id = &self.info.huly_channel;

        let mut entry_builder = StateEntryBuilder::default();

        entry_builder.telegram_message_id(message.id());
        entry_builder.date(message.edit_date().unwrap_or(message.date()));

        let create_message = async || -> Result<MessageId> {
            let huly_message_id = message.huly_message_id();

            let create_event = CreateMessageEventBuilder::default()
                .id(&huly_message_id.to_string())
                .external_id(format!("{}:{}", self.info.telegram_chat_id, message.id()))
                .card(huly_channel_id)
                .card_type("chat:masterTag:Channel")
                .content(message.markdown_text())
                .creator(&social_id)
                .created(message.date())
                .build()
                .unwrap();

            if !dry_run {
                self.global_services
                    .hulygun()
                    .request(
                        self.info.huly_workspace,
                        MessageRequestType::CreateMessage,
                        create_event,
                    )
                    .await?;
            }

            Ok(huly_message_id)
        };

        let huly_message_id = if let Some(grouped_id) = message.grouped_id() {
            if let Some(root_message_id) = self.groups.get(&grouped_id) {
                // the message is not first in the group, do update
                if !message.markdown_text().is_empty() {
                    let patch_data = PatchData::Update {
                        content: Some(message.markdown_text()),
                        data: None,
                    };

                    let patch_event = CreatePatchEventBuilder::default()
                        .message(root_message_id.to_string())
                        .message_created(message.date())
                        .creator(&social_id)
                        .card(huly_channel_id)
                        .data(patch_data)
                        .build()
                        .unwrap();

                    if !dry_run {
                        self.global_services
                            .hulygun()
                            .request(
                                self.info.huly_workspace,
                                MessageRequestType::CreatePatch,
                                patch_event,
                            )
                            .await?;
                    }
                }

                entry_builder.huly_message_id(*root_message_id);
                entry_builder.group_role(Some(GroupRole::Member));

                *root_message_id
            } else {
                // the message is first in the group
                let huly_message_id = create_message().await?;
                self.groups.insert(grouped_id, huly_message_id);
                trace!(%huly_message_id, "Group message created");

                entry_builder.huly_message_id(huly_message_id);
                entry_builder.group_role(Some(GroupRole::Root));

                huly_message_id
            }
        } else {
            // the message is not grouped
            let huly_message_id = create_message().await?;
            trace!(%huly_message_id, "Message created");

            entry_builder.huly_message_id(huly_message_id);

            huly_message_id
        };

        match message.media() {
            Some(Media::Photo(photo)) => {
                let blob = self
                    .telegram
                    .download_all(&photo, &self.global_services.limiters().get_file)
                    .await?;

                let huly_blob_id = message.huly_blob_id();
                let length = blob.len();

                if let Ok(image_info) = imageinfo::ImageInfo::from_raw_data(&blob) {
                    let ready = {
                        let (sender, ready) =
                            self.blobs
                                .upload(huly_blob_id, length, image_info.mimetype)?;

                        sender.send(Ok(blob)).await?;

                        ready
                    };

                    // wait for upload to complete
                    let _ = ready.await?;

                    let file_data = FileDataBuilder::default()
                        .blob_id(huly_blob_id)
                        .size(length as u32)
                        .mime_type(image_info.mimetype)
                        .filename("photo.jpg")
                        .meta([
                            (
                                "originalWidth".to_owned(),
                                image_info.size.width.to_string(),
                            ),
                            (
                                "originalHeight".to_owned(),
                                image_info.size.height.to_string(),
                            ),
                        ])
                        .build()?;

                    let create_file_event = CreateFileEventBuilder::default()
                        .card(huly_channel_id)
                        .message(huly_message_id.to_string())
                        .message_created(message.date())
                        .creator(&social_id)
                        .data(file_data)
                        .build()?;

                    if !dry_run {
                        self.global_services
                            .hulygun()
                            .request(
                                self.info.huly_workspace,
                                MessageRequestType::CreateFile,
                                create_file_event,
                            )
                            .await?;
                    }

                    entry_builder.huly_image_id(Some(huly_blob_id));

                    trace!(blob=%huly_blob_id, "Blob attached");
                }
            }

            Some(Media::Document(document)) => {
                let huly_blob_id = message.huly_blob_id();
                let mime_type = document.mime_type().unwrap_or("application/binary");
                let length = document.size();

                let (sender, ready) =
                    self.blobs
                        .upload(huly_blob_id, length as usize, mime_type)?;
                let download_result = self
                    .telegram
                    .download_in_chunks(
                        &document,
                        sender,
                        &self.global_services.limiters().get_file,
                    )
                    .await;

                let _ = ready.await?;

                if download_result.is_ok() {
                    let file_data = FileDataBuilder::default()
                        .blob_id(huly_blob_id)
                        .size(length as u32)
                        .mime_type(mime_type)
                        .filename(document.name())
                        .build()?;

                    let create_file = CreateFileEventBuilder::default()
                        .card(huly_channel_id)
                        .message(huly_message_id.to_string())
                        .message_created(message.date())
                        .creator(&social_id)
                        .data(file_data)
                        .build()?;

                    if !dry_run {
                        self.global_services
                            .hulygun()
                            .request(
                                self.info.huly_workspace,
                                MessageRequestType::CreateFile,
                                create_file,
                            )
                            .await?;
                    }

                    entry_builder.huly_image_id(Some(huly_blob_id));
                }
            }
            _ => {
                //
            }
        }

        Ok(entry_builder.build().unwrap())
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn update(
        &mut self,
        message: &Message,
        mut state_entry: StateEntry,
    ) -> Result<StateEntry> {
        let dry_run = crate::config::CONFIG.dry_run;

        let person_request = message
            .sender()
            .unwrap_or(message.chat())
            .ensure_person_request();

        let social_id = self.ensure_person(person_request).await?;

        let card = &self.info.huly_channel;

        let message_id = if let Some(group) = message.grouped_id() {
            self.groups.get(&group).map(ToOwned::to_owned)
        } else {
            Some(message.huly_message_id())
        };

        if let Some(message_id) = message_id {
            if !message.markdown_text().is_empty() {
                let patch_data = PatchData::Update {
                    content: Some(message.markdown_text()),
                    data: None,
                };

                let patch = CreatePatchEventBuilder::default()
                    .message(&message_id.to_string())
                    .message_created(message.date())
                    .creator(&social_id)
                    .card(card)
                    .data(patch_data)
                    .build()
                    .unwrap();

                if !dry_run {
                    self.global_services
                        .hulygun()
                        .request(
                            self.info.huly_workspace,
                            MessageRequestType::CreatePatch,
                            patch.clone(),
                        )
                        .await?;
                }
            }
        }

        state_entry.date = message.edit_date().unwrap_or(message.date());

        Ok(state_entry)
    }

    pub(super) async fn delete(&mut self, message: i32) -> Result<()> {
        let _message_id = (self.info.telegram_chat_id, message).huly_message_id();

        let _workspace = &self.info.huly_workspace;

        /*
        let remove = RemoveMessagesEventBuilder::default()
            .card(&self.info.huly_channel)
            .messages(vec![message_id.clone()])
            .build()
            .unwrap();

        self.global_services
            .hulygun()
            .request(*workspace, MessageRequestType::RemoveMessages, remove)
            .await?;
        */

        Ok(())
    }
}
