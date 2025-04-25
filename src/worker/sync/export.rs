use std::{collections::HashMap, hash::Hasher, time::Duration};

use anyhow::Result;
use grammers_client::{
    Client as TelegramClient,
    client::files::DownloadIter,
    types::{Chat, Downloadable, Media, Message, User},
};
use hulyrs::services::{
    transactor::{
        document::{CreateDocumentBuilder, DocumentClient},
        event::{
            CreateFileEventBuilder, CreateMessageEventBuilder, CreatePatchEventBuilder,
            FileDataBuilder, MessageRequestType, RemoveMessagesEventBuilder,
        },
        person::{EnsurePerson, EnsurePersonRequest},
    },
    types::PersonId,
};
use serde_json::{self as json};
use tokio::time;
use tracing::*;

use super::{
    blob::{BlobClient, Sender as BlobSender},
    sync::SyncInfo,
};
use crate::{
    integration::WorkspaceIntegration,
    worker::{
        chat::ChatExt,
        services::{GlobalServices, Limiter, LimiterExt, WorkspaceServices},
    },
};

trait HulyMessageId {
    fn huly_message_id(&self) -> String;
    fn huly_blob_id(&self) -> uuid::Uuid;
}

impl HulyMessageId for Message {
    fn huly_message_id(&self) -> String {
        (self.chat().id(), self.id()).huly_message_id()
    }

    fn huly_blob_id(&self) -> uuid::Uuid {
        (self.chat().id(), self.id()).huly_blob_id()
    }
}

impl HulyMessageId for (i64, i32) {
    fn huly_message_id(&self) -> String {
        let mut hasher = std::hash::DefaultHasher::new();

        hasher.write_i64(self.0);
        hasher.write_i32(self.1);

        //??
        (hasher.finish() as i64).to_string()
    }

    fn huly_blob_id(&self) -> uuid::Uuid {
        let lower_u128 = (self.0 as u128) & 0xFFFF_FFFF_FFFF_FFFF;
        let middle_u128 = ((self.1 as u128) & 0xFFFF_FFFF) << 64;
        let upper_u128 = (0x2605_2025) << 96;

        uuid::Builder::from_u128(lower_u128 | middle_u128 | upper_u128).into_uuid()
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
    async fn download_all<D: Downloadable>(&mut self, d: &D, limiter: &Limiter) -> Result<Vec<u8>>;
    async fn download_in_chunks<D: Downloadable>(
        &mut self,
        d: &D,
        sender: BlobSender,
        limiter: &Limiter,
    ) -> Result<()>;
}

impl TelegramExt for TelegramClient {
    async fn download_all<D: Downloadable>(&mut self, d: &D, limiter: &Limiter) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut download = self.iter_download(d);

        let limiter_key = self.get_me().await?.id().to_string();

        while let Some(chunk) = download.next_timeout().await? {
            bytes.extend_from_slice(&chunk);
            limiter.wait(&limiter_key).await;
        }

        Ok(bytes)
    }

    #[instrument(level = "trace", skip_all)]
    async fn download_in_chunks<D: Downloadable>(
        &mut self,
        d: &D,
        sender: BlobSender,
        limiter: &Limiter,
    ) -> Result<()> {
        trace!("Download start");

        let mut download = self.iter_download(d);

        let limiter_key = self.get_me().await?.id().to_string();

        let mut nchunk = 0;
        loop {
            limiter.wait(&limiter_key).await;

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
    pub info: SyncInfo,
    telegram: TelegramClient,
    global_services: GlobalServices,
    workspace_services: WorkspaceServices,
    blobs: BlobClient,
    message_groups: HashMap<i64, String>,
    social_ids: HashMap<String, PersonId>,
}

impl Exporter {
    pub(super) async fn new(
        info_key: String,
        user: &User,
        chat: &Chat,
        ws: &WorkspaceIntegration,
        telegram: TelegramClient,
        global_services: GlobalServices,
        workspace_services: WorkspaceServices,
    ) -> Result<Self> {
        let info = global_services.kvs().get(&info_key).await?;

        let exporter = if let Some(info) = &info {
            let info = serde_json::from_slice::<SyncInfo>(info)?;
            let blobs = BlobClient::new(info.huly_workspace)?;

            trace!(user = user.id(), chat = chat.id(), "Dialog info was found");

            Self {
                info_key,
                info,
                telegram,
                global_services,
                workspace_services,
                blobs,
                message_groups: HashMap::new(),
                social_ids: HashMap::new(),
            }
        } else {
            trace!(
                user = user.id(),
                chat = chat.id(),
                "Dialog info was not found, creating new huly channel"
            );

            let channel_id = ksuid::Ksuid::generate().to_base62();
            let now = chrono::Utc::now();
            let create_channel = CreateDocumentBuilder::default()
                .object_id(&channel_id)
                .object_class("chat:masterTag:Channel")
                .created_by(&ws.person)
                .created_on(now)
                .modified_by(&ws.person)
                .modified_on(now)
                .object_space("card:space:Default")
                .attributes(serde_json::json!({
                    "title": format!("{}", chat.name().unwrap_or("no chat name")),
                    "rank": "0|i0000f:",
                    "content": "",
                    "parentInfo": [],
                    "blobs": {}
                }))
                .build()?;

            let _value: json::Value = workspace_services.transactor().tx(create_channel).await?;

            let info = SyncInfo {
                telegram_user: user.id(),
                telegram_type: chat.r#type(),
                telegram_chat: chat.id(),

                huly_workspace: ws.workspace,
                huly_account: ws.account,
                huly_channel: channel_id.clone(),
                complete: false,
            };

            trace!(user = info.telegram_user, chat = chat.id(), channel = %info.huly_channel, "Channel created");

            global_services
                .kvs()
                .upsert(&info_key, &json::to_vec(&info)?)
                .await?;

            let blobs = BlobClient::new(info.huly_workspace)?;

            Self {
                info_key,
                info,
                telegram,
                global_services,
                workspace_services,
                blobs,
                message_groups: HashMap::new(),
                social_ids: HashMap::new(),
            }
        };

        Ok(exporter)
    }

    pub fn limit(&self) -> Option<u32> {
        if self.info.complete { Some(1000) } else { None }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn set_complete(&mut self) -> Result<()> {
        self.info.complete = true;
        self.global_services
            .kvs()
            .upsert(&self.info_key, &json::to_vec(&self.info)?)
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
    pub async fn create(&mut self, message: &Message) -> Result<()> {
        let person_request = message
            .sender()
            .unwrap_or(message.chat())
            .ensure_person_request();

        let social_id = self.ensure_person(person_request).await?;

        let card = &self.info.huly_channel;

        let create_message = async || -> Result<String> {
            let huly_id = message.huly_message_id();

            let create = CreateMessageEventBuilder::default()
                .id(&huly_id)
                .external_id(format!("{}:{}", self.info.telegram_chat, message.id()))
                .card(&self.info.huly_channel)
                .card_type("chat:masterTag:Channel")
                .content(message.markdown_text())
                .creator(&social_id)
                .created(message.date())
                .build()
                .unwrap();

            self.global_services
                .hulygun()
                .request(
                    self.info.huly_workspace,
                    MessageRequestType::CreateMessage,
                    create,
                )
                .await?;

            Ok(huly_id)
        };

        let attach_to = if let Some(group) = message.grouped_id() {
            if let Some(huly_id) = self.message_groups.get(&group) {
                // the message is not first in the group, do update
                if !message.markdown_text().is_empty() {
                    let patch = CreatePatchEventBuilder::default()
                        .message(huly_id)
                        .message_created(message.date())
                        .card(card)
                        .content(message.markdown_text())
                        .creator(&social_id)
                        .build()
                        .unwrap();

                    self.global_services
                        .hulygun()
                        .request(
                            self.info.huly_workspace,
                            MessageRequestType::CreatePatch,
                            patch.clone(),
                        )
                        .await?;
                }

                huly_id.clone()
            } else {
                // the message is first in the group
                let huly_id = create_message().await?;
                self.message_groups.insert(group, huly_id.clone());
                trace!(huly_id, "Group message created");
                huly_id
            }
        } else {
            // the message is not grouped
            let huly_id = create_message().await?;
            trace!(huly_id, "Message created");
            huly_id
        };

        match message.media() {
            Some(Media::Photo(photo)) => {
                let bytes = self
                    .telegram
                    .download_all(&photo, self.global_services.limiter())
                    .await?;

                let id = message.huly_blob_id();
                let length = bytes.len();

                if let Ok(image_info) = imageinfo::ImageInfo::from_raw_data(&bytes) {
                    let sender = self.blobs.upload(id, length, image_info.mimetype)?;

                    sender.send(Ok(bytes)).await?;

                    let file_data = FileDataBuilder::default()
                        .blob_id(id)
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

                    let create_file = CreateFileEventBuilder::default()
                        .card(card)
                        .message(attach_to.clone())
                        .message_created(message.date())
                        .creator(&social_id)
                        .data(file_data)
                        .build()?;

                    self.global_services
                        .hulygun()
                        .request(
                            self.info.huly_workspace,
                            MessageRequestType::CreateFile,
                            create_file,
                        )
                        .await
                        .unwrap();

                    trace!(blob=%id, "Blob attached");
                }
            }

            Some(Media::Document(document)) => {
                let id = message.huly_blob_id();
                let mime_type = document.mime_type().unwrap_or("application/binary");
                let length = document.size();

                let sender = self.blobs.upload(id, length as usize, mime_type)?;
                let download_result = self
                    .telegram
                    .download_in_chunks(&document, sender, self.global_services.limiter())
                    .await;

                if download_result.is_ok() {
                    let file_data = FileDataBuilder::default()
                        .blob_id(id)
                        .size(length as u32)
                        .mime_type(mime_type)
                        .filename(document.name())
                        .build()?;

                    let create_file = CreateFileEventBuilder::default()
                        .card(card)
                        .message(attach_to.clone())
                        .message_created(message.date())
                        .creator(&social_id)
                        .data(file_data)
                        .build()?;

                    self.global_services
                        .hulygun()
                        .request(
                            self.info.huly_workspace,
                            MessageRequestType::CreateFile,
                            create_file,
                        )
                        .await
                        .unwrap();
                }
            }
            _ => {
                //
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn update(&mut self, message: &Message) -> Result<()> {
        let person_request = message
            .sender()
            .unwrap_or(message.chat())
            .ensure_person_request();

        let social_id = self.ensure_person(person_request).await?;

        let card = &self.info.huly_channel;

        let message_id = if let Some(group) = message.grouped_id() {
            self.message_groups.get(&group).map(ToOwned::to_owned)
        } else {
            Some(message.huly_message_id())
        };

        if let Some(message_id) = message_id {
            if !message.markdown_text().is_empty() {
                let patch = CreatePatchEventBuilder::default()
                    .message(&message_id)
                    .message_created(message.date())
                    .card(card)
                    .content(message.markdown_text())
                    .creator(&social_id)
                    .build()
                    .unwrap();

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

        Ok(())
    }

    pub(super) async fn delete(&mut self, message: i32) -> Result<()> {
        let message_id = (self.info.telegram_chat, message).huly_message_id();

        let workspace = &self.info.huly_workspace;

        let remove = RemoveMessagesEventBuilder::default()
            .card(&self.info.huly_channel)
            .messages(vec![message_id.clone()])
            .build()
            .unwrap();

        self.global_services
            .hulygun()
            .request(*workspace, MessageRequestType::RemoveMessages, remove)
            .await?;

        Ok(())
    }
}
