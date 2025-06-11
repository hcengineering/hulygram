use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use grammers_client::types::{Chat, Media, Message};
use hulyrs::services::{
    transactor::{
        event::{
            BlobDataBuilder, BlobPatchEventBuilder, BlobPatchOperation, CreateMessageEventBuilder,
            MessageRequestType, MessageType, RemovePatchEventBuilder, UpdatePatchEventBuilder,
        },
        person::{EnsurePerson, EnsurePersonRequest, EnsurePersonRequestBuilder},
    },
    types::{PersonId, SocialIdType},
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json as json;
use tracing::*;

use super::{context::SyncContext, media::MediaTransfer, tx::TransactorExt};
use crate::{
    CONFIG,
    context::GlobalContext,
    worker::{chat::ChatExt, sync::state::BlobDescriptor},
};

pub type MessageId = String;

pub trait HulyMessageId {
    fn huly_message_id(&self) -> MessageId;
}

impl HulyMessageId for Message {
    fn huly_message_id(&self) -> MessageId {
        self.id().to_string()
    }
}

trait MessageExt {
    fn ensure_person_request(&self) -> EnsurePersonRequest;
}

impl MessageExt for Message {
    fn ensure_person_request(&self) -> EnsurePersonRequest {
        fn names(chat: &Chat) -> (String, Option<String>) {
            match chat {
                Chat::User(user) => (
                    user.first_name()
                        .map(ToString::to_string)
                        .unwrap_or("Deleted User".to_string()),
                    user.last_name().map(ToOwned::to_owned),
                ),

                Chat::Channel(channel) => (channel.title().to_owned(), None),

                Chat::Group(group) => (group.title().unwrap_or("Telegram Group").to_owned(), None),
            }
        }

        let sender = self.sender().unwrap_or(self.chat());

        let (first_name, last_name) = names(&sender);

        EnsurePersonRequestBuilder::default()
            .first_name(first_name)
            .last_name(last_name)
            .social_type(SocialIdType::Telegram)
            .social_value(sender.id().to_string())
            .build()
            .unwrap()
    }
}

#[derive(Clone)]
pub(super) struct Exporter {
    global_context: Arc<GlobalContext>,
    pub context: Arc<SyncContext>,
    groups: HashMap<i64, MessageId>,
    social_ids: HashMap<String, PersonId>,
}

#[derive(Deserialize, Serialize)]
pub struct CardInfo {
    space_id: String,
    card_id: String,
}

impl Exporter {
    #[instrument(level = "trace", skip_all, fields(chat = %context.chat.id(), user = %context.worker.me.id(), account = %context.worker.account_id, workspace = %context.workspace_id))]
    pub(super) async fn new(context: Arc<SyncContext>) -> Result<Self> {
        let global_context = context.worker.global.clone();

        let exporter = Self {
            global_context,
            context,
            groups: HashMap::new(),
            social_ids: HashMap::new(),
        };

        Ok(exporter)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn ensure_card(&mut self) -> Result<Option<CardInfo>> {
        let chat = &self.context.chat;
        let tx = self.context.transactor();
        let mut redis = self.global_context.redis();
        let key = format!("{}.card", self.context.sync_id);

        let card = match redis.get::<_, Option<Vec<u8>>>(&key).await? {
            Some(card) => {
                let card = json::from_slice::<CardInfo>(&card)?;

                if tx.find_channel(&card.card_id).await? {
                    Some(card)
                } else {
                    None
                }
            }
            None => {
                let card_title = chat.card_title();
                let is_private = !CONFIG.allowed_dialog_ids.contains(&chat.id().to_string());
                let card_id = ksuid::Ksuid::generate().to_base62();

                let space_id = if is_private {
                    let person_id = tx
                        .find_person(self.context.worker.account_id)
                        .await?
                        .ok_or_else(|| {
                            warn!("Person not found");
                            anyhow!("NoPerson")
                        })?;

                    let space_id = tx.find_personal_space(&person_id).await?.ok_or_else(|| {
                        warn!(%person_id, "Personal space not found");
                        anyhow!("NoPersonSpace")
                    })?;

                    tx.create_channel(
                        &card_id,
                        &self.context.worker.social_id,
                        &space_id,
                        &card_title,
                    )
                    .await?;

                    space_id
                } else {
                    let space = "card:space:Default".to_owned();

                    tx.create_channel(
                        &card_id,
                        &self.context.worker.social_id,
                        &space,
                        &card_title,
                    )
                    .await?;

                    space
                };

                let card = CardInfo { space_id, card_id };

                let _: () = redis.set(&key, &json::to_vec(&card)?).await?;

                Some(card)
            }
        };

        Ok(card)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn ensure_person(&mut self, message: &Message) -> Result<PersonId> {
        let request = message.ensure_person_request();

        if let Some(person_id) = self.social_ids.get(&request.social_value) {
            return Ok(person_id.clone());
        } else {
            trace!(social_value = request.social_value, "CacheMiss");
            let ensured = self.context.transactor().ensure_person(&request).await?;

            self.social_ids
                .insert(request.social_value, ensured.social_id.clone());

            return Ok(ensured.social_id);
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn new_message(
        &mut self,
        card: &CardInfo,
        person_id: &String,
        message: &Message,
    ) -> Result<String> {
        let dry_run = crate::config::CONFIG.dry_run;

        let create_message = async || -> Result<MessageId> {
            let huly_message_id = message.huly_message_id();

            let create_event = CreateMessageEventBuilder::default()
                .message_id(huly_message_id.clone())
                .message_type(MessageType::Message)
                .card_id(card.card_id.clone())
                .card_type("chat:masterTag:Channel")
                .content(message.markdown_text())
                .social_id(person_id)
                .date(message.date())
                .build()
                .unwrap();

            if !dry_run {
                self.global_context
                    .hulygun()
                    .request(
                        self.context.workspace_id,
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
                    let patch_event = UpdatePatchEventBuilder::default()
                        .message_id(root_message_id.to_string())
                        .date(message.date())
                        .social_id(person_id)
                        .card_id(card.card_id.clone())
                        .content(message.markdown_text())
                        .build()
                        .unwrap();

                    if !dry_run {
                        self.global_context
                            .hulygun()
                            .request(
                                self.context.workspace_id,
                                MessageRequestType::UpdatePatch,
                                patch_event,
                            )
                            .await?;
                    }
                }

                root_message_id.clone()
            } else {
                // the message is first in the group
                let huly_message_id = create_message().await?;

                self.groups.insert(grouped_id, huly_message_id.clone());

                trace!(%huly_message_id, "Group message created");

                huly_message_id
            }
        } else {
            // the message is not grouped
            let huly_message_id = create_message().await?;
            trace!(%huly_message_id, "Message created");

            huly_message_id
        };

        if let Some(media) = message.media() {
            match media {
                Media::Photo(photo) => {
                    photo
                        .transfer(
                            self,
                            message,
                            card,
                            huly_message_id.clone(),
                            person_id.clone(),
                        )
                        .await?;
                }

                Media::Document(document) => {
                    document
                        .transfer(
                            self,
                            message,
                            card,
                            huly_message_id.clone(),
                            person_id.clone(),
                        )
                        .await?;
                }

                _ => {
                    //
                }
            }
        }

        Ok(huly_message_id)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn edit(
        &mut self,
        card: &CardInfo,
        person_id: &PersonId,
        huly_id: &String,
        message: &Message,
    ) -> Result<()> {
        if !message.markdown_text().is_empty() {
            let patch_event = UpdatePatchEventBuilder::default()
                .message_id(huly_id)
                .date(message.date())
                .social_id(person_id)
                .card_id(&card.card_id)
                .content(message.markdown_text())
                .build()
                .unwrap();

            if !crate::config::CONFIG.dry_run {
                self.global_context
                    .hulygun()
                    .request(
                        self.context.workspace_id,
                        MessageRequestType::UpdatePatch,
                        patch_event,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub(super) async fn delete(&mut self, card: &CardInfo, huly_id: &String) -> Result<()> {
        let patch = RemovePatchEventBuilder::default()
            .card_id(&card.card_id)
            .message_id(huly_id)
            .social_id(&self.context.worker.social_id)
            .date(Utc::now())
            .build()
            .unwrap();

        if !crate::config::CONFIG.dry_run {
            self.global_context
                .hulygun()
                .request(
                    self.context.workspace_id,
                    MessageRequestType::RemovePatch,
                    patch,
                )
                .await?;
        }

        Ok(())
    }

    pub async fn attach(
        &self,
        blob: BlobDescriptor,
        card: &CardInfo,
        message_id: MessageId,
        social_id: PersonId,
        date: DateTime<Utc>,
    ) -> Result<()> {
        let mut blob_data = BlobDataBuilder::default();

        blob_data
            .blob_id(blob.blob_id.to_string())
            .size(blob.length as u32)
            .mime_type(blob.mimetype);

        if let Some((width, height)) = blob.size {
            blob_data.metadata([
                ("originalWidth".to_owned(), width.to_string().into()),
                ("originalHeight".to_owned(), height.to_string().into()),
            ]);
        }

        if let Some(file_name) = blob.file_name {
            blob_data.file_name(file_name);
        } else {
            // choose extension based on mimetype
            blob_data.file_name("photo.jpg");
        }

        let blob_data = blob_data.build()?;

        let attach_event = BlobPatchEventBuilder::default()
            .card_id(&card.card_id)
            .message_id(message_id)
            .date(date)
            .social_id(social_id)
            .operations(vec![BlobPatchOperation::Attach {
                blobs: vec![blob_data],
            }])
            .build()?;

        if !CONFIG.dry_run {
            self.context
                .worker
                .global
                .hulygun()
                .request(
                    self.context.workspace_id,
                    MessageRequestType::BlobPatch,
                    attach_event,
                )
                .await?;

            trace!(blob_id=%blob.blob_id, "Blob attached");
        }

        Ok(())
    }
}
