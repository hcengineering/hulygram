use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use grammers_client::types::{Media, Message};
use hulyrs::services::{
    transactor::{
        event::{
            BlobDataBuilder, BlobPatchEventBuilder, BlobPatchOperation, CreateMessageEventBuilder,
            MessageRequestType, MessageType, RemovePatchEventBuilder, UpdatePatchEventBuilder,
        },
        person::EnsurePerson,
    },
    types::PersonId,
};
use redis::AsyncCommands;
use tracing::*;

use super::{context::SyncContext, media::MediaTransfer, telegram::MessageExt, tx::TransactorExt};
use crate::{
    CONFIG,
    context::GlobalContext,
    worker::sync::state::{BlobDescriptor, HulyMessage},
};

pub type MessageId = String;

#[derive(Clone)]
pub(super) struct Exporter {
    global_context: Arc<GlobalContext>,
    pub context: Arc<SyncContext>,
    groups: HashMap<i64, MessageId>,
}

pub enum CardState {
    Exists,
    Created,
    NotExists,
}

impl Exporter {
    #[instrument(level = "trace", skip_all, fields(chat = %context.chat.id(), user = %context.worker.me.id(), account = %context.worker.account_id, workspace = %context.info.huly_workspace_id))]
    pub(super) async fn new(context: Arc<SyncContext>) -> Result<Self> {
        let global_context = context.worker.global.clone();

        let exporter = Self {
            global_context,
            context,
            groups: HashMap::new(),
        };

        Ok(exporter)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn ensure_card(&mut self, is_fresh: bool) -> Result<CardState> {
        let tx = self.context.transactor();
        let info = &self.context.info;

        let ensured = if tx.find_channel(&self.context.info.huly_card_id).await? {
            CardState::Exists
        } else {
            if is_fresh {
                let space_id = if info.is_private {
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

                    space_id
                } else {
                    "card:space:Default".to_owned()
                };

                tx.create_channel(
                    &info.huly_card_id,
                    &self.context.worker.social_id,
                    &space_id,
                    &info.huly_card_title,
                )
                .await?;

                CardState::Created
            } else {
                CardState::NotExists
            }
        };

        Ok(ensured)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn ensure_person(&mut self, message: &Message) -> Result<PersonId> {
        let request = message.ensure_person_request();

        let mut redis = self.global_context.redis();

        if let Some(person_id) = redis
            .hget::<_, _, Option<PersonId>>("socialid", &request.social_value)
            .await?
        {
            return Ok(person_id.clone());
        } else {
            trace!(social_value = request.social_value, "CacheMiss");
            let ensured = self.context.transactor().ensure_person(&request).await?;

            let _: () = redis
                .hset("socialid", &request.social_value, &ensured.social_id)
                .await?;

            return Ok(ensured.social_id);
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn new_message(
        &mut self,
        //card: &CardInfo,
        person_id: &String,
        message: &Message,
    ) -> Result<HulyMessage> {
        let info = &self.context.info;
        let workspace_id = info.huly_workspace_id;

        let dry_run = crate::config::CONFIG.dry_run;

        let create_message = async || -> Result<MessageId> {
            let huly_message_id = message.as_huly_message().id;

            let create_event = CreateMessageEventBuilder::default()
                .message_id(huly_message_id.clone())
                .message_type(MessageType::Message)
                .card_id(info.huly_card_id.clone())
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
                        workspace_id,
                        MessageRequestType::CreateMessage,
                        create_event,
                    )
                    .await?;
            }

            Ok(huly_message_id)
        };

        let huly_id = if let Some(grouped_id) = message.grouped_id() {
            if let Some(root_message_id) = self.groups.get(&grouped_id) {
                // the message is not first in the group, do update
                if !message.markdown_text().is_empty() {
                    let patch_event = UpdatePatchEventBuilder::default()
                        .message_id(root_message_id.to_string())
                        .date(message.date())
                        .social_id(person_id)
                        .card_id(&info.huly_card_id)
                        .content(message.markdown_text())
                        .build()
                        .unwrap();

                    if !dry_run {
                        self.global_context
                            .hulygun()
                            .request(workspace_id, MessageRequestType::UpdatePatch, patch_event)
                            .await?;
                    }
                }

                root_message_id.clone()
            } else {
                // the message is first in the group
                let huly_id = create_message().await?;

                self.groups.insert(grouped_id, huly_id.clone());

                trace!(%huly_id, "Group message created");

                huly_id
            }
        } else {
            // the message is not grouped
            let huly_id = create_message().await?;
            trace!(%huly_id, "Message created");

            huly_id
        };

        if let Some(media) = message.media() {
            match media {
                Media::Photo(photo) => {
                    photo
                        .transfer(self, message, huly_id.clone(), person_id.clone())
                        .await?;
                }

                Media::Document(document) => {
                    document
                        .transfer(self, message, huly_id.clone(), person_id.clone())
                        .await?;
                }

                _ => {
                    //
                }
            }
        }

        Ok(message.as_huly_message())
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn edit(
        &mut self,
        person_id: &PersonId,
        huly_message: HulyMessage,
        telegram_message: &Message,
    ) -> Result<HulyMessage> {
        if !telegram_message.markdown_text().is_empty() {
            let patch_event = UpdatePatchEventBuilder::default()
                .message_id(&huly_message.id)
                .date(telegram_message.last_date())
                .social_id(person_id)
                .card_id(&self.context.info.huly_card_id)
                .content(telegram_message.markdown_text())
                .build()
                .unwrap();

            if !crate::config::CONFIG.dry_run {
                self.global_context
                    .hulygun()
                    .request(
                        self.context.info.huly_workspace_id,
                        MessageRequestType::UpdatePatch,
                        patch_event,
                    )
                    .await?;
            }
        }

        Ok(HulyMessage {
            date: telegram_message.last_date(),
            ..huly_message
        })
    }

    pub(super) async fn delete(&mut self, huly_id: &String) -> Result<()> {
        let patch = RemovePatchEventBuilder::default()
            .card_id(&self.context.info.huly_card_id)
            .message_id(huly_id)
            .social_id(&self.context.worker.social_id)
            .date(Utc::now())
            .build()
            .unwrap();

        if !crate::config::CONFIG.dry_run {
            self.global_context
                .hulygun()
                .request(
                    self.context.info.huly_workspace_id,
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
        //  info: &SyncInfo,
        message_id: MessageId,
        social_id: PersonId,
        date: DateTime<Utc>,
    ) -> Result<()> {
        let mut blob_data = BlobDataBuilder::default();
        let info = &self.context.info;

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
            .card_id(&info.huly_card_id)
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
                    self.context.info.huly_workspace_id,
                    MessageRequestType::BlobPatch,
                    attach_event,
                )
                .await?;

            trace!(blob_id=%blob.blob_id, "Blob attached");
        }

        Ok(())
    }
}
