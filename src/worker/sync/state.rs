use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize};
use serde_json as json;
use uuid::Uuid;

use crate::{context::GlobalContext, worker::sync::context::SyncInfo};

#[derive(
    Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default, derive_more::IsVariant,
)]
pub enum Progress {
    #[default]
    Unsynced,
    Progress(i32),
    Complete,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct HulyMessage {
    pub id: String,
    pub date: DateTime<Utc>,
}

#[derive(Deserialize, Serialize)]
pub struct BlobDescriptor {
    pub blob_id: Uuid,
    pub length: usize,
    pub mimetype: String,
    pub file_name: Option<String>,
    pub size: Option<(u16, u16)>,
}

pub struct SyncState {
    info: SyncInfo,
    redis: MultiplexedConnection,
}

pub trait KeyPrefixes {
    fn with_base_prefix(&self, s: &str) -> String;
    fn with_reverse_prefix(&self, s: &str) -> String;
}

impl KeyPrefixes for SyncInfo {
    fn with_base_prefix(&self, s: &str) -> String {
        format!(
            "{}:t{}:{}.{}",
            self.huly_workspace_id, self.telegram_user_id, self.telegram_chat_id, s
        )
    }

    fn with_reverse_prefix(&self, s: &str) -> String {
        format!("{}:h{}:{}", self.huly_workspace_id, self.huly_card_id, s)
    }
}

impl SyncState {
    pub async fn new(info: SyncInfo, context: Arc<GlobalContext>) -> Result<Self> {
        Ok(Self {
            redis: context.redis(),
            info,
        })
    }

    pub async fn set_message(&self, telegram_id: i32, huly_message: HulyMessage) -> Result<()> {
        let mut redis = self.redis.clone();

        let _: () = redis
            .hset(
                self.info.with_base_prefix("messages"),
                telegram_id,
                json::to_vec(&huly_message)?,
            )
            .await?;

        let _: () = redis
            .hset(
                self.info.with_reverse_prefix("messages"),
                huly_message.id,
                telegram_id,
            )
            .await?;

        Ok(())
    }

    // huly by telegram
    pub async fn get_h_message(&self, telegram_id: i32) -> Result<Option<HulyMessage>> {
        let mut redis = self.redis.clone();

        Ok(redis
            .hget::<_, _, Option<Vec<u8>>>(self.info.with_base_prefix("messages"), telegram_id)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok()))
    }

    pub async fn set_progress(&self, progress: Progress) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = self.info.with_base_prefix("progress");

        let _: () = redis.set(&key, json::to_vec(&progress)?).await?;

        Ok(())
    }

    pub async fn get_progress(&self) -> Result<Progress> {
        let mut redis = self.redis.clone();
        let key = self.info.with_base_prefix("progress");

        Ok(redis
            .get::<_, Option<Vec<u8>>>(&key)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok())
            .unwrap_or_default())
    }

    pub async fn get_blob(&self, id: i64) -> Result<Option<BlobDescriptor>> {
        let mut redis = self.redis.clone();
        let key = self.info.with_base_prefix("blobs");

        Ok(redis
            .hget::<_, _, Option<Vec<u8>>>(key, id)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok()))
    }

    pub async fn set_blob(&self, id: i64, blob: &BlobDescriptor) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = self.info.with_base_prefix("blobs");

        Ok(redis.hset(key, id, json::to_vec(blob)?).await?)
    }
}
