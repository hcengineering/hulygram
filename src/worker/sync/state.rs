use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, FromRedisValue, ToRedisArgs, aio::ConnectionManager};
use serde::{Deserialize, Serialize};
use serde_json as json;
use uuid::Uuid;

use crate::{context::GlobalContext, worker::sync::context::SyncId};

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

pub struct SyncState {
    prefix: String,
    redis: ConnectionManager,
}

impl SyncState {
    pub async fn new(sync_id: SyncId, context: Arc<GlobalContext>) -> Result<Self> {
        let prefix = sync_id.to_string();

        Ok(Self {
            redis: context.redis(),
            prefix,
        })
    }

    async fn hget<K: ToRedisArgs + Send + Sync, V: FromRedisValue>(
        &self,
        suffix: &str,
        hkey: K,
    ) -> Result<V> {
        let mut redis = self.redis.clone();
        let key = format!("{}.{}", self.prefix, suffix);

        Ok(redis.hget::<_, K, V>(&key, hkey).await?)
    }

    async fn hset<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &self,
        suffix: &str,
        hkey: K,
        value: V,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = format!("{}.{}", self.prefix, suffix);

        let _: () = redis.hset(key, hkey, value).await?;

        Ok(())
    }

    pub async fn set_t_message(&self, telegram_id: i32, message: HulyMessage) -> Result<()> {
        self.hset("tmessages", telegram_id, json::to_vec(&message)?)
            .await
    }

    // huly by telegram
    pub async fn get_h_message(&self, telegram_id: i32) -> Result<Option<HulyMessage>> {
        Ok(self
            .hget::<_, Option<Vec<u8>>>("tmessages", telegram_id)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok()))
    }

    pub async fn set_progress(&self, progress: Progress) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = format!("{}.progress", self.prefix);

        let _: () = redis.set(&key, json::to_vec(&progress)?).await?;

        Ok(())
    }

    pub async fn get_progress(&self) -> Result<Progress> {
        let mut redis = self.redis.clone();
        let key = format!("{}.progress", self.prefix);

        Ok(redis
            .get::<_, Option<Vec<u8>>>(&key)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok())
            .unwrap_or_default())
    }

    pub async fn get_blob(&self, id: i64) -> Result<Option<BlobDescriptor>> {
        Ok(self
            .hget::<_, Option<Vec<u8>>>("blobs", id)
            .await?
            .and_then(|bytes| json::from_slice(&bytes).ok()))
    }

    pub async fn set_blob(&self, id: i64, blob: &BlobDescriptor) -> Result<()> {
        self.hset("blobs", id, json::to_vec(blob)?).await
    }
}

#[derive(Deserialize, Serialize)]
pub struct BlobDescriptor {
    pub blob_id: Uuid,
    pub length: usize,
    pub mimetype: String,
    pub file_name: Option<String>,
    pub size: Option<(u16, u16)>,
}
