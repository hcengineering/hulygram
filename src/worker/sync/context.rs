use std::sync::Arc;

use anyhow::Result;
use grammers_client::types::Chat;
use hulyrs::services::{jwt::ClaimsBuilder, transactor::TransactorClient, types::WorkspaceUuid};
use redis::AsyncCommands as _;
use serde::{Deserialize, Serialize};
use serde_json as json;

use super::state::{KeyPrefixes, SyncState};
use super::telegram::ChatExt;
use crate::config::CONFIG;
use crate::config::hulyrs::SERVICES;
use crate::integration::WorkspaceIntegration;
use crate::worker::context::WorkerContext;
use crate::worker::sync::blob::BlobClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncInfo {
    pub huly_workspace_id: WorkspaceUuid,

    pub telegram_user_id: i64,
    pub telegram_chat_id: String,

    pub huly_card_id: String,
    pub huly_card_title: String,

    pub is_private: bool,
}

fn sync_key(workspace: WorkspaceUuid, user: i64, chat: &String) -> String {
    format!("{workspace}:t{user}:{chat}.sync")
}

pub struct SyncContext {
    pub worker: Arc<WorkerContext>,

    pub transactor: TransactorClient,
    pub blobs: BlobClient,

    pub info: SyncInfo,
    pub state: SyncState,
    pub chat: Arc<Chat>,
    pub is_fresh: bool,
}

impl SyncContext {
    pub async fn new(
        worker: Arc<WorkerContext>,
        chat: Arc<Chat>,
        integration: &WorkspaceIntegration,
    ) -> anyhow::Result<Self> {
        let huly_workspace_id = integration.workspace_id;
        let transactor = SERVICES.new_transactor_client(
            integration.endpoint.clone(),
            &ClaimsBuilder::default()
                .system_account()
                .workspace(huly_workspace_id)
                .extra("service", &CONFIG.service_id)
                .build()?,
        )?;

        let mut redis = worker.global.redis();

        let key = sync_key(huly_workspace_id, worker.me.id(), &chat.global_id());

        let (info, is_fresh) = match redis.get::<_, Option<Vec<u8>>>(&key).await? {
            Some(card) => {
                //
                (json::from_slice::<SyncInfo>(&card)?, false)
            }
            None => {
                let info = SyncInfo {
                    telegram_user_id: worker.me.id(),
                    telegram_chat_id: chat.global_id(),

                    huly_workspace_id,
                    huly_card_id: ksuid::Ksuid::generate().to_base62(),
                    huly_card_title: chat.card_title(),

                    is_private: !CONFIG.allowed_dialog_ids.contains(&chat.id().to_string()),
                };

                (info, true)
            }
        };

        let state = SyncState::new(info.clone(), worker.global.clone()).await?;

        let blobs = BlobClient::new(huly_workspace_id)?;

        Ok(Self {
            info,
            transactor,
            chat,
            worker,
            state,
            blobs,
            is_fresh,
        })
    }

    pub async fn persist_info(&self) -> Result<()> {
        let key = sync_key(
            self.info.huly_workspace_id,
            self.info.telegram_user_id,
            &self.info.telegram_chat_id,
        );

        let mut redis = self.worker.global.redis();
        let rkey = self.info.with_reverse_prefix("ref");

        let _: () = redis
            .mset(&[(&key, &json::to_string(&self.info)?), (&rkey, &key)])
            .await?;

        Ok(())
    }

    pub fn transactor(&self) -> &TransactorClient {
        &self.transactor
    }
}
