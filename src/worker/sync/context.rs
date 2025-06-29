use std::sync::Arc;

use anyhow::Result;
use grammers_client::types::Chat;
use hulyrs::services::{jwt::ClaimsBuilder, transactor::TransactorClient, types::WorkspaceUuid};
use serde::{Deserialize, Serialize};
use serde_json as json;

use super::state::SyncState;
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub telegram_phone_number: Option<String>,
    pub telegram_chat_id: String,

    pub huly_card_id: String,
    pub huly_card_title: String,

    pub is_private: bool,
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

fn sync_key(workspace: WorkspaceUuid, user: i64, chat: &String) -> String {
    format!("syn_{workspace}:{user}:{chat}")
}

fn sync_key_ref(info: &SyncInfo) -> String {
    format!("syn_ref_{}:{}", info.huly_workspace_id, info.huly_card_id)
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

        let kvs = worker.global.kvs();
        let key = sync_key(huly_workspace_id, worker.me.id(), &chat.global_id());

        let (info, is_fresh) = match kvs.get(&key).await? {
            Some(card) => {
                //
                (json::from_slice::<SyncInfo>(&card)?, false)
            }
            None => {
                let info = SyncInfo {
                    telegram_user_id: worker.me.id(),
                    telegram_chat_id: chat.global_id(),
                    telegram_phone_number: worker.me.phone().map(ToOwned::to_owned),

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
        let kvs = self.worker.global.kvs();

        let key = sync_key(
            self.info.huly_workspace_id,
            self.info.telegram_user_id,
            &self.info.telegram_chat_id,
        );
        let r#ref = sync_key_ref(&self.info);

        kvs.upsert(&key, &json::to_vec(&self.info)?).await?;
        kvs.upsert(&r#ref, &key.as_bytes()).await?;

        Ok(())
    }

    pub fn transactor(&self) -> &TransactorClient {
        &self.transactor
    }
}
