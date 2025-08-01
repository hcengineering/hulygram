use std::sync::Arc;

use anyhow::Result;
use grammers_client::types::Chat;
use hulyrs::services::kvs::KvsClient;
use hulyrs::services::transactor::backend::http::HttpBackend;
use hulyrs::services::{core::WorkspaceUuid, jwt::ClaimsBuilder, transactor::TransactorClient};
use serde::{Deserialize, Serialize};
use serde_json as json;

use super::state::SyncState;
use crate::config::CONFIG;
use crate::config::hulyrs::SERVICES;
use crate::integration::{Access, ChannelConfig, WorkspaceIntegration};
use crate::telegram::ChatExt;
use crate::worker::context::WorkerContext;
use crate::worker::sync::blob::BlobClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncInfo {
    pub huly_workspace_id: WorkspaceUuid,

    pub telegram_user_id: i64,

    pub telegram_phone_number: String,
    pub telegram_chat_id: String,

    pub huly_card_id: String,
    pub huly_card_title: String,

    pub is_private: bool,
}

pub struct SyncContext {
    pub worker: Arc<WorkerContext>,

    pub transactor: TransactorClient<HttpBackend>,
    pub blobs: BlobClient,

    pub info: SyncInfo,
    pub state: SyncState,
    pub chat: Arc<Chat>,
    pub is_fresh: bool,
}

fn sync_key(workspace: WorkspaceUuid, user: i64, chat: &String) -> String {
    format!("syn_{workspace}:{user}:{chat}")
}

fn sync_key_ref(workspace: WorkspaceUuid, card: &String) -> String {
    format!("syn_ref_{}:{}", workspace, card)
}

impl SyncContext {
    pub async fn new(
        worker: Arc<WorkerContext>,
        chat: Arc<Chat>,
        integration: &WorkspaceIntegration,
        config: &ChannelConfig,
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
                    telegram_phone_number: worker.me.phone().unwrap().to_string(),

                    huly_workspace_id,
                    huly_card_id: ksuid::Ksuid::generate().to_base62(),
                    huly_card_title: chat.card_title(),

                    is_private: matches!(config.access, Access::Private),
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
        let ref_key = sync_key_ref(self.info.huly_workspace_id, &self.info.huly_card_id);

        kvs.upsert(&key, &json::to_vec(&self.info)?).await?;
        kvs.upsert(&ref_key, &key.as_bytes()).await?;

        Ok(())
    }

    pub async fn ref_lookup(
        kvs: &KvsClient,
        workspace: WorkspaceUuid,
        card_id: &String,
    ) -> Result<Option<SyncInfo>> {
        let ref_key = sync_key_ref(workspace, &card_id);

        let ptr = kvs.get(&ref_key).await?;

        if let Some(ptr) = ptr {
            let ptr = std::str::from_utf8(&ptr)?;

            let sync_info = kvs
                .get(ptr)
                .await?
                .map(|i| json::from_slice::<SyncInfo>(&i))
                .transpose()?;

            Ok(sync_info)
        } else {
            Ok(None)
        }
    }

    pub fn transactor(&self) -> &TransactorClient<HttpBackend> {
        &self.transactor
    }
}
