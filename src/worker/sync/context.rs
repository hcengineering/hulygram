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
use crate::context::GlobalContext;
use crate::integration::WorkspaceIntegration;
use crate::worker::context::WorkerContext;
use crate::worker::sync::blob::BlobClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncInfo {
    pub huly_workspace_id: WorkspaceUuid,

    pub telegram_user_id: i64,

    pub telegram_phone_number: String,
    pub telegram_chat_id: String,

    #[serde(default)]
    pub huly_space_id: String,
    pub huly_card_id: String,
    pub huly_card_title: String,
}

pub struct SyncContext {
    pub worker: Arc<WorkerContext>,

    pub transactor: TransactorClient<HttpBackend>,
    pub blobs: BlobClient,

    pub info: SyncInfo,
    pub state: SyncState,
    pub chat: Arc<Chat>,
}

fn sync_key(workspace: WorkspaceUuid, user: i64, chat: &String) -> String {
    format!("syn_{workspace}:{user}:{chat}")
}

fn sync_key_ref(workspace: WorkspaceUuid, card: &String) -> String {
    format!("syn_ref_{}:{}", workspace, card)
}

impl SyncContext {
    pub async fn load_sync_info(
        global: &Arc<GlobalContext>,
        workspace: WorkspaceUuid,
        user: i64,
        chat: &String,
    ) -> Result<Option<SyncInfo>> {
        let key = sync_key(workspace, user, chat);

        if let Some(bytes) = global.kvs().get(&key).await? {
            Ok(Some(json::from_slice::<SyncInfo>(&bytes)?))
        } else {
            Ok(None)
        }
    }

    pub async fn store_sync_info(global: &Arc<GlobalContext>, info: &SyncInfo) -> Result<()> {
        let kvs = global.kvs();

        let key = sync_key(
            info.huly_workspace_id,
            info.telegram_user_id,
            &info.telegram_chat_id,
        );
        let ref_key = sync_key_ref(info.huly_workspace_id, &info.huly_card_id);

        kvs.upsert(&key, &json::to_vec(&info)?).await?;
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

    pub async fn new(
        worker: Arc<WorkerContext>,
        chat: Arc<Chat>,
        integration: &WorkspaceIntegration,
        info: SyncInfo,
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

        let state = SyncState::new(info.clone(), worker.global.clone());
        let blobs = BlobClient::new(huly_workspace_id)?;

        Ok(Self {
            info,
            transactor,
            chat,
            worker,
            state,
            blobs,
        })
    }

    pub fn set_huly_card_id(self, huly_card_id: String) -> Self {
        let info = SyncInfo {
            huly_card_id,
            ..self.info
        };

        let state = SyncState::new(info.clone(), self.worker.global.clone());

        Self {
            info,
            state,
            ..self
        }
    }

    pub async fn cleanup(
        global: &Arc<GlobalContext>,
        workspace: WorkspaceUuid,
        user: i64,
        chat: &String,
    ) -> Result<bool> {
        let kvs = global.kvs();

        let key = sync_key(workspace, user, chat);

        // probably check progress (SyncState)
        if let Some(bytes) = kvs.get(&key).await? {
            if let Ok(info) = json::from_slice::<SyncInfo>(&bytes) {
                let ref_key = sync_key_ref(info.huly_workspace_id, &info.huly_card_id);
                kvs.delete(&ref_key).await?;
                kvs.delete(&key).await?;

                SyncState::delete(
                    global,
                    info.huly_workspace_id,
                    user,
                    chat,
                    Some(&info.huly_card_id),
                )
                .await?;
            }
            Ok(true)
        } else {
            SyncState::delete(global, workspace, user, chat, None).await?;

            Ok(false)
        }
    }

    pub fn transactor(&self) -> &TransactorClient<HttpBackend> {
        &self.transactor
    }
}
