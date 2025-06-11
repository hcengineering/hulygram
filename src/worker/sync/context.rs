use std::fmt::Display;
use std::sync::Arc;

use grammers_client::types::Chat;
use hulyrs::services::{jwt::ClaimsBuilder, transactor::TransactorClient, types::WorkspaceUuid};

use super::state::SyncState;
use crate::config::CONFIG;
use crate::integration::WorkspaceIntegration;
use crate::worker::chat::ChatExt;
use crate::worker::context::WorkerContext;
use crate::worker::sync::blob::BlobClient;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SyncId {
    workspace_id: WorkspaceUuid,
    telegram_user_id: i64,
    chat_id: String,
}

impl Display for SyncId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.workspace_id, self.telegram_user_id, self.chat_id
        )
    }
}

#[derive(Clone)]
pub struct SyncContext {
    pub chat: Arc<Chat>,
    pub transactor: Arc<TransactorClient>,
    pub worker: Arc<WorkerContext>,
    pub workspace_id: WorkspaceUuid,
    pub state: Arc<SyncState>,
    pub sync_id: SyncId,
    pub blobs: Arc<BlobClient>,
}

use crate::config::hulyrs::SERVICES;

impl SyncContext {
    pub async fn new(
        worker: Arc<WorkerContext>,
        chat: Arc<Chat>,
        integration: &WorkspaceIntegration,
    ) -> anyhow::Result<Self> {
        let workspace_id = integration.workspace_id;
        let transactor = SERVICES.new_transactor_client(
            integration.endpoint.clone(),
            &ClaimsBuilder::default()
                .system_account()
                .workspace(workspace_id)
                .extra("service", &CONFIG.service_id)
                .build()?,
        )?;

        let sync_id = SyncId {
            workspace_id,
            telegram_user_id: worker.me.id(),
            chat_id: chat.global_id(),
        };

        let index = Arc::new(SyncState::new(sync_id.clone(), worker.global.clone()).await?);

        let blobs = Arc::new(BlobClient::new(workspace_id)?);

        Ok(Self {
            sync_id,
            transactor: Arc::new(transactor),
            chat,
            worker,
            workspace_id,
            state: index,
            blobs,
        })
    }

    pub fn transactor(&self) -> &TransactorClient {
        &self.transactor
    }
}
