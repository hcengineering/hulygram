use std::sync::Arc;

use grammers_client::types::Chat;
use hulyrs::services::{jwt::ClaimsBuilder, transactor::TransactorClient, types::WorkspaceUuid};
use url::Url;

use crate::config::CONFIG;
use crate::worker::context::WorkerContext;

#[derive(Clone)]
pub struct SyncContext {
    pub chat: Arc<Chat>,
    pub transactor: Arc<TransactorClient>,
    pub worker: Arc<WorkerContext>,
    pub workspace_id: WorkspaceUuid,
}

use crate::config::hulyrs::SERVICES;

impl SyncContext {
    pub fn new(
        worker: Arc<WorkerContext>,
        chat: Arc<Chat>,
        url: &Url,
        workspace_id: WorkspaceUuid,
    ) -> anyhow::Result<Self> {
        let transactor = SERVICES.new_transactor_client(
            url.clone(),
            &ClaimsBuilder::default()
                .system_account()
                .workspace(workspace_id)
                .extra("service", &CONFIG.service_id)
                .build()?,
        )?;

        Ok(Self {
            transactor: Arc::new(transactor),
            chat,
            worker,
            workspace_id,
        })
    }

    pub fn transactor(&self) -> &TransactorClient {
        &self.transactor
    }
}
