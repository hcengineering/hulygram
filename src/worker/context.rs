use std::sync::Arc;

use anyhow::Result;
use grammers_client::{Client as TelegramClient, types::User};
use hulyrs::services::types::{AccountUuid, SocialIdId};

use crate::context::GlobalContext;
use crate::worker::sync::state::CatalogManager;

pub struct WorkerContext {
    pub global: Arc<GlobalContext>,
    pub telegram: TelegramClient,
    pub catalog: CatalogManager,
    pub me: User,
    pub account_id: AccountUuid,
    pub social_id: SocialIdId,
}

use crate::integration::TelegramIntegration;

impl WorkerContext {
    pub async fn new(global: Arc<GlobalContext>, telegram: TelegramClient) -> Result<Self> {
        let me = telegram.get_me().await?;

        let (account_id, social_id) = global.account().find_ids(me.id()).await?;

        let catalog = super::sync::state::CatalogManager::load_or_default(
            global.clone(),
            me.id(),
            account_id,
            social_id.clone(),
        )
        .await?;

        Ok(Self {
            global,
            telegram,
            catalog,
            me,
            account_id,
            social_id,
        })
    }
}
