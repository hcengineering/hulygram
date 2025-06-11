use std::sync::Arc;

use anyhow::Result;
use grammers_client::{Client as TelegramClient, types::User};
use hulyrs::services::types::{AccountUuid, SocialIdId};

use crate::context::GlobalContext;

pub struct WorkerContext {
    pub global: Arc<GlobalContext>,
    pub telegram: TelegramClient,
    pub me: User,
    pub account_id: AccountUuid,
    pub social_id: SocialIdId,
}

use crate::integration::TelegramIntegration;

impl WorkerContext {
    pub async fn new(global: Arc<GlobalContext>, telegram: TelegramClient) -> Result<Self> {
        let me = telegram.get_me().await?;

        let (account_id, social_id) = global.account().find_ids(me.id()).await?;

        Ok(Self {
            global,
            telegram,
            me,
            account_id,
            social_id,
        })
    }
}
