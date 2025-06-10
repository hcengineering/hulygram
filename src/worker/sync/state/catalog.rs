use std::sync::Arc;

use anyhow::Result;
use grammers_client::types::Chat as TelegramChat;
use serde::{Deserialize, Serialize};
use serde_json as json;

use hulyrs::services::types::{AccountUuid, SocialIdId, WorkspaceUuid};

use crate::context::GlobalContext;
use crate::worker::chat::{ChatExt, DialogType};

#[derive(Serialize, Deserialize)]
struct CatalogEntry {
    telegram_user_id: i64,
    huly_account_id: AccountUuid,
    huly_social_id: SocialIdId,

    chats: Vec<TelegramChatEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
struct HulyCardEntry {
    huly_workspace: WorkspaceUuid,
    huly_card: String,
    huly_space: String,
}

#[derive(Serialize, Deserialize)]
struct TelegramChatEntry {
    telegram_chat_id: i64,
    telegram_type: DialogType,
    telegram_title: String,

    cards: Vec<HulyCardEntry>,
}

pub struct CatalogManager {
    catalog_key: String,
    services: Arc<GlobalContext>,
    catalog: CatalogEntry,
}

impl CatalogManager {
    pub async fn load_or_default(
        services: Arc<GlobalContext>,
        telegram_user_id: i64,
        huly_account_id: AccountUuid,
        huly_social_id: SocialIdId,
    ) -> Result<Self> {
        let key = format!("catalog_{}", telegram_user_id);

        let catalog = if let Some(catalog) = services.kvs().get(&key).await? {
            json::from_slice::<CatalogEntry>(catalog.as_slice())?
        } else {
            CatalogEntry {
                telegram_user_id,
                huly_account_id,
                huly_social_id,
                chats: Vec::new(),
            }
        };

        Ok(Self {
            catalog_key: key,
            services,
            catalog,
        })
    }

    pub async fn add_card(&mut self, chat: TelegramChat, card: HulyCardEntry) -> Result<()> {
        if let Some(tchat) = self
            .catalog
            .chats
            .iter_mut()
            .find(|tchat| tchat.telegram_chat_id == chat.id())
        {
            tchat.cards.push(card.clone());

            self.services
                .kvs()
                .upsert(
                    &format!("card_{}_{}", card.huly_workspace, card.huly_card),
                    &json::to_vec(&self.catalog.telegram_user_id)?,
                )
                .await?;
        } else {
            let tchat = TelegramChatEntry {
                telegram_chat_id: chat.id(),
                telegram_type: chat.r#type(),
                telegram_title: chat.card_title(),
                cards: vec![card],
            };

            self.catalog.chats.push(tchat);
        }

        self.persist().await
    }

    pub async fn persist(&self) -> Result<()> {
        let bytes = json::to_vec(&self.catalog)?;
        self.services
            .kvs()
            .upsert(&self.catalog_key, &bytes)
            .await?;
        Ok(())
    }
}

// h_chan->telegram_user_id
