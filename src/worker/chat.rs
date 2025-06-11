use grammers_client::{grammers_tl_types::types::Message, types::Chat};
use serde::{Deserialize, Serialize};

use hulyrs::services::{
    transactor::person::{EnsurePersonRequest, EnsurePersonRequestBuilder},
    types::SocialIdType,
};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum DialogType {
    User,
    Group,
    Channel,
}

pub trait ChatExt {
    fn is_deleted(&self) -> bool;
    fn global_id(&self) -> String;
    fn r#type(&self) -> DialogType;
    fn card_title(&self) -> String;
}

impl ChatExt for Chat {
    fn is_deleted(&self) -> bool {
        if let Chat::User(user) = self {
            return user.deleted();
        } else {
            false
        }
    }

    fn r#type(&self) -> DialogType {
        match self {
            Chat::User(_) => DialogType::User,
            Chat::Group(_) => DialogType::Group,
            Chat::Channel(_) => DialogType::Channel,
        }
    }

    fn card_title(&self) -> String {
        match self {
            Chat::User(user) => user.full_name().clone(),
            Chat::Group(group) => group.title().unwrap_or("Unknown Group").to_owned(),
            Chat::Channel(channel) => channel.title().to_owned(),
        }
    }

    fn global_id(&self) -> String {
        match self {
            Chat::User(user) => format!("u{}", user.id()),
            Chat::Group(group) => {
                format!("g{}", group.id())
            }
            Chat::Channel(channel) => format!("c{}", channel.id()),
        }
    }
}
