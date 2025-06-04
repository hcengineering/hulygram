use grammers_client::types::Chat;
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
    fn r#type(&self) -> DialogType;
    fn card_title(&self) -> String;
    fn ensure_person_request(&self) -> EnsurePersonRequest;
}

impl ChatExt for Chat {
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

    fn ensure_person_request(&self) -> EnsurePersonRequest {
        match self {
            Chat::User(user) => {
                let id = user.id();

                let mut builder = EnsurePersonRequestBuilder::default();
                match (user.first_name(), user.last_name()) {
                    (Some(first), Some(last)) => builder.first_name(first).last_name(last),
                    (Some(first), None) => builder.first_name(first),
                    _ => builder.first_name("Telegram User"),
                }
                .social_type(SocialIdType::Telegram)
                .social_value(id.to_string())
                .build()
            }

            Chat::Channel(channel) => {
                let id = channel.id();

                EnsurePersonRequestBuilder::default()
                    .first_name(channel.title())
                    .last_name(String::default())
                    .social_type(SocialIdType::Telegram)
                    .social_value(id.to_string())
                    .build()
            }

            Chat::Group(group) => {
                let id = group.id();
                EnsurePersonRequestBuilder::default()
                    .first_name(group.title().unwrap_or("Telegram Group"))
                    .last_name(String::default())
                    .social_type(SocialIdType::Telegram)
                    .social_value(id.to_string())
                    .build()
            }
        }
        .unwrap()
    }
}
