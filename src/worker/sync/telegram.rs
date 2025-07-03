use chrono::{DateTime, Utc};
use grammers_client::types::{Chat, Message, User};
use grammers_tl_types as tl;
use hulyrs::services::{
    transactor::person::{EnsurePersonRequest, EnsurePersonRequestBuilder},
    types::SocialIdType,
};

pub trait MessageExt {
    fn last_date(&self) -> DateTime<Utc>;
    fn huly_markdown_text(&self) -> String;
}

impl MessageExt for Message {
    fn last_date(&self) -> DateTime<Utc> {
        self.edit_date().unwrap_or_else(|| self.date())
    }

    fn huly_markdown_text(&self) -> String {
        fn entities(message: &Message) -> Option<&Vec<tl::enums::MessageEntity>> {
            match &message.raw {
                tl::enums::Message::Empty(_) => None,
                tl::enums::Message::Message(message) => message.entities.as_ref(),
                tl::enums::Message::Service(_) => None,
            }
        }

        if let Some(entities) = entities(self) {
            super::markdown::generate_markdown_message(self.text(), entities)
        } else {
            self.text().to_owned()
        }
    }
}

pub trait ChatExt {
    fn is_deleted(&self) -> bool;
    fn global_id(&self) -> String;
    fn channel_global_id(channel_id: i64) -> String;

    fn card_title(&self) -> String;
}

pub trait EnsurePersonRequestExt {
    fn ensure_person_request(&self) -> EnsurePersonRequest;
}

impl ChatExt for Chat {
    fn is_deleted(&self) -> bool {
        if let Chat::User(user) = self {
            return user.deleted();
        } else {
            false
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
            Chat::Channel(channel) => Self::channel_global_id(channel.id()),
        }
    }

    fn channel_global_id(channel_id: i64) -> String {
        format!("c{}", channel_id)
    }
}

impl EnsurePersonRequestExt for Chat {
    fn ensure_person_request(&self) -> EnsurePersonRequest {
        let mut builder = EnsurePersonRequestBuilder::default();

        builder
            .social_type(SocialIdType::Telegram)
            .social_value(self.id().to_string());

        match self {
            Chat::User(user) => {
                return user.ensure_person_request();
            }

            Chat::Channel(channel) => {
                builder.first_name(channel.title().to_owned());
            }

            Chat::Group(group) => {
                builder.first_name(group.title().unwrap_or("Telegram Group").to_owned());
            }
        }

        builder.build().unwrap()
    }
}

impl EnsurePersonRequestExt for User {
    fn ensure_person_request(&self) -> EnsurePersonRequest {
        let first_name = self
            .first_name()
            .map(ToString::to_string)
            .unwrap_or("Deleted User".to_string());
        let last_name = self.last_name().map(ToOwned::to_owned);

        EnsurePersonRequestBuilder::default()
            .first_name(first_name)
            .last_name(last_name)
            .social_type(SocialIdType::Telegram)
            .social_value(self.id().to_string())
            .build()
            .unwrap()
    }
}
