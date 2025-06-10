use std::sync::Arc;

use anyhow::Result;
use hulyrs::services::{
    account::AccountClient, jwt::Claims, kvs::KvsClient,
    transactor::event::kafka::KafkaEventPublisher,
};

use crate::config::CONFIG;
use crate::worker::limiters::Limiters;

use crate::config::hulyrs::SERVICES;

struct GlobalServicesInner {
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaEventPublisher,
    limiters: Limiters,
}

pub struct GlobalContext {
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaEventPublisher,
    limiters: Limiters,
}

impl GlobalContext {
    pub fn new(claims: Claims) -> Result<Self> {
        Ok(Self {
            kvs: SERVICES.new_kvs_client(&CONFIG.kvs_namespace, &claims)?,
            account: SERVICES.new_account_client(&claims)?,
            hulygun: SERVICES.new_kafka_event_publisher(&CONFIG.event_topic)?,
            limiters: Limiters::new(),
        })
    }

    pub fn kvs(&self) -> &KvsClient {
        &self.kvs
    }

    pub fn account(&self) -> &AccountClient {
        &self.account
    }

    pub fn hulygun(&self) -> &KafkaEventPublisher {
        &self.hulygun
    }

    pub fn limiters(&self) -> &Limiters {
        &self.limiters
    }
}
