use std::sync::Arc;

use anyhow::Result;
use governor::{
    Quota, RateLimiter,
    clock::{Clock, MonotonicClock},
    middleware::NoOpMiddleware,
    state::keyed::{DefaultHasher, DefaultKeyedStateStore},
};
use hulyrs::services::{
    account::AccountClient,
    jwt::{Claims, ClaimsBuilder},
    kvs::KvsClient,
    transactor::{TransactorClient, event::KafkaEventPublisher},
    types::WorkspaceUuid,
};
use url::Url;

use crate::config::CONFIG;

#[derive(Clone)]
pub struct WorkspaceServices {
    transactor: Arc<TransactorClient>,
}

impl WorkspaceServices {
    pub fn new(url: &Url, workspace: WorkspaceUuid) -> anyhow::Result<Self> {
        let transactor = TransactorClient::new(
            url.clone(),
            &ClaimsBuilder::default()
                .system_account()
                .workspace(workspace)
                .build()?,
        )?;

        Ok(Self {
            transactor: Arc::new(transactor),
        })
    }

    pub fn transactor(&self) -> &TransactorClient {
        &self.transactor
    }
}

struct GlobalServicesInner {
    pub kvs: KvsClient,
    pub account: AccountClient,
    pub hulygun: KafkaEventPublisher,
    pub limiter: Limiter,
}

#[derive(Clone)]
pub struct GlobalServices {
    inner: Arc<GlobalServicesInner>,
}

pub type Limiter<MW = NoOpMiddleware<<MonotonicClock as Clock>::Instant>> =
    RateLimiter<String, DefaultKeyedStateStore<String, DefaultHasher>, MonotonicClock, MW>;

pub trait LimiterExt {
    async fn wait(&self, key: &String);
}

impl LimiterExt for Limiter {
    async fn wait(&self, key: &String) {
        if let Err(delay) = self.check_key(key) {
            tokio::time::sleep_until(delay.earliest_possible().into()).await;
        }
    }
}

impl GlobalServices {
    pub fn new(claims: Claims) -> Result<Self> {
        let limiter = RateLimiter::dashmap_with_clock(
            Quota::per_second(CONFIG.get_file_rate_limit).allow_burst(1.try_into().unwrap()),
            MonotonicClock,
        );

        let inner = GlobalServicesInner {
            kvs: KvsClient::new(CONFIG.kvs_namespace.to_owned(), claims.clone())?,
            account: AccountClient::new(&claims)?,
            hulygun: KafkaEventPublisher::new(&CONFIG.event_topic)?,
            limiter,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn kvs(&self) -> &KvsClient {
        &self.inner.kvs
    }

    pub fn account(&self) -> &AccountClient {
        &self.inner.account
    }

    pub fn hulygun(&self) -> &KafkaEventPublisher {
        &self.inner.hulygun
    }

    pub fn limiter(&self) -> &Limiter {
        &self.inner.limiter
    }
}
