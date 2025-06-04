use std::sync::Arc;

use anyhow::Result;
use hulyrs::services::{
    account::AccountClient,
    jwt::{Claims, ClaimsBuilder},
    kvs::KvsClient,
    transactor::{TransactorClient, event::KafkaEventPublisher},
    types::WorkspaceUuid,
};
use url::Url;

use super::limiters::Limiters;
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
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaEventPublisher,
    limiters: Limiters,
}

#[derive(Clone)]
pub struct GlobalServices {
    inner: Arc<GlobalServicesInner>,
}

impl GlobalServices {
    pub fn new(claims: Claims) -> Result<Self> {
        let inner = GlobalServicesInner {
            kvs: KvsClient::new(CONFIG.kvs_namespace.to_owned(), claims.clone())?,
            account: AccountClient::new(&claims)?,
            hulygun: KafkaEventPublisher::new(&CONFIG.event_topic)?,
            limiters: Limiters::new(),
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

    pub fn limiters(&self) -> &Limiters {
        &self.inner.limiters
    }
}
