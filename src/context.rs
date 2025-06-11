use anyhow::{Result, bail};
use hulyrs::services::{
    account::AccountClient, jwt::Claims, kvs::KvsClient,
    transactor::event::kafka::KafkaEventPublisher,
};
use redis::{
    ConnectionInfo, RedisConnectionInfo,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::config::CONFIG;
use crate::worker::limiters::Limiters;

use crate::config::hulyrs::SERVICES;

pub struct GlobalContext {
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaEventPublisher,
    limiters: Limiters,
    redis: ConnectionManager,
}

impl GlobalContext {
    pub async fn new(claims: Claims) -> Result<Self> {
        #[cfg(feature = "redis-sentinel")]
        let redis = {
            unimplemented!("sentinel mode is not implemented yet");
        };

        #[cfg(not(feature = "redis-sentinel"))]
        let redis = match CONFIG.redis_urls.as_slice() {
            [single] => {
                let connection_info = ConnectionInfo {
                    addr: redis::ConnectionAddr::Tcp(
                        single.host().unwrap().to_string(),
                        single.port().unwrap_or(6379),
                    ),
                    redis: RedisConnectionInfo {
                        db: 0,
                        username: None,
                        password: Some(CONFIG.redis_password.to_owned()),
                        protocol: redis::ProtocolVersion::RESP3,
                    },
                };

                let client = redis::Client::open(connection_info)?;

                let config = ConnectionManagerConfig::default();
                let config = config.set_number_of_retries(1);

                ConnectionManager::new_with_config(client, config).await?
            }

            _ => {
                bail!("Multiple redis urls are not supported in non sentinel mode");
            }
        };

        Ok(Self {
            kvs: SERVICES.new_kvs_client(&CONFIG.kvs_namespace, &claims)?,
            account: SERVICES.new_account_client(&claims)?,
            hulygun: SERVICES.new_kafka_event_publisher(&CONFIG.event_topic)?,
            limiters: Limiters::new(),
            redis,
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

    pub fn redis(&self) -> ConnectionManager {
        self.redis.clone()
    }
}
