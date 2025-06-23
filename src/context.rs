use anyhow::{Result, bail};
use hulyrs::services::{
    account::AccountClient, jwt::Claims, kvs::KvsClient,
    transactor::event::kafka::KafkaEventPublisher,
};
use redis::{ConnectionInfo, RedisConnectionInfo, aio::MultiplexedConnection};

use crate::config::CONFIG;
use crate::worker::limiters::Limiters;

use crate::config::hulyrs::SERVICES;

pub struct GlobalContext {
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaEventPublisher,
    limiters: Limiters,
    redis: MultiplexedConnection,
}

impl GlobalContext {
    pub async fn new(claims: Claims) -> Result<Self> {
        #[cfg(feature = "redis-sentinel")]
        let redis = {
            use redis::{sentinel::SentinelClient, sentinel::SentinelNodeConnectionInfo};

            let mut sentinel = SentinelClient::build(
                CONFIG.redis_urls.as_slice().into(),
                String::from("mymaster"),
                Some(SentinelNodeConnectionInfo {
                    tls_mode: Some(redis::TlsMode::Insecure),
                    redis_connection_info: None,
                }),
                redis::sentinel::SentinelServerType::Master,
            )
            .unwrap();

            sentinel.get_async_connection().await.unwrap()
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
                        db: 10,
                        username: None,
                        password: Some(CONFIG.redis_password.to_owned()),
                        protocol: redis::ProtocolVersion::RESP3,
                    },
                };

                let client = redis::Client::open(connection_info)?;

                client.get_multiplexed_async_connection().await?
            }

            _ => {
                bail!("Multiple redis url's are not supported in non sentinel mode");
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

    pub fn redis(&self) -> MultiplexedConnection {
        self.redis.clone()
    }
}
