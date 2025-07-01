use anyhow::{Result, bail};
use hulyrs::services::{
    account::AccountClient, jwt::Claims, kvs::KvsClient, transactor::kafka::KafkaProducer,
};

use redis::{ConnectionInfo, RedisConnectionInfo, aio::MultiplexedConnection};
use tracing::*;

use crate::config::{CONFIG, RedisMode};
use crate::worker::limiters::Limiters;

use crate::config::hulyrs::SERVICES;

pub struct GlobalContext {
    kvs: KvsClient,
    account: AccountClient,
    hulygun: KafkaProducer,
    limiters: Limiters,
    redis: MultiplexedConnection,
}

impl GlobalContext {
    pub async fn new(claims: Claims) -> Result<Self> {
        let default_port = match CONFIG.redis_mode {
            RedisMode::Sentinel => 6379,
            RedisMode::Direct => 6380,
        };

        let urls = CONFIG
            .redis_urls
            .iter()
            .map(|url| {
                redis::ConnectionAddr::Tcp(
                    url.host().unwrap().to_string(),
                    url.port().unwrap_or(default_port),
                )
            })
            .collect::<Vec<_>>();

        let redis = if CONFIG.redis_mode == RedisMode::Sentinel {
            use redis::sentinel::SentinelClientBuilder;

            let urls1 = CONFIG
                .redis_urls
                .iter()
                .map(|url| url.as_str())
                .collect::<Vec<_>>()
                .join(",");

            info!(urls = urls1, "Connecting to redis (sentinel mode)");

            let mut sentinel = SentinelClientBuilder::new(
                urls,
                CONFIG.redis_service.to_owned(),
                redis::sentinel::SentinelServerType::Master,
            )
            .unwrap()
            .set_client_to_redis_protocol(redis::ProtocolVersion::RESP3)
            .set_client_to_redis_db(10)
            .set_client_to_redis_password(CONFIG.redis_password.to_owned())
            .set_client_to_sentinel_password(CONFIG.redis_password.to_owned())
            .build()?;

            sentinel.get_async_connection().await?
        } else {
            match urls.as_slice() {
                [single] => {
                    let redis_connection_info = RedisConnectionInfo {
                        db: 10,
                        username: None,
                        password: Some(CONFIG.redis_password.to_owned()),
                        protocol: redis::ProtocolVersion::RESP3,
                    };

                    let connection_info = ConnectionInfo {
                        addr: single.to_owned(),
                        redis: redis_connection_info,
                    };

                    info!(address = %connection_info.addr, "Connecting to redis (direct)");

                    let client = redis::Client::open(connection_info)?;

                    client.get_multiplexed_async_connection().await?
                }

                _ => {
                    bail!("Multiple redis url's are not supported in non sentinel mode");
                }
            }
        };

        Ok(Self {
            kvs: SERVICES.new_kvs_client(&CONFIG.kvs_namespace, &claims)?,
            account: SERVICES.new_account_client(&claims)?,
            hulygun: SERVICES.new_kafka_publisher(&CONFIG.outbound_tx_topic)?,
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

    pub fn hulygun(&self) -> &KafkaProducer {
        &self.hulygun
    }

    pub fn limiters(&self) -> &Limiters {
        &self.limiters
    }

    pub fn redis(&self) -> MultiplexedConnection {
        self.redis.clone()
    }
}
