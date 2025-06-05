use std::sync::Arc;

use governor::{
    Quota, RateLimiter,
    clock::{Clock, MonotonicClock},
    middleware::NoOpMiddleware,
    state::keyed::{DefaultHasher, DefaultKeyedStateStore},
};

use tokio::sync::Semaphore;

use crate::config::CONFIG;

type LimiterMiddleware = NoOpMiddleware<<MonotonicClock as Clock>::Instant>;

pub type Limiter<K, MW = LimiterMiddleware> =
    RateLimiter<K, DefaultKeyedStateStore<K, DefaultHasher>, MonotonicClock, MW>;

pub type TelegramLimiter = Limiter<i64>;

pub struct Limiters {
    pub get_file: TelegramLimiter,
    pub get_history: TelegramLimiter,
    pub sync_semaphore: Arc<Semaphore>,
}

impl Limiters {
    pub fn new() -> Self {
        let burst = 1.try_into().unwrap();

        let get_file = RateLimiter::dashmap_with_clock(
            Quota::per_second(CONFIG.get_file_rate_limit).allow_burst(burst),
            MonotonicClock,
        );

        let get_history = RateLimiter::dashmap_with_clock(
            Quota::per_second(CONFIG.get_history_rate_limit).allow_burst(burst),
            MonotonicClock,
        );

        let sync_semaphore = Arc::new(Semaphore::new(CONFIG.sync_process_limit));

        Self {
            get_file,
            get_history,
            sync_semaphore,
        }
    }
}
