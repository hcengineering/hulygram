use governor::{
    Quota, RateLimiter,
    clock::{Clock, MonotonicClock},
    middleware::NoOpMiddleware,
    state::keyed::{DefaultHasher, DefaultKeyedStateStore},
};

use crate::config::CONFIG;

type LimiterMiddleware = NoOpMiddleware<<MonotonicClock as Clock>::Instant>;

pub type Limiter<K, MW = LimiterMiddleware> =
    RateLimiter<K, DefaultKeyedStateStore<K, DefaultHasher>, MonotonicClock, MW>;

pub type TelegramLimiter = Limiter<i64>;

pub struct Limiters {
    pub get_file: TelegramLimiter,
    pub get_history: TelegramLimiter,
    pub get_dialog: TelegramLimiter,
}

impl Limiters {
    pub fn new() -> Self {
        let burst = 1.try_into().unwrap();

        let get_file = RateLimiter::dashmap_with_clock(
            Quota::per_second(CONFIG.get_file_rate_limit).allow_burst(burst),
            MonotonicClock,
        );

        let get_history = RateLimiter::dashmap_with_clock(
            Quota::per_second(100.try_into().unwrap()).allow_burst(burst),
            MonotonicClock,
        );

        let get_dialog = RateLimiter::dashmap_with_clock(
            Quota::per_second(5.try_into().unwrap()).allow_burst(burst),
            MonotonicClock,
        );

        Self {
            get_file,
            get_history,
            get_dialog,
        }
    }
}
