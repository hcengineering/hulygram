use std::{collections::HashSet, num::NonZero, path::Path, sync::LazyLock};

use serde_with::{StringWithSeparator, formats::CommaSeparator, serde_as};

use config::FileFormat;
use serde::Deserialize;
use url::Url;

pub mod hulyrs {
    use std::sync::LazyLock;

    pub static CONFIG: LazyLock<hulyrs::Config> = LazyLock::new(|| match hulyrs::Config::auto() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration error: {}", error);
            std::process::exit(1);
        }
    });

    pub static SERVICES: LazyLock<hulyrs::ServiceFactory> =
        LazyLock::new(|| hulyrs::ServiceFactory::new(CONFIG.clone()));
}

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct Config {
    pub bind_port: u16,
    pub bind_host: String,

    pub telegram_api_id: i32,
    pub telegram_api_hash: String,

    pub kvs_namespace: String,
    pub service_id: String,
    pub event_topic: String,

    #[serde_as(as = "StringWithSeparator::<CommaSeparator, url::Url>")]
    pub redis_urls: Vec<Url>,
    pub redis_password: String,

    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub allowed_dialog_ids: HashSet<String>,

    pub get_file_rate_limit: NonZero<u32>,
    pub get_history_rate_limit: NonZero<u32>,

    pub sync_process_limit: usize,

    pub sync_process_limit_local: usize,

    pub blob_service_path: Url,

    pub base_url: Url,

    pub dry_run: bool,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    const DEFAULTS: &str = std::include_str!("config/default.toml");

    let mut builder =
        config::Config::builder().add_source(config::File::from_str(DEFAULTS, FileFormat::Toml));

    let path = Path::new("etc/config.toml");

    if path.exists() {
        builder = builder.add_source(config::File::with_name(path.as_os_str().to_str().unwrap()));
    }

    let settings = builder
        .add_source(config::Environment::with_prefix("HULY"))
        .build()
        .and_then(|c| c.try_deserialize::<Config>());

    match settings {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("configuration error: {}", error);
            std::process::exit(1);
        }
    }
});
