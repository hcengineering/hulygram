use std::sync::Arc;

#[allow(unused_imports)]
use console_subscriber::ConsoleLayer;
use hulyrs::services::jwt::ClaimsBuilder;
use tokio::{
    select,
    signal::unix::{SignalKind, signal},
};

mod config;
mod context;
mod etc;
mod http;
mod integration;
mod worker;
use tikv_jemallocator::Jemalloc;

use config::CONFIG;

pub fn initialize_tracing() {
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    /*
    let tokio_console = {
        ConsoleLayer::builder()
            .retention(Duration::from_secs(30))
            .spawn()
    };*/

    let stdout_logger = {
        let level = config::hulyrs::CONFIG.log;

        let filter = Targets::default()
            .with_default(tracing::Level::WARN)
            .with_target(env!("CARGO_PKG_NAME"), level)
            .with_target("grammers", tracing::Level::INFO)
            //.with_target("tokio", tracing::Level::TRACE)
            //.with_target("runtime", tracing::Level::TRACE)
            .with_target("hulyrs::service", config::hulyrs::CONFIG.log);
        //.with_target("actix", tracing::Level::DEBUG)
        //.with_target("reqwest", tracing::Level::TRACE)

        tracing_subscriber::fmt::layer()
            .compact()
            .with_filter(filter)
    };

    tracing_subscriber::registry()
        //.with(tokio_console)
        .with(stdout_logger)
        .init();
}

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    //b"prof:true,prof_active:true,lg_prof_sample:19,dirty_decay_ms:0,muzzy_decay_ms:0\0";
    b"dirty_decay_ms:0,muzzy_decay_ms:0\0";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    initialize_tracing();

    tracing::info!(
        "{}/{} started",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let system_account = ClaimsBuilder::default()
        .system_account()
        .service(&CONFIG.service_id)
        .build()?;

    let context = Arc::new(context::GlobalContext::new(system_account).await?);

    let supervisor = worker::new_supervisor(context.clone())?;
    supervisor.spawn_all().await?;

    let (http, abort_http) = http::spawn(supervisor.clone(), context)?;

    let mut term = signal(SignalKind::terminate())?;

    let mut int = signal(SignalKind::interrupt())?;
    let mut quit = signal(SignalKind::quit())?;

    select! {
        _ = term.recv() => {
            abort_http.stop(true).await;
            tracing::info!("Received SIGTERM");
        }

        _ = int.recv() => {
            abort_http.stop(false).await;
            tracing::info!("Received SIGINT");
        }

        _ = quit.recv() => {
            abort_http.stop(false).await;
            tracing::info!("Received SIGQUIT");
        }

        _ = http  => {
            tracing::error!("http server terminated unexpectedly");
        }
    };

    tracing::debug!("Shutting down workers");
    supervisor.shutdown_all().await;

    std::process::exit(0);
}
