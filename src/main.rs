use std::sync::Arc;

#[allow(unused_imports)]
use console_subscriber::ConsoleLayer;
use hulyrs::services::jwt::ClaimsBuilder;
use hulyrs::services::otel;
use tokio::{
    select,
    signal::unix::{SignalKind, signal},
};
use tracing::*;

mod config;
mod context;
mod etc;
mod http;
mod integration;
mod reverse;
mod telegram;
mod worker;

use tikv_jemallocator::Jemalloc;

use config::CONFIG;

pub fn initialize_tracing() {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use tracing::Level;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    let otel_config = otel::OtelConfig {
        mode: config::hulyrs::CONFIG.otel_mode.clone(),
        service_name: env!("CARGO_PKG_NAME").to_string(),
        service_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    otel::init(&otel_config);

    let filter = Targets::default()
        .with_target(env!("CARGO_BIN_NAME"), config::hulyrs::CONFIG.log)
        .with_target("actix", Level::WARN);
    let format = tracing_subscriber::fmt::layer().compact();

    match &config::hulyrs::CONFIG.otel_mode {
        otel::OtelMode::Off => {
            tracing_subscriber::registry()
                .with(filter)
                .with(format)
                .init();
        }

        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(format)
                .with(otel::tracer_provider(&otel_config).map(|provider| {
                    let filter = Targets::default()
                        .with_default(Level::DEBUG)
                        .with_target(env!("CARGO_PKG_NAME"), config::hulyrs::CONFIG.log);

                    OpenTelemetryLayer::new(provider.tracer("hulylake")).with_filter(filter)
                }))
                .with(otel::logger_provider(&otel_config).as_ref().map(|logger| {
                    let filter = Targets::default()
                        .with_default(Level::DEBUG)
                        .with_target(env!("CARGO_PKG_NAME"), Level::DEBUG);

                    OpenTelemetryTracingBridge::new(logger).with_filter(filter)
                }))
                .init();
        }
    }

    info!(
        otel = ?config::hulyrs::CONFIG.otel_mode,
        log = ?config::hulyrs::CONFIG.log,
        "tracing with initialized"
    );
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

    if config::CONFIG.dry_run {
        warn!("Dry-run mode is on, no messages will be replicated");
    }

    let system_account = ClaimsBuilder::default()
        .system_account()
        .service(&CONFIG.service_id)
        .build()?;

    let context = Arc::new(context::GlobalContext::new(system_account).await?);

    let supervisor = worker::new_supervisor(context.clone())?;
    supervisor.spawn_all().await?;

    let (http, abort_http) = http::spawn(supervisor.clone(), context.clone())?;

    reverse::start(supervisor.clone(), context.clone())?;

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
