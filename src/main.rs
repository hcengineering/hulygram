use tokio::{select, signal::unix::{signal, SignalKind}};
use config::CONFIG;
use hulyrs::services::jwt::ClaimsBuilder;
use worker::{GlobalServices};

mod config;
mod etc;
mod http;
mod integration;
mod worker;

pub fn initialize_tracing() {
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    //let console_layer = console_subscriber::spawn();

    let level = tracing::Level::TRACE;

    let filter = Targets::default()
        .with_target(env!("CARGO_PKG_NAME"), level)
        .with_target("grammers", tracing::Level::INFO)
        //.with_target("tokio", tracing::Level::TRACE)
        //.with_target("runtime", tracing::Level::TRACE)
        .with_target("hulyrs::service", config::hulyrs::CONFIG.log)
        //.with_target("actix", tracing::Level::DEBUG)
        //.with_target("reqwest", tracing::Level::TRACE)
        
        ;
    let format = tracing_subscriber::fmt::layer().compact();

    tracing_subscriber::registry()
        //.with(console_layer)
        .with(filter)
        .with(format)
        .init();
}

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

    let global_services = GlobalServices::new(system_account)?;

    let supervisor = worker::new_supervisor(global_services.clone())?;
    supervisor.spawn_all().await?;

    let (http, abort_http) = http::spawn(supervisor.clone(), global_services)?;

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
