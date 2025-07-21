use std::{sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_web::{
    App, HttpMessage, HttpRequest, HttpResponse, HttpServer,
    body::MessageBody,
    dev::{ServerHandle, ServiceRequest, ServiceResponse},
    middleware::{self, Next},
    web::{self, Data, Json, Path},
};
use hulyrs::services::{
    core::SocialIdId,
    jwt::{Claims, actix::ServiceRequestExt},
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::*;

use crate::{
    config::CONFIG,
    context::GlobalContext,
    integration::TelegramIntegration,
    worker::{
        Supervisor, WorkerAccess, WorkerHintsBuilder, WorkerRequestError, WorkerStateResponse,
    },
};

#[derive(thiserror::Error, Debug)]
enum ApiError {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    #[error(transparent)]
    Huly(#[from] hulyrs::Error),

    #[error(transparent)]
    Http(#[from] actix_web::error::Error),

    #[error(transparent)]
    Worker(#[from] WorkerRequestError),
}

impl actix_web::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::Http(error) => error.error_response(),

            ApiError::Worker(WorkerRequestError::Unauthorized) => {
                HttpResponse::Unauthorized().body("Unauthorized")
            }

            _ => {
                tracing::error!(error=%self, "Internal error in http handler");
                HttpResponse::InternalServerError().body("Internal Server Error")
            }
        }
    }
}

type HandlerResult<T> = Result<T, ApiError>;

async fn interceptor(
    request: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let claims = request
        .extract_claims(crate::config::hulyrs::CONFIG.token_secret.as_ref().unwrap())?
        .to_owned();

    request.extensions_mut().insert(claims);

    next.call(request).await
}

pub fn spawn(
    supervisor: Arc<Supervisor>,
    services: Arc<GlobalContext>,
) -> anyhow::Result<(JoinHandle<Result<(), std::io::Error>>, ServerHandle)> {
    let socket = std::net::SocketAddr::new(CONFIG.bind_host.as_str().parse()?, CONFIG.bind_port);

    info!(bind = ?socket, "Starting http server");

    let server = HttpServer::new(move || {
        let workers = supervisor.clone();
        let services = services.clone();

        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .supports_credentials()
            .max_age(3600);

        App::new()
            .app_data(Data::new(services))
            .app_data(Data::new(workers))
            .wrap(middleware::Logger::default())
            .wrap(cors)
            .service(
                web::scope("/api")
                    .wrap(middleware::from_fn(interceptor))
                    .route("/integrations/", web::get().to(enumerate))
                    .route("/integrations/{number}", web::get().to(get_state))
                    .route("/integrations/{number}", web::post().to(command))
                    .route("/integrations/{number}", web::delete().to(signout))
                    .route("/integrations/{number}/chats", web::get().to(get_chats))
                    .route("/integrations/{number}/restart", web::post().to(restart)),
            )
            .route("/push/{number}", web::put().to(push))
            .route("/push/{number}", web::post().to(push))
            .route(
                "/status",
                web::get().to(async || {
                    format!(
                        "OK {}/{}",
                        env!("CARGO_PKG_NAME"),
                        env!("CARGO_PKG_VERSION")
                    )
                }),
            )
            .route("/heap", web::get().to(heap))
    })
    .disable_signals()
    .bind(socket)?
    .run();

    let server_handle = server.handle();
    let server = tokio::spawn(server);

    Ok((server, server_handle))
}

async fn heap(_request: HttpRequest) -> HandlerResult<HttpResponse> {
    #[cfg(not(debug_assertions))]
    return Ok(HttpResponse::NotFound().finish());

    let mut pprof = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    if !pprof.activated() {
        return Ok(HttpResponse::Forbidden().body("heap profiling not activated"));
    }

    let response = pprof
        .dump_pprof()
        .map(|data| HttpResponse::Ok().body(data))
        .unwrap_or_else(|error| {
            error!(%error, "Failed to dump heap profile");
            HttpResponse::InternalServerError().finish()
        });

    Ok(response)
}

async fn push(
    request: HttpRequest,
    phone: Path<String>,
    supervisor: Data<Supervisor>,
) -> HandlerResult<HttpResponse> {
    let phone = phone.into_inner();

    trace!(%phone, method=%request.method(), "Push request");

    let hints = WorkerHintsBuilder::default()
        .support_auth(false)
        .ttl(Duration::from_secs(10))
        .build()
        .unwrap();

    // wake up the worker
    let _ = supervisor.spawn_worker(&phone, hints).await;

    Ok(HttpResponse::Ok().body("ok"))
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum IntegrationStatus {
    Authorized,
    WantCode,
    WantPassword,
}

impl From<WorkerStateResponse> for IntegrationStatus {
    fn from(value: WorkerStateResponse) -> Self {
        match value {
            WorkerStateResponse::Authorized(_) => IntegrationStatus::Authorized,
            WorkerStateResponse::WantCode => IntegrationStatus::WantCode,
            WorkerStateResponse::WantPassword(_) => IntegrationStatus::WantPassword,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Integration {
    number: String,
    status: IntegrationStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    social_id: Option<SocialIdId>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "command", rename_all = "lowercase")]
enum Command {
    Start,
    Next { input: String },
    Disconnect,
}

async fn enumerate(
    request: HttpRequest,
    services: Data<Arc<GlobalContext>>,
    supervisor: Data<Arc<Supervisor>>,
) -> HandlerResult<Json<Vec<Integration>>> {
    let services = services.to_owned();

    let mut integrations = Vec::new();

    for integration in services
        .account()
        .find_account_integrations(request.extensions().get::<Claims>().unwrap())
        .await?
        .into_iter()
    {
        let phone = &integration.phone;

        let hints = WorkerHintsBuilder::default()
            .support_auth(true)
            .build()
            .unwrap();

        let worker = supervisor.spawn_worker(phone, hints).await;
        let state = worker.request_state().await?;

        trace!(%phone, %state, "Worker state");

        integrations.push(Integration {
            social_id: Some(integration.social_id),
            number: integration.phone.clone(),
            status: state.into(),
        });
    }

    trace!(?integrations, "Enumerate done");

    Ok(Json(integrations))
}

async fn get_state(
    request: HttpRequest,
    phone: Path<String>,
    services: Data<Arc<GlobalContext>>,
    supervisor: Data<Arc<Supervisor>>,
) -> HandlerResult<HttpResponse> {
    let phone = normalize_phone_number(&phone)?;
    let services = services.to_owned();

    let found = services
        .account()
        .find_account_integrations(request.extensions().get::<Claims>().unwrap())
        .await?
        .into_iter()
        .find(|i| i.phone == phone);

    if let Some(integration) = found {
        let hints = WorkerHintsBuilder::default()
            .support_auth(true)
            .build()
            .unwrap();

        let worker = supervisor.spawn_worker(&integration.phone, hints).await;
        let state = worker.request_state().await?;

        let response = HttpResponse::Ok().json(Integration {
            social_id: Some(integration.social_id),
            number: integration.phone,
            status: state.into(),
        });

        trace!(%phone, ?response, "Get request");

        Ok(response)
    } else {
        trace!(%phone, "Integration not found");

        Ok(HttpResponse::NotFound().finish())
    }
}

#[instrument(level = "debug", skip_all, fields(phone = %phone))]
async fn get_chats(
    request: HttpRequest,
    phone: Path<String>,
    services: Data<Arc<GlobalContext>>,
    supervisor: Data<Arc<Supervisor>>,
) -> HandlerResult<HttpResponse> {
    let phone = normalize_phone_number(&phone)?;
    let services = services.to_owned();

    let claims = request.extensions().get::<Claims>().unwrap().to_owned();

    let integration = services
        .account()
        .find_account_integrations(&claims)
        .await?
        .into_iter()
        .find(|i| i.phone == phone);

    if let Some(integration) = integration {
        let hints = WorkerHintsBuilder::default()
            .support_auth(false)
            .build()
            .unwrap();

        let worker = supervisor.spawn_worker(&integration.phone, hints).await;

        if let Ok(workspace) = claims.workspace() {
            let chats = worker.request_chats(workspace).await?;

            Ok(HttpResponse::Ok().json(chats))
        } else {
            Ok(HttpResponse::Forbidden().finish())
        }
    } else {
        trace!(%phone, "Integration not found");

        Ok(HttpResponse::NotFound().finish())
    }
}

async fn restart(
    phone: Path<String>,
    supervisor: Data<Arc<Supervisor>>,
) -> HandlerResult<HttpResponse> {
    let phone = normalize_phone_number(&phone)?;

    Ok(if supervisor.get_ref().restart_worker(&phone).await? {
        HttpResponse::Accepted()
    } else {
        HttpResponse::NotFound()
    }
    .finish())
}

async fn signout(
    phone: Path<String>,
    supervisor: Data<Arc<Supervisor>>,
) -> HandlerResult<HttpResponse> {
    let phone = normalize_phone_number(&phone)?;

    supervisor.get_ref().shutdown(&phone, true).await?;

    Ok(HttpResponse::Accepted().finish())
}

async fn command(
    request: HttpRequest,
    phone: Path<String>,
    services: Data<Arc<GlobalContext>>,
    supervisor: Data<Arc<Supervisor>>,
    command: Json<Command>,
) -> HandlerResult<HttpResponse> {
    let phone = normalize_phone_number(&phone)?;
    let command = command.into_inner();

    let services = services.to_owned();

    let hints = WorkerHintsBuilder::default()
        .support_auth(true)
        .build()
        .unwrap();

    let worker = supervisor.spawn_worker(&phone, hints).await;
    let state = worker.request_state().await?;

    debug!(?phone, ?command, "Integration command");

    //    let old_state = discriminant(&state);

    let state = match (&state, command) {
        (WorkerStateResponse::WantCode, Command::Next { input }) => {
            debug!(?phone, ?state, "Code was requested and provided");
            worker.provide_code(input).await
        }
        (WorkerStateResponse::WantPassword(_), Command::Next { input }) => {
            debug!(?phone, ?state, "Password was requested and provided");
            worker.provide_password(input).await
        }

        _ => Ok(state),
    };

    match state {
        Ok(state) => {
            let social_id = if let WorkerStateResponse::Authorized(user) = &state {
                let claims = request.extensions().get::<Claims>().unwrap().to_owned();

                let account = services.account();
                let social_id = account.ensure_social_id(&claims, user.id()).await?;

                Some(social_id)
            } else {
                None
            };

            let status = Integration {
                social_id,
                number: phone,
                status: state.into(),
            };

            Ok(HttpResponse::Ok().json(status))
        }

        Err(_) => Ok(HttpResponse::Unauthorized().finish()),
    }
}

fn normalize_phone_number(number: &str) -> Result<String, actix_web::error::Error> {
    Ok(number
        .to_owned()
        .chars()
        .filter(|c| c.is_numeric())
        .collect::<String>())
}
