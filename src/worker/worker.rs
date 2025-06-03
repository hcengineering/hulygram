use std::{sync::Arc, time::Duration};

use anyhow::Result;
use grammers_client::grammers_tl_types::functions::account::RegisterDevice;
use grammers_client::{
    Client, Config, InitParams,
    session::Session,
    types::{LoginToken, PasswordToken, User},
};
use tokio::{
    select,
    sync::{
        mpsc,
        oneshot::{Sender, channel},
    },
    time,
};
use tracing::*;

use super::services::GlobalServices;
use super::supervisor::WorkerId;
use super::sync::Sync;
use crate::config::CONFIG;

#[derive(Debug)]
pub enum Message {
    RequestState(Sender<WorkerStateResponse>),

    #[allow(dead_code)]
    RequestUser(Sender<User>),
    ProvideCode(String, Sender<WorkerStateResponse>),
    ProvidePassword(String, Sender<WorkerStateResponse>),
    Shutdown,
}

#[derive(Debug, strum::Display)]
pub enum WorkerStateResponse {
    Authorized(User),
    WantCode,
    WantPassword(Option<String>),
}

pub trait WorkerAccess {
    async fn request_state(&self) -> Result<WorkerStateResponse>;
    async fn provide_code(&self, code: String) -> Result<WorkerStateResponse>;
    async fn provide_password(&self, code: String) -> Result<WorkerStateResponse>;
}

impl WorkerAccess for mpsc::Sender<Message> {
    async fn request_state(&self) -> Result<WorkerStateResponse> {
        // FIXME handle request timeout
        let (sender, receiver) = channel();
        self.send(Message::RequestState(sender)).await?;
        Ok(receiver.await?)
    }

    async fn provide_code(&self, code: String) -> Result<WorkerStateResponse> {
        // FIXME handle request timeout
        let (sender, receiver) = channel();
        self.send(Message::ProvideCode(code, sender)).await?;
        Ok(receiver.await?)
    }

    async fn provide_password(&self, password: String) -> Result<WorkerStateResponse> {
        // FIXME handle request timeout
        let (sender, receiver) = channel();
        self.send(Message::ProvidePassword(password, sender))
            .await?;
        Ok(receiver.await?)
    }
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct WorkerHints {
    #[builder(default)]
    pub support_auth: bool,

    #[builder(default = "Duration::from_secs(600)")]
    pub ttl: Duration,
}

#[derive(Clone)]
pub struct WorkerConfig {
    pub id: WorkerId,
    pub phone: String,
    pub global_services: GlobalServices,
    pub hints: WorkerHints,

    pub _self_sender: mpsc::Sender<Message>,
}

#[derive(strum::Display, derive_more::IsVariant)]
pub(super) enum WorkerState {
    Authorized(User),
    WantCode(LoginToken),
    WantPassword(Option<String>, PasswordToken),
}

pub struct Worker {
    pub id: WorkerId,
    pub config: Arc<WorkerConfig>,
    pub client: Client,
    session_key: String,
    sync: Sync,
}

pub enum ExitReason {
    Shutdown,
    Hibenate,
    NotAuthorized,
    Error(anyhow::Error),
}

impl Worker {
    pub async fn new(config: WorkerConfig) -> Result<Self> {
        let config = Arc::new(config);

        let phone = &config.phone;
        let params: InitParams = Default::default();

        let session_key = format!("ses_{}", phone);

        let session = if let Some(session) = config.global_services.kvs().get(&session_key).await? {
            trace!(%phone, "Persisted session found");
            Session::load(session.as_slice())?
        } else {
            trace!(%phone, "Persisted session not found");
            Session::new()
        };

        let client = Client::connect(Config {
            session,
            api_id: CONFIG.telegram_api_id,
            api_hash: CONFIG.telegram_api_hash.clone(),
            params,
        })
        .await?;

        trace!(%phone, %config.id, "Connected");

        let sync = Sync::new(client.clone(), config.clone());

        Ok(Self {
            id: config.id.clone(),
            config,
            client,
            session_key,
            sync,
        })
    }

    async fn persist_session(&self) -> Result<()> {
        Ok(self
            .config
            .global_services
            .kvs()
            .upsert(&self.session_key, &self.client.session().save())
            .await?)
    }

    async fn state_response(&self, state: &WorkerState) -> Result<WorkerStateResponse> {
        match state {
            WorkerState::Authorized(user) => Ok(WorkerStateResponse::Authorized(user.clone())),
            WorkerState::WantCode(_) => Ok(WorkerStateResponse::WantCode),
            WorkerState::WantPassword(hint, _) => {
                Ok(WorkerStateResponse::WantPassword(hint.clone()))
            }
        }
    }

    pub async fn run(&mut self, inbox: &mut mpsc::Receiver<Message>) -> ExitReason {
        let result = self.run0(inbox).await;

        if self.client.is_authorized().await.unwrap_or(false) {
            let _ = self.persist_session().await;
        }

        result.unwrap_or_else(ExitReason::Error)
    }

    pub async fn run0(&mut self, inbox: &mut mpsc::Receiver<Message>) -> Result<ExitReason> {
        let phone = &self.config.phone;

        let mut state = match self.auth_init().await {
            Ok(state) => {
                self.persist_session().await?;
                state
            }
            Err(error) => {
                error!(%phone, %error, "Authorization failed");
                return Ok(ExitReason::NotAuthorized);
            }
        };

        let mut startup_complete = false;
        let mut persist_session = time::interval(Duration::from_secs(60));

        let hibernate = time::Instant::now() + self.config.hints.ttl;

        loop {
            if matches!(state, WorkerState::Authorized(_)) && !startup_complete {
                let token = CONFIG.base_url.join("/push/")?.join(phone)?.to_string();

                let register = RegisterDevice {
                    no_muted: false,
                    token_type: 4, // simple push
                    token,
                    app_sandbox: false,
                    secret: vec![],
                    other_uids: vec![],
                };

                match self.client.invoke(&register).await {
                    Ok(true) => {
                        trace!(%phone, endpoint=%register.token, "Push endpoint registered");
                    }
                    Ok(false) => {
                        warn!(%phone, endpoint=%register.token, "Push endpoint registration failed");
                    }

                    Err(error) => {
                        warn!(%phone, endpoint=%register.token, %error, "Push endpoint registration failed");
                    }
                }

                self.sync.spawn().await?;

                startup_complete = true;
            }

            select! {
                _ = persist_session.tick() => {
                    if matches!(state, WorkerState::Authorized(_)) {
                        self.persist_session().await?;
                    }
                }

                message = inbox.recv() => {
                    if let Some(message) = message {
                        match (&state, message) {
                            (_, Message::RequestState(sender)) => {
                                trace!(%phone, %state, "Request state");

                                #[allow(unused)]
                                sender.send(self.state_response(&state).await?);
                            }

                            (WorkerState::Authorized(_user), Message::RequestUser(sender)) => {
                                trace!(%phone, %state, "Request user");

                                let user = self.client.get_me().await?;

                                #[allow(unused)]
                                sender.send(user);
                            }

                            (WorkerState::WantCode(token), Message::ProvideCode(code, response)) => {
                                state = self.auth_code(token, &code).await?;

                                #[allow(unused)]
                                response.send(self.state_response(&state).await?);
                            }

                            (WorkerState::WantPassword(_, token), Message::ProvidePassword(password, response)) => {
                                state = self.auth_password(token, &password).await?;

                                #[allow(unused)]
                                response.send(self.state_response(&state).await?);
                            }

                            (_, Message::Shutdown) => {
                                trace!(%phone, "Shutdown requested");

                                break Ok(ExitReason::Shutdown);
                            }

                            _ => {
                                error!("Unexpected state and message")
                            }
                        }
                    } else {
                        unreachable!()
                    }
                }

                update = self.client.next_update() => {
                    match update {
                        Ok(update) => {
                            self.sync.handle_update(update).await?;
                        }

                        Err(error) => {
                            error!(?error, "Cannot receive next update");
                            break Err(error.into())
                        }
                    }
                }

                _ = tokio::time::sleep_until(hibernate) => {
                    self.sync.abort();

                    break Ok(ExitReason::Hibenate);
                }
            }
        }
    }
}
