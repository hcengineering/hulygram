use std::{sync::Arc, time::Duration};

use anyhow::{Result, bail};
use grammers_client::grammers_tl_types::functions::account::RegisterDevice;
use grammers_client::{
    Client as TelegramClient, Config, InitParams,
    session::Session,
    types::{LoginToken, PasswordToken, User},
};
use hulyrs::services::core::WorkspaceUuid;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use tokio::{
    select,
    sync::{
        mpsc,
        oneshot::{Sender, channel, error::RecvError},
    },
    time,
};
use tracing::*;

use super::supervisor::WorkerId;
use super::sync::Sync;
use crate::config::CONFIG;
use crate::context::GlobalContext;
use crate::reverse::ReverseEvent;
use crate::telegram::{ChatExt, ChatType};
use crate::worker::sync::{SyncMode, context::SyncInfo};

#[derive(Debug, thiserror::Error)]
pub enum WorkerRequestError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Timeout: {0}")]
    Timeout(&'static str),

    #[error("No worker")]
    NoWorker,
}

impl From<mpsc::error::SendError<WorkerRequest>> for WorkerRequestError {
    fn from(_: mpsc::error::SendError<WorkerRequest>) -> Self {
        WorkerRequestError::NoWorker
    }
}

impl From<RecvError> for WorkerRequestError {
    fn from(_: RecvError) -> Self {
        WorkerRequestError::NoWorker
    }
}

#[derive(Debug, strum::Display)]
pub enum WorkerRequest {
    RequestState(Sender<Result<WorkerStateResponse, WorkerRequestError>>),

    RequestChats(
        WorkspaceUuid,
        Sender<Result<Vec<ChatEntry>, WorkerRequestError>>,
    ),

    //#[allow(dead_code)]
    //RequestUser(Sender<User>),
    Reverse(SyncInfo, ReverseEvent),
    ProvideCode(
        SecretString,
        Sender<Result<WorkerStateResponse, WorkerRequestError>>,
    ),
    ProvidePassword(
        SecretString,
        Sender<Result<WorkerStateResponse, WorkerRequestError>>,
    ),
    Shutdown(bool),
    Restart,
}

#[derive(Debug, strum::Display)]
pub enum WorkerStateResponse {
    Authorized(User),
    WantCode,
    WantPassword(Option<String>),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatEntry {
    id: String,
    name: String,

    #[serde(rename = "type")]
    chat_type: ChatType,

    mode: SyncMode,
}

pub type WorkerResponse<T> = Result<T, WorkerRequestError>;

pub trait WorkerAccess {
    async fn request_state(&self) -> WorkerResponse<WorkerStateResponse>;
    async fn request_chats(&self, workspace: WorkspaceUuid) -> WorkerResponse<Vec<ChatEntry>>;
    async fn provide_code(&self, code: SecretString) -> WorkerResponse<WorkerStateResponse>;
    async fn provide_password(&self, code: SecretString) -> WorkerResponse<WorkerStateResponse>;
}

trait WorkerReceiver<T>: Future<Output = Result<WorkerResponse<T>, RecvError>> {
    async fn response(self, op: &'static str) -> Result<T, WorkerRequestError>
    where
        Self: Sized,
    {
        const DURATION: Duration = Duration::from_secs(10);

        time::timeout(DURATION, self).await.map_err(|_| {
            error!(%op, "Worker request timed out");
            WorkerRequestError::Timeout(op)
        })??
    }
}

impl<T, F: Future<Output = Result<WorkerResponse<T>, RecvError>>> WorkerReceiver<T> for F {
    //
}

impl WorkerAccess for mpsc::Sender<WorkerRequest> {
    async fn request_state(&self) -> Result<WorkerStateResponse, WorkerRequestError> {
        let (sender, receiver) = channel();
        self.send(WorkerRequest::RequestState(sender)).await?;

        receiver.response("request_state").await
    }

    async fn request_chats(
        &self,
        workspace: WorkspaceUuid,
    ) -> Result<Vec<ChatEntry>, WorkerRequestError> {
        let (sender, receiver) = channel();
        self.send(WorkerRequest::RequestChats(workspace, sender))
            .await?;

        receiver.response("request_chats").await
    }

    async fn provide_code(
        &self,
        code: SecretString,
    ) -> Result<WorkerStateResponse, WorkerRequestError> {
        let (sender, receiver) = channel();
        self.send(WorkerRequest::ProvideCode(code, sender)).await?;

        receiver.response("provide_code").await
    }

    async fn provide_password(
        &self,
        password: SecretString,
    ) -> Result<WorkerStateResponse, WorkerRequestError> {
        let (sender, receiver) = channel();
        self.send(WorkerRequest::ProvidePassword(password, sender))
            .await?;
        receiver.response("provide_password").await
    }
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct WorkerHints {
    #[builder(default)]
    pub support_auth: bool,

    #[builder(default = "Duration::from_secs(1800)")]
    pub ttl: Duration,
}

#[derive(Clone)]
pub struct WorkerConfig {
    pub id: WorkerId,
    pub phone: String,
    pub hints: WorkerHints,

    pub _self_sender: mpsc::Sender<WorkerRequest>,
}

#[derive(strum::Display, derive_more::IsVariant)]
pub(super) enum WorkerState {
    Authorized(User),
    WantCode(LoginToken),
    WantPassword(Option<String>, PasswordToken),
}

pub struct Worker {
    pub config: WorkerConfig,
    pub global_context: Arc<GlobalContext>,
    pub session_key: String,
}

pub enum ExitReason {
    Shutdown,
    Hibenate,
    Restart,
    NotAuthorized,
    Error(anyhow::Error),
}

async fn connect(
    telegram_config: Config,
    config: &WorkerConfig,
    generation: u32,
) -> Result<(TelegramClient, WorkerState)> {
    let connect = TelegramClient::connect(telegram_config).await;

    match connect {
        Ok(telegram) => {
            let state = telegram.is_authorized().await;

            match state {
                Ok(true) => {
                    trace!(%config.id, phase="init", "Authorization successfull");

                    let me = telegram.get_me().await?;

                    Ok((telegram, WorkerState::Authorized(me)))
                }

                Ok(false) => {
                    debug!(%config.id, phase="init", support_auth=config.hints.support_auth, "Not authorized");

                    if config.hints.support_auth && generation == 0 {
                        let token = telegram
                            .request_login_code(&config.phone.to_string())
                            .await?;

                        debug!(%config.id, phase="init", "Login code requested");

                        Ok((telegram, WorkerState::WantCode(token)))
                    } else {
                        debug!(%config.id, phase="init", "Authorization not supported, exiting");
                        bail!("NotAuthorized");
                    }
                }

                Err(error) => {
                    debug!(%config.id, phase="init", %error, "Authorization error");
                    bail!(error)
                }
            }
        }

        Err(error) => {
            error!(%config.phone, %error, "Cannot connect to Telegram");
            bail!(error)
        }
    }
}

impl Worker {
    pub async fn new(global_context: Arc<GlobalContext>, config: WorkerConfig) -> Result<Self> {
        let session_key = format!("ses_{}", config.phone);

        Ok(Self {
            config,
            session_key,
            global_context,
        })
    }

    fn state_response(&self, state: &WorkerState) -> WorkerStateResponse {
        match state {
            WorkerState::Authorized(user) => WorkerStateResponse::Authorized(user.clone()),
            WorkerState::WantCode(_) => WorkerStateResponse::WantCode,
            WorkerState::WantPassword(hint, _) => WorkerStateResponse::WantPassword(hint.clone()),
        }
    }

    pub async fn run(
        mut self,
        inbox: &mut mpsc::Receiver<WorkerRequest>,
        generation: u32,
    ) -> ExitReason {
        let phone = &self.config.phone;
        let config = &self.config;

        let session_key = &self.session_key;
        let global_context = &self.global_context;

        let (session, loaded) = if let Some(session) = global_context
            .kvs()
            .get(session_key)
            .await
            .expect("KVS get failed")
        {
            debug!(%phone, "Persisted session found");

            Session::load(session.as_slice())
                .map(|session| (session, true))
                .unwrap_or_else(|_| {
                    warn!(%phone, "Cannot load persisted session, starting new");
                    (Session::new(), false)
                })
        } else {
            debug!(%phone, "Persisted session not found");
            (Session::new(), false)
        };

        let telegram_config = Config {
            session,
            api_id: CONFIG.telegram_api_id,
            api_hash: CONFIG.telegram_api_hash.clone(),
            params: InitParams {
                catch_up: true,
                update_queue_limit: None,
                ..Default::default()
            },
        };

        let connect = connect(telegram_config, config, generation).await;

        match connect {
            Ok((telegram, state)) => {
                let mut sync = Sync::new(global_context.clone());

                let result = self.run0(inbox, telegram.clone(), state, &mut sync).await;

                if telegram.is_authorized().await.unwrap_or(false) {
                    let _ = self.persist_session(&telegram).await;
                }

                sync.abort().await;

                result.unwrap_or_else(ExitReason::Error)
            }

            Err(error) => {
                error!(%phone, %error, "Cannot connect to Telegram");

                if loaded {
                    debug!(%phone, "Deleting invalid session");
                    let _ = global_context.kvs().delete(session_key).await;
                }

                while let Ok(Some(request)) =
                    time::timeout(Duration::from_millis(500), inbox.recv()).await
                {
                    match request {
                        WorkerRequest::RequestChats(_, response) => {
                            let _ = response.send(Err(WorkerRequestError::Unauthorized));
                        }

                        WorkerRequest::ProvideCode(_, response)
                        | WorkerRequest::ProvidePassword(_, response)
                        | WorkerRequest::RequestState(response) => {
                            let _ = response.send(Err(WorkerRequestError::Unauthorized));
                        }

                        _ => {
                            //
                        }
                    }
                }

                ExitReason::NotAuthorized
            }
        }
    }

    #[instrument(name = "run", level = "trace", skip_all)]
    pub async fn run0(
        &mut self,
        inbox: &mut mpsc::Receiver<WorkerRequest>,
        telegram: TelegramClient,
        mut state: WorkerState,
        sync: &mut Sync,
    ) -> Result<ExitReason> {
        let phone = &self.config.phone;

        let mut startup_complete = false;
        let mut persist_session = time::interval(Duration::from_secs(60));

        let hibernate = time::Instant::now() + self.config.hints.ttl;

        loop {
            if matches!(state, WorkerState::Authorized(_)) && !startup_complete {
                trace!("Startup Begin");

                sync.spawn(telegram.clone()).await?;

                let token = CONFIG.base_url.join("/push/")?.join(phone)?.to_string();

                let register = RegisterDevice {
                    no_muted: false,
                    token_type: 4, // simple push
                    token,
                    app_sandbox: false,
                    secret: vec![],
                    other_uids: vec![],
                };

                match telegram.invoke(&register).await {
                    Ok(true) => {
                        debug!(%phone, endpoint=%register.token, "Push endpoint registered");
                    }
                    Ok(false) => {
                        warn!(%phone, endpoint=%register.token, "Push endpoint registration failed");
                    }

                    Err(error) => {
                        warn!(%phone, endpoint=%register.token, %error, "Push endpoint registration failed");
                    }
                }

                self.persist_session(&telegram).await?;

                startup_complete = true;

                trace!("Startup Complete");
            }

            select! {
                _ = persist_session.tick() => {
                    if matches!(state, WorkerState::Authorized(_)) {
                        self.persist_session(&telegram).await?;
                    }
                }

                message = inbox.recv() => {
                    if let Some(request) = message {
                        let span = span!(Level::TRACE, "Worker request", %phone, %state, %request);
                        let _enter = span.enter();


                        match (&state, request) {
                            (_, WorkerRequest::RequestState(sender)) => {
                                trace!(%phone, %state, "Request state");

                                #[allow(unused)]
                                sender.send(Ok(self.state_response(&state)));
                            }

                            (WorkerState::Authorized(_user), WorkerRequest::RequestChats(requested_workspace_id, sender)) => {
                                trace!(workspace_id = %requested_workspace_id,"Request channels");

                                let mut result = Vec::default();

                                for (chat, mode) in sync.chats(requested_workspace_id) {
                                        result.push(ChatEntry {
                                            id: chat.id().to_string(),
                                            name: chat.card_title(),
                                            chat_type: chat.chat_type(),
                                            mode,
                                        });

                                }

                                let _ = sender.send(Ok(result));
                            }

                            (WorkerState::WantCode(token), WorkerRequest::ProvideCode(code, response)) => {
                                state = self.auth_code(&telegram, token, &code.expose_secret().to_string()).await?;

                                #[allow(unused)]
                                response.send(Ok(self.state_response(&state)));
                            }

                            (WorkerState::WantPassword(_, token), WorkerRequest::ProvidePassword(password, response)) => {
                                state = self.auth_password(&telegram, token, &password.expose_secret().to_string()).await?;

                                #[allow(unused)]
                                response.send(Ok(self.state_response(&state)));
                            }

                            (_, WorkerRequest::Shutdown(false)) => {
                                trace!(%phone, "Shutdown requested");

                                break Ok(ExitReason::Shutdown);
                            }

                            (_, WorkerRequest::Shutdown(true)) => {
                                debug!(%phone, "Sign out requested");

                                telegram.sign_out().await?;
                                self.delete_session().await?;

                                break Ok(ExitReason::Shutdown);
                            }

                            (_, WorkerRequest::Restart) => {
                                trace!(%phone, "Restart requested");
                                break Ok(ExitReason::Restart);
                            }

                            (WorkerState::Authorized(_), WorkerRequest::Reverse(sync_info, reverse)) => {
                                let huly_message = reverse.huly_message_id().clone();

                                if let Err(error) = sync.handle_reverse_update(&sync_info, reverse).await {
                                    warn!(
                                        huly_workspace = %sync_info.huly_workspace_id,
                                        huly_card = %sync_info.huly_card_id,
                                        huly_card_title = %sync_info.huly_card_title,
                                        %huly_message,
                                        telegram_phone=%sync_info.telegram_phone_number,
                                        telegram_chat = %sync_info.telegram_chat_id,
                                        %error, "Cannot handle reverse update");
                                }
                            }

                            _ => {
                                error!("Unexpected state and message")
                            }
                        }
                    } else {
                        unreachable!()
                    }
                }

                update = telegram.next_update() => {
                    match update {
                        Ok(update) => {
                            sync.handle_update(update).await?;
                        }

                        Err(error) => {
                            error!(?error, "Cannot receive next update");
                            break Err(error.into())
                        }
                    }
                }

                _ = tokio::time::sleep_until(hibernate) => {
                    break Ok(ExitReason::Hibenate);
                }
            }
        }
    }
}
