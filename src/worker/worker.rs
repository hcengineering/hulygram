use std::{sync::Arc, time::Duration};

use anyhow::Result;
use grammers_client::grammers_tl_types::functions::account::RegisterDevice;
use grammers_client::{
    Client as TelegramClient, Config, InitParams,
    session::Session,
    types::{LoginToken, PasswordToken, User},
};
use hulyrs::services::core::WorkspaceUuid;
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
use super::sync::{ReverseUpdate, Sync};
use crate::config::CONFIG;
use crate::context::GlobalContext;
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
    Reverse(SyncInfo, ReverseUpdate),
    ProvideCode(
        String,
        Sender<Result<WorkerStateResponse, WorkerRequestError>>,
    ),
    ProvidePassword(
        String,
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
    async fn provide_code(&self, code: String) -> WorkerResponse<WorkerStateResponse>;
    async fn provide_password(&self, code: String) -> WorkerResponse<WorkerStateResponse>;
}

trait WorkerReceiver<T>: Future<Output = Result<WorkerResponse<T>, RecvError>> {
    async fn response(self, op: &'static str) -> Result<T, WorkerRequestError>
    where
        Self: Sized,
    {
        const DURATION: Duration = Duration::from_secs(5);

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

    async fn provide_code(&self, code: String) -> Result<WorkerStateResponse, WorkerRequestError> {
        let (sender, receiver) = channel();
        self.send(WorkerRequest::ProvideCode(code, sender)).await?;

        receiver.response("provide_code").await
    }

    async fn provide_password(
        &self,
        password: String,
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
    pub id: WorkerId,
    pub config: Arc<WorkerConfig>,
    pub telegram: TelegramClient,
    session_key: String,
    sync: Sync,
    global_context: Arc<GlobalContext>,
}

pub enum ExitReason {
    Shutdown,
    Hibenate,
    Restart,
    NotAuthorized,
    Error(anyhow::Error),
}

impl Worker {
    pub async fn new(global_context: Arc<GlobalContext>, config: WorkerConfig) -> Result<Self> {
        let config = Arc::new(config);

        let phone = &config.phone;
        let params: InitParams = InitParams {
            catch_up: true,
            update_queue_limit: None,
            ..Default::default()
        };

        let session_key = format!("ses_{}", phone);

        let session = if let Some(session) = global_context.kvs().get(&session_key).await? {
            trace!(%phone, "Persisted session found");
            Session::load(session.as_slice())?
        } else {
            trace!(%phone, "Persisted session not found");
            Session::new()
        };

        let sync = Sync::new(global_context.clone());

        let telegram = TelegramClient::connect(Config {
            session,
            api_id: CONFIG.telegram_api_id,
            api_hash: CONFIG.telegram_api_hash.clone(),
            params,
        })
        .await?;

        trace!(%phone, %config.id, "Connected");

        Ok(Self {
            id: config.id.clone(),
            config,
            telegram,
            session_key,
            sync,
            global_context,
        })
    }

    async fn persist_session(&self) -> Result<()> {
        Ok(self
            .global_context
            .kvs()
            .upsert(&self.session_key, &self.telegram.session().save())
            .await?)
    }

    async fn delete_session(&self) -> Result<()> {
        Ok(self.global_context.kvs().delete(&self.session_key).await?)
    }

    fn state_response(&self, state: &WorkerState) -> WorkerStateResponse {
        match state {
            WorkerState::Authorized(user) => WorkerStateResponse::Authorized(user.clone()),
            WorkerState::WantCode(_) => WorkerStateResponse::WantCode,
            WorkerState::WantPassword(hint, _) => WorkerStateResponse::WantPassword(hint.clone()),
        }
    }

    pub async fn run(mut self, inbox: &mut mpsc::Receiver<WorkerRequest>) -> ExitReason {
        let result = self.run0(inbox).await;

        if self.telegram.is_authorized().await.unwrap_or(false) {
            let _ = self.persist_session().await;
        }

        //if let Some(sync) = self.sync.take() {
        self.sync.abort().await;

        result.unwrap_or_else(ExitReason::Error)
    }

    #[instrument(name = "run", level = "trace", skip_all)]
    pub async fn run0(&mut self, inbox: &mut mpsc::Receiver<WorkerRequest>) -> Result<ExitReason> {
        let phone = &self.config.phone;

        let mut state = match self.auth_init().await {
            Ok(state) => state,
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
                trace!("Startup Begin");

                self.sync.spawn(self.telegram.clone()).await?;

                let token = CONFIG.base_url.join("/push/")?.join(phone)?.to_string();

                let register = RegisterDevice {
                    no_muted: false,
                    token_type: 4, // simple push
                    token,
                    app_sandbox: false,
                    secret: vec![],
                    other_uids: vec![],
                };

                match self.telegram.invoke(&register).await {
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

                self.persist_session().await?;

                startup_complete = true;

                trace!("Startup Complete");
            }

            select! {
                _ = persist_session.tick() => {
                    if matches!(state, WorkerState::Authorized(_)) {
                        self.persist_session().await?;
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

                                for (chat, mode) in self.sync.chats(requested_workspace_id) {
                                        result.push(ChatEntry {
                                            id: chat.id().to_string(),
                                            name: chat.card_title(),
                                            chat_type: chat.chat_type(),
                                            mode,
                                        });

                                }

                                let _ = sender.send(Ok(result));
                            }

                           // (WorkerState::Authorized(_user), WorkerRequest::RequestUser(sender)) => {
                             //   trace!(%phone, %state, "Request user");

                           //     let user = self.telegram.get_me().await?;

                            //    #[allow(unused)]
                           //     sender.send(user);
                          //  }

                            (WorkerState::WantCode(token), WorkerRequest::ProvideCode(code, response)) => {
                                state = self.auth_code(token, &code).await?;

                                #[allow(unused)]
                                response.send(Ok(self.state_response(&state)));
                            }

                            (WorkerState::WantPassword(_, token), WorkerRequest::ProvidePassword(password, response)) => {
                                state = self.auth_password(token, &password).await?;

                                #[allow(unused)]
                                response.send(Ok(self.state_response(&state)));
                            }

                            (_, WorkerRequest::Shutdown(false)) => {
                                trace!(%phone, "Shutdown requested");

                                break Ok(ExitReason::Shutdown);
                            }

                            (_, WorkerRequest::Shutdown(true)) => {
                                debug!(%phone, "Sign out requested");

                                self.telegram.sign_out().await?;
                                self.delete_session().await?;

                                break Ok(ExitReason::Shutdown);
                            }

                            (_, WorkerRequest::Restart) => {
                                trace!(%phone, "Restart requested");
                                break Ok(ExitReason::Restart);
                            }

                            (WorkerState::Authorized(_), WorkerRequest::Reverse(sync_info, reverse)) => {
                                if let Err(error) = self.sync.handle_reverse_update(sync_info, reverse).await {
                                    error!(%error, "Error while handling reverse update");
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

                update = self.telegram.next_update() => {
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
                    break Ok(ExitReason::Hibenate);
                }
            }
        }
    }
}
