use anyhow::{Result, bail};
use grammers_client::{
    SignInError,
    types::{LoginToken, PasswordToken},
};
use tracing::trace;

use super::worker::{Worker, WorkerState};

impl Worker {
    pub(super) async fn auth_init(&self) -> Result<WorkerState> {
        let phone = &self.config.phone;

        let state = if self.telegram.is_authorized().await? {
            trace!(%self.id, phase="init", "Authorization successfull");
            WorkerState::Authorized(self.telegram.get_me().await?)
        } else {
            trace!(%self.id, phase="init", "Not authorized");

            if self.config.hints.support_auth {
                let token = self.telegram.request_login_code(&phone.to_string()).await?;

                trace!(%self.id, phase="init", "Login code requested");

                WorkerState::WantCode(token)
            } else {
                bail!("AuthFailed");
            }
        };

        Ok(state)
    }

    pub(super) async fn auth_code(
        &self,
        token: &LoginToken,
        code: &String,
    ) -> Result<WorkerState, SignInError> {
        match self.telegram.sign_in(token, code).await {
            Ok(_) => {
                trace!(%self.id, phase="code", "Authorization successfull");

                let user = self.telegram.get_me().await.map_err(SignInError::Other)?;

                Ok(WorkerState::Authorized(user))
            }

            Err(SignInError::PasswordRequired(token)) => {
                trace!(%self.id, phase="code", "Password required");
                let hint = token.hint().map(ToString::to_string);
                Ok(WorkerState::WantPassword(hint, token))
            }

            Err(error) => {
                trace!(%self.id, phase="code", %error, "Authorization failed");
                Err(error)
            }
        }
    }

    pub(super) async fn auth_password(
        &self,
        token: &PasswordToken,
        password: &String,
    ) -> Result<WorkerState, SignInError> {
        match self
            .telegram
            .check_password(token.to_owned(), password.as_bytes())
            .await
        {
            Ok(_) => {
                trace!(%self.id, phase="password", "Authorization successfull");

                let user = self.telegram.get_me().await.map_err(SignInError::Other)?;

                Ok(WorkerState::Authorized(user))
            }

            Err(error) => {
                trace!(%self.id, %error, phase="password", "Authorization failed");
                Err(error)
            }
        }
    }
}
