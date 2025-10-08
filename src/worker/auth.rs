use anyhow::Result;
use grammers_client::{
    SignInError,
    client::Client as TelegramClient,
    types::{LoginToken, PasswordToken},
};
use tracing::*;

use super::worker::{Worker, WorkerState};

impl Worker {
    pub(super) async fn auth_code(
        &self,
        telegram: &TelegramClient,
        token: &LoginToken,
        code: &String,
    ) -> Result<WorkerState, SignInError> {
        let id = &self.config.id;

        match telegram.sign_in(token, code).await {
            Ok(_) => {
                trace!(%id, phase="code", "Authorization successfull");

                let user = telegram.get_me().await.map_err(SignInError::Other)?;

                Ok(WorkerState::Authorized(user))
            }

            Err(SignInError::PasswordRequired(token)) => {
                trace!(%id, phase="code", "Password required");
                let hint = token.hint().map(ToString::to_string);
                Ok(WorkerState::WantPassword(hint, token))
            }

            Err(error) => {
                trace!(%id, phase="code", %error, "Authorization failed");
                Err(error)
            }
        }
    }

    pub(super) async fn auth_password(
        &self,
        telegram: &TelegramClient,
        token: &PasswordToken,
        password: &String,
    ) -> Result<WorkerState, SignInError> {
        let id = &self.config.id;

        match telegram
            .check_password(token.to_owned(), password.as_bytes())
            .await
        {
            Ok(_) => {
                trace!(%id, phase="password", "Authorization successfull");

                let user = telegram.get_me().await.map_err(SignInError::Other)?;

                Ok(WorkerState::Authorized(user))
            }

            Err(error) => {
                trace!(%id, %error, phase="password", "Authorization failed");
                Err(error)
            }
        }
    }

    pub async fn persist_session(&self, telegram: &TelegramClient) -> Result<()> {
        Ok(self
            .global_context
            .kvs()
            .upsert(&self.session_key, &telegram.session().save())
            .await?)
    }

    pub async fn delete_session(&self) -> Result<()> {
        Ok(self.global_context.kvs().delete(&self.session_key).await?)
    }
}
