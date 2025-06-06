use anyhow::Result;
use hulyrs::services::{jwt::ClaimsBuilder, types::WorkspaceUuid};
use reqwest::{
    Body, Client,
    multipart::{Form, Part},
};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::*;
use url::Url;
use uuid::Uuid;

use crate::config::CONFIG;

#[derive(Clone)]
pub struct BlobClient {
    token: SecretString,
    base: Url,
    http: Client,
}

pub type Sender = mpsc::Sender<std::io::Result<Vec<u8>>>;

impl BlobClient {
    pub fn new(workspace: WorkspaceUuid) -> hulyrs::Result<Self> {
        let base = CONFIG
            .blob_service_path
            .join("/upload/form-data/")?
            .join(workspace.to_string().as_str())?;

        let http = Client::new();
        let token = ClaimsBuilder::default()
            .system_account()
            .workspace(workspace)
            .build()
            .unwrap()
            .encode()?;

        Ok(Self { token, base, http })
    }

    pub fn upload(
        &self,
        id: Uuid,
        length: usize,
        mime_type: &str,
    ) -> Result<(Sender, oneshot::Receiver<Result<(), reqwest::Error>>)> {
        let (sender, receiver) = mpsc::channel::<std::io::Result<Vec<u8>>>(1);
        let (ready_sender, ready_receiver) = oneshot::channel();

        let body = Body::wrap_stream(ReceiverStream::new(receiver));

        let file = Part::stream(body)
            .file_name(id.to_string())
            .mime_str(mime_type)?;

        let form = Form::new()
            .text("filename", id.to_string())
            .text("contentType", mime_type.to_owned())
            .text("knownLength", length.to_string())
            .part("file", file);

        let request = self
            .http
            .post(self.base.clone())
            .bearer_auth(self.token.expose_secret())
            .multipart(form);

        tokio::spawn(async move {
            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        let _ = response.bytes().await;
                    } else {
                        error!(%id,
                            status = %response.status(),
                            "Error status, while uploading file"
                        );
                    }

                    let _ = ready_sender.send(Ok(()));
                }

                Err(error) => {
                    error!(%id, ?error, "Error while uploading file");

                    let _ = ready_sender.send(Err(error));
                }
            }
        });

        Ok((sender, ready_receiver))
    }
}
