use std::time::Duration;

use anyhow::Result;
use grammers_client::{
    Client as TelegramClient,
    client::files::DownloadIter,
    types::{
        Downloadable, Message,
        media::{Document, Photo},
    },
};
use hulyrs::services::core::PersonId;
use tokio::time;
use tracing::*;
use uuid::Uuid;

use super::blob::Sender as BlobSender;
use crate::worker::{limiters::TelegramLimiter, sync::state::BlobDescriptor};

use super::export::{Exporter, MessageId};

trait DownloadIterExt {
    async fn next_timeout(&mut self) -> Result<Option<Vec<u8>>>;
}

impl DownloadIterExt for DownloadIter {
    async fn next_timeout(&mut self) -> Result<Option<Vec<u8>>> {
        match time::timeout(Duration::from_secs(60), self.next()).await {
            Ok(x) => x.map_err(Into::into),
            Err(_) => {
                anyhow::bail!("Timeout, downloadning blob chunk");
            }
        }
    }
}

trait TelegramExt {
    async fn download<D: Downloadable>(&self, d: &D, limiter: &TelegramLimiter) -> Result<Vec<u8>>;
    async fn stream<D: Downloadable>(
        &self,
        d: &D,
        sender: BlobSender,
        limiter: &TelegramLimiter,
    ) -> Result<()>;
}

impl TelegramExt for TelegramClient {
    async fn download<D: Downloadable>(&self, d: &D, limiter: &TelegramLimiter) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut download = self.iter_download(d);

        // use me from worker context
        let limiter_key = self.get_me().await?.id();

        while let Some(chunk) = download.next_timeout().await? {
            bytes.extend_from_slice(&chunk);
            limiter.until_key_ready(&limiter_key).await;
        }

        Ok(bytes)
    }

    #[instrument(level = "trace", skip_all)]
    async fn stream<D: Downloadable>(
        &self,
        d: &D,
        sender: BlobSender,
        limiter: &TelegramLimiter,
    ) -> Result<()> {
        trace!("Download start");

        let mut download = self.iter_download(d);

        let limiter_key = self.get_me().await?.id();

        let mut nchunk = 0;
        loop {
            limiter.until_key_ready(&limiter_key).await;

            match download.next_timeout().await {
                Ok(Some(chunk)) => {
                    nchunk += 1;
                    trace!(nchunk, "Chunk");
                    sender.send(Ok(chunk)).await?
                }
                Ok(None) => {
                    break {
                        trace!("Download complete");
                        Ok(())
                    };
                }
                Err(error) => {
                    let message = error.to_string();

                    warn!(%error, "Chunk error");

                    sender
                        .send(Err(std::io::Error::new(std::io::ErrorKind::Other, error)))
                        .await?;

                    break Err(anyhow::anyhow!("{}", message));
                }
            }
        }
    }
}

pub trait MediaTransfer {
    async fn transfer(
        self,
        exporter: &Exporter,
        message: &Message,
        message_id: MessageId,
        social_id: PersonId,
    ) -> Result<()>;
}

impl MediaTransfer for Photo {
    #[instrument(level = "debug", skip_all)]
    async fn transfer(
        self,
        exporter: &Exporter,
        message: &Message,
        message_id: MessageId,
        social_id: PersonId,
    ) -> Result<()> {
        let blob = if let Some(blob) = exporter.context.state.get_blob(self.id()).await? {
            blob
        } else {
            let telegram = &exporter.context.worker.telegram;
            let limiters = &exporter.context.worker.global.limiters();

            let blob = telegram.download(&self, &limiters.get_file).await?;

            let blob_id = Uuid::new_v4();
            let length = blob.len();

            let image_info = imageinfo::ImageInfo::from_raw_data(&blob)?;
            let ready = {
                let blobs = &exporter.context.blobs;

                let (sender, ready) = blobs.upload(blob_id, length, image_info.mimetype)?;

                sender.send(Ok(blob)).await?;

                ready
            };

            // wait for upload to complete
            let _ = ready.await?;

            let blob = BlobDescriptor {
                blob_id,
                length,
                file_name: None,
                mimetype: image_info.mimetype.to_string(),
                size: Some((image_info.size.width as u16, image_info.size.height as u16)),
            };

            trace!(blob_id=%blob.blob_id, "Photo transferred");

            exporter.context.state.set_blob(self.id(), &blob).await?;

            blob
        };

        exporter
            .attach(blob, message_id, social_id, message.date())
            .await?;

        Ok(())
    }
}

impl MediaTransfer for Document {
    #[instrument(level = "debug", skip_all)]
    async fn transfer(
        self,
        exporter: &Exporter,
        message: &Message,
        message_id: MessageId,
        social_id: PersonId,
    ) -> Result<()> {
        let blob = if let Some(blob) = exporter.context.state.get_blob(self.id()).await? {
            blob
        } else {
            let telegram = &exporter.context.worker.telegram;
            let limiters = &exporter.context.worker.global.limiters();
            let blobs = &exporter.context.blobs;

            let blob_id = Uuid::new_v4();
            let length = self.size() as usize;
            let mime_type = self.mime_type().unwrap_or("application/binary");

            debug!(%blob_id, length, mime_type, "Transferring document");

            let (upload, ready) = blobs.upload(blob_id, length, mime_type)?;
            telegram.stream(&self, upload, &limiters.get_file).await?;

            let _ = ready.await?;

            let blob = BlobDescriptor {
                blob_id,
                length,
                file_name: None,
                mimetype: mime_type.to_string(),
                size: None,
            };

            trace!(blob_id=%blob.blob_id, "Document transferred");

            exporter.context.state.set_blob(self.id(), &blob).await?;

            blob
        };

        exporter
            .attach(blob, message_id, social_id, message.date())
            .await?;

        Ok(())
    }
}
