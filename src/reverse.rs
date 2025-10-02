use std::sync::{Arc, LazyLock};

use anyhow::Result;
use hulyrs::services::transactor::{
    TransactionValue,
    comm::{
        CreateMessageEvent, Envelope, MessageRequestType, MessageType, RemovePatchEvent,
        UpdatePatchEvent,
    },
    kafka::parse_message,
};

use hulyrs::services::core::tx::TxDomainEvent;

use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
};
use serde_json::{self as json, Value};
use tracing::*;

use crate::{
    config::{CONFIG, hulyrs::CONFIG as hconfig},
    context::GlobalContext,
    worker::{
        SyncContext, WorkerHintsBuilder, WorkerRequest,
        sync::{ReverseUpdate, SyncInfo, export},
    },
};

pub fn create_consumer(topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &CONFIG.inbound_tx_group_id)
        .set(
            "bootstrap.servers",
            hconfig.kafka_bootstrap_servers.join(","),
        )
        .set("enable.partition.eof", "false")
        .set("heartbeat.interval.ms", "2000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    Ok(consumer)
}

const TELEGRAM: LazyLock<Value> = LazyLock::new(|| serde_json::to_value(export::TELEGRAM).unwrap());

pub fn start(
    supervisor: Arc<super::worker::Supervisor>,
    context: Arc<GlobalContext>,
) -> Result<()> {
    let topic = &CONFIG.inbound_tx_topic;

    let consumer = create_consumer(topic)?;

    info!(topic, "Inbound transactions consumer started");

    #[instrument(level = "trace", skip_all)]
    async fn process_message(
        message: &BorrowedMessage<'_>,
        supervisor: &Arc<super::worker::Supervisor>,
        context: &Arc<GlobalContext>,
    ) -> Result<()> {
        let (workspace, transaction) = parse_message(message)?;

        let acquire_worker = async |card_id| -> Result<
            Option<(tokio::sync::mpsc::Sender<WorkerRequest>, SyncInfo)>,
        > {
            let sync_info = SyncContext::ref_lookup(&context.kvs(), workspace, card_id).await?;

            let result = if let Some(sync_info) = sync_info {
                let hints = WorkerHintsBuilder::default().support_auth(false).build()?;

                let phone = &sync_info.telegram_phone_number;
                Some((supervisor.spawn_worker(phone, hints).await, sync_info))
            } else {
                None
            };

            Ok(result)
        };

        if transaction.matches(Some("core:class:TxDomainEvent"), Some("communication")) {
            let event = json::from_value::<TxDomainEvent<Envelope<Value>>>(transaction.clone());

            let event = match event {
                Ok(domain_event) => domain_event,

                Err(error) => {
                    warn!(event = %transaction, error = ?error, "Cannot parse communication domain event");
                    return Ok(());
                }
            };

            match event.event.r#type {
                MessageRequestType::CreateMessage => {
                    let create_message =
                        json::from_value::<CreateMessageEvent>(event.event.request)?;

                    //debug!("CreateMessageEvent: {:#?}", create_message);

                    let from_telegram = create_message
                        .extra
                        .as_ref()
                        .map(|e| e.get(export::HULYGRAM_ORIGIN) == Some(&TELEGRAM))
                        .unwrap_or(false);

                    if !from_telegram
                        && matches!(create_message.message_type, MessageType::Text)
                        && let Some((worker, sync_info)) =
                            acquire_worker(&create_message.card_id).await?
                    {
                        worker
                            .send(WorkerRequest::Reverse(
                                sync_info,
                                ReverseUpdate::MessageCreated {
                                    huly_message_id: create_message.message_id.unwrap(),
                                    content: create_message.content,
                                },
                            ))
                            .await?;
                    }
                }

                MessageRequestType::UpdatePatch => {
                    let patch = json::from_value::<UpdatePatchEvent>(event.event.request)?;

                    let from_telegram = patch
                        .extra
                        .as_ref()
                        .map(|e| e.get(export::HULYGRAM_ORIGIN) == Some(&TELEGRAM))
                        .unwrap_or(false);

                    if !from_telegram
                        && let Some((worker, sync_info)) = acquire_worker(&patch.card_id).await?
                    {
                        worker
                            .send(WorkerRequest::Reverse(
                                sync_info,
                                ReverseUpdate::MessageUpdated {
                                    huly_message_id: patch.message_id,
                                    content: patch.content,
                                },
                            ))
                            .await?;
                    }
                }

                MessageRequestType::RemovePatch => {
                    let patch = json::from_value::<RemovePatchEvent>(event.event.request)?;

                    if let Some((worker, sync_info)) = acquire_worker(&patch.card_id).await? {
                        worker
                            .send(WorkerRequest::Reverse(
                                sync_info,
                                ReverseUpdate::MessageDeleted {
                                    huly_message_id: patch.message_id,
                                },
                            ))
                            .await?;
                    }
                }

                _ => {
                    //
                }
            }
        }

        Ok(())
    }

    tokio::spawn(async move {
        loop {
            let message = consumer.recv().await;

            if let Ok(message) = message {
                if let Err(error) = process_message(&message, &supervisor, &context).await {
                    warn!(%error, "Transaction error");
                }

                if let Err(error) =
                    consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async)
                {
                    error!(%error, "Cannot commit message");
                }
            }
        }
    });

    Ok(())
}
