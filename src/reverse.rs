use std::sync::Arc;

use anyhow::Result;
use hulyrs::services::transactor::{
    TransactionValue,
    comm::{
        CreateMessageEvent, Envelope, MessageRequestType, MessageType, RemovePatchEvent,
        UpdatePatchEvent,
    },
    kafka::parse_message,
    tx::TxDomainEvent,
};
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
        WorkerRequest, SyncContext, WorkerHintsBuilder,
        sync::{ReverseUpdate, SyncInfo},
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

pub fn start(
    supervisor: Arc<super::worker::Supervisor>,
    context: Arc<GlobalContext>,
) -> Result<()> {
    let topic = &CONFIG.inbound_tx_topic;

    let consumer = create_consumer(topic)?;

    info!(topic, "inbound transactions consumer started");

    #[instrument(level = "trace", skip_all)]
    async fn process_message(
        message: &BorrowedMessage<'_>,
        supervisor: &Arc<super::worker::Supervisor>,
        context: &Arc<GlobalContext>,
    ) -> Result<()> {
        let (workspace, transaction) = parse_message(message)?;

        let acquire_worker =
            async |card_id| -> Result<Option<(tokio::sync::mpsc::Sender<WorkerRequest>, SyncInfo)>> {
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
            let domain_event =
                json::from_value::<TxDomainEvent<Envelope<Value>>>(transaction).unwrap();

            match domain_event.event.r#type {
                MessageRequestType::CreateMessage => {
                    let create_message =
                        json::from_value::<CreateMessageEvent>(domain_event.event.request)?;

                    if matches!(create_message.message_type, MessageType::Message) {
                        if let Some((worker, sync_info)) =
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
                }

                MessageRequestType::UpdatePatch => {
                    let patch = json::from_value::<UpdatePatchEvent>(domain_event.event.request)?;

                    if let Some((worker, sync_info)) = acquire_worker(&patch.card_id).await? {
                        worker
                            .send(WorkerRequest::Reverse(
                                sync_info,
                                ReverseUpdate::MessageUpdated {
                                    huly_message_id: patch.message_id,
                                    content: patch.content,
                                },
                            ))
                            .await?;

                        //
                    }
                }

                MessageRequestType::RemovePatch => {
                    let patch = json::from_value::<RemovePatchEvent>(domain_event.event.request)?;

                    if let Some((worker, sync_info)) = acquire_worker(&patch.card_id).await? {
                        worker
                            .send(WorkerRequest::Reverse(
                                sync_info,
                                ReverseUpdate::MessageDeleted {
                                    huly_message_id: patch.message_id,
                                },
                            ))
                            .await?;

                        //
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
                    warn!(%error, "transaction error");
                }

                consumer
                    .commit_message(&message, rdkafka::consumer::CommitMode::Async)
                    .unwrap();
            }
        }
    });

    Ok(())
}
