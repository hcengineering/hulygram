use std::collections::HashSet;

use anyhow::{Ok, Result};
use hulyrs::services::{
    transactor::{
        TransactorClient,
        document::{DocumentClient, FindOptionsBuilder},
    },
    types::{PersonId, PersonUuid},
};
use serde_json::{Value, json};
use tracing::*;

pub(super) trait TransactorExt {
    #[allow(dead_code)]
    async fn enumerate_channels(&self) -> Result<HashSet<String>>;

    fn find_channel(&self, channel_id: &str) -> impl Future<Output = Result<bool>>;

    fn find_person(&self, person: PersonUuid) -> impl Future<Output = Result<Option<PersonId>>>;

    fn find_personal_space(
        &self,
        person: &PersonId,
    ) -> impl Future<Output = Result<Option<String>>>;
}

fn id(v: Option<Value>) -> Option<String> {
    v.and_then(|v| v["_id"].as_str().map(ToOwned::to_owned))
}

impl TransactorExt for TransactorClient {
    async fn find_channel(&self, channel_id: &str) -> Result<bool> {
        let query = json!({
            "_id": channel_id,
        });

        let options = FindOptionsBuilder::default().project("_id").build()?;

        let is_found = self
            .find_one::<_, serde_json::Value>("chat:masterTag:Channel", query, &options)
            .await?
            .is_some();

        Ok(is_found)
    }

    async fn enumerate_channels(&self) -> Result<HashSet<String>> {
        let query = json!({
            //
        });

        let options = FindOptionsBuilder::default().project("_id").build()?;

        #[derive(serde::Deserialize)]
        struct Channel {
            _id: String,
        }

        Ok(self
            .find_all::<_, Channel>("chat:masterTag:Channel", query, &options)
            .await?
            .value
            .into_iter()
            .map(|v| v._id)
            .collect::<HashSet<_>>())
    }

    #[instrument(level = "trace", skip(self))]
    async fn find_person(&self, person_uuid: PersonUuid) -> Result<Option<PersonId>> {
        let query = json!({
              "personUuid": person_uuid
        });

        let options = FindOptionsBuilder::default().project("_id").build()?;

        let person_id = id(self
            .find_one::<_, serde_json::Value>("contact:class:Person", query, &options)
            .await?);

        trace!(?person_id);

        Ok(person_id)
    }

    #[instrument(level = "trace", skip(self))]
    async fn find_personal_space(&self, person_id: &PersonId) -> Result<Option<String>> {
        let query = json!({"person": person_id});

        let space_id = id(self
            .find_one::<_, serde_json::Value>(
                "contact:class:PersonSpace",
                query,
                &FindOptionsBuilder::default().build()?,
            )
            .await?);

        trace!(?space_id);

        Ok(space_id)
    }
}
