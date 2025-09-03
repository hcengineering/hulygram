use anyhow::{Ok, Result};
use hulyrs::services::{
    core::{PersonId, PersonUuid},
    transactor::{
        TransactorClient,
        backend::http::HttpBackend,
        document::{DocumentClient, FindOptionsBuilder},
    },
};
use serde_json::{Value, json};
use tracing::*;

#[derive(serde::Deserialize, Debug)]
pub struct Channel {
    #[serde(rename = "_id")]
    pub id: String,
    pub space: String,
    pub title: String,
}

pub(super) trait TransactorExt {
    #[allow(dead_code)]
    async fn enumerate_channels(&self) -> Result<Vec<Channel>>;

    fn find_person(&self, person: PersonUuid) -> impl Future<Output = Result<Option<PersonId>>>;

    fn find_personal_space(
        &self,
        person: PersonUuid,
    ) -> impl Future<Output = Result<Option<String>>>;
}

fn id(v: Option<Value>) -> Option<String> {
    v.and_then(|v| v["_id"].as_str().map(ToOwned::to_owned))
}

impl TransactorExt for TransactorClient<HttpBackend> {
    async fn enumerate_channels(&self) -> Result<Vec<Channel>> {
        let query = json!({
            //
        });

        let options = FindOptionsBuilder::default()
            .project("_id")
            .project("space")
            .project("title")
            .build()?;

        let found = self
            .find_all::<_, Channel>("chat:masterTag:Thread", query, &options)
            .await?
            .value;

        Ok(found)
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
    async fn find_personal_space(&self, person_id: PersonUuid) -> Result<Option<String>> {
        let person_id = self
            .find_person(person_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("NoPerson"))?;

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
