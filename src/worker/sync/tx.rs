use anyhow::{Ok, Result};
use grammers_client::types::inline::query;
use hulyrs::services::{
    transactor::{
        TransactorClient,
        document::{CreateDocumentBuilder, DocumentClient, FindOptionsBuilder},
    },
    types::{PersonId, PersonUuid, SocialIdId},
};
use serde_json::{Value, json};
use tracing::*;

pub(super) trait TransactorExt {
    fn create_channel(
        &self,
        social_id: &SocialIdId,
        space_id: &str,
        title: &str,
    ) -> impl Future<Output = Result<String>>;

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
    #[instrument(level = "trace", skip(self))]
    async fn create_channel(
        &self,
        social_id: &SocialIdId,
        space_id: &str,
        title: &str,
    ) -> Result<String> {
        let channel_id = ksuid::Ksuid::generate().to_base62();
        let now = chrono::Utc::now();
        let create_channel = CreateDocumentBuilder::default()
            .object_id(&channel_id)
            .object_class("chat:masterTag:Channel")
            .created_by(social_id)
            .created_on(now)
            .modified_by(social_id)
            .modified_on(now)
            .object_space(space_id)
            .attributes(serde_json::json!({
                "title": title,
                "private": true,
            }))
            .build()?;

        self.tx::<Value, _>(create_channel).await?;

        trace!(channel_id);

        Ok(channel_id)
    }

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
