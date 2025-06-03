use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json as json;
use tracing::*;
use url::Url;

use hulyrs::services::{
    account::{
        AccountClient, AddSocialIdToPersonParams, Integration, IntegrationKey,
        PartialIntegrationKey, SelectWorkspaceParams, WorkspaceKind,
    },
    jwt::{Claims, ClaimsBuilder},
    types::{AccountUuid, PersonId, SocialIdId, SocialIdType, WorkspaceUuid},
};

const INTEGRATION_KIND: &str = "hulygram";
const SOCIAL_KIND: &str = "telegram";

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountIntegrationData {
    pub phone: String,
}

#[derive(Clone)]
pub struct WorkspaceIntegration {
    pub account: AccountUuid,
    pub workspace: WorkspaceUuid,
    pub social_id: SocialIdId,
    pub transactor_url: Url,
}

pub trait TelegramIntegration {
    async fn list_all_integrations(&self) -> Result<Vec<AccountIntegrationData>>;

    async fn find_account_integrations(&self, claims: &Claims) -> Result<Vec<AccountIntegration>>;

    async fn find_workspace_integrations(&self, user_id: i64) -> Result<Vec<WorkspaceIntegration>>;

    async fn ensure_social_id(&self, claims: &Claims, id: i64) -> Result<PersonId>;

    async fn ensure_account_integration(
        &self,
        social_id: &PersonId,
        data: AccountIntegrationData,
    ) -> Result<()>;

    async fn ensure_workspace_integration(
        &self,
        social_id: &PersonId,
        workspace: WorkspaceUuid,
    ) -> Result<()>;
}

pub struct AccountIntegration {
    pub phone: String,

    #[allow(dead_code)]
    pub social_id: SocialIdId,

    #[allow(dead_code)]
    pub data: AccountIntegrationData,
}

impl TelegramIntegration for AccountClient {
    async fn list_all_integrations(&self) -> Result<Vec<AccountIntegrationData>> {
        let mut key = PartialIntegrationKey::default();
        key.kind = Some(INTEGRATION_KIND.to_owned());

        let ints = self
            .list_integrations(&key)
            .await?
            .into_iter()
            .filter(|i| i.workspace_uuid.is_none());

        let mut result = Vec::new();

        for int in ints {
            if let Some(data) = int.data {
                match json::from_value::<AccountIntegrationData>(data) {
                    Ok(data) => {
                        result.push(data);
                    }
                    Err(error) => {
                        warn!(persion = int.social_id, %error, "Cannot parse integration data");
                    }
                }
            }
        }

        Ok(result)
    }

    // all hulygram integrations for all account social ids's of type hulygram
    async fn find_account_integrations(&self, claims: &Claims) -> Result<Vec<AccountIntegration>> {
        let caller_account = self.assume_claims(claims)?;

        let account = &self.account;

        trace!(%account, "Find account integrations");

        let mut result = Vec::new();

        let social_ids = caller_account.get_social_ids(true).await?;

        for sid in social_ids
            .into_iter()
            .filter(|i| i.base.r#type == SocialIdType::Telegram)
        {
            let key = IntegrationKey {
                social_id: sid.base.id.clone(),
                kind: INTEGRATION_KIND.to_string(),
                workspace_uuid: None,
            };

            match self.get_integration(&key).await {
                Ok(Some(integration)) => {
                    trace!(%account, social_id = %sid.base.id, "Integration found");

                    if let Some(data) = integration
                        .data
                        .map(json::from_value::<AccountIntegrationData>)
                        .transpose()?
                    {
                        result.push(AccountIntegration {
                            phone: data.phone.clone(),
                            social_id: sid.base.id,
                            data,
                        });
                    } else {
                        warn!(%account, social_id = %sid.base.id,"No or invalid integration data");
                    }
                }

                Ok(None) => {}

                Err(error) => {
                    error!(%account, social_id = %sid.base.id, ?error, "Cannot get integration");
                }
            }
        }

        Ok(result)
    }

    // HORROR!!! (4+N requests)
    // finds all workspace integrations for the telegram user id
    async fn find_workspace_integrations(&self, user_id: i64) -> Result<Vec<WorkspaceIntegration>> {
        let key = format!("{}:{}", SOCIAL_KIND, user_id);
        let social_id = self.find_social_id_by_social_key(&key, true).await?;
        let account_id = self.find_person_by_social_key(&key, true).await?;

        trace!(id = %user_id, ?social_id, ?account_id, "Find workspace integrations");

        if let (Some(social_id), Some(account)) = (social_id, account_id) {
            let claims = ClaimsBuilder::default().account(account).build()?;

            let accountc = self.assume_claims(&claims)?;

            let mut ws_indexed = HashMap::new();
            for ws in accountc.get_user_workspaces().await?.into_iter() {
                let ws_login_info = accountc
                    .select_workspace(&SelectWorkspaceParams {
                        workspace_url: ws.url,
                        kind: WorkspaceKind::Internal,
                        external_regions: Vec::default(),
                    })
                    .await?;

                ws_indexed.insert(ws.uuid, ws_login_info.endpoint);
            }

            let integrations = self
                .list_integrations(&PartialIntegrationKey {
                    social_id: Some(social_id.clone()),
                    kind: Some(INTEGRATION_KIND.to_string()),
                    workspace_uuid: None,
                })
                .await
                .unwrap()
                .into_iter()
                .filter(|i| i.workspace_uuid.is_some());

            let mut result = Vec::new();

            for i in integrations {
                let workspace = i.workspace_uuid.unwrap();

                if let Some(transactor_url) = ws_indexed.get(&workspace) {
                    result.push(WorkspaceIntegration {
                        account,
                        workspace,
                        transactor_url: transactor_url.to_owned(),
                        social_id: social_id.clone(),
                    });
                }
            }

            Ok(result)
        } else {
            Ok(Vec::default())
        }
    }

    // finds or creates social id for the account, identified by claims
    async fn ensure_social_id(&self, claims: &Claims, id: i64) -> Result<PersonId> {
        let id = id.to_string();

        let social_id = self
            .assume_claims(claims)?
            .get_social_ids(true)
            .await?
            .iter()
            .find(|i| i.base.r#type == SocialIdType::Telegram && i.base.value == id)
            .map(ToOwned::to_owned);

        let social_id = if let Some(sid) = social_id {
            sid.base.id
        } else {
            let add = AddSocialIdToPersonParams {
                person: claims.account,
                r#type: SocialIdType::Telegram,
                value: id,
                confirmed: true,
            };

            self.add_social_id_to_person(&add).await?
        };

        Ok(social_id)
    }

    // finds or creates integration for the account, identified by claims
    async fn ensure_account_integration(
        &self,
        social_id: &PersonId,
        data: AccountIntegrationData,
    ) -> Result<()> {
        if self
            .get_integration(&IntegrationKey {
                social_id: social_id.to_owned(),
                kind: INTEGRATION_KIND.to_string(),
                workspace_uuid: None,
            })
            .await?
            .is_none()
        {
            // personal integration
            let personal = Integration {
                social_id: social_id.to_owned(),
                kind: INTEGRATION_KIND.to_string(),
                workspace_uuid: None,
                data: Some(json::to_value(data)?),
            };

            self.create_integration(&personal).await?;
        }

        Ok(())
    }

    async fn ensure_workspace_integration(
        &self,
        social_id: &PersonId,
        workspace: WorkspaceUuid,
    ) -> Result<()> {
        if self
            .get_integration(&IntegrationKey {
                social_id: social_id.to_owned(),
                kind: INTEGRATION_KIND.to_string(),
                workspace_uuid: Some(workspace),
            })
            .await?
            .is_none()
        {
            let workspace = Integration {
                social_id: social_id.to_owned(),
                kind: INTEGRATION_KIND.to_string(),
                workspace_uuid: Some(workspace),
                data: None,
            };

            self.create_integration(&workspace).await?;
        }

        Ok(())
    }
}
