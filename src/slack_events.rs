//! Durable idempotency receipts for Slack Events API callbacks.

use anyhow::Result;
use deadpool_postgres::Pool;

const CLAIM_TTL_SECONDS: i64 = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlackEventClaim {
    Claimed(String),
    InProgress,
    Complete,
}

#[derive(Clone)]
pub struct PostgresSlackEventStore {
    pool: Pool,
    table: String,
}

impl PostgresSlackEventStore {
    pub(crate) fn new(pool: Pool, registry_schema: &str) -> Self {
        let table = format!(
            "{}.thelake_slack_event",
            crate::workspace::quote_pg_ident(registry_schema)
        );
        Self { pool, table }
    }

    pub(crate) fn from_resolver(resolver: &crate::workspace::DuckLakeScopeResolver) -> Self {
        Self::new(resolver.pool().clone(), resolver.registry_schema())
    }

    pub async fn claim(&self, team_id: &str, event_id: &str) -> Result<SlackEventClaim> {
        let client = self.pool.get().await?;
        let claim_id = uuid::Uuid::new_v4().to_string();
        let table = &self.table;
        let prune_sql = include_str!("sql/slack/prune_events.sql").replace("__TABLE__", table);
        client.execute(&prune_sql, &[]).await?;
        let claim_sql = include_str!("sql/slack/claim_event.sql").replace("__TABLE__", table);
        let claimed = client
            .query_opt(
                &claim_sql,
                &[&team_id, &event_id, &claim_id, &CLAIM_TTL_SECONDS],
            )
            .await?;
        if claimed.is_some() {
            return Ok(SlackEventClaim::Claimed(claim_id));
        }
        let read_sql = include_str!("sql/slack/read_event_state.sql").replace("__TABLE__", table);
        let state = client
            .query_opt(&read_sql, &[&team_id, &event_id])
            .await?
            .map(|row| row.get::<_, String>(0));
        Ok(match state.as_deref() {
            Some("complete") => SlackEventClaim::Complete,
            _ => SlackEventClaim::InProgress,
        })
    }

    pub async fn complete(&self, team_id: &str, event_id: &str, claim_id: &str) -> Result<()> {
        let client = self.pool.get().await?;
        let sql = include_str!("sql/slack/complete_event.sql").replace("__TABLE__", &self.table);
        let updated = client
            .execute(&sql, &[&team_id, &event_id, &claim_id])
            .await?;
        if updated != 1 {
            anyhow::bail!("Slack event claim is no longer current");
        }
        Ok(())
    }

    pub async fn release(&self, team_id: &str, event_id: &str, claim_id: &str) -> Result<()> {
        let client = self.pool.get().await?;
        let sql = include_str!("sql/slack/release_event.sql").replace("__TABLE__", &self.table);
        client
            .execute(&sql, &[&team_id, &event_id, &claim_id])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn event_receipt_claim_is_atomic_releasable_and_durable() {
        let (_router, state, _temp_dir) = crate::test_support::local_router_and_state()
            .await
            .expect("router");
        let store = state.workspaces.slack_event_store();
        let event_id = uuid::Uuid::new_v4().to_string();
        let (first, second) = tokio::join!(
            store.claim("slack-test-team", &event_id),
            store.claim("slack-test-team", &event_id)
        );
        let first = first.expect("first claim");
        let second = second.expect("second claim");
        let claim_id = match (first, second) {
            (SlackEventClaim::Claimed(claim_id), SlackEventClaim::InProgress)
            | (SlackEventClaim::InProgress, SlackEventClaim::Claimed(claim_id)) => claim_id,
            claims => panic!("expected exactly one active claim, received {claims:?}"),
        };

        store
            .release("slack-test-team", &event_id, &claim_id)
            .await
            .expect("release claim");
        let stale_claim_id = claim_id;
        let claim_id = match store
            .claim("slack-test-team", &event_id)
            .await
            .expect("retry claim")
        {
            SlackEventClaim::Claimed(claim_id) => claim_id,
            result => panic!("expected claim after release, received {result:?}"),
        };
        assert!(store
            .complete("slack-test-team", &event_id, &stale_claim_id)
            .await
            .is_err());
        store
            .complete("slack-test-team", &event_id, &claim_id)
            .await
            .expect("complete claim");
        assert_eq!(
            store
                .claim("slack-test-team", &event_id)
                .await
                .expect("completed retry"),
            SlackEventClaim::Complete
        );
    }
}
