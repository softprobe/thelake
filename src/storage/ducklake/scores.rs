use crate::models::{Score, ScoreConfig};
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{ScoreConfigTable, ScoreTable};
use anyhow::{anyhow, Result};

use super::DuckLakeWriter;

impl DuckLakeWriter {
    fn validate_shared_score_ownership(&self, scores: &[Score]) -> Result<()> {
        for score in scores {
            self.validate_shared_ownership(score.workspace_id.as_deref(), "score")?;
        }
        Ok(())
    }

    fn validate_shared_score_config_ownership(&self, configs: &[ScoreConfig]) -> Result<()> {
        for config in configs {
            self.validate_shared_ownership(config.workspace_id.as_deref(), "score config")?;
        }
        Ok(())
    }

    pub(crate) async fn write_score_batches(&self, batches: Vec<Vec<Score>>) -> Result<()> {
        let scores: Vec<Score> = batches.into_iter().flatten().collect();
        if scores.is_empty() {
            return Ok(());
        }
        self.validate_shared_score_ownership(&scores)?;
        for score in &scores {
            score
                .validate()
                .map_err(|message| anyhow!("invalid score: {message}"))?;
        }

        let min_timestamp = scores
            .iter()
            .map(|score| score.timestamp)
            .min()
            .expect("non-empty scores");
        let max_timestamp = scores
            .iter()
            .map(|score| score.timestamp)
            .max()
            .expect("non-empty scores");
        let dedupe_window =
            crate::sql::QueryWindow::try_new(min_timestamp, max_timestamp).expect("min <= max");
        let schema = ScoreTable::schema();
        let record_batch = arrow::scores_to_record_batch(&scores, &schema)?;
        self.write_record_batches_internal_with_ducklake(
            self.physical_scope(),
            ScoreTable::table_name(),
            vec![record_batch],
            Some(dedupe_window),
        )
        .await
    }

    pub(crate) async fn write_score_config_batches(
        &self,
        batches: Vec<Vec<ScoreConfig>>,
    ) -> Result<()> {
        let configs: Vec<ScoreConfig> = batches.into_iter().flatten().collect();
        if configs.is_empty() {
            return Ok(());
        }
        self.validate_shared_score_config_ownership(&configs)?;
        for config in &configs {
            config
                .validate()
                .map_err(|message| anyhow!("invalid score config: {message}"))?;
        }

        let schema = ScoreConfigTable::schema();
        let record_batch = arrow::score_configs_to_record_batch(&configs, &schema)?;
        self.write_record_batches_internal_with_ducklake(
            self.physical_scope(),
            ScoreConfigTable::table_name(),
            vec![record_batch],
            None,
        )
        .await
    }
}
