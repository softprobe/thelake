use crate::models::{Score, ScoreConfig};
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{ScoreConfigTable, ScoreTable};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::attach::ducklake_qualified_table_name;
use super::DuckLakeWriter;

fn score_config_from_sql_row(row: &duckdb::Row<'_>) -> Result<Option<ScoreConfig>> {
    let config_id: String = row.get(0)?;
    let timestamp_raw: String = row.get(1)?;
    let name: String = row.get(2)?;
    let data_type_raw: String = row.get(3)?;
    let description: Option<String> = row.get(4)?;
    let min_value: Option<f64> = row.get(5)?;
    let max_value: Option<f64> = row.get(6)?;
    let categories_raw: Option<String> = row.get(7)?;
    let author_id: Option<String> = row.get(8)?;
    let metadata_raw: Option<String> = row.get(9)?;
    let workspace_id: Option<String> = row.get(10)?;
    let data_type = match data_type_raw.as_str() {
        "numeric" => crate::models::ScoreDataType::Numeric,
        "categorical" => crate::models::ScoreDataType::Categorical,
        "boolean" => crate::models::ScoreDataType::Boolean,
        "text" => crate::models::ScoreDataType::Text,
        _ => return Ok(None),
    };
    let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_raw)
        .or_else(|_| chrono::DateTime::parse_from_str(&timestamp_raw, "%Y-%m-%dT%H:%M:%S%.fZ"))
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&timestamp_raw, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| chrono::DateTime::from_naive_utc_and_offset(value, chrono::Utc))
        })
        .unwrap_or_else(|_| chrono::Utc::now());
    let categories = categories_raw
        .as_deref()
        .filter(|raw| !raw.is_empty())
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let metadata = metadata_raw
        .as_deref()
        .filter(|raw| !raw.is_empty() && *raw != "null")
        .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(raw).ok())
        .unwrap_or_default();
    Ok(Some(ScoreConfig {
        config_id,
        timestamp,
        name,
        data_type,
        description,
        min_value,
        max_value,
        categories,
        author_id,
        metadata,
        workspace_id,
    }))
}

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

    pub async fn score_exists(&self, score_id: &str, timestamp: DateTime<Utc>) -> Result<bool> {
        let table = ducklake_qualified_table_name(self.physical_scope(), ScoreTable::table_name());
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let score_id = score_id.to_string();
        let workspace_id = self.shared_workspace_id()?.map(str::to_owned);
        let window = crate::sql::QueryWindow::try_new(timestamp, timestamp)
            .map_err(|error| anyhow!(error))?;
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                let sql = workspace_id.as_deref().map_or_else(
                    || crate::sql::writer::score_exists_sql(&table, &window),
                    |workspace_id| {
                        crate::sql::writer::score_exists_sql_for_workspace(
                            &table,
                            workspace_id,
                            &window,
                        )
                    },
                );
                // The only bind is score_id (VARCHAR). Plan the identical
                // scan shape with a harmless literal because DuckDB EXPLAIN
                // cannot accept unresolved prepared-statement parameters.
                let plan_sql = sql.replace('?', "'__score_id__'");
                crate::sql::ensure_fact_scan_uses_timestamp_pruning(conn, &plan_sql)
                    .map_err(|e| anyhow!("SQL gate: {e}"))?;
                match conn.query_row(&sql, [&score_id], |row| row.get::<_, bool>(0)) {
                    Ok(exists) => Ok(exists),
                    Err(error) if error.to_string().contains("does not exist") => Ok(false),
                    Err(error) => Err(error.into()),
                }
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score lookup task failed: {error}"))?
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

    pub async fn score_config_exists(&self, config_id: &str) -> Result<bool> {
        let table =
            ducklake_qualified_table_name(self.physical_scope(), ScoreConfigTable::table_name());
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let config_id = config_id.to_string();
        let workspace_id = self.shared_workspace_id()?.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                let sql = workspace_id.as_deref().map_or_else(
                    || crate::sql::writer::score_config_exists_sql(&table),
                    |workspace_id| {
                        crate::sql::writer::score_config_exists_sql_for_workspace(
                            &table,
                            workspace_id,
                        )
                    },
                );
                match conn.query_row(&sql, [&config_id], |row| row.get::<_, bool>(0)) {
                    Ok(exists) => Ok(exists),
                    Err(error) if error.to_string().contains("does not exist") => Ok(false),
                    Err(error) => Err(error.into()),
                }
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config lookup task failed: {error}"))?
    }

    pub async fn list_score_configs(&self) -> Result<Vec<ScoreConfig>> {
        let table =
            ducklake_qualified_table_name(self.physical_scope(), ScoreConfigTable::table_name());
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let workspace_id = self.shared_workspace_id()?.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                let sql = workspace_id.as_deref().map_or_else(
                    || crate::sql::writer::score_config_select_sql(&table),
                    |workspace_id| {
                        crate::sql::writer::score_config_select_sql_for_workspace(
                            &table,
                            workspace_id,
                        )
                    },
                );
                let mut stmt = match conn.prepare(&sql) {
                    Ok(stmt) => stmt,
                    Err(error) if error.to_string().contains("does not exist") => {
                        return Ok(Vec::new())
                    }
                    Err(error) => return Err(error.into()),
                };
                let mut rows = stmt.query([])?;
                let mut configs = Vec::new();
                while let Some(row) = rows.next()? {
                    if let Some(config) = score_config_from_sql_row(row)? {
                        configs.push(config);
                    }
                }
                Ok(configs)
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config list task failed: {error}"))?
    }

    pub async fn get_score_config(&self, config_id: &str) -> Result<Option<ScoreConfig>> {
        let table =
            ducklake_qualified_table_name(self.physical_scope(), ScoreConfigTable::table_name());
        let pool = self.get_or_create_pool(self.physical_scope())?;
        let config_id = config_id.to_string();
        let workspace_id = self.shared_workspace_id()?.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                let sql = workspace_id.as_deref().map_or_else(
                    || crate::sql::writer::score_config_by_id_sql(&table),
                    |workspace_id| {
                        crate::sql::writer::score_config_by_id_sql_for_workspace(
                            &table,
                            workspace_id,
                        )
                    },
                );
                let mut stmt = match conn.prepare(&sql) {
                    Ok(stmt) => stmt,
                    Err(error) if error.to_string().contains("does not exist") => return Ok(None),
                    Err(error) => return Err(error.into()),
                };
                let mut rows = stmt.query([&config_id])?;
                if let Some(row) = rows.next()? {
                    return score_config_from_sql_row(row);
                }
                Ok(None)
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config get task failed: {error}"))?
    }
}
