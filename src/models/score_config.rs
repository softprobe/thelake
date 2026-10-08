use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Score, ScoreDataType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreConfig {
    pub config_id: String,
    pub timestamp: DateTime<Utc>,
    pub name: String,
    pub data_type: ScoreDataType,
    pub description: Option<String>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    #[serde(default)]
    pub categories: Vec<String>,
    pub author_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// Authenticated workspace ownership, stamped by `IngestEngine`.
    #[serde(default, skip_serializing)]
    pub workspace_id: Option<String>,
}

impl ScoreConfig {
    pub fn from_json_row(row: &[serde_json::Value]) -> Option<Self> {
        let config_id = row.first()?.as_str()?.to_string();
        let timestamp_raw = row.get(1)?.as_str()?;
        let name = row.get(2)?.as_str()?.to_string();
        let data_type_raw = row.get(3)?.as_str()?;
        let data_type = match data_type_raw {
            "numeric" => ScoreDataType::Numeric,
            "categorical" => ScoreDataType::Categorical,
            "boolean" => ScoreDataType::Boolean,
            "text" => ScoreDataType::Text,
            _ => return None,
        };
        let description = row.get(4).and_then(|v| v.as_str()).map(str::to_owned);
        let min_value = row.get(5).and_then(|v| v.as_f64());
        let max_value = row.get(6).and_then(|v| v.as_f64());
        let categories = row
            .get(7)
            .and_then(|v| v.as_str())
            .filter(|raw| !raw.is_empty())
            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            .unwrap_or_default();
        let author_id = row.get(8).and_then(|v| v.as_str()).map(str::to_owned);
        let metadata = row
            .get(9)
            .and_then(|v| v.as_str())
            .filter(|raw| !raw.is_empty() && *raw != "null")
            .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(raw).ok())
            .unwrap_or_default();
        let workspace_id = row.get(10).and_then(|v| v.as_str()).map(str::to_owned);
        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_raw)
            .or_else(|_| chrono::DateTime::parse_from_str(timestamp_raw, "%Y-%m-%dT%H:%M:%S%.fZ"))
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(timestamp_raw, "%Y-%m-%d %H:%M:%S%.f")
                    .map(|value| chrono::DateTime::from_naive_utc_and_offset(value, chrono::Utc))
            })
            .unwrap_or_else(|_| chrono::Utc::now());
        Some(Self {
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
        })
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.config_id.trim().is_empty() {
            return Err("config_id cannot be empty");
        }
        if self.name.trim().is_empty() {
            return Err("name cannot be empty");
        }
        if matches!(self.data_type, ScoreDataType::Categorical) && self.categories.is_empty() {
            return Err("categorical configs require at least one category");
        }
        if let (Some(min), Some(max)) = (self.min_value, self.max_value) {
            if min > max {
                return Err("min_value must be <= max_value");
            }
        }
        Ok(())
    }

    /// Hard-validate a score against this config when `config_id` is set on the score.
    pub fn validate_score(&self, score: &Score) -> Result<(), &'static str> {
        if score.name != self.name {
            return Err("score name does not match config");
        }
        if score.data_type != self.data_type {
            return Err("score data_type does not match config");
        }
        if let Some(value) = score.numeric_value {
            if let Some(min) = self.min_value {
                if value < min {
                    return Err("numeric score below config min_value");
                }
            }
            if let Some(max) = self.max_value {
                if value > max {
                    return Err("numeric score above config max_value");
                }
            }
        }
        if matches!(self.data_type, ScoreDataType::Categorical) {
            let Some(value) = score.string_value.as_deref() else {
                return Err("categorical score requires string_value");
            };
            if !self.categories.iter().any(|c| c == value) {
                return Err("categorical score value not in config categories");
            }
        }
        Ok(())
    }

    pub fn seed_defaults(now: DateTime<Utc>) -> Vec<Self> {
        vec![
            Self {
                config_id: "cfg-correctness".to_string(),
                timestamp: now,
                name: "correctness".to_string(),
                data_type: ScoreDataType::Boolean,
                description: Some("Whether the turn was correct".to_string()),
                min_value: None,
                max_value: None,
                categories: vec![],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                workspace_id: None,
            },
            Self {
                config_id: "cfg-quality".to_string(),
                timestamp: now,
                name: "quality".to_string(),
                data_type: ScoreDataType::Categorical,
                description: Some("Coarse quality label".to_string()),
                min_value: None,
                max_value: None,
                categories: vec!["good".to_string(), "ok".to_string(), "bad".to_string()],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                workspace_id: None,
            },
            Self {
                config_id: "cfg-expected-output".to_string(),
                timestamp: now,
                name: "expected_output".to_string(),
                data_type: ScoreDataType::Text,
                description: Some("Corrected assistant text for eval gold".to_string()),
                min_value: None,
                max_value: None,
                categories: vec![],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                workspace_id: None,
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ScoreSource;

    fn boolean_config() -> ScoreConfig {
        let timestamp = Utc::now();
        ScoreConfig {
            config_id: "cfg-1".to_string(),
            timestamp,
            name: "correctness".to_string(),
            data_type: ScoreDataType::Boolean,
            description: None,
            min_value: None,
            max_value: None,
            categories: vec![],
            author_id: None,
            metadata: HashMap::new(),
            workspace_id: None,
        }
    }

    #[test]
    fn rejects_empty_categorical_categories() {
        let mut config = boolean_config();
        config.data_type = ScoreDataType::Categorical;
        assert_eq!(
            config.validate(),
            Err("categorical configs require at least one category")
        );
    }

    #[test]
    fn validates_score_name_and_type() {
        let config = boolean_config();
        let timestamp = Utc::now();
        let score = Score {
            score_id: "s1".to_string(),
            timestamp,
            trace_id: Some("t".to_string()),
            span_id: None,
            session_id: None,
            name: "correctness".to_string(),
            data_type: ScoreDataType::Boolean,
            numeric_value: None,
            string_value: None,
            boolean_value: Some(true),
            source: ScoreSource::Annotation,
            comment: None,
            config_id: Some("cfg-1".to_string()),
            author_id: None,
            metadata: HashMap::new(),
            workspace_id: None,
        };
        assert_eq!(config.validate_score(&score), Ok(()));
    }
}
