use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDataType {
    Numeric,
    Categorical,
    Boolean,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreSource {
    Api,
    User,
    Evaluator,
    Annotation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Score {
    pub score_id: String,
    pub timestamp: DateTime<Utc>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub session_id: Option<String>,
    pub name: String,
    pub data_type: ScoreDataType,
    pub numeric_value: Option<f64>,
    pub string_value: Option<String>,
    pub boolean_value: Option<bool>,
    pub source: ScoreSource,
    pub comment: Option<String>,
    pub config_id: Option<String>,
    pub author_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Score {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.score_id.trim().is_empty() {
            return Err("score_id cannot be empty");
        }
        if self.name.trim().is_empty() {
            return Err("name cannot be empty");
        }
        if self.trace_id.is_none() && self.span_id.is_none() && self.session_id.is_none() {
            return Err("at least one of trace_id, span_id, or session_id is required");
        }
        // D8: sort leads with session_id; require session_id or trace_id so the
        // lead is not a nullable void for layout locality.
        let session_empty = self
            .session_id
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);
        let trace_empty = self
            .trace_id
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);
        if session_empty && trace_empty {
            return Err("session_id or trace_id is required for score layout sort");
        }

        let value_count = usize::from(self.numeric_value.is_some())
            + usize::from(self.string_value.is_some())
            + usize::from(self.boolean_value.is_some());
        if value_count != 1 {
            return Err("exactly one score value is required");
        }

        let value_matches_type = match self.data_type {
            ScoreDataType::Numeric => self.numeric_value.is_some(),
            ScoreDataType::Categorical | ScoreDataType::Text => self.string_value.is_some(),
            ScoreDataType::Boolean => self.boolean_value.is_some(),
        };
        if !value_matches_type {
            return Err("score value does not match data_type");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_score() -> Score {
        let timestamp = Utc::now();
        Score {
            score_id: "score-1".to_string(),
            timestamp,
            trace_id: Some("trace-1".to_string()),
            span_id: None,
            session_id: None,
            name: "correctness".to_string(),
            data_type: ScoreDataType::Numeric,
            numeric_value: Some(0.9),
            string_value: None,
            boolean_value: None,
            source: ScoreSource::Evaluator,
            comment: None,
            config_id: None,
            author_id: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn validates_matching_value_and_target() {
        assert_eq!(numeric_score().validate(), Ok(()));
    }

    #[test]
    fn rejects_missing_target() {
        let mut score = numeric_score();
        score.trace_id = None;
        assert_eq!(
            score.validate(),
            Err("at least one of trace_id, span_id, or session_id is required")
        );
    }

    #[test]
    fn rejects_span_only_without_session_or_trace() {
        let mut score = numeric_score();
        score.trace_id = None;
        score.session_id = None;
        score.span_id = Some("span-1".to_string());
        assert_eq!(
            score.validate(),
            Err("session_id or trace_id is required for score layout sort")
        );
    }

    #[test]
    fn rejects_value_that_does_not_match_type() {
        let mut score = numeric_score();
        score.numeric_value = None;
        score.string_value = Some("good".to_string());
        assert_eq!(
            score.validate(),
            Err("score value does not match data_type")
        );
    }
}
