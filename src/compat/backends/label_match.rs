//! Label matchers shared by Loki (and formerly Prometheus) backends.

use crate::compat::errors::{CompatError, CompatErrorCode};
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherOp {
    Eq,
    Ne,
    Re,
    Nre,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatcherOp,
    pub value: String,
}

/// Apply AND matchers against a projected label map.
pub fn labels_match(
    labels: &BTreeMap<String, String>,
    matchers: &[LabelMatcher],
) -> Result<bool, CompatError> {
    for m in matchers {
        let actual = labels.get(&m.name).map(String::as_str).unwrap_or("");
        let ok = match m.op {
            MatcherOp::Eq => actual == m.value,
            MatcherOp::Ne => actual != m.value,
            MatcherOp::Re => regex_full_match(&m.value, actual)?,
            MatcherOp::Nre => !regex_full_match(&m.value, actual)?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// OR across selector groups; empty groups means no filter (match all).
pub fn labels_match_any(
    labels: &BTreeMap<String, String>,
    selector_groups: &[Vec<LabelMatcher>],
) -> Result<bool, CompatError> {
    if selector_groups.is_empty() {
        return Ok(true);
    }
    for group in selector_groups {
        if labels_match(labels, group)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn matcher_regex(pattern: &str) -> Result<Regex, CompatError> {
    static CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache.lock().expect("matcher regex cache");
        if let Some(re) = guard.get(pattern) {
            return Ok(re.clone());
        }
    }
    let anchored = format!("^(?:{pattern})$");
    let re = Regex::new(&anchored).map_err(|e| {
        CompatError::new(
            CompatErrorCode::BadRequest,
            format!("invalid matcher regex '{pattern}': {e}"),
        )
    })?;
    let mut guard = cache.lock().expect("matcher regex cache");
    if guard.len() >= 1024 {
        guard.clear();
    }
    guard.insert(pattern.to_string(), re.clone());
    Ok(re)
}

fn regex_full_match(pattern: &str, value: &str) -> Result<bool, CompatError> {
    Ok(matcher_regex(pattern)?.is_match(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn labels_match_eq_and_ne() {
        let mut labels = BTreeMap::new();
        labels.insert("job".into(), "api".into());
        assert!(labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Eq,
                value: "api".into(),
            }]
        )
        .unwrap());
        assert!(!labels_match(
            &labels,
            &[LabelMatcher {
                name: "job".into(),
                op: MatcherOp::Ne,
                value: "api".into(),
            }]
        )
        .unwrap());
    }
}
