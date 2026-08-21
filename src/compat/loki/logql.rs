use crate::compat::backends::logs::{LogLineFilter, LogParser, LogsQueryRequest};
use crate::compat::backends::metrics::{LabelMatcher, MatcherOp};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::prometheus::sanitize_label_name;

pub fn parse_logql(query: &str) -> Result<LogsQueryRequest, CompatError> {
    let query = query.trim();
    if let Some(function) = leading_function_name(query) {
        return Err(unsupported(format!(
            "LogQL function '{function}' is not supported for stream responses"
        )));
    }
    let (selector, pipeline) = split_selector(query)?;
    let mut request = LogsQueryRequest {
        start_ns: None,
        end_ns: None,
        matchers: parse_selector(selector)?,
        line_filters: Vec::new(),
        parser: None,
        parsed_filters: Vec::new(),
        unwrap: None,
        limit: 1000,
        direction: crate::compat::backends::logs::LogDirection::Backward,
    };
    for stage in split_pipeline(pipeline)? {
        let stage = stage.trim();
        if stage.is_empty() {
            continue;
        }
        match stage {
            "json" => request.parser = Some(LogParser::Json),
            "logfmt" => request.parser = Some(LogParser::Logfmt),
            s if s.starts_with("json ") || s.starts_with("logfmt ") => {
                return Err(unsupported("parser field expressions"));
            }
            s if s.starts_with("unwrap ") => {
                return Err(unsupported("LogQL unwrap"));
            }
            s if s.starts_with('=')
                || s.starts_with("!=")
                || s.starts_with('~')
                || s.starts_with("!~") =>
            {
                let (op, raw) = if let Some(v) = s.strip_prefix("!=") {
                    ("!=", v)
                } else if let Some(v) = s.strip_prefix("!~") {
                    ("!~", v)
                } else if let Some(v) = s.strip_prefix('=') {
                    ("=", v)
                } else {
                    ("~", &s[1..])
                };
                let value = quoted(raw.trim())?;
                match op {
                    "=" => request.line_filters.push(LogLineFilter::Contains(value)),
                    "!=" => request.line_filters.push(LogLineFilter::NotContains(value)),
                    "~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::Regex(value));
                    }
                    "!~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::NotRegex(value));
                    }
                    _ => unreachable!(),
                }
            }
            s if s.contains('=') || s.contains('!') => {
                let (name, op, value) = parse_matcher(s)?;
                if request.parser.is_none() {
                    return Err(unsupported(
                        "parsed-field matchers require json or logfmt parser",
                    ));
                }
                request.parsed_filters.push(LabelMatcher {
                    name: sanitize_label_name(name),
                    op,
                    value,
                });
            }
            s => {
                let (op, raw) = if let Some(v) = s.strip_prefix("=") {
                    ("=", v)
                } else if let Some(v) = s.strip_prefix("!=") {
                    ("!=", v)
                } else if let Some(v) = s.strip_prefix("~") {
                    ("~", v)
                } else if let Some(v) = s.strip_prefix("!~") {
                    ("!~", v)
                } else {
                    return Err(unsupported(format!("LogQL pipeline stage '{s}'")));
                };
                let value = quoted(raw.trim())?;
                match op {
                    "=" => request.line_filters.push(LogLineFilter::Contains(value)),
                    "!=" => request.line_filters.push(LogLineFilter::NotContains(value)),
                    "~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::Regex(value));
                    }
                    "!~" => {
                        regex(&value)?;
                        request.line_filters.push(LogLineFilter::NotRegex(value));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    Ok(request)
}

fn split_selector(input: &str) -> Result<(&str, &str), CompatError> {
    if !input.starts_with('{') {
        return Err(bad("LogQL must start with a stream selector"));
    }
    let mut quote = false;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        if quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = false;
            }
        } else if ch == '"' {
            quote = true;
        } else if ch == '}' {
            return Ok((
                &input[1..i],
                input[i + 1..]
                    .trim_start()
                    .trim_start_matches('|')
                    .trim_start(),
            ));
        }
    }
    Err(bad("unterminated stream selector"))
}

fn leading_function_name(input: &str) -> Option<&str> {
    let open = input.find('(')?;
    let name = input[..open].trim();
    if !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(name)
    } else {
        None
    }
}

fn split_pipeline(input: &str) -> Result<Vec<String>, CompatError> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        if quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = false;
            }
        } else if ch == '"' {
            quote = true;
        } else if ch == '|' {
            out.push(input[start..i].trim().to_string());
            start = i + 1;
        }
    }
    if quote {
        return Err(bad("unterminated pipeline string"));
    }
    out.push(input[start..].trim().to_string());
    Ok(out)
}

pub fn parse_selector(input: &str) -> Result<Vec<LabelMatcher>, CompatError> {
    let input = if input.trim_start().starts_with('{') {
        let (selector, pipeline) = split_selector(input.trim_start())?;
        if !pipeline.trim().is_empty() {
            return Err(bad("selector must not include a pipeline"));
        }
        selector
    } else {
        input
    };
    let mut out = Vec::new();
    for item in split_commas(input)? {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (name, op, value) = parse_matcher(item)?;
        out.push(LabelMatcher {
            name: sanitize_label_name(name),
            op,
            value,
        });
    }
    Ok(out)
}

fn parse_matcher(input: &str) -> Result<(&str, MatcherOp, String), CompatError> {
    for (token, op) in [
        ("=~", MatcherOp::Re),
        ("!~", MatcherOp::Nre),
        ("!=", MatcherOp::Ne),
        ("=", MatcherOp::Eq),
    ] {
        if let Some((name, raw)) = input.split_once(token) {
            let name = name.trim();
            if !valid_name(name) {
                return Err(bad("invalid label matcher name"));
            }
            let value = quoted(raw.trim())?;
            if matches!(op, MatcherOp::Re | MatcherOp::Nre) {
                regex(&value)?;
            }
            return Ok((name, op, value));
        }
    }
    Err(bad("invalid label matcher"))
}

fn split_commas(input: &str) -> Result<Vec<&str>, CompatError> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut escaped = false;
    for (i, ch) in input.char_indices() {
        if quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = false;
            }
        } else if ch == '"' {
            quote = true;
        } else if ch == ',' {
            out.push(&input[start..i]);
            start = i + 1;
        }
    }
    if quote {
        return Err(bad("unterminated matcher string"));
    }
    out.push(&input[start..]);
    Ok(out)
}

fn quoted(raw: &str) -> Result<String, CompatError> {
    serde_json::from_str(raw).map_err(|_| bad("LogQL values must be double-quoted strings"))
}

fn regex(pattern: &str) -> Result<(), CompatError> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map(|_| ())
        .map_err(|e| bad(format!("invalid LogQL regex: {e}")))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn bad(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn unsupported(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::UnsupportedFeature, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_selector_filters_and_json_parser() {
        let request = parse_logql(
            r#"{service.name="api", level=~"warn|error"} |= "failed" | json | status="500""#,
        )
        .unwrap();
        assert_eq!(request.matchers[0].name, "service_name");
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::Contains("failed".into())]
        );
        assert_eq!(request.parser, Some(LogParser::Json));
        assert_eq!(request.parsed_filters[0].name, "status");
    }

    #[test]
    fn rejects_parsed_field_matchers_without_a_parser() {
        let err = parse_logql(r#"{service.name="api"} | status="500""#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        assert_eq!(
            err.message,
            "parsed-field matchers require json or logfmt parser"
        );
    }

    #[test]
    fn rejects_unbounded_or_unknown_functions() {
        let err = parse_logql(r#"rate({job="api"}[5m])"#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        let err = parse_logql(r#"{job="api"} | regexp "x""#).unwrap_err();
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
    }

    #[test]
    fn rejects_unwrap_and_metric_functions_before_backend_execution() {
        for query in [
            r#"{job="api"} | json | unwrap duration_ms"#,
            r#"sum_over_time({job="api"}[5m])"#,
        ] {
            let err = parse_logql(query).unwrap_err();
            assert_eq!(err.code, CompatErrorCode::UnsupportedFeature, "{query}");
        }
    }

    #[test]
    fn parses_negative_regex_line_filters_as_line_filters() {
        let request = parse_logql(r#"{job="api"} !~ "debug""#).unwrap();
        assert_eq!(
            request.line_filters,
            vec![LogLineFilter::NotRegex("debug".into())]
        );
    }
}
