//! Shared label-name sanitization (Prometheus-compatible rules).
//! Used by Loki projection and stream-label handling.

/// Sanitize a key to Prometheus label name rules.
pub fn sanitize_label_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 1);
    for (i, ch) in raw.chars().enumerate() {
        let ok = ch.is_ascii_alphanumeric() || ch == '_';
        if ok {
            if i == 0 && ch.is_ascii_digit() {
                out.push('_');
            }
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".into()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_dots_and_leading_digits() {
        assert_eq!(sanitize_label_name("http.method"), "http_method");
        assert_eq!(sanitize_label_name("9bad"), "_9bad");
    }
}
