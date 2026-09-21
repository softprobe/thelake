//! Loki log scan SQL.

pub fn scan_sql(window: &str, promoted: &str, cap: usize) -> String {
    format!(
        "SELECT CAST(epoch_ns(timestamp) AS BIGINT) AS timestamp_ns, body, \
         CAST(attributes AS JSON) AS attributes, \
         CAST(resource_attributes AS JSON) AS resource_attributes, \
         {promoted} \
         FROM logs WHERE 1=1{window} ORDER BY timestamp ASC LIMIT {}",
        cap.saturating_add(1)
    )
}
