//! Convert OTel SDK ResourceMetrics into domain [`Metric`] rows for DuckLake.

use crate::models::Metric;
use chrono::Utc;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{
    Gauge as SdkGauge, Histogram as SdkHistogram, ResourceMetrics, Sum as SdkSum,
};
use std::collections::HashMap;

fn kv_map(kvs: &[KeyValue]) -> HashMap<String, String> {
    kvs.iter()
        .map(|kv| (kv.key.as_str().to_string(), kv.value.to_string()))
        .collect()
}

fn resource_map(resource: &opentelemetry_sdk::Resource) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (k, v) in resource.iter() {
        m.insert(k.as_str().to_string(), v.to_string());
    }
    if !m.contains_key("service.name") {
        m.insert("service.name".to_string(), "thelake".to_string());
    }
    m
}

fn prom_base_name(otel_name: &str, unit: &str) -> String {
    let mut n = otel_name.replace('.', "_");
    if (unit == "ms" || unit == "milliseconds")
        && !n.contains("milliseconds")
        && !n.ends_with("_ms")
    {
        n.push_str("_milliseconds");
    }
    n
}

fn sum_metric(
    name: String,
    value: f64,
    attrs: HashMap<String, String>,
    resource: &HashMap<String, String>,
    monotonic: bool,
) -> Metric {
    Metric {
        metric_name: name,
        description: String::new(),
        unit: "1".to_string(),
        metric_type: "sum".to_string(),
        timestamp: Utc::now(),
        value,
        attributes: attrs,
        resource_attributes: resource.clone(),
        count: None,
        sum: Some(value),
        bucket_counts: None,
        explicit_bounds: None,
        quantiles: None,
        aggregation_temporality: Some("CUMULATIVE".to_string()),
        is_monotonic: Some(monotonic),
        exemplars_json: None,
    }
}

fn gauge_metric(
    name: String,
    value: f64,
    attrs: HashMap<String, String>,
    resource: &HashMap<String, String>,
) -> Metric {
    Metric {
        metric_name: name,
        description: String::new(),
        unit: "1".to_string(),
        metric_type: "gauge".to_string(),
        timestamp: Utc::now(),
        value,
        attributes: attrs,
        resource_attributes: resource.clone(),
        count: None,
        sum: None,
        bucket_counts: None,
        explicit_bounds: None,
        quantiles: None,
        aggregation_temporality: None,
        is_monotonic: None,
        exemplars_json: None,
    }
}

fn convert_sum_u64(
    name: &str,
    unit: &str,
    sum: &SdkSum<u64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let base = prom_base_name(name, unit);
    let prom = if base.ends_with("_total") {
        base
    } else {
        format!("{base}_total")
    };
    for dp in &sum.data_points {
        out.push(sum_metric(
            prom.clone(),
            dp.value as f64,
            kv_map(&dp.attributes),
            resource,
            sum.is_monotonic,
        ));
    }
}

fn convert_sum_f64(
    name: &str,
    unit: &str,
    sum: &SdkSum<f64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let base = prom_base_name(name, unit);
    let prom = if base.ends_with("_total") {
        base
    } else {
        format!("{base}_total")
    };
    for dp in &sum.data_points {
        out.push(sum_metric(
            prom.clone(),
            dp.value,
            kv_map(&dp.attributes),
            resource,
            sum.is_monotonic,
        ));
    }
}

fn convert_gauge_u64(
    name: &str,
    unit: &str,
    gauge: &SdkGauge<u64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let prom = prom_base_name(name, unit);
    for dp in &gauge.data_points {
        out.push(gauge_metric(
            prom.clone(),
            dp.value as f64,
            kv_map(&dp.attributes),
            resource,
        ));
    }
}

fn convert_gauge_f64(
    name: &str,
    unit: &str,
    gauge: &SdkGauge<f64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let prom = prom_base_name(name, unit);
    for dp in &gauge.data_points {
        out.push(gauge_metric(
            prom.clone(),
            dp.value,
            kv_map(&dp.attributes),
            resource,
        ));
    }
}

fn convert_histogram_f64(
    name: &str,
    unit: &str,
    hist: &SdkHistogram<f64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let base = prom_base_name(name, unit);
    for dp in &hist.data_points {
        let attrs = kv_map(&dp.attributes);
        out.push(sum_metric(
            format!("{base}_sum"),
            dp.sum,
            attrs.clone(),
            resource,
            true,
        ));
        out.push(sum_metric(
            format!("{base}_count"),
            dp.count as f64,
            attrs,
            resource,
            true,
        ));
    }
}

fn convert_histogram_u64(
    name: &str,
    unit: &str,
    hist: &SdkHistogram<u64>,
    resource: &HashMap<String, String>,
    out: &mut Vec<Metric>,
) {
    let base = prom_base_name(name, unit);
    for dp in &hist.data_points {
        let attrs = kv_map(&dp.attributes);
        out.push(sum_metric(
            format!("{base}_sum"),
            dp.sum as f64,
            attrs.clone(),
            resource,
            true,
        ));
        out.push(sum_metric(
            format!("{base}_count"),
            dp.count as f64,
            attrs,
            resource,
            true,
        ));
    }
}

/// Flatten SDK export payload into DuckLake metric rows.
pub fn metrics_from_resource_metrics(rm: &ResourceMetrics) -> Vec<Metric> {
    let resource = resource_map(&rm.resource);
    let mut out = Vec::new();
    for scope in &rm.scope_metrics {
        for metric in &scope.metrics {
            let name = metric.name.as_ref();
            let unit = metric.unit.as_ref();
            let data = metric.data.as_any();
            if let Some(sum) = data.downcast_ref::<SdkSum<u64>>() {
                convert_sum_u64(name, unit, sum, &resource, &mut out);
            } else if let Some(sum) = data.downcast_ref::<SdkSum<f64>>() {
                convert_sum_f64(name, unit, sum, &resource, &mut out);
            } else if let Some(g) = data.downcast_ref::<SdkGauge<u64>>() {
                convert_gauge_u64(name, unit, g, &resource, &mut out);
            } else if let Some(g) = data.downcast_ref::<SdkGauge<f64>>() {
                convert_gauge_f64(name, unit, g, &resource, &mut out);
            } else if let Some(h) = data.downcast_ref::<SdkHistogram<f64>>() {
                convert_histogram_f64(name, unit, h, &resource, &mut out);
            } else if let Some(h) = data.downcast_ref::<SdkHistogram<u64>>() {
                convert_histogram_u64(name, unit, h, &resource, &mut out);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::metrics::data::{DataPoint, Sum as SdkSum};
    use opentelemetry_sdk::metrics::Temporality;

    #[test]
    fn converts_sum_with_attrs_to_prom_total() {
        let sum = SdkSum {
            data_points: vec![DataPoint {
                attributes: vec![KeyValue::new("tenant", "t1")],
                start_time: None,
                time: None,
                value: 3u64,
                exemplars: vec![],
            }],
            temporality: Temporality::Cumulative,
            is_monotonic: true,
        };
        let mut out = Vec::new();
        let resource = HashMap::from([("service.name".into(), "thelake".into())]);
        convert_sum_u64("thelake.ingest.requests", "1", &sum, &resource, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, "thelake_ingest_requests_total");
        assert_eq!(
            out[0].attributes.get("tenant").map(String::as_str),
            Some("t1")
        );
        assert_eq!(out[0].value, 3.0);
    }

    #[test]
    fn duration_histogram_becomes_sum_and_count() {
        use opentelemetry_sdk::metrics::data::HistogramDataPoint;
        use std::time::SystemTime;
        let hist = SdkHistogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time: SystemTime::now(),
                time: SystemTime::now(),
                count: 2,
                bounds: vec![],
                bucket_counts: vec![],
                min: Some(1.0),
                max: Some(5.0),
                sum: 6.0,
                exemplars: vec![],
            }],
            temporality: Temporality::Cumulative,
        };
        let mut out = Vec::new();
        let resource = HashMap::new();
        convert_histogram_f64("thelake.ingest.duration", "ms", &hist, &resource, &mut out);
        let names: Vec<_> = out.iter().map(|m| m.metric_name.as_str()).collect();
        assert!(names.contains(&"thelake_ingest_duration_milliseconds_sum"));
        assert!(names.contains(&"thelake_ingest_duration_milliseconds_count"));
    }
}
