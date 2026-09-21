-- One-clock catalog copy (old → new).
-- Design: docs/design-sql-and-schema.md §1.4
--
-- Prerequisites: ATTACH old catalog as `old` and new empty catalog as `new`.
-- New tables must already have one-clock DDL (no record_date / window_ts;
-- PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp))).
--
-- OTLP facts: drop record_date column on copy.
INSERT INTO new.traces BY NAME
SELECT * EXCLUDE (record_date) FROM old.traces;

INSERT INTO new.logs BY NAME
SELECT * EXCLUDE (record_date) FROM old.logs;

INSERT INTO new.scores BY NAME
SELECT * EXCLUDE (record_date) FROM old.scores;

-- Metrics samples / series / postings / hist: drop record_date.
INSERT INTO new.metric_samples BY NAME
SELECT * EXCLUDE (record_date) FROM old.metric_samples;

INSERT INTO new.metric_hist_samples BY NAME
SELECT * EXCLUDE (record_date) FROM old.metric_hist_samples;

INSERT INTO new.metric_series BY NAME
SELECT series_id, metric_name, metric_type, unit, description,
       aggregation_temporality, is_monotonic, labels,
       CAST(record_date AS TIMESTAMP) AS timestamp
FROM old.metric_series;

INSERT INTO new.metric_postings BY NAME
SELECT label_name, label_value, series_id,
       CAST(record_date AS TIMESTAMP) AS timestamp
FROM old.metric_postings;

-- Downsample / collapse: rename window_ts → timestamp, drop record_date.
INSERT INTO new.metric_samples_5m BY NAME
SELECT series_id, window_ts AS timestamp, count, sum, min, max, last, last_ts
FROM old.metric_samples_5m;

INSERT INTO new.metric_samples_1h BY NAME
SELECT series_id, window_ts AS timestamp, count, sum, min, max, last, last_ts
FROM old.metric_samples_1h;

INSERT INTO new.metric_hist_samples_5m BY NAME
SELECT series_id, window_ts AS timestamp, count, sum, bucket_counts, explicit_bounds, last_ts
FROM old.metric_hist_samples_5m;

INSERT INTO new.metric_hist_samples_1h BY NAME
SELECT series_id, window_ts AS timestamp, count, sum, bucket_counts, explicit_bounds, last_ts
FROM old.metric_hist_samples_1h;

INSERT INTO new.metric_collapse_job_1h BY NAME
SELECT metric_name, job, window_ts AS timestamp, count, sum, min, max, last
FROM old.metric_collapse_job_1h;

-- After verify + EXPLAIN prune: flip config to `new`, DETACH/drop `old`.
