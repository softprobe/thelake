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

-- After verify + EXPLAIN prune: flip config to `new`, DETACH/drop `old`.
