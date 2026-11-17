CREATE OR REPLACE SECRET gcs_hmac (
    TYPE GCS,
    KEY_ID '{{key_id}}',
    SECRET '{{secret}}'
);
