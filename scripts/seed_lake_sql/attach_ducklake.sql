INSTALL ducklake; LOAD ducklake;
INSTALL postgres; LOAD postgres;
SET unsafe_enable_version_guessing = true;
ATTACH 'ducklake:postgres:{{metadata_path}}' AS {{alias}} (
  DATA_PATH '{{data_path}}',
  METADATA_SCHEMA '{{schema}}',
  META_SCHEMA '{{schema}}'
);
