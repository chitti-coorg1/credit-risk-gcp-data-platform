CREATE SCHEMA IF NOT EXISTS `chitti-data-dev-1795012096.silver` OPTIONS(location = 'asia-south1');
CREATE SCHEMA IF NOT EXISTS `chitti-data-dev-1795012096.metadata` OPTIONS(location = 'asia-south1');

CREATE TABLE IF NOT EXISTS `chitti-data-dev-1795012096.metadata.etl_audit_log` (
  run_id STRING NOT NULL,
  entity_name STRING,
  file_name STRING,
  target_table STRING,
  rows_loaded INT64,
  run_ts TIMESTAMP,
  status STRING,
  error_message STRING
)
PARTITION BY DATE(run_ts);
