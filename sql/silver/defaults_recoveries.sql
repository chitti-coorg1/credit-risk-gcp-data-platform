CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_defaults`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/default*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.defaults_recoveries`
PARTITION BY default_date
CLUSTER BY loan_id AS
SELECT
  TRIM(loan_id) AS loan_id,
  SAFE_CAST(TRIM(default_date) AS DATE) AS default_date,
  SAFE_CAST(TRIM(write_off_amount) AS NUMERIC) AS write_off_amount,
  SAFE_CAST(TRIM(recovery_amount) AS NUMERIC) AS recovery_amount,
  SAFE_CAST(NULLIF(TRIM(recovery_date), '') AS DATE) AS recovery_date,
  (SAFE_CAST(TRIM(write_off_amount) AS NUMERIC) - COALESCE(SAFE_CAST(TRIM(recovery_amount) AS NUMERIC), 0)) AS net_loss_amount,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_defaults`
WHERE TRIM(loan_id) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id), TRIM(default_date) ORDER BY _FILE_NAME DESC) = 1;
