CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_collateral`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/collateral*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.collateral`
CLUSTER BY loan_id AS
SELECT
  TRIM(loan_id) AS loan_id,
  TRIM(collateral_type) AS collateral_type,
  SAFE_CAST(TRIM(collateral_value) AS NUMERIC) AS collateral_value,
  SAFE_CAST(TRIM(ltv_pct) AS NUMERIC) AS ltv_pct,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_collateral`
WHERE TRIM(loan_id) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id) ORDER BY _FILE_NAME DESC) = 1;
