CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_repayments`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/repayment*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.repayments`
PARTITION BY due_date
CLUSTER BY loan_id AS
SELECT
  TRIM(loan_id) AS loan_id,
  SAFE_CAST(TRIM(due_date) AS DATE) AS due_date,
  SAFE_CAST(NULLIF(TRIM(paid_date), '') AS DATE) AS paid_date,
  SAFE_CAST(TRIM(dpd) AS INT64) AS dpd,
  SAFE_CAST(TRIM(amount_paid) AS NUMERIC) AS amount_paid,
  CASE
    WHEN SAFE_CAST(TRIM(dpd) AS INT64) = 0 THEN 'Current'
    WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 1 AND 30 THEN 'SMA-0 (1-30 DPD)'
    WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 31 AND 60 THEN 'SMA-1 (31-60 DPD)'
    WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 61 AND 90 THEN 'SMA-2 (61-90 DPD)'
    ELSE 'NPA (>90 DPD)'
  END AS delinquency_bucket,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_repayments`
WHERE TRIM(loan_id) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id), TRIM(due_date) ORDER BY _FILE_NAME DESC) = 1;
