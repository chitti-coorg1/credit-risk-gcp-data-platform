CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_loans`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/loan*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.loans`
PARTITION BY origination_date
CLUSTER BY loan_id, customer_id AS
SELECT
  TRIM(loan_id) AS loan_id,
  TRIM(customer_id) AS customer_id,
  TRIM(product_type) AS product_type,
  SAFE_CAST(TRIM(origination_date) AS DATE) AS origination_date,
  SAFE_CAST(TRIM(sanctioned_amount) AS NUMERIC) AS sanctioned_amount,
  SAFE_CAST(TRIM(outstanding_balance) AS NUMERIC) AS outstanding_balance,
  SAFE_CAST(TRIM(interest_rate_pct) AS NUMERIC) AS interest_rate_pct,
  SAFE_CAST(TRIM(tenure_months) AS INT64) AS tenure_months,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_loans`
WHERE TRIM(loan_id) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id) ORDER BY _FILE_NAME DESC) = 1;
