CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_customers`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/customer*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.customers`
CLUSTER BY customer_id, region AS
SELECT
  TRIM(customer_id) AS customer_id,
  TRIM(customer_name) AS customer_name,
  INITCAP(TRIM(segment)) AS segment,
  SAFE_CAST(TRIM(annual_income_inr) AS NUMERIC) AS annual_income_inr,
  INITCAP(TRIM(employment_status)) AS employment_status,
  INITCAP(TRIM(region)) AS region,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_customers`
WHERE TRIM(customer_id) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _FILE_NAME DESC) = 1;
