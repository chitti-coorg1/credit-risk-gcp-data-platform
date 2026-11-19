CREATE OR REPLACE EXTERNAL TABLE `chitti-data-dev-1795012096.silver.ext_macro`
OPTIONS (format = 'CSV', uris = ['gs://chitti-data-dev-1795012096-bronze/macro*.csv'], skip_leading_rows = 1);

CREATE OR REPLACE TABLE `chitti-data-dev-1795012096.silver.macro_indicators`
PARTITION BY as_of_date AS
SELECT
  SAFE_CAST(TRIM(as_of_date) AS DATE) AS as_of_date,
  SAFE_CAST(TRIM(gdp_growth_pct) AS NUMERIC) AS gdp_growth_pct,
  SAFE_CAST(TRIM(unemployment_rate_pct) AS NUMERIC) AS unemployment_rate_pct,
  SAFE_CAST(TRIM(repo_rate_pct) AS NUMERIC) AS repo_rate_pct,
  SAFE_CAST(TRIM(inflation_pct) AS NUMERIC) AS inflation_pct,
  _FILE_NAME AS _bronze_source_file,
  CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM `chitti-data-dev-1795012096.silver.ext_macro`
WHERE TRIM(as_of_date) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(as_of_date) ORDER BY _FILE_NAME DESC) = 1;
