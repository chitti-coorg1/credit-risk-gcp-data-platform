import os
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT", "chitti-data-dev-1795012096")
BRONZE_BUCKET = f"gs://{PROJECT_ID}-bronze"

ENTITY_QUERIES = {
    "customers": {
        "uri": f"{BRONZE_BUCKET}/customer*.csv",
        "table": f"{PROJECT_ID}.silver.customers",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_customers`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/customer*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.customers`
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
            FROM `{PROJECT_ID}.silver.ext_customers`
            WHERE TRIM(customer_id) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
    "loans": {
        "uri": f"{BRONZE_BUCKET}/loan*.csv",
        "table": f"{PROJECT_ID}.silver.loans",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_loans`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/loan*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.loans`
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
            FROM `{PROJECT_ID}.silver.ext_loans`
            WHERE TRIM(loan_id) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
    "collateral": {
        "uri": f"{BRONZE_BUCKET}/collateral*.csv",
        "table": f"{PROJECT_ID}.silver.collateral",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_collateral`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/collateral*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.collateral`
            CLUSTER BY loan_id AS
            SELECT
              TRIM(loan_id) AS loan_id,
              TRIM(collateral_type) AS collateral_type,
              SAFE_CAST(TRIM(collateral_value) AS NUMERIC) AS collateral_value,
              SAFE_CAST(TRIM(ltv_pct) AS NUMERIC) AS ltv_pct,
              _FILE_NAME AS _bronze_source_file,
              CURRENT_TIMESTAMP() AS _silver_loaded_at
            FROM `{PROJECT_ID}.silver.ext_collateral`
            WHERE TRIM(loan_id) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
    "repayments": {
        "uri": f"{BRONZE_BUCKET}/repayment*.csv",
        "table": f"{PROJECT_ID}.silver.repayments",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_repayments`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/repayment*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.repayments`
            PARTITION BY due_date
            CLUSTER BY loan_id AS
            SELECT
              TRIM(loan_id) AS loan_id,
              SAFE_CAST(TRIM(due_date) AS DATE) AS due_date,
              SAFE_CAST(NULLIF(TRIM(paid_date), "") AS DATE) AS paid_date,
              SAFE_CAST(TRIM(dpd) AS INT64) AS dpd,
              SAFE_CAST(TRIM(amount_paid) AS NUMERIC) AS amount_paid,
              CASE
                WHEN SAFE_CAST(TRIM(dpd) AS INT64) = 0 THEN "Current"
                WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 1 AND 30 THEN "SMA-0 (1-30 DPD)"
                WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 31 AND 60 THEN "SMA-1 (31-60 DPD)"
                WHEN SAFE_CAST(TRIM(dpd) AS INT64) BETWEEN 61 AND 90 THEN "SMA-2 (61-90 DPD)"
                ELSE "NPA (>90 DPD)"
              END AS delinquency_bucket,
              _FILE_NAME AS _bronze_source_file,
              CURRENT_TIMESTAMP() AS _silver_loaded_at
            FROM `{PROJECT_ID}.silver.ext_repayments`
            WHERE TRIM(loan_id) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id), TRIM(due_date) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
    "defaults_recoveries": {
        "uri": f"{BRONZE_BUCKET}/default*.csv",
        "table": f"{PROJECT_ID}.silver.defaults_recoveries",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_defaults`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/default*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.defaults_recoveries`
            PARTITION BY default_date
            CLUSTER BY loan_id AS
            SELECT
              TRIM(loan_id) AS loan_id,
              SAFE_CAST(TRIM(default_date) AS DATE) AS default_date,
              SAFE_CAST(TRIM(write_off_amount) AS NUMERIC) AS write_off_amount,
              SAFE_CAST(TRIM(recovery_amount) AS NUMERIC) AS recovery_amount,
              SAFE_CAST(NULLIF(TRIM(recovery_date), "") AS DATE) AS recovery_date,
              (SAFE_CAST(TRIM(write_off_amount) AS NUMERIC) - COALESCE(SAFE_CAST(TRIM(recovery_amount) AS NUMERIC), 0)) AS net_loss_amount,
              _FILE_NAME AS _bronze_source_file,
              CURRENT_TIMESTAMP() AS _silver_loaded_at
            FROM `{PROJECT_ID}.silver.ext_defaults`
            WHERE TRIM(loan_id) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id), TRIM(default_date) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
    "macro_indicators": {
        "uri": f"{BRONZE_BUCKET}/macro*.csv",
        "table": f"{PROJECT_ID}.silver.macro_indicators",
        "sql": f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.silver.ext_macro`
            OPTIONS (format = "CSV", uris = ["{BRONZE_BUCKET}/macro*.csv"], skip_leading_rows = 1);

            CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.macro_indicators`
            PARTITION BY as_of_date AS
            SELECT
              SAFE_CAST(TRIM(as_of_date) AS DATE) AS as_of_date,
              SAFE_CAST(TRIM(gdp_growth_pct) AS NUMERIC) AS gdp_growth_pct,
              SAFE_CAST(TRIM(unemployment_rate_pct) AS NUMERIC) AS unemployment_rate_pct,
              SAFE_CAST(TRIM(repo_rate_pct) AS NUMERIC) AS repo_rate_pct,
              SAFE_CAST(TRIM(inflation_pct) AS NUMERIC) AS inflation_pct,
              _FILE_NAME AS _bronze_source_file,
              CURRENT_TIMESTAMP() AS _silver_loaded_at
            FROM `{PROJECT_ID}.silver.ext_macro`
            WHERE TRIM(as_of_date) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(as_of_date) ORDER BY _FILE_NAME DESC) = 1;
        """,
    },
}


def write_audit(client, run_id, entity_name, file_name, target_table, rows_loaded, run_ts, status, err_msg=None):
    audit_sql = f"""
        INSERT INTO `{PROJECT_ID}.metadata.etl_audit_log`
        (run_id, entity_name, file_name, target_table, rows_loaded, run_ts, status, error_message)
        VALUES (@run_id, @entity_name, @file_name, @target_table, @rows_loaded, @run_ts, @status, @error_message)
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name),
        bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
        bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
        bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
        bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("error_message", "STRING", err_msg),
    ]
    client.query(audit_sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def process_entity(entity_name: str):
    client = bigquery.Client(project=PROJECT_ID, location="asia-south1")
    config = ENTITY_QUERIES[entity_name]
    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    try:
        client.query(config["sql"]).result()
        row_count = client.get_table(config["table"]).num_rows
        write_audit(client, run_id, entity_name, config["uri"], config["table"], row_count, run_ts, "SUCCESS", None)
    except Exception as exc:
        write_audit(client, run_id, entity_name, config["uri"], config["table"], 0, run_ts, "FAILED", str(exc)[:1000])
        raise
