def process_entity(entity_name: str):
    """Loads raw CSV from Bronze GCS into BigQuery Silver table and logs audit metrics."""
    client = bigquery.Client(project=PROJECT_ID, location="asia-south1")
    config = ENTITY_QUERIES[entity_name]
    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)

    audit_sql = f"""
        INSERT INTO `{PROJECT_ID}.metadata.etl_audit_log`
        (run_id, entity_name, file_name, target_table, rows_loaded, run_ts, status, error_message)
        VALUES (@run_id, @entity_name, @file_name, @target_table, @rows_loaded, @run_ts, @status, @error_message)
    """

    try:
        client.query(config["sql"]).result()
        row_count = client.get_table(config["table"]).num_rows

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name),
                bigquery.ScalarQueryParameter("file_name", "STRING", config["uri"]),
                bigquery.ScalarQueryParameter("target_table", "STRING", config["table"]),
                bigquery.ScalarQueryParameter("rows_loaded", "INT64", row_count),
                bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
                bigquery.ScalarQueryParameter("status", "STRING", "SUCCESS"),
                bigquery.ScalarQueryParameter("error_message", "STRING", None),
            ]
        )
        client.query(audit_sql, job_config=job_config).result()

    except Exception as exc:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name),
                bigquery.ScalarQueryParameter("file_name", "STRING", config["uri"]),
                bigquery.ScalarQueryParameter("target_table", "STRING", config["table"]),
                bigquery.ScalarQueryParameter("rows_loaded", "INT64", 0),
                bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
                bigquery.ScalarQueryParameter("status", "STRING", "FAILED"),
                bigquery.ScalarQueryParameter("error_message", "STRING", str(exc)[:1000]),
            ]
        )
        client.query(audit_sql, job_config=job_config).result()
        raise