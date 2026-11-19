# Credit Risk GCP Data Platform

Industry-standard Medallion Architecture (Bronze -> Silver -> Gold) on Google Cloud Platform using GCS, BigQuery, Cloud Composer (Airflow), and Cloud Build.

## Folder Structure
- `dags/`: Airflow DAG definitions and Python transformation modules
- `sql/ddl/`: BigQuery dataset & metadata control/audit table DDLs
- `sql/silver/`: Modular Bronze-to-Silver SQL transformation scripts
- `config/`: Environment-specific YAML configurations (`dev`, `prod`)
- `tests/`: Automated pytest DAG integrity & syntax checks
- `cloudbuild.yaml` & `.github/workflows/`: Automated CI/CD deployment pipelines
