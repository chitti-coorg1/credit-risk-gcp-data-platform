import os
from airflow.models import DagBag

def test_no_import_errors():
    dag_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    assert "credit_risk_bronze_to_silver" in dag_bag.dags
