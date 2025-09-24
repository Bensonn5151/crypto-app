from airflow import DAG
from airflow.operators.dummy import DummyOperator  # works in 2.8.1
from datetime import datetime

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define DAG
with DAG(
    dag_id="test_dag_281",
    default_args=default_args,
    description="Simple DAG for Airflow 2.8.1 test",
    schedule_interval=None,   # no automatic schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> end
