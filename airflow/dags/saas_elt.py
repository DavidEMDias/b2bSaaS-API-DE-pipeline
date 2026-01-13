from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.helpers import extract_and_ingest_table
from src.data_utils import create_elt_schemas

DEFAULT_ARGS = {
    'owner': 'data_eng',
    'depends_on_past': False, # Tasks don’t wait for previous DAG run to finish
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="SaaS_ELT",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    #schedule_interval="0 * * * *",
    schedule_interval=None,
    catchup=False,
    tags=["api", "postgres", "dbt", "bi"],
) as dag:


    create_objects = PythonOperator(
        task_id="create_schemas_and_tables",
        python_callable=create_elt_schemas
        )

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_and_ingest_table,
        op_kwargs={
            "table": "raw.customers",
            "endpoint": "customers",
            "ts_field": "updated_at",
            "wm_name": "wm_customers",
        },
    )

    extract_payments = PythonOperator(
        task_id="extract_payments",
        python_callable=extract_and_ingest_table,
        op_kwargs={
            "table": "raw.payments",
            "endpoint": "payments",
            "ts_field": "updated_at",
            "wm_name": "wm_payments",
        },
    )

    extract_sessions = PythonOperator(
        task_id="extract_sessions",
        python_callable=extract_and_ingest_table,
        op_kwargs={
            "table": "raw.sessions",
            "endpoint": "sessions",
            "ts_field": "updated_at",
            "wm_name": "wm_sessions",
        },
    )

# Run all in parallel
create_objects >> [extract_customers, extract_payments, extract_sessions]