import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from custom_functions.helpers import extract_and_ingest_table
from custom_functions.db.db_objects_utils import create_elt_schemas
from custom_functions.upload_to_bucket import extract_and_upload_table

GCS_BUCKET = os.environ["BRONZE_GCS_BUCKET_NAME"]


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

    # extract_customers = PythonOperator(
    #     task_id="extract_customers",
    #     python_callable=extract_and_ingest_table,
    #     op_kwargs={
    #         "table": "raw.customers",
    #         "endpoint": "customers",
    #         "ts_field": "updated_at",
    #         "wm_name": "wm_customers",
    #     },
    # )

    # extract_payments = PythonOperator(
    #     task_id="extract_payments",
    #     python_callable=extract_and_ingest_table,
    #     op_kwargs={
    #         "table": "raw.payments",
    #         "endpoint": "payments",
    #         "ts_field": "updated_at",
    #         "wm_name": "wm_payments",
    #     },
    # )

    # extract_sessions = PythonOperator(
    #     task_id="extract_sessions",
    #     python_callable=extract_and_ingest_table,
    #     op_kwargs={
    #         "table": "raw.sessions",
    #         "endpoint": "sessions",
    #         "ts_field": "updated_at",
    #         "wm_name": "wm_sessions",
    #     },
    # )


    uploadCustomers = PythonOperator(
    task_id="extract_and_upload_customers",
    python_callable=extract_and_upload_table,
    op_kwargs={
        "endpoint":"customers",
        "ts_field":"updated_at",
        "wm_name":"wm_customers",
        "bucket_name":GCS_BUCKET,
        "gcs_prefix":"raw",
    },
    )

    uploadPayments = PythonOperator(
    task_id="extract_and_upload_payments",
    python_callable=extract_and_upload_table,
    op_kwargs={
        "endpoint":"payments",
        "ts_field":"updated_at",
        "wm_name":"wm_payments",
        "bucket_name":GCS_BUCKET,
        "gcs_prefix":"raw",
    },
    )

    uploadSessions = PythonOperator(
    task_id="extract_and_upload_sessions",
    python_callable=extract_and_upload_table,
    op_kwargs={
        "endpoint":"sessions",
        "ts_field":"updated_at",
        "wm_name":"wm_sessions",
        "bucket_name":GCS_BUCKET,
        "gcs_prefix":"raw",
    },
    )

# Run all in parallel
create_objects >> [uploadCustomers, uploadPayments, uploadSessions]