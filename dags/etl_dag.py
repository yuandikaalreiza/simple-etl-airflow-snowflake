from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import include.etl as etl

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    'etl_dag',
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False
) as dag:
    extract = PythonOperator(
        task_id='extract_task',
        python_callable=etl.extract
    )

    extract