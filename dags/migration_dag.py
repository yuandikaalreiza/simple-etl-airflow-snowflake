from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync

SNOWFLAKE_CONN_ID = "snowflake_default"

def extract_table(table_name, **kwargs):
    # connecting the sql
    import sqlite3
    con = sqlite3.connect('include/sample.db')
    cur = con.cursor()
    cur.execute(f'SELECT * FROM {table_name}')
    table_data = cur.fetchall()
    print(type(table_data))
    for row in table_data:
        print(row)
    cur.close()
    con.close()
    ti = kwargs['ti']
    ti.xcom_push(key=f'table_data_{table_name}', value=table_data)

def transform(table_name, **kwargs):
    ti = kwargs['ti']
    table_data = ti.xcom_pull(task_ids=f'extract_{table_name}', key=f'table_data_{table_name}')
    query = f'insert into {table_name} values'
    for row in table_data:
        query += f" ({str(row)[1:-1]}),"
    query = query[:-1] + ';'
    return query

with DAG(
    'migration_dag',
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False
) as dag:
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_table,
        op_kwargs={'table_name':'orders'},
        provide_context=True
    )

    transform_orders = PythonOperator(
        task_id='transform_orders',
        python_callable=transform,
        op_kwargs={'table_name':'orders'},
        provide_context=True
    )

    load_orders = SnowflakeOperator(
        task_id='load_orders',
        sql=transform_orders.output,
    )

    extract_agents = PythonOperator(
        task_id='extract_agents',
        python_callable=extract_table,
        op_kwargs={'table_name':'agents'},
        provide_context=True
    )

    transform_agents = PythonOperator(
        task_id='transform_agents',
        python_callable=transform,
        op_kwargs={'table_name':'agents'},
        provide_context=True
    )

    load_agents = SnowflakeOperator(
        task_id='load_agents',
        sql=transform_agents.output,
    )

    extract_customer = PythonOperator(
        task_id='extract_customer',
        python_callable=extract_table,
        op_kwargs={'table_name':'customer'},
        provide_context=True
    )

    transform_customer = PythonOperator(
        task_id='transform_customer',
        python_callable=transform,
        op_kwargs={'table_name':'customer'},
        provide_context=True
    )

    load_customer = SnowflakeOperator(
        task_id='load_customer',
        sql=transform_customer.output,
    )

    extract_orders >> transform_orders >> load_orders
    extract_agents >> transform_agents >> load_agents
    extract_customer >> transform_customer >> load_customer