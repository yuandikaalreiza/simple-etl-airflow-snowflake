from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = "snowflake_default"

# This function extracts the data from the specified table in the sqlite database.
def extract_table(table_name, **kwargs):

    # connects to the sqlite database and creates a cursor object.
    import sqlite3
    con = sqlite3.connect('include/sample.db')
    cur = con.cursor()

    #executes the query to select all data from the specified table.
    cur.execute(f'SELECT * FROM {table_name}')

    # Store the results of the query in a variable.
    table_data = cur.fetchall()

    # closes the cursor object and datbase connection.
    cur.close()
    con.close()

    # gets the task instance object from the kwargs dictionary.
    ti = kwargs['ti']

    # pushes the table data to the XCOM store with the specified key.
    ti.xcom_push(key=f'table_data_{table_name}', value=table_data)

# transforms the data from the specified table in the sqlite database
def transform(table_name, **kwargs):

    # gets the task instance object from the kwargs dictionary
    ti = kwargs['ti']

    # gets the table data from the XCOM store.
    table_data = ti.xcom_pull(task_ids=f'extract_{table_name}', key=f'table_data_{table_name}')

    # creates a query to insert the data into the specified table
    query = f'insert into {table_name} values'

    # iterates over the table data and adds each row to the query.
    for row in table_data:
        # remove the square bracket
        query += f" ({str(row)[1:-1]}),"
    
    # adds a semicolon to the end of the query.
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