from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
#test code to connect to database

def test_query():
    hook = PostgresHook(postgres_conn_id="postgres_dbt")
    result = hook.get_first("SELECT 'Airflow is connected!' AS msg;")
    print(result)

with DAG(
    dag_id="test_postgres_dbt",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False
):
    PythonOperator(
        task_id="run_test",
        python_callable=test_query
    )
