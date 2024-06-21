from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def fn_run_me():
    print("Hi")


with DAG(
    dag_id='second_dag',
    start_date=datetime(2023, 1, 1),
    tags=['sensor'],
    catchup=False
):
    run_me = PythonOperator(
        task_id='run_me',
        python_callable=fn_run_me
    )
