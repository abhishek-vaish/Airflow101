from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_a():
    print("I processed the file!")


with DAG(
    dag_id='first_dag',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['sensor'],
    catchup=False
):
    wait_for_files = FileSensor.partial(
        task_id='wait_for_files',
        fs_conn_id='fs_default'
    ).expand(
        filepath=['data1.csv', 'data2.csv', 'data3.csv']
    )

    process_file = PythonOperator(
        task_id='process_file',
        python_callable=print_a
    )

    wait_for_files >> process_file
