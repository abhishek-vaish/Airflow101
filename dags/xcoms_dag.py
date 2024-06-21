from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def select_phone(ti=None):
    ti.xcom_push(key='phone_key', value='Android')


def print_phone(ti=None):
    phone = ti.xcom_pull(task_ids='task_a', key='phone_key')
    print(phone)


with DAG(dag_id='xcom_learning',
         start_date=datetime(2023, 1, 1),
         catchup=False,
         schedule='@daily',
         description='Creating a XCOM'
         ):
    task_a = PythonOperator(task_id='task_a', python_callable=select_phone)
    task_b = PythonOperator(task_id='task_b', python_callable=print_phone)

    task_a >> task_b
