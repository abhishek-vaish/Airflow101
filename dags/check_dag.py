from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(schedule='@daily',
         start_date=datetime(2023, 1, 1, 00, 00),
         dag_id='check_dag',
         catchup=True,
         description='DAG to check data',
         tags=['data_engineering team']):

    create_file = BashOperator(
        bash_command='echo "Hi there!" >/tmp/dummy',
        task_id='create_file'
    )

    check_file = BashOperator(
        bash_command='test -f /tmp/dummy',
        task_id='check_file'
    )

    read_file = PythonOperator(task_id="read_file",
                               python_callable=lambda: print(open('/tmp/dummy', 'rb').read()))

    create_file >> check_file >> read_file

