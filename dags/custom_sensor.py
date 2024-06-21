from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

from airflow.sensors.base import PokeReturnValue


def fn_print_shibe_picture_url(url):
    print(url)


with DAG(
    dag_id='sensor_decorator',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False
):

    @task.sensor(poke_interval=30, timeout=3600, mode='poke')
    def check_shibe_availability() -> PokeReturnValue:
        r = requests.get("https://jsonplaceholder.typicode.com/todos/1")

        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.json()
        else:
            condition_met = False
            operator_return_value = None

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)


    print_shibe_picture_url = PythonOperator(
        task_id='print_shibe_picture_url',
        python_callable=fn_print_shibe_picture_url,
        op_kwargs={"url": check_shibe_availability()}
    )

