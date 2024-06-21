from airflow import DAG
# from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
# from airflow.utils.helpers import chain
from datetime import datetime


def print_a():
    print("Hello from task a")


def print_b():
    print("Hello from task b")


def print_c():
    print("Hello from task c")


def print_d():
    print("Hello from task d")


def print_e():
    print("Hello from task e")


with DAG('my_dag', start_date=datetime(2023, 1, 1),
         description='A simple tutorial DAG',
         tags=['data_science'], schedule='@daily',
         catchup=False):
    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)
    task_c = PythonOperator(task_id='task_c', python_callable=print_c)
    task_d = PythonOperator(task_id='task_d', python_callable=print_d)
    task_e = PythonOperator(task_id='task_e', python_callable=print_e)

    task_a >> [task_b, task_c, task_d] >> task_e

    '''
    If you need to create a dependency between the two list of task,
    then upstream and downstream will going to help. It will throw the error.
    for ex: task_a >> [task_b, task_c] >> [task_d, task_e]
    
    In that case, you need to use from airflow.utils.helpers import chain
    for ex: chain(task_a, [task_b, task_c], [task_d, task_e])
    '''


# @dag(start_date=datetime(2023, 1, 1), description='A simple tutorial DAG',
#      tags=['data_science'], schedule='@daily', catchup=False)
# def my_dag():
#
#     @task
#     def print_c():
#         print('Hi from task c')
