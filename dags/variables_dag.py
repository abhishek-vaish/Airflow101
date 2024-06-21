from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable


def print_variable(v_param):
    print(f'{v_param}:  {datetime.now()}')


with DAG(dag_id='variable_dag',
         start_date=datetime(2023, 1, 1),
         catchup=True,
         schedule='@monthly',
         description='print the current date'
         ):
    v_param = Variable.get('dag_variable', deserialize_json=True)
    task_a = PythonOperator(task_id='task_a',
                            python_callable=print_variable,
                            op_kwargs={"v_param": v_param}
                            )
