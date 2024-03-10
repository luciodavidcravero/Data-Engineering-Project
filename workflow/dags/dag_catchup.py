from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_something(**op_kwargs):
    print(f'Fecha de ejecucion: {op_kwargs["data_interval_start"]}\n Prox fecha de ejecucion: {op_kwargs["data_interval_end"]}')

default_args = {
    'start_date': datetime(2023, 11, 10),
    'catchup': True
}

with DAG(
    'dag_catchup',
    start_date=datetime(2023, 11, 10),
    catchup=True,
    schedule_interval='@daily') as dag:

    print_task = PythonOperator(
        task_id='print_variable',
        python_callable=print_something,
        op_kwargs={'data_interval_start': '{{ data_interval_start }}',
                   'data_interval_end': '{{ data_interval_end }}'}
    )