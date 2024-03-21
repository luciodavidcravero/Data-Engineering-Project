from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='dag_incremental',
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 1),
    catchup=True
)

def incremental(start, end):
    print(f'Start: {start} & End: {end}')

t = PythonOperator(
    task_id='task_incremental',
    python_callable=incremental,
    op_kwargs={
        'start': '{{ds}}',
        'end': '{{next_ds}}'
        },
    dag=dag
)