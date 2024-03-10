from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.main import load_measurements_data, load_stations_data

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
    }

with DAG(
    dag_id="dag_luchmeetnet_catchup",
    start_date=datetime(2023, 11, 27),
    catchup=True,
    schedule_interval="0 * * * *",
    default_args=default_args
) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="dummy_start"
        )

    load_stations_data_task = PythonOperator(
        task_id="load_stations_data",
        python_callable=load_stations_data,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini"
        }
    )

    load_measurements_data_task = PythonOperator(
        task_id="load_measurements_data",
        python_callable=load_measurements_data,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "start": "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}",
            "end": "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}"
        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_end"
        )

    load_stations_data_task >> load_measurements_data_task
    #dummy_start_task >> load_stations_data_task
    dummy_start_task >> load_measurements_data_task
    #load_stations_data_task >> dummy_end_task
    load_measurements_data_task >> dummy_end_task
