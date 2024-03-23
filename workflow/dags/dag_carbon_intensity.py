from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.utils import load_carbon_intensity_data, send_alert

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_carbon_intensity",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    schedule_interval="@daily",
    default_args=default_args
) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_table_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/create_carbon_intensity.sql",
        hook_params={	
            "options": "-c search_path=craverolucio_coderhouse"
        }
    )

    create_stage_table_task = PostgresOperator(
        task_id="create_stage_table_task",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/create_carbon_intensity_stg.sql",
        hook_params={	
            "options": "-c search_path=craverolucio_coderhouse"
        }
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_carbon_intensity_data,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.cfg",
            "start": "{{ data_interval_start }}",
            "end": "{{ data_interval_end }}"
        }
    )
    
    alert_task = PythonOperator(
        task_id="alert_task",
        python_callable=send_alert,
    )

    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_table_task
    dummy_start_task >> create_stage_table_task
    create_table_task >> load_data_task
    create_stage_table_task >> load_data_task
    load_data_task >> alert_task
    alert_task >> dummy_end_task