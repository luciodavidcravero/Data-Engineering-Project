from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def enviar(**context):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(f"Mi clave es: {Variable.get('GMAIL_SECRET')}")
        x.login(
            'guidonfranco@gmail.com',
            Variable.get('GMAIL_SECRET')
        )

        subject = f'Airflow reporte {context["dag"]} {context["ds"]}'
        body_text = f'Tarea {context["task_instance_key_str"]} ejecutada'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        
        x.sendmail('guidonfranco@gmail.com', 'guidonfranco@gmail.com', message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')


def simple():
    print("Helllo, world")

with DAG( 
    dag_id='dag_smtp_email_automatico',
    schedule_interval="* * * * *",
    on_success_callback=None,
    catchup=False,
    start_date=datetime(2023,11,10)
):
    tarea_1=PythonOperator(
        task_id='smtp',
        python_callable=enviar,
        )