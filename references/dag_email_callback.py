from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def enviar(context):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(Variable.get('GMAIL_SECRET'))
        x.login(
            'guidonfranco@gmail.com',
            Variable.get('GMAIL_SECRET')
        )

        subject = f'Airflow reporte {context["dag"]} {context["ds"]}'
        body_text = f'Tarea {context["task_instance_key_str"]} ejecutada exitosamente'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        
        x.sendmail('guidonfranco@gmail.com','guidonfranco@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')


def simple():
    print("Hello, world")

with DAG( 
    dag_id='dag_smtp_email_callback',
    schedule_interval="* * * * *",
    catchup=False,
    start_date=datetime(2023,9,10)
):
    tarea_1 = PythonOperator(
        task_id='hola_mundo',
        python_callable=simple,
        on_success_callback=enviar
        )