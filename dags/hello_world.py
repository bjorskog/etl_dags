#!/usr/bin/env python

import os

from datetime import datetime
from airflow import DAG

from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


from homemade import hello_function


RECIPIENT_EMAIL = os.environ.get("EMAIL_RECIPIENT")


dag = DAG(
    'hello_world', 
    description='Simple tutorial DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20), 
    catchup=False)

dummy_operator = DummyOperator(
    task_id='dummy_task', retries=3, dag=dag
    )

hello_operator = PythonOperator(
    task_id='hello_task', 
    python_callable=hello_function.print_hello, 
    dag=dag
    )

second_hello_operator = PythonOperator(
    task_id='another_hello_task',
    python_callable=hello_function.another_hello,
    dag=dag
)


send_email_task = EmailOperator(
    to=RECIPIENT_EMAIL,
    subject="Notification from Airflow",
    html_content=
    "{{ task_instance.xcom_pull(task_ids='prepare_email_task', key='email') }}",
    task_id='send_email',
    dag=dag
)


# dummy_operator.set_downstream(hello_operator)

dummy_operator << hello_operator << second_hello_operator << send_email_task