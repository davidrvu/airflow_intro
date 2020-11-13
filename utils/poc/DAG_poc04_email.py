# DAVIDRVU - 2020

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils import dates
import base64
import datetime
import json
import numpy as np

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG-poc04-email'
project           = 'project-test-01-295316'
owner             = 'DAVIDRVU'
email             = ['astroboticapps@gmail.com'] # [email1, email2, email3]
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

html_email_content = """
<hr />
<p>Inicio de mensaje personalizado en HTML desde DAG: <strong>{{ params.name_dag }}</strong></p>
<br /> <strong>MUCHO EXITO EN SUS PROYECTOS!!</strong>
<br />
<p>Templated Content:</p>
<p>
    <br />param1&nbsp; &nbsp; &nbsp;: {{ params.param1 }}
    <br />task_key&nbsp; &nbsp; &nbsp;:{{ task_instance_key_str }}&nbsp;
    <br />test_mode&nbsp; :{{ test_mode }}
    <br />task_owner:{{ task.owner}}
    <br />hostname&nbsp; &nbsp;:{{ ti.hostname }}</p>
<hr />

SALUDOS!
David
"""

default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = "0 12 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    email_task = EmailOperator(to=email,
                               task_id='email_task',
                               subject='SALUDOS -> Templated Subject: start_date {{ ds }}',
                               params={'param1': 'Parametro personalizado',
                                       'name_dag': nameDAG
                                      },
                               html_content=html_email_content,
                               dag=dag)

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> email_task >> t_end

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### SET PROJECT
#       gcloud config set project txd-capacityplanning-tst

### Ejecuta  fechas NO ejecutadas anteriormente (Tiene que tener schedule_interval)
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 backfill -- -s 20201101 -e 20201105 DAG-poc01-python-funct
#       -s: start date -> INTERVALO CERRADO
#       -e: end date   -> INTERVALO ABIERTO

### RE-ejecuta fechas anteriores
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 clear -- -c -s 20201106 -e 20201108 DAG-poc01-python-funct02

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
