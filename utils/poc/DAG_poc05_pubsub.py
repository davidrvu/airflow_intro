# DAVIDRVU - 2020

from airflow import DAG
from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import base64
import datetime
import json
import numpy as np

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG-poc05-pubsub'
project           = 'txd-capacityplanning-tst' #'project-test-01-295316'
owner             = 'DAVIDRVU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

def python_func(ds, **kwargs):
    print("Inicio de función python_func")

    data_dict = {}
    data_dict["Mensaje123"] = "Mensaje personalizado generado en python_func"
    data_dict["otro_valor"] = 123

    data_to_publish = base64.b64encode(str(json.dumps(data_dict)).encode('utf-8')).decode() # IMPORTANTE: decode() JSON AS STRING IN BINARY
    kwargs['ti'].xcom_push(key='mensaje_param',value=data_to_publish) # PASAR VARIABLES ENTRE OPERATORS con xcom_push y xcom_pull 

    print("Fin de la función python_func")

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
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=python_func
                                 )

    task_pubsub1 = PubSubPublishOperator(task_id='task_pubsub1',
                                         project = project,
                                         topic="test-poc-01-3129e6b7-dd62-42cd",
                                         messages=[{'data':base64.b64encode("Mensaje custom 01".encode('utf-8')).decode(), 'attributes':{'atrib01': "ATRIBUTO1", 'atrib02': "ATRIBUTO2"}}])

    task_pubsub2 = PubSubPublishOperator(task_id='task_pubsub2',
                                         project = project,
                                         topic="test-poc-01-3129e6b7-dd62-42cd",
                                         messages=[{'data':"{{task_instance.xcom_pull(key='mensaje_param')}}"}])

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_python >> task_pubsub1 >> task_pubsub2 >> t_end

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
