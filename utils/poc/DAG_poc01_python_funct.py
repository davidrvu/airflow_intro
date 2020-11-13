# DAVIDRVU - 2020

from airflow import DAG
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
nameDAG           = 'DAG-poc01-python-funct02'
project           = 'project-test-01-295316'
owner             = 'DAVIDRVU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

def python_func(ds, **kwargs):
    print("Inicio de función python_func")

    print("ds = ")
    print(ds)

    print("kwargs = ")
    print(kwargs)

    print("kwargs['numeric_input'] = ")
    print(kwargs['numeric_input'])

    print("kwargs['var1'] = ")
    print(kwargs['var1'])    

    print("kwargs['dag_run'].conf = ") # SOLO SI provide_context=True
    print(kwargs['dag_run'].conf)

    json_params = kwargs['dag_run'].conf

    print("json_params = ")
    print(json_params)

    #OJO: dag_run.conf['KEY'] -> PARAMETROS DE EJECUCIÓN DEL DAG
    #OJO: params.KEY          -> PARAMETROS DE EJECUCIÓN DEL TASK

    if json_params is not None:
        for i in json_params:
            print (str(i) + " = " + str(json_params[i]))

    custom_string = str(datetime.datetime.utcnow()) + " Lo que se retorna se printea en los logs."

    a_var = 0
    if a_var == 1:
        raise ValueError("Error levantado artificialmente!")

    print("Fin de la función python_func")
    return custom_string

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
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=python_func,
                                 op_kwargs={
                                    'numeric_input': np.pi,
                                    'var1': "Variable1"
                                    }
                                 )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_python >> t_end

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
