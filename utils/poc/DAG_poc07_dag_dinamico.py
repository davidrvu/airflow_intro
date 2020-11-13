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
nameDAG_base      = 'DAG-poc07-dag-dinamico'
project           = 'project-test-01-295316'
owner             = 'DAVIDRVU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'

config_data = [
    {
        "pais_id" :           "cl",
        "schedule_interval" : "0 12 * * *", # 9:00 AM hora Chile (GMT-3)
        "variable_custom":    "SOLO CHILE"
    },
    {
        "pais_id" :           "pe",
        "schedule_interval" : "0 14 * * *", # 9:00 AM hora Perú (GMT-5)
        "variable_custom":    "SOLO PERÚ"
    } 
]

default_args = {
    'owner': owner,           # The owner of the task.
    'depends_on_past': False, # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}
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


for dag_data in config_data:

    client_dag_id = nameDAG_base + "-" + dag_data["pais_id"]

    with DAG(dag_id=client_dag_id,
             default_args = default_args,
             catchup = False,  # Ver caso catchup = True
             max_active_runs = 3,
             schedule_interval = dag_data["schedule_interval"]) as dag:
        #############################################################
        
        t_begin = DummyOperator(task_id="begin")
        
        task_python = PythonOperator(task_id='task_python',
                                     provide_context=True,
                                     python_callable=python_func,
                                     op_kwargs={
                                        'numeric_input': np.pi,
                                        'var1': dag_data["variable_custom"]
                                        }
                                     )

        #task_python_bash = BashOperator(task_id='task_python_bash',
        #                                bash_command = "gcloud ai-platform jobs submit training modelo_predictivo_" + datetime_now + " --region us-central1 --master-image-uri gcr.io/nombre_proyecto/modelo001"
        #                               )

        t_end = DummyOperator(task_id="end")

        #############################################################
        t_begin >> task_python >> t_end

        globals()[client_dag_id] = dag  # ULTIMA LÍNEA IMPORTANTE para correcto funcionamiento de DAG DINÁMICO!!!

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
