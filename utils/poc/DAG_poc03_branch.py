# DAVIDRVU - 2020

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import base64
import datetime
import json
import numpy as np

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG-poc03-branch'
project           = 'project-test-01-295316'
owner             = 'DAVIDRVU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

def python_func1(ds, **kwargs):
    print("Inicio de función python_func1")

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


    kwargs['ti'].xcom_push(key='variableX',value="MENSAJE DESDE FUNCT 1") # PASAR VARIABLES ENTRE OPERATORS con xcom_push y xcom_pull 


    print("Fin de la función python_func1")
    return custom_string

def python_func2(ds, **kwargs):
    print("Inicio de función python_func2")

    print("ds = ")
    print(ds)

    variableX = kwargs['ti'].xcom_pull(key='variableX')
    print("variableX = " + str(variableX))

    print("FIN python_func2")

def python_func3(ds, **kwargs):
    # FUENTE: https://stackoverflow.com/questions/46059161/airflow-how-to-pass-xcom-variable-into-python-function

    print("kwargs['templates_dict']['var_pull'] = ")
    print(kwargs['templates_dict']['var_pull'])    

    print("Fin de la función python_func3")

def python_func4(ds, **kwargs):
    print("Fin de la función python_func4")

def task_branching_callable(**kwargs):

    option = 0

    if ( option == 0 ):
        return "task_opt0" # DESDE AIRFLOW 1.10.3 SE PODRÁ RETORNAR 2 TASK SIMULTANEAMENTE ["task_path1","task_path2"]
        # OJO: https://issues.apache.org/jira/browse/AIRFLOW-3375 (para versión AIRFLOW 1.10.3)
    elif ( option == 1 ):
        return "task_opt1" 
    elif ( option == 2 ):
        return "task_opt2"                

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
         schedule_interval = "0 12 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")

    tarea1 = PythonOperator(task_id='tarea1',
                            provide_context=True,
                            python_callable=python_func1,
                            op_kwargs={
                               'numeric_input': np.pi,
                               'var1': "Variable1"
                               }
                            )

    tarea2 = PythonOperator(task_id='tarea2',
                            provide_context=True,
                            python_callable=python_func2
                            )

    tarea3 = PythonOperator(task_id='tarea3',
                            provide_context=True,
                            python_callable=python_func3,
                            templates_dict={
                               'var_pull': "{{task_instance.xcom_pull(key='variableX')}}"
                               }
                            )

    tarea4 = PythonOperator(task_id='tarea4',
                            provide_context=True,
                            python_callable=python_func4
                            )

    t_join = DummyOperator(task_id='t_join', dag=dag, trigger_rule='all_success') # all_failed | all_done | one_success | none_failed | 

    task_branching = BranchPythonOperator(task_id='task_branching',
                                          provide_context=True,
                                          python_callable=task_branching_callable
                                          )

    task_opt0 = DummyOperator(task_id="task_opt0")

    task_opt1 = DummyOperator(task_id="task_opt1")

    task_opt2 = DummyOperator(task_id="task_opt2")


    t_end = DummyOperator(task_id="end", trigger_rule='none_failed')

    #############################################################
    t_begin >> tarea1

    tarea1 >> tarea2 >> t_join
    tarea1 >> tarea3 >> t_join
    tarea1 >> tarea4 >> t_join

    t_join >> task_branching 
    task_branching >> [task_opt0, task_opt1, task_opt2] 

    task_opt0 >> t_end
    task_opt1 >> t_end
    task_opt2 >> t_end
    

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
