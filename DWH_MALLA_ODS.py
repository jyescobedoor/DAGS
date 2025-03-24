from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Configuraciones del DAG principal
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 22),
    'retries': 1,
}

# Definición del DAG principal
with DAG('dag_personal_2025',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start_main = DummyOperator(
        task_id='Inicio',
    )

    # Creación de un grupo de tareas
    with TaskGroup("Grupo1") as group1:
        start = DummyOperator(
            task_id='start',
        )

        process = DummyOperator(
            task_id='process',
        )

        end = DummyOperator(
            task_id='end',
        )

        # Configuración del flujo del grupo de tareas
        start >> process >> end

# Creación de un grupo de tareas
    with TaskGroup("Grupo2") as group2:
        start = DummyOperator(
            task_id='start',
        )

        process = DummyOperator(
            task_id='process',
        )

        end = DummyOperator(
            task_id='end',
        )

        # Configuración del flujo del grupo de tareas
        start >> process >> end


    pre_main = DummyOperator(
        task_id='Tarea_adicional',
    )

    end_main = DummyOperator(
        task_id='Tarea_fin',
    )



    # Configuración del flujo del DAG
    start_main >> group1 >> group2 >> pre_main >> end_main
