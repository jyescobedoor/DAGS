from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Configuraciones del DAG principal
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 13),
    'retries': 1,
}

# Definición del DAG principal
with DAG('main_dag_with_task_groups',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start_main = DummyOperator(
        task_id='inicia_malla_ods',
    )

    # Creación de un grupo de tareas
    with TaskGroup("DWHODS_EXT_Catalog") as group1:
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
    with TaskGroup("DWHODS_STG_Catalog") as group2:
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
        task_id='DWHODS_Adicionales',
    )

    end_main = DummyOperator(
        task_id='fin_malla_ods',
    )



    # Configuración del flujo del DAG
    start_main >> group1 >> group2 >> pre_main >> end_main
