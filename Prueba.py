from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 3, 22),
}
# Definición del DAG principal
with DAG('Dags_grupos',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


    start_main = DummyOperator(
        task_id='Inicio',
    )

    # Grupo de tareas 1
    with TaskGroup('grupo1') as group1:
        task1_1 = DummyOperator(task_id='task1_1')
        task1_2 = DummyOperator(task_id='task1_2')

    # Grupo de tareas 2
    with TaskGroup('grupo2') as group2:
        task2_1 = DummyOperator(task_id='task2_1')
        task2_2 = DummyOperator(task_id='task2_2')

    # Grupo de tareas 3
    with TaskGroup('grupo3') as group3:
        task3_1 = DummyOperator(task_id='task3_1')
        task3_2 = DummyOperator(task_id='task3_2')

    pre_main = DummyOperator(
        task_id='tarea_dependiente',
    )

    end_main = DummyOperator(
        task_id='Fin',
    )

    # Establecer dependencias entre tareas específicas de diferentes grupos
    start_main >> task1_1 >> task2_1 >> task3_1
    task1_2 >> task2_2 >> task3_2
    group3 >> end_main >> pre_main
