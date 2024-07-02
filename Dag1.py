# Import the Pendulum library.
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# DAG principal
with DAG('dag_principal', start_date=datetime(2024, 7, 1, 00, 00, 00, tzinfo=local_tz), schedule_interval='5,7 * * * *',catchup=False,) as dag1:
    task1 = DummyOperator(task_id='task_1')

# DAG dependiente
with DAG('dag_dependiente', start_date=datetime(2024, 7, 1, 00, 00, 00, tzinfo=local_tz), schedule_interval=None,catchup=False,) as dag2:
    wait_for_task_1 = ExternalTaskSensor(
        task_id='wait_for_task_1',
        external_dag_id='dag_principal',  # ID del DAG que estás esperando
        external_task_id='task_1',  # ID de la tarea que estás esperando
        allowed_states=['success'],  # Estado que se requiere para proceder
        timeout=300,  # Tiempo de espera en segundos

    )

    task2 = DummyOperator(task_id='task_2')


    wait_for_task_1 >> task2
