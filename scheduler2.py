from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# Import the Pendulum library.
import pendulum

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# Define the SSH connection details
#ssh_conn_id = 'roaming'

dag = DAG('scheduler',
           description="pruebas inicio automatico de tareas",
           start_date=datetime(2024, 3, 4, 3, 00, 00, tzinfo=local_tz),
           schedule_interval="15 * * * *",
           catchup=False,
           tags=["pruebas"],
)

# Define the SSHOperator
Tarea1= BashOperator(
    task_id='Valida_conexion',
    bash_command='date;hostname;pwd;',
    dag=dag,
)

# Set the task dependencies
