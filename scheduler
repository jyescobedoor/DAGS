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
           start_date=datetime(2024, 2, 7, 11, 59, 00, tzinfo=local_tz),
           schedule_interval="0,10,20,30,40,50 * * * *",
           #schedule=timedelta(minutes=3),      
           catchup=False,
           tags=["pruebas"],
)

# Define the SSHOperator
Tarea1= BashOperator(
    task_id='Valida_conexion',
 #  ssh_conn_id=ssh_conn_id,
    bash_command='date;hostname;pwd;',
    dag=dag,
)

# Set the task dependencies
Tarea1
