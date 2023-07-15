from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

# Import the Pendulum library.
import pendulum

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# Define the SSH connection details
ssh_conn_id = 'reportes'


dag = DAG('A2_conexion_ssh',
           description="Conexion remota reportes",
           schedule=timedelta(minutes=30),
           start_date=datetime(2023,5,28, 2,00,00, tzinfo=local_tz),
           catchup=False,
           tags=["conexion_sss"],

)   

# Define the SSHOperator
ssh_task = SSHOperator(
    task_id='ssh_command_task',
    ssh_conn_id=ssh_conn_id,
    command='pwd',
    dag=dag,
)

ssh_task_1 = SSHOperator(
    task_id='ssh_command_task_1',
    ssh_conn_id=ssh_conn_id,
    command='hostname',
    dag=dag,
)

ssh_task_2 = SSHOperator(
    task_id='ssh_command_task_2',
    ssh_conn_id=ssh_conn_id,
    command='id;date;echo prueba de conexion remota ok',
    dag=dag,
)

# Set the task dependencies
ssh_task >> [ssh_task_1,ssh_task_2]

