from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

# Import the Pendulum library.
import pendulum

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# Define the SSH connection details
ssh_conn_id = 'prosclbt00c'


dag = DAG('A3_Acceso_Batch',
           description="Conexion remota prosclbt00c",
           schedule=timedelta(minutes=30),
           start_date=datetime(2023, 7, 15, 17, 00, 00, tzinfo=local_tz),
           catchup=False,
           tags=["prosclbt00c"],

)

# Define the SSHOperator
Suplantacion_xpbatch = SSHOperator(
    task_id='prueba de conexion',
    ssh_conn_id=ssh_conn_id,
    command='date;hostname;pwd;id',
	dag=dag,
)

# Set the task dependencies
Suplantacion_xpbatch
