from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")


# Define the SSH connection details
#ssh_conn_id = "datastage"

dag = DAG(
    "coscl_tol_particionamiento_datos",
    description="Particion de cdrs TOL",
    start_date=datetime(2024, 4, 27, 00, 00, 00, tzinfo=local_tz),
    schedule_interval="	0,10,20,30,40,50 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *",
    catchup=False,
    tags=["10.203.34.30"],
)

#Define comando a ejecutar
comando_remoto='echo Inicia Proceso; sleep 60 ;echo Finaliza Ejecucion del Proceso'

# Define the SSHOperator
Tarea1 = SSHOperator(
    task_id="partitionCDRtol",
    #ssh_conn_id=ssh_conn_id, # Define tu conexión SSH en Airflow
    command=comando_remoto,
    #conn_timeout = None,
    #cmd_timeout = None, 
    dag=dag,
)
