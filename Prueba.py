from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
#from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")


# Define the SSH connection details
#ssh_conn_id = "datastage"

dag = DAG(
    "mallaods",
    description="Consulta detalle Roaming para Ingresos",
    start_date=datetime(2024, 8, 13, 00, 00, 00, tzinfo=local_tz),
    schedule_interval="00 9 * * *",
    catchup=False,
    tags=["10.203.34.30"],
)

#Define comando a ejecutar
#comando_remoto='echo inicia proceso;sh prueba.sh ;echo Finaliza ejecucion de la prueba.'

# Define the SSHOperator
Tarea1 = BashOperator(
    task_id="Valida_conexion",
    #ssh_conn_id=ssh_conn_id, # Define tu conexión SSH en Airflow
    command="date",
    #conn_timeout = None,
    #cmd_timeout = None,
    dag=dag,
)
