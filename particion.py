from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")


# Define the SSH connection details
#ssh_conn_id = "datastage"

dag = DAG(
    "coscl_tol_particionamiento_datos",
    description="Particion de cdrs TOL",
    start_date=datetime(2024, 4, 27, 00, 00, 00, tzinfo=local_tz),
    schedule_interval="	0,10,20,30,40,50 * * * *",
    catchup=False,
    tags=["10.203.34.30"],
)

# DAG dependiente
wait_for_task_1 = ExternalTaskSensor(
        task_id='wait_for_task_1',
        external_dag_id='My_dag',  # ID del DAG que estás esperando
        external_task_id='hello_world',  # ID de la tarea que estás esperando
        allowed_states=['success'],  # Estado que se requiere para proceder
        timeout=5,  # Tiempo de espera en segundos
        dag=dag,
    )


#Define comando a ejecutar
comando_remoto='date;id;df -k;'

# Define the SSHOperator
Tarea1 = BashOperator(
    task_id="partitionCDRtol",
    #ssh_conn_id=ssh_conn_id, # Define tu conexión SSH en Airflow
    bash_command=comando_remoto,
    #conn_timeout = None,
    #cmd_timeout = None, 
    dag=dag,
)

wait_for_task_1 >> Tarea1