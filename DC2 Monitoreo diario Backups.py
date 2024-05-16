from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

#Variables de entorno validar al tener servidor asignado
servers = Variable.get('servers')
credentials = Variable.get('credentials')
path_output_file = Variable.get('credentials')
path_script = f'.../monitoreo_ejecuciones_diarias.py {servers['Orion']} {credentials['netbackup_user']} {credentials['netbackup_password']} {credentials['events_user']} {credentials['events_password']}' # esperar asignacion del servidor
# Define the SSH connection details
ssh_conn_id = "SERVWSUS2012"

dag = DAG(
    "DC2_monitoreo_ejecuciones_diarias",
    description="Proceso encargado de realizar el monitoreo e informe diario de las ejecuciones de políticas de Backups, este se realiza por medio de Python, haciendo uso de la API NetBackups. Retorna archivo de Excel con informe de ejecuciones.",
    start_date=datetime(2024, 5, 15, 10, 00, 00, tzinfo=local_tz),     # Fecha primera ejecucion
    schedule_interval="0 10,22 * * *",                                   # Todos los dias - 10:00 a.m y 10:00 p.m
    catchup=False,
    tags=["Backups","Monitoreo Backups", "DC2"],
)

# Define comando a ejecutar
comando_remoto=f'echo Inicia Proceso;sh {path_script} ;echo Finaliza Ejecucion del Proceso' ##comandos remotos a ejecutar

# Define the SSHOperator
get_executes_daily = SSHOperator(
    task_id="execute_monitoreo_ejecuciones_diarias",                             # Nombre de la tarea especifico de la tarea a ejecutar
    ssh_conn_id=ssh_conn_id,                                        # Define tu conexión SSH en Airflow
    command=comando_remoto,                                         # Invoca los shell a ejecutar
    conn_timeout = None,                                            # No permite que la conexion se cierre hasta finalizar el shell remoto
    cmd_timeout = None, 
    dag=dag,
)

get_executes_daily