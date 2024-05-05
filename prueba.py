from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("America/Bogota")

dag = DAG(
    "My_dag",
    description="prueba dag",
    start_date=datetime(2024, 5, 4, 00, 00, 00, tzinfo=local_tz),
    schedule_interval="	50 * * * *",
    catchup=False,
    tags=["pruebas"],
)
op = BashOperator(task_id="hello_world", bash_command="echo Hello World!",do_xcom_push="false", dag=dag)
print(op.retries)  # 3
