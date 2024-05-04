from datetime import datetime

# Import the Pendulum library.
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("America/Bogota")

dag = DAG(
    dag_id="my_dag",
    start_date=pendulum.datetime(2024, 5, 4),
    schedule="@daily",
    default_args={"retries": 2},
    catchup=False,
)
op = BashOperator(task_id="hello_world", bash_command="Hello World!")
print(op.retries)  # 2