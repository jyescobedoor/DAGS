from datetime import datetime, timedelta
from textwrap import dedent

# Import the Pendulum library.
import pendulum

# Instantiate Pendulum and set your timezone.
local_tz = pendulum.timezone("America/Bogota")

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "A1_relacion_tareas",
    description="mi primer DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 2, 00, 00, 00, tzinfo=local_tz),
    catchup=False,
    tags=["jorge"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="Fecha",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="Mensaje",
        bash_command="echo Pruebas Implementacion Airflow Telefonica 2024 ",
     )
    


    t3 = BashOperator(
        task_id="Ruta",
        bash_command="pwd"
    )

    t1 >> [t2, t3]
