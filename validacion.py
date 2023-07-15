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
    "comandos",
    description="ejecucion comandos",
    schedule=timedelta(minutes= 30),
    start_date=datetime(2023, 7, 15, 17, 00, 00, tzinfo=local_tz),
    catchup=False,
    tags=["practica"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="mensaje",
        bash_command="echo prueba de comandos linux",
    )

    t2 = BashOperator(
        task_id="mostra_fecha",
        bash_command="date",
     )
    
    t3 = BashOperator(
        task_id="mostrar_ruta_Actual",
        bash_command="pwd",
      )
  
    t1 >> [t2, t3]
