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
    schedule=timedelta(minutes= 30),
    start_date=datetime(2023, 5, 28, 2, 00, 00, tzinfo=local_tz),
    catchup=False,
    tags=["practica"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="fecha",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="mensaje",
        bash_command="echo Implementacion Airflow Telefonica 2023 ",
     )
    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id="plantilla",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3]