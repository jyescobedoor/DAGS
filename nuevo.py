from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def create_subdag(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@weekly",  # Diferente intervalo de programación
    )

    with subdag:
        start = DummyOperator(task_id="start")
        task = DummyOperator(task_id="task")
        end = DummyOperator(task_id="end")

        start >> task >> end

    return subdag

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

parent_dag = DAG(
    'parent_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

start = DummyOperator(task_id='start', dag=parent_dag)

subdag_task = SubDagOperator(
    task_id='subdag_task',
    subdag=create_subdag('parent_dag', 'subdag_task', default_args),
    dag=parent_dag,
)

end = DummyOperator(task_id='end', dag=parent_dag)

start >> subdag_task >> end
