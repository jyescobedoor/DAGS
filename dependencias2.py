from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag1 = DAG(
    'dag1',
    default_args=default_args,
    schedule_interval='@daily',
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag1,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag1,
)

start_task >> end_task
############################################################

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag2 = DAG(
    'dag2',
    default_args=default_args,
    schedule_interval='@daily',
)

wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_dag1',
    external_dag_id='dag1',
    external_task_id='end_task',
    dag=dag2,
    mode='reschedule',  # puedes usar 'poke' también
)

start_task_dag2 = DummyOperator(
    task_id='start_task_dag2',
    dag=dag2,
)

wait_for_dag1 >> start_task_dag2
