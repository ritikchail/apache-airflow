from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5,
}

with DAG(
    dag_id='our_first_dag',
    description='Our first DAG',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    default_args=default_args
) as dag:

    task_1 = BashOperator(
        task_id='our_first_task',
        bash_command='echo "Hello World! Ritik is here!"'
)

task_1