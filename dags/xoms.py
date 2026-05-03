from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

def push_two_names(ti):
    ti.xcom_push(key='name1', value='Ritik')
    ti.xcom_push(key='name2', value='Rahul')

def pull_two_names(ti, age):
    name1 = ti.xcom_pull(task_ids='task_two_names', key='name1')
    name2 = ti.xcom_pull(task_ids='task_two_names', key='name2')
    print(f"Name 1: {name1}, Name 2: {name2}")
    print(f"Age: {age}")

def master_func(ti,age):
    print("calling pull_two_names from master_func")
    pull_two_names(ti, age)

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5,
}

with DAG(
    dag_id='dag_xcom_push_pull',
    description='Our first DAG',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    default_args=default_args
) as dag:
    task_1 = PythonOperator(
        task_id='task_two_names',
        python_callable=push_two_names
    )
    task2 = PythonOperator(
        task_id='task_pull_two_names',
        python_callable=master_func,
        op_kwargs={'age': 25}
    )

    task_1 >> task2