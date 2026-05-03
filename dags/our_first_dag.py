from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

def greet():
    print("Hello from the Python Operator!")

def person(name, age):
    print(f"Hello, my name is {name} and I am {age} years old.")

def get_name():
    return "Ritik"

def get_name_xpull(ti):
    name = ti.xcom_pull(task_ids='our_sixth_task')
    print(f"The name pulled from XCom is: {name}")


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5,
}

with DAG(
    dag_id='dag_seven_tasks',
    description='Our first DAG',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    default_args=default_args
) as dag:

    task_1 = BashOperator(
        task_id='our_first_task',
        bash_command='echo "Hello World! Ritik is here!"')


    task2 = BashOperator(
        task_id='our_second_task',
        bash_command='echo "This is our second task!"')

    task3 = BashOperator(
        task_id='our_third_task',
        bash_command='echo "This is our third task!"'
    )
    task4 = PythonOperator(
        task_id='our_fourth_task',
        python_callable=greet
    )
    task5 = PythonOperator(
        task_id='our_fifth_task',
        python_callable=person,
        op_kwargs={'name': 'Ritik', 'age': 25}
    )
    task6 = PythonOperator(
        task_id='our_sixth_task',
        python_callable=get_name
    )
    task7 = PythonOperator(
        task_id='our_seventh_task',
        python_callable=get_name_xpull
    )


    task_1 >> [task2, task4]
    task2 >> task3
    task4 >> task5 >> task6 >> task7