from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def task1():
    print("Tarea 1 ejecutada")

def task2():
    print("Tarea 2 ejecutada")

def task3():
    print("Tarea 3 ejecutada")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('tareas_dependientes', default_args=default_args, schedule_interval=None)

tarea1 = PythonOperator(
    task_id='tarea_1',
    python_callable=task1,
    dag=dag,
)

tarea2 = PythonOperator(
    task_id='tarea_2',
    python_callable=task2,
    dag=dag,
)

tarea3 = PythonOperator(
    task_id='tarea_3',
    python_callable=task3,
    dag=dag,
)

tarea1 >> tarea2 >> tarea3
