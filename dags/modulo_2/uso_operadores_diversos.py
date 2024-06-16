from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

def tarea_python():
    print("Tarea Python ejecutada")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('uso_operadores_diversos', default_args=default_args, schedule_interval=None)

inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

tarea_bash = BashOperator(
    task_id='tarea_bash',
    bash_command='echo "Tarea Bash ejecutada"',
    dag=dag,
)

tarea_python = PythonOperator(
    task_id='tarea_python',
    python_callable=tarea_python,
    dag=dag,
)

fin = DummyOperator(
    task_id='fin',
    dag=dag,
)

inicio >> tarea_bash
inicio >> tarea_python
tarea_bash >> fin
tarea_python >> fin
