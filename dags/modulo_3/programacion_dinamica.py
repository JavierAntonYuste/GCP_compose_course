from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def dynamic_task(task_name, task_params):
    def task_function(**kwargs):
        print(f"Tarea {task_name} ejecutada con parámetros: {task_params}")
    
    return PythonOperator(
        task_id=task_name,
        python_callable=task_function,
        provide_context=True,
        dag=dag,
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('programacion_dinamica', default_args=default_args, schedule_interval=None)

config_list = [
    {"task_name": "Tarea_A", "params": {"param1": "valor1"}},
    {"task_name": "Tarea_B", "params": {"param2": "valor2"}},
    {"task_name": "Tarea_C", "params": {"param3": "valor3"}},
]

for config in config_list:
    dynamic_task(config["task_name"], config["params"])
