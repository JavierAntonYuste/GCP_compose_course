from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def tarea_optimizada(**kwargs):
    print("Tarea optimizada ejecutada")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('optimizacion_recursos', default_args=default_args, schedule_interval=None)

tarea_optimizada = PythonOperator(
    task_id='tarea_optimizada',
    python_callable=tarea_optimizada,
    provide_context=True,
    dag=dag,
    pool='mi_pool',  # Asignar la tarea a un grupo de trabajadores (pool) específico
    #TODO checl this param
    # #max_active_runs=1,  # Limitar a una ejecución concurrente
)

