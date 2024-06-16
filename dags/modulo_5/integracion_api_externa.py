from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime

def procesar_respuesta(**kwargs):
    respuesta = kwargs['ti'].xcom_pull(key=None, task_ids='llamada_api')
    # Procesar la respuesta de la API
    print("Respuesta de la API:", respuesta)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('integracion_api_externa', default_args=default_args, schedule_interval=None)

llamada_api = SimpleHttpOperator(
    task_id='llamada_api',
    http_conn_id='mi_api_conn',  # Conexión a la API configurada previamente
    endpoint='mi_endpoint',
    method='GET',
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    provide_context=True,
    dag=dag,
)

procesar = PythonOperator(
    task_id='procesar_respuesta',
    python_callable=procesar_respuesta,
    provide_context=True,
    dag=dag,
)

llamada_api >> procesar
