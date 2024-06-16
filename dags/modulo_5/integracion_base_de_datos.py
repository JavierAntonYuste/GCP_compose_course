from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SQLToS3Operator
from datetime import datetime

def extraer_datos():
    # Lógica para extraer datos de la base de datos SQL
    print("Datos extraídos de la base de datos SQL")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('integracion_base_de_datos', default_args=default_args, schedule_interval=None)

extraer = PythonOperator(
    task_id='extraer_datos',
    python_callable=extraer_datos,
    provide_context=True,
    dag=dag,
)

cargar_a_s3 = SQLToS3Operator(
    task_id='cargar_a_s3',
    sql='SELECT * FROM mi_tabla',
    filename='datos_extraidos.csv',
    bucket_name='mi_bucket_s3',
    filename_key='ruta_en_s3/datos_extraidos.csv',
    aws_conn_id='aws_conn',  # Conexión a AWS configurada previamente
    google_cloud_storage_conn_id='gcs_conn',  # Conexión a Google Cloud Storage
    verify='False',
    schema='public',  # Esquema de la base de datos
    do_xcom_push=True,
    provide_context=True,
    dag=dag,
)

extraer >> cargar_a_s3
