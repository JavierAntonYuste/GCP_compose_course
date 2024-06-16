from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago

# Definición del DAG
with models.DAG(
    'gcs_to_dataflow_to_bigquery',
    schedule_interval=None,  # Ejecutar bajo demanda
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    # Definir variables de entorno
    project_id = 'tu-proyecto'
    region = 'us-central1'
    gcs_input_file = 'gs://bucket-name/input-file.txt'
    gcs_temp_location = 'gs://bucket-name/temp/'
    gcs_staging_location = 'gs://bucket-name/staging/'
    output_table = 'proyecto.dataset.tabla_destino'
    job_name = 'dataflow-job'

    # Transferir archivo en GCS (si es necesario)
    transfer_gcs_file = GCSToGCSOperator(
        task_id='transfer_gcs_file',
        source_bucket='bucket-name',
        source_object='input-file.txt',
        destination_bucket='bucket-name',
        destination_object='processed/input-file.txt'
    )

    # Ejecutar el trabajo de Dataflow
    dataflow_task = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_job',
        py_file='gs://bucket-name/path/to/your/dataflow-job-template.py',
        job_name=job_name,
        options={
            'project': project_id,
            'region': region,
            'inputFile': gcs_input_file,
            'outputTable': output_table,
            'tempLocation': gcs_temp_location,
            'stagingLocation': gcs_staging_location,
        },
        location=region,
    )

    # Insertar datos en BigQuery
    bq_insert_job = BigQueryInsertJobOperator(
        task_id='bq_insert_job',
        configuration={
            "query": {
                "query": f"SELECT * FROM {output_table}",
                "useLegacySql": False,
            }
        },
        location=region,
    )

    # Definir las dependencias de las tareas
    transfer_gcs_file >> dataflow_task >> bq_insert_job
