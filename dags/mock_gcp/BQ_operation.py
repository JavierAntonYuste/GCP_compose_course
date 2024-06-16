from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define el DAG
with DAG(
    'bigquery_multiple_operations',
    default_args=default_args,
    description='DAG that performs multiple operations in BigQuery',
    schedule_interval=None,  # Ejecutar bajo demanda
    catchup=False,
    tags=['example'],
) as dag:

    # Variables de entorno
    project_id = 'tu-proyecto'
    dataset_id = 'tu_dataset'
    source_table_1 = f'{project_id}.{dataset_id}.tabla_origen_1'
    source_table_2 = f'{project_id}.{dataset_id}.tabla_origen_2'
    temp_table = f'{project_id}.{dataset_id}.tabla_temporal'
    destination_table = f'{project_id}.{dataset_id}.tabla_destino'

    # Operación 1: Crear una tabla temporal a partir de una transformación de la tabla original
    sql_query_1 = f"""
    SELECT
        columna1,
        columna2,
        columna3
    FROM
        `{source_table_1}`
    WHERE
        columna_filtro IS NOT NULL
    """

    create_temp_table = BigQueryInsertJobOperator(
        task_id='create_temp_table',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": 'tabla_temporal',
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location='US',
    )

    # Operación 2: Realizar un join entre la tabla temporal y otra tabla
    sql_query_2 = f"""
    SELECT
        t1.columna1,
        t1.columna2,
        t2.columna3,
        t2.columna4
    FROM
        `{temp_table}` t1
    JOIN
        `{source_table_2}` t2
    ON
        t1.columna_clave = t2.columna_clave
    """

    join_tables = BigQueryInsertJobOperator(
        task_id='join_tables',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": 'tabla_destino',
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location='US',
    )

    # Operación 3: (Opcional) Realizar una transformación adicional en la tabla de destino
    sql_query_3 = f"""
    SELECT
        columna1,
        columna2,
        columna3,
        columna4,
        CURRENT_TIMESTAMP() AS processed_at
    FROM
        `{destination_table}`
    """

    transform_final_table = BigQueryInsertJobOperator(
        task_id='transform_final_table',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": 'tabla_destino_final',
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location='US',
    )

    # Definir las dependencias de las tareas
    create_temp_table >> join_tables >> transform_final_table
