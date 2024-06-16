from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('integracion_sftp', default_args=default_args, schedule_interval=None)

transferir_a_sftp = SSHOperator(
    task_id='transferir_a_sftp',
    ssh_conn_id='SFTPserver',  # Nombre de la conexión SFTP configurada previamente en Airflow
    command='put /home/sbd/Documentos/Formaciones/airflow/airflow-test/SFTP/data/test_put.txt /home/airflow/archivo_subido.txt',
    do_xcom_push=False,
    dag=dag,
)
