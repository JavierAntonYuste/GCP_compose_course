from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('configuracion_alertas', default_args=default_args, schedule_interval=None)

inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

tarea_que_falla = DummyOperator(
    task_id='tarea_que_falla',
    dag=dag,
)

enviar_alerta = EmailOperator(
    task_id='enviar_alerta',
    to='destinatario@example.com',
    subject='Alerta: Tarea que falló',
    html_content='<p>La tarea ha fallado.</p>',
    dag=dag,
)

fin = DummyOperator(
    task_id='fin',
    dag=dag,
)

inicio >> tarea_que_falla >> enviar_alerta >> fin
