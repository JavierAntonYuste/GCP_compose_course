from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Define la función que se ejecutará en la tarea
def use_variable(**kwargs):
    my_variable = Variable.get("mi_variable")  # Obtén el valor de la variable definida en Airflow
    print(f"Valor de la variable: {my_variable}")

# Definir los argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Crear el objeto DAG
dag = DAG('uso_de_variable', default_args=default_args, schedule_interval=None)

# Crear una tarea que utiliza la variable definida en Airflow
tarea_uso_variable = PythonOperator(
    task_id='tarea_uso_variable',
    python_callable=use_variable,
    provide_context=True,
    dag=dag,
)

# Definir la relación de las tareas
tarea_uso_variable
