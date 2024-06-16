from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime



def subdag(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        schedule_interval=None,
    )

    tarea_subdag_1 = DummyOperator(
        task_id='tarea_subdag_1',
        dag=subdag,
    )

    tarea_subdag_2 = DummyOperator(
        task_id='tarea_subdag_2',
        dag=subdag,
    )

    tarea_subdag_1 >> tarea_subdag_2

    return subdag

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('caso_de_uso_avanzado', default_args=default_args, schedule_interval=None)

inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

with dag:
    tarea1 = SubDagOperator(
        task_id='subdag_1',
        subdag=subdag('caso_de_uso_avanzado', 'subdag_1', default_args),
    )

    tarea2 = SubDagOperator(
        task_id='subdag_2',
        subdag=subdag('caso_de_uso_avanzado', 'subdag_2', default_args),
    )

    tarea3 = SubDagOperator(
        task_id='subdag_3',
        subdag=subdag('caso_de_uso_avanzado', 'subdag_3', default_args),
    )

    fin = DummyOperator(
        task_id='fin',
        dag=dag,
    )

# inicio >> [tarea1, tarea2] >> fin

inicio >> tarea1 >> [tarea2, tarea3]  >> fin 
