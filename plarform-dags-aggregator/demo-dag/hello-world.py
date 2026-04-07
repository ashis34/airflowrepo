from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, world!")

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='hello_world_manual_dag',
    default_args=default_args,
    description='A Hello World DAG that runs only when triggered manually',
    schedule=None,      # <- No automatic schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,               # <- Don't backfill
    tags=['example'],
        access_control={
        'role_ashis123': {
           'DAG Runs': {
                'can_read',
                'can_delete'
           },
           'DAGs': {
                'can_read',
               'can_delete',
               'can_edit'
               
            }},
        'role_ashis-pattjoshi': {
            'can_read',
            'can_edit',
            'can_delete'
        }
}
) as dag:

    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world,
    )

    hello_task
