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
    schedule_interval=None,      # <- No automatic schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,               # <- Don't backfill
    tags=['example'],
) as dag:

    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world,
    )

    hello_task
