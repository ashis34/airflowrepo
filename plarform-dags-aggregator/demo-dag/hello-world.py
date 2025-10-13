from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function to run
def hello_world():
    print("Hello, world!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='@daily',  # run once a day
    start_date=datetime(2023, 1, 1),
    catchup=False,  # prevent backfilling
    tags=['example'],
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world
    )

    # Set task order
    hello_task
