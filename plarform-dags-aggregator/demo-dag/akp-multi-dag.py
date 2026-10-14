from __future__ import annotations

from datetime import datetime

from airflow import DAG
#from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(**context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    logical_date = context["logical_date"]
    print(f"Hello from DAG={dag_id}, run_id={run_id}, logical_date={logical_date}")


# -------------------------
# DAG 1: Simple hello DAG
# -------------------------
with DAG(
    dag_id="akp_multi_dag_1",
    description="DAG 1: prints context and runs a shell command",
    #start_date=datetime(2025, 1, 1),
    #start_date=days_ago(1),
    start_date=datetime.now(),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["demo", "hello"],
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as akp_multi_dag_1:
    t1_print = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
    )

    t2_echo = BashOperator(
        task_id="echo_done",
        bash_command='echo "akp_multi_dag_1 done"',
    )

    t1_print >> t2_echo


# -------------------------
# DAG 2: Tiny ETL-ish DAG
# -------------------------
def extract(**context):
    # pretend we extracted rows
    rows = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]
    # push to XCom for downstream tasks
    context["ti"].xcom_push(key="rows", value=rows)


def transform(**context):
    rows = context["ti"].xcom_pull(key="rows", task_ids="extract")
    transformed = [{"id": r["id"], "val2": r["val"] * 2} for r in (rows or [])]
    context["ti"].xcom_push(key="transformed", value=transformed)


def load(**context):
    transformed = context["ti"].xcom_pull(key="transformed", task_ids="transform")
    print("Loaded rows:", transformed)


with DAG(
    dag_id="akp_multi_dag_2",
    description="DAG 2: toy ETL using PythonOperators and XCom",
    #start_date=datetime(2025, 1, 1),
    #start_date=days_ago(1),
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False,
    tags=["demo", "etl"],
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as akp_multi_dag_2:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load", python_callable=load)

    t_extract >> t_transform >> t_load
