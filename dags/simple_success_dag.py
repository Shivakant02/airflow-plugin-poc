# dags/simple_success_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import uuid
from airflow.utils.context import Context

default_args = {
    'owner': 'metadata_test',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'simple_success_dag',
    default_args=default_args,
    description='A simple DAG that always succeeds',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['metadata', 'test', 'success']
)

# ✅ Task to generate and push unique pipeline run ID
def push_pipeline_run_id(**context: Context):
    pipeline_run_id = str(uuid.uuid4())
    context['ti'].xcom_push(key='pipeline_run_id', value=pipeline_run_id)
    print(f"[INIT] Generated pipeline_run_id: {pipeline_run_id}")

# ✅ All other tasks pull the ID
def start_task(**context: Context):
    run_id = context['ti'].xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id')
    print(f"[START] pipeline_run_id = {run_id}")
    time.sleep(30)

def process_data(**context: Context):
    run_id = context['ti'].xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id')
    print(f"[PROCESS] pipeline_run_id = {run_id}")
    time.sleep(45)

def end_task(**context: Context):
    run_id = context['ti'].xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id')
    print(f"[END] pipeline_run_id = {run_id}")
    time.sleep(15)

# ✅ Define all tasks
push_id = PythonOperator(
    task_id='push_pipeline_run_id',
    python_callable=push_pipeline_run_id,
    provide_context=True,
    dag=dag
)

start = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    provide_context=True,
    dag=dag
)

process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

end = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    provide_context=True,
    dag=dag
)

# ✅ Set task execution order
push_id >> start >> process >> end
