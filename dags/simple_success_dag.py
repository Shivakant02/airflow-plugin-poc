# dags/simple_success_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

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

def start_task():
    print("DAG starting - this should always work")
    print(f"Current time: {datetime.now()}")
    time.sleep(30)  # Run for at least 30 seconds
    print("Start task completed successfully")

def process_data():
    print("Processing some sample data...")
    time.sleep(45)  # Run for 45 seconds
    print("Data processing completed")

def end_task():
    print("DAG ending - wrapping up")
    time.sleep(15)  # Run for 15 seconds
    print("End task completed successfully")

# Task definitions
start = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    dag=dag
)

process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

end = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    dag=dag
)

# Task dependencies
start >> process >> end
