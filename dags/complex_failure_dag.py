# dags/complex_failure_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import random

default_args = {
    'owner': 'metadata_test',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'complex_failure_dag',
    default_args=default_args,
    description='A DAG with mixed success/failure/retry behavior',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['metadata', 'test', 'failure', 'retry']
)

def start_task():
    print("Starting complex failure DAG")
    print(f"Current time: {datetime.now()}")
    time.sleep(20)  # Run for 20 seconds
    print("Start task completed successfully")

def sometimes_fail_task():
    """Task that randomly fails to demonstrate retry behavior"""[37][41][42]
    print("Attempting potentially failing task...")
    time.sleep(30)  # Run for 30 seconds
    
    # 60% chance of failure on first attempt, 30% on retries
    failure_probability = 0.6 if not hasattr(sometimes_fail_task, 'attempt_count') else 0.3
    
    if random.random() < failure_probability:
        print("Task failed! Will be retried...")
        raise Exception("Random failure occurred - this should trigger retry")
    else:
        print("Task succeeded after potential retries")

def long_running_task():
    """Task that runs for over a minute"""
    print("Starting long running task...")
    time.sleep(75)  # Run for 1 minute 15 seconds
    print("Long running task completed")

def final_cleanup():
    print("Performing final cleanup...")
    time.sleep(25)  # Run for 25 seconds
    print("Cleanup completed successfully")

def guaranteed_failure():
    """Task that always fails after retries"""[54]
    print("This task will always fail...")
    time.sleep(20)  # Run for 20 seconds
    raise Exception("This task always fails - testing failure metadata")

# Task definitions with different retry configurations
start = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    dag=dag
)

maybe_fail = PythonOperator(
    task_id='sometimes_fail_task',
    python_callable=sometimes_fail_task,
    retries=2,  # Override default retries[42][43]
    retry_delay=timedelta(seconds=20),
    dag=dag
)

long_task = PythonOperator(
    task_id='long_running_task',
    python_callable=long_running_task,
    dag=dag
)

cleanup = PythonOperator(
    task_id='final_cleanup',
    python_callable=final_cleanup,
    dag=dag
)

always_fail = PythonOperator(
    task_id='guaranteed_failure',
    python_callable=guaranteed_failure,
    retries=1,  # Only retry once
    retry_delay=timedelta(seconds=15),
    dag=dag
)

# Task dependencies - create a complex graph
start >> [maybe_fail, long_task]
maybe_fail >> cleanup
long_task >> cleanup
cleanup >> always_fail  # This will cause the DAG to ultimately fail
