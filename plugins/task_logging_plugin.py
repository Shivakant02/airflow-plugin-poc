# plugins/task_logging_plugin.py

import logging
from datetime import datetime
from urllib.parse import quote_plus
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models.taskinstance import TaskInstance
from urllib.parse import quote_plus

logger = logging.getLogger("airflow.task")

def log_task_info(state: str, ti: TaskInstance):
    base_url = "http://localhost:8080"  # Change to your Airflow Webserver if needed

    encoded_run_id = quote_plus(ti.run_id)
    log_url = f"{base_url}/dags/{ti.dag_id}/grid?dag_run_id={encoded_run_id}&task_id={ti.task_id}&tab=logs"

    metadata = {
        "event": f"TASK_{state.upper()}",
        "timestamp": datetime.utcnow().isoformat(),
        "dag_id": ti.dag_id,
        "dag_run_id": ti.run_id,
        "task_id": ti.task_id,
        "execution_date": str(ti.execution_date),
        "try_number": ti.try_number,
        "start_date": str(ti.start_date),
        "end_date": str(ti.end_date),
        "state": ti.state,
        "queue": ti.queue,
        "operator": ti.task.__class__.__name__,
        "hostname": ti.hostname,
        "log_url": log_url,
    }

    # Print to terminal
    logger.info(f"[Task Metadata] ({state}): \n{metadata}")

    # Print to task logs (if context is available)
    if hasattr(ti, 'log'):
        ti.log.info(f"[Task Metadata]-logs**** ({state}): \n{metadata}")

class TaskLoggingListener:
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, session=None):
        log_task_info("running", task_instance)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session=None):
        log_task_info("success", task_instance)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, session=None):
        log_task_info("failed", task_instance)

class TaskLoggingPlugin(AirflowPlugin):
    name = "task_logging_plugin"
    listeners = [TaskLoggingListener()]
