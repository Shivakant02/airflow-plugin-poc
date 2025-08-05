# plugins/task_logging_plugin.py

import logging
from datetime import datetime
from urllib.parse import quote_plus
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models.taskinstance import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from urllib.parse import quote_plus
from airflow.models.dagrun import DagRun

logger = logging.getLogger("airflow.task")

def log_task_info(state: str, ti: TaskInstance):
    # base_url = "http://localhost:8080"  # Change to your Airflow Webserver if needed

    # encoded_run_id = quote_plus(ti.run_id)
    # log_url = f"{base_url}/dags/{ti.dag_id}/grid?dag_run_id={encoded_run_id}&task_id={ti.task_id}&tab=logs"

    pipeline_run_id = ti.xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id') if ti.dag_id == 'simple_success_dag' else None

    logger.info(f"[Pipeline Run Id] Xcom -->>: {pipeline_run_id}")

    metadata = {
        "event": f"TASK_{state.upper()}",
        "pipeline_run_id": pipeline_run_id,
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
        "log_url": ti.log_url,
    }

    dagrun = ti.get_dagrun()
    if dagrun:
        dag_metadata = {
            "dag_id": dagrun.dag_id,
            "dag_run_id": dagrun.run_id,
            "dag_execution_date": str(dagrun.execution_date),
            "dag_state": dagrun.state,
            "start_date": str(dagrun.start_date),
            "end_date": str(dagrun.end_date),
            "pipeline_run_id": pipeline_run_id
        }

    serialized_dag = SerializedDagModel.get(dag_id=ti.dag_id)

    if serialized_dag:
        dag = serialized_dag.dag
        graph = {}

        for task in dag.tasks:
            graph[task.task_id] = {
                "upstream": [t.task_id for t in task.upstream_list],
                "downstream": [t.task_id for t in task.downstream_list],
                "operator": task.__class__.__name__,
            }
        

        log_msg = f"\n[task_graph_logs from the tasks] Task Graph for DAG 123'{ti.dag_id}':\n"
        log_msg+=f"graph: \n{graph}\n"
        logger.info(log_msg)

    # Print to terminal
    logger.info(f"[Task Metadata] ({state}): \n{metadata}")

    logger.info(f"[DAG Metadata] ({dagrun.state}): \n{dag_metadata}") if 'dag_metadata' in locals() else None

    # Print to task logs (if context is available)
    if hasattr(ti, 'log'):
        ti.log.info(f"[Task Metadata]-logs**** ({state}): \n{metadata}")

def log_dag_run_info(state: str, dag_run: DagRun):
    start = dag_run.start_date
    end = dag_run.end_date
    duration = None
    if start and end:
        duration = (end - start).total_seconds()

    # ti=dag_run.get_task_instances()[0] if dag_run.get_task_instances() else None

    # pipeline_run_id = ti.xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id') if ti and ti.dag_id == 'simple_success_dag' else None

    # logger.info(f"[Pipeline Run Id] Xcom from the dagrun-->>: {pipeline_run_id}")

    

    metadata = {
        "event": f"DAG_{state.upper()}",
        "timestamp": datetime.utcnow().isoformat(),
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "execution_date": str(dag_run.execution_date),
        "start_date": str(start),
        "end_date": str(end),
        "duration_seconds": duration,
        "state": getattr(dag_run.state, "value", str(dag_run.state)),
        "external_trigger": dag_run.external_trigger,
    }

    logger.info(f"[DAG Metadata] ({state}): \n{metadata}")



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

    @hookimpl
    def on_dag_run_running(self, dag_run, session=None):
        log_dag_run_info("running", dag_run)

    @hookimpl
    def on_dag_run_success(self, dag_run, session=None):
        log_dag_run_info("success", dag_run)

    @hookimpl
    def on_dag_run_failed(self, dag_run, session=None):
        log_dag_run_info("failed", dag_run)

class TaskLoggingPlugin(AirflowPlugin):
    name = "task_logging_plugin"
    listeners = [TaskLoggingListener()]
