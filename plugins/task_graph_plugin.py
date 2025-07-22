# plugins/task_graph_plugin.py

import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from collections import defaultdict

logger = logging.getLogger("airflow.graph")

def extract_task_graph(dag_id: str):
    """Extracts a dict representing the task graph from SerializedDagModel"""
    serialized_dag = SerializedDagModel.get(dag_id=dag_id)
    if not serialized_dag:
        logger.warning(f"No Serialized DAG found for DAG ID: {dag_id}")
        return {}

    dag = serialized_dag.dag
    graph = {}

    for task in dag.tasks:
        graph[task.task_id] = {
            "upstream": [t.task_id for t in task.upstream_list],
            "downstream": [t.task_id for t in task.downstream_list],
            "operator": task.__class__.__name__,
        }

    return graph

def log_task_graph(ti: TaskInstance):
    dag_id = ti.dag_id
    graph_data = extract_task_graph(dag_id)

    logger.info(f"T[task_grap_logs] --> ask Graph Structure for DAG '{dag_id}':\n")
    for task_id, info in graph_data.items():
        logger.info(f"  Task: {task_id}, Upstream: {info['upstream']}, Downstream: {info['downstream']}, Operator: {info['operator']}")

class TaskGraphLoggingListener:
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance: TaskInstance, session=None):
        log_task_graph(task_instance)

class TaskGraphPlugin(AirflowPlugin):
    name = "task_graph_plugin"
    listeners = [TaskGraphLoggingListener()]
