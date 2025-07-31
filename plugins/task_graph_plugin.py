# plugins/task_graph_plugin.py

from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models.taskinstance import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log  # ✅ Use Airflow's logger

def extract_task_graph(dag_id: str):
    serialized_dag = SerializedDagModel.get(dag_id=dag_id)
    if not serialized_dag:
        logger.warning(f"[task_graph_logs] No Serialized DAG found for DAG ID: {dag_id}")
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

    log_msg = f"\n[task_graph_logs] Task Graph for DAG '{dag_id}':\n"
    for task_id, info in graph_data.items():
        log_msg += f"  Task: {task_id}, Upstream: {info['upstream']}, Downstream: {info['downstream']}, Operator: {info['operator']}\n"

    # ✅ Scheduler/console
    logger.info(log_msg)

    # ✅ Log to task logs (UI)
    if hasattr(ti, "log"):
        ti.log.info(log_msg)

class TaskGraphLoggingListener:
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance: TaskInstance, session=None):
        log_task_graph(task_instance)

class TaskGraphPlugin(AirflowPlugin):
    name = "task_graph_plugin"
    listeners = [TaskGraphLoggingListener()]
