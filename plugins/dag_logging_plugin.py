# # plugins/dag_logging_plugin.py

# import logging
# from datetime import datetime
# from airflow.plugins_manager import AirflowPlugin
# from airflow.listeners import hookimpl
# from airflow.models.dagrun import DagRun

# logger = logging.getLogger("airflow.dag")

# def log_dag_run_info(state: str, dag_run: DagRun):
#     start = dag_run.start_date
#     end = dag_run.end_date
#     duration = None
#     if start and end:
#         duration = (end - start).total_seconds()

#     metadata = {
#         "event": f"DAG_{state.upper()}",
#         "timestamp": datetime.utcnow().isoformat(),
#         "dag_id": dag_run.dag_id,
#         "dag_run_id": dag_run.run_id,
#         "execution_date": str(dag_run.execution_date),
#         "start_date": str(start),
#         "end_date": str(end),
#         "duration_seconds": duration,
#         "state": getattr(dag_run.state, "value", str(dag_run.state)),
#         "external_trigger": dag_run.external_trigger,
#     }

#     logger.info(f"[DAG Metadata] ({state}): \n{metadata}")

# class DagLoggingListener:
#     @hookimpl
#     def on_dag_run_running(self, dag_run, session=None):
#         log_dag_run_info("running", dag_run)

#     @hookimpl
#     def on_dag_run_success(self, dag_run, session=None):
#         log_dag_run_info("success", dag_run)

#     @hookimpl
#     def on_dag_run_failed(self, dag_run, session=None):
#         log_dag_run_info("failed", dag_run)

# class DagLoggingPlugin(AirflowPlugin):
#     name = "dag_logging_plugin"
#     listeners = [DagLoggingListener()]
    
