# plugins/task_logging_plugin.py

import logging
import json
import os
from datetime import datetime
from urllib.parse import quote_plus
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models.taskinstance import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from urllib.parse import quote_plus
from airflow.models.dagrun import DagRun

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import sys
    sys.path.append('/opt/airflow/plugins')
    from avro_utils import serialize_message, compare_sizes
    AVRO_AVAILABLE = True
except ImportError as e:
    AVRO_AVAILABLE = False
    print(f"Avro not available: {e}")

logger = logging.getLogger("airflow.task")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9093']  # Use Docker service name with internal port
KAFKA_TOPIC = 'pryzm'

def get_kafka_producer():
    """Initialize and return Kafka producer with error handling"""
    if not KAFKA_AVAILABLE:
        logger.warning("Kafka client not available. Install kafka-python package.")
        return None
    
    try:
        # Use binary serializer for Avro data
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=None,  # We'll handle serialization manually
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks='all',
            request_timeout_ms=30000,
            connections_max_idle_ms=540000,
            max_block_ms=10000
        )
        logger.info(f"Kafka producer initialized successfully with servers: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {str(e)}")
        return None

def push_to_kafka(data, key=None, message_type=None):
    """Push data to Kafka topic with Avro serialization and error handling"""
    producer = get_kafka_producer()
    if not producer:
        logger.warning("Kafka producer not available. Skipping Kafka push.")
        return False
    
    try:
        logger.info(f"Pushing data to Kafka topic '{KAFKA_TOPIC}'...")
        
        # Serialize data using Avro if available, otherwise use JSON
        if AVRO_AVAILABLE and message_type:
            try:
                # Get size comparison
                json_size, avro_size = compare_sizes(data, message_type)
                compression_ratio = round((1 - avro_size/json_size) * 100, 2)
                
                logger.info(f"Message size comparison - JSON: {json_size} bytes, Avro: {avro_size} bytes, Compression: {compression_ratio}%")
                
                # Serialize with Avro
                serialized_data = serialize_message(data, message_type)
                logger.info(f"Successfully serialized message using Avro schema: {message_type}")
                
            except Exception as e:
                logger.warning(f"Avro serialization failed, falling back to JSON: {str(e)}")
                serialized_data = json.dumps(data).encode('utf-8')
        else:
            # Fallback to JSON serialization
            serialized_data = json.dumps(data).encode('utf-8')
            logger.info("Using JSON serialization (Avro not available)")
        
        future = producer.send(KAFKA_TOPIC, value=serialized_data, key=key)
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Successfully pushed data to Kafka topic '{KAFKA_TOPIC}' "
                   f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})")
        return True
        
    except Exception as e:
        logger.error(f"Failed to push data to Kafka topic '{KAFKA_TOPIC}': {str(e)}")
        return False
    finally:
        try:
            producer.close()
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {str(e)}")

def log_task_info(state: str, ti: TaskInstance):
    # base_url = "http://localhost:8080"  # Change to your Airflow Webserver if needed

    # encoded_run_id = quote_plus(ti.run_id)
    # log_url = f"{base_url}/dags/{ti.dag_id}/grid?dag_run_id={encoded_run_id}&task_id={ti.task_id}&tab=logs"

    pipeline_run_id = ti.xcom_pull(task_ids='push_pipeline_run_id', key='pipeline_run_id') if ti.dag_id == 'simple_success_dag' else None

    logger.info(f"[Pipeline Run Id] Xcom -->>: {pipeline_run_id}")

    metadata = {
        'type':'task_metadata',
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
            'type':'dag_metadata',
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
        
        graph_metadata = {
            'type':'graph_metadata',
            "event": f"TASK_GRAPH_{state.upper()}",
            "timestamp": datetime.utcnow().isoformat(),
            "dag_id": ti.dag_id,
            "dag_run_id": ti.run_id,
            'task_id': ti.task_id,
            "pipeline_run_id": pipeline_run_id,
            "graph": graph
        }
        log_msg = f"\n[graph_metadata from the tasks] Task Graph for DAG 123'{ti.dag_id}':\n"
        log_msg+=f"graph: \n{graph_metadata}\n"
        logger.info(log_msg)

    # Print to terminal
    logger.info(f"[Task Metadata] ({state}): \n{metadata}")

    logger.info(f"[DAG Metadata] ({dagrun.state}): \n{dag_metadata}") if 'dag_metadata' in locals() else None

    # Push task metadata to Kafka
    try:
        kafka_key = f"{ti.dag_id}_{ti.run_id}_{ti.task_id}"
        push_success = push_to_kafka(metadata, key=kafka_key, message_type='task_metadata')
        if push_success:
            logger.info(f"Task metadata successfully pushed to Kafka for task {ti.task_id}")
        else:
            logger.warning(f"Failed to push task metadata to Kafka for task {ti.task_id}")
    except Exception as e:
        logger.error(f"Exception while pushing task metadata to Kafka: {str(e)}")

    # Push DAG metadata to Kafka (if available)
    if 'dag_metadata' in locals():
        try:
            dag_kafka_key = f"{dagrun.dag_id}_{dagrun.run_id}_dag"
            dag_push_success = push_to_kafka(dag_metadata, key=dag_kafka_key, message_type='dag_metadata')
            if dag_push_success:
                logger.info(f"DAG metadata successfully pushed to Kafka for DAG {dagrun.dag_id}")
            else:
                logger.warning(f"Failed to push DAG metadata to Kafka for DAG {dagrun.dag_id}")
        except Exception as e:
            logger.error(f"Exception while pushing DAG metadata to Kafka: {str(e)}")

    # Push graph metadata to Kafka (if available)
    if 'graph_metadata' in locals():
        try:
            graph_kafka_key = f"{ti.dag_id}_{ti.run_id}_graph"
            graph_push_success = push_to_kafka(graph_metadata, key=graph_kafka_key, message_type='graph_metadata')
            if graph_push_success:
                logger.info(f"Graph metadata successfully pushed to Kafka for DAG {ti.dag_id}")
            else:
                logger.warning(f"Failed to push graph metadata to Kafka for DAG {ti.dag_id}")
        except Exception as e:
            logger.error(f"Exception while pushing graph metadata to Kafka: {str(e)}")

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

    # Push DAG run metadata to Kafka
    try:
        kafka_key = f"{dag_run.dag_id}_{dag_run.run_id}_dagrun"
        push_success = push_to_kafka(metadata, key=kafka_key)
        if push_success:
            logger.info(f"DAG run metadata successfully pushed to Kafka for DAG run {dag_run.run_id}")
        else:
            logger.warning(f"Failed to push DAG run metadata to Kafka for DAG run {dag_run.run_id}")
    except Exception as e:
        logger.error(f"Exception while pushing DAG run metadata to Kafka: {str(e)}")



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

    # @hookimpl
    # def on_dag_run_running(self, dag_run, session=None):
    #     log_dag_run_info("running", dag_run)

    # @hookimpl
    # def on_dag_run_success(self, dag_run, session=None):
    #     log_dag_run_info("success", dag_run)

    # @hookimpl
    # def on_dag_run_failed(self, dag_run, session=None):
    #     log_dag_run_info("failed", dag_run)

class TaskLoggingPlugin(AirflowPlugin):
    name = "task_logging_plugin"
    listeners = [TaskLoggingListener()]
