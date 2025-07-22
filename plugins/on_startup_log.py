# plugins/on_startup_log.py
from airflow.plugins_manager import AirflowPlugin
import datetime

def write_test_log():
    with open('/opt/airflow/logs/airflow_test.log', 'a') as f:
        f.write("Plugin loaded at {}\n".format(datetime.datetime.now()))

write_test_log()

class TestLogPlugin(AirflowPlugin):
    name = "test_log_plugin"
