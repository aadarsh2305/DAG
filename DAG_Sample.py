import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import os
from nbconvert import ScriptExporter
from nbformat import read
from datetime import datetime

# Function to convert notebooks to Python scripts
def convert_notebook_to_py(notebook_path, output_dir):
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = read(f, as_version=4)

    exporter = ScriptExporter()
    script, _ = exporter.from_notebook_node(nb)

    base_name = os.path.basename(notebook_path).replace('.ipynb', '.py')
    output_path = os.path.join(output_dir, base_name)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(script)

    return output_path

# Convert the specific notebook files
PY_SCRIPTS_DIR = "/home/innv1admn/migrated_scripts"

os.makedirs(PY_SCRIPTS_DIR, exist_ok=True)

notebook_1_path = "/home/innv1admn/pyspark_notebooks/AccountSync.ipynb"
notebook_3_path = "/home/innv1admn/pyspark_notebooks/AllocationsSync.ipynb"
notebook_4_path = "/home/innv1admn/pyspark_notebooks/ProjectApprovingCostCenter.ipynb"
notebook_5_path = "/home/innv1admn/pyspark_notebooks/EngagementSync.ipynb"
notebook_6_path = "/home/innv1admn/pyspark_notebooks/DailyAllocationsSync.ipynb"
notebook_7_path = "/home/innv1admn/pyspark_notebooks/TimesheetDump.ipynb"
notebook_8_path = "/home/innv1admn/pyspark_notebooks/Revenuesync.ipynb"
notebook_9_path = "/home/innv1admn/pyspark_notebooks/DailyPPMGMA.ipynb"
notebook_10_path = "/home/innv1admn/pyspark_notebooks/PlannedRevenueProgression.ipynb"
notebook_11_path = "/home/innv1admn/pyspark_notebooks/PracticeDashboard.ipynb"

script_1 = convert_notebook_to_py(notebook_1_path, PY_SCRIPTS_DIR)
script_3 = convert_notebook_to_py(notebook_3_path, PY_SCRIPTS_DIR)
script_4 = convert_notebook_to_py(notebook_4_path, PY_SCRIPTS_DIR)
script_5 = convert_notebook_to_py(notebook_5_path, PY_SCRIPTS_DIR)
script_6 = convert_notebook_to_py(notebook_6_path, PY_SCRIPTS_DIR)
script_7 = convert_notebook_to_py(notebook_7_path, PY_SCRIPTS_DIR)
script_8 = convert_notebook_to_py(notebook_8_path, PY_SCRIPTS_DIR)
script_9 = convert_notebook_to_py(notebook_9_path, PY_SCRIPTS_DIR)
script_10 = convert_notebook_to_py(notebook_10_path, PY_SCRIPTS_DIR)
script_11 = convert_notebook_to_py(notebook_11_path, PY_SCRIPTS_DIR)

# File path to log errors
ERROR_LOG_FILE = "/home/innv1admn/airflow/error_log.txt"

# Ensure directory exists for the error log file
os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)

def log_error_to_file(context):
    """
    Logs task failure information to a file and console.
    """
    task_instance = context['task_instance']
    error_message = (
        f"Task Failed:\n"
        f"DAG ID: {context['dag'].dag_id}\n"
        f"Task ID: {task_instance.task_id}\n"
        f"Execution Time: {context['execution_date']}\n"
        f"Log URL: {task_instance.log_url}\n"
        f"Time: {datetime.now()}\n"
        f"{'-'*50}\n"
    )

    try:
        # Log to the file
        with open(ERROR_LOG_FILE, 'a', encoding='utf-8') as log_file:
            log_file.write(error_message)

        # Log to Airflow console
        logging.error(error_message)
    except Exception as e:
        # Handle exceptions during logging
        logging.error(f"Failed to log error: {e}")

# Define the DAG
dag = DAG(
    dag_id='PPM_MASTER_DAG',
    dag_display_name='PPM MASTER DAG',
    description='DAG to run migrated ppm pyspark jobs',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['PPM', 'PySpark']
)

# Tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

error = DummyOperator(
    task_id='error',
    dag=dag,
)

def create_task(task_id, script_path, ui_port):
    return BashOperator(
        task_id=task_id,
        bash_command=f"spark-submit --conf spark.ui.port={ui_port} --jars /usr/share/java/mysql-connector-java-9.1.0.jar {script_path}",
        on_failure_callback=log_error_to_file,
        dag=dag,
    )

# Creating tasks
spark_ui_port = 5100

task1 = create_task("AccountSync", script_1, spark_ui_port)
task3 = create_task("AllocationsSync", script_3, spark_ui_port)
task4 = create_task("ProjectApprovingCostCenter", script_4, spark_ui_port)
task5 = create_task("EngagementSync", script_5, spark_ui_port)
task6 = create_task("DailyAllocationsSync", script_6, spark_ui_port)
task7 = create_task("TimesheetDump", script_7, spark_ui_port)
task8 = create_task("Revenuesync", script_8, spark_ui_port)
task9 = create_task("DailyPPMGMA", script_9, spark_ui_port)
task10 = create_task("PlannedRevenueProgression", script_10, spark_ui_port)
task11 = create_task("PracticeDashboard", script_11, spark_ui_port)

# Task Dependencies
start >> task1 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task10 >> task11

# Redirect any task failure to error block
for task in [task1, task3, task4, task5, task6, task7, task8, task9, task10, task11]:
    task >> error

error >> end
