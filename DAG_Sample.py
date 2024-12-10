from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
import os
from nbconvert import ScriptExporter
from nbformat import read

# Base path for your PySpark job notebooks and temporary converted Python scripts
PY_SCRIPTS_DIR = "/home/innv1admn/migrated_scripts"

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
os.makedirs(PY_SCRIPTS_DIR, exist_ok=True)

notebook_1_path = "/home/innv1admn/pyspark_notebooks/DailyPPMGMA.ipynb"
notebook_2_path = "/home/innv1admn/pyspark_notebooks/talendtopysparkgit.ipynb"

script_1 = convert_notebook_to_py(notebook_1_path, PY_SCRIPTS_DIR)
script_2 = convert_notebook_to_py(notebook_2_path, PY_SCRIPTS_DIR)

# Function to send email notifications on failure
def send_failure_email(context):
    subject = f"Task Failed: {context['task_instance'].task_id}"
    message = f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}. Please check the logs for more details."
    send_email(to='your-email@example.com', subject=subject, html_content=message)

dag = DAG(
    dag_id='PPM_MASTER_DAG',
    dag_display_name='PPM MASTER DAG',
    description='DAG to run migrated PPM pyspark jobs',
    schedule_interval=None,  # Trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['PPM', 'PySpark'],
    default_args={
        'on_failure_callback': send_failure_email,  # Send email on failure
    }
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

task1 = BashOperator(
    task_id="AccountSync",
    bash_command=f"spark-submit --conf spark.ui.port=5200 --jars /usr/share/java/mysql-connector-java-9.1.0.jar {script_2}",
    dag=dag,
)

task2 = BashOperator(
    task_id="DailyPPMGMA",
    bash_command=f"spark-submit --conf spark.ui.port=5100 --jars /usr/share/java/mysql-connector-java-9.1.0.jar {script_1}",
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set dependencies with conditional execution
start >> task1
task1 >> task2  # task2 will run only if task1 succeeds
task1.on_failure_callback = send_failure_email  # Send email on failure of task1
task2.on_failure_callback = send_failure_email  # Send email on failure of task2
task2 >> end  # task2 will run before end if it succeeds

# Set task dependencies with failure handling
task1.trigger_rule = TriggerRule.ALL_SUCCESS  # task1 runs only if start is successful
task2.trigger_rule = TriggerRule.ALL_SUCCESS  # task2 runs only if task1 is successful
