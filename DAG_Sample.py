from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import os
from nbconvert import ScriptExporter
from nbformat import read

# Base path for your PySpark job notebooks and temporary converted Python scripts
# NOTEBOOKS_DIR = "../../pyspark_notebooks"
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


dag = DAG(
    dag_id = 'PPM_MASTER_DAG',
    dag_display_name  = 'PPM MASTER DAG',
    description='DAG to run migrated ppm pyspark jobs',
    schedule_interval=None,  # Trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags = ['PPM','PySpark']
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

error_notification = EmailOperator(
    task_id='send_error_email',
    to='silara3333@iminko.com',
    subject='PPM DAG Failure Alert',
    html_content="""<h3>Error in PPM DAG Execution</h3>
                    <p>The task {{ task_instance.task_id }} has failed.</p>
                    <p>Please check the Airflow logs for more details.</p>""",
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)


# Set dependencies if needed
start >> task1
task1 >> task2
task1 >> error_notification
task2 >> end
task2 >> error_notification
