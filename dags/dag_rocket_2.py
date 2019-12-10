import airflow
from airflow.models import DAG

from dags.operators.launch_to_gcs_operator import LaunchToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="download_rocket_launches_2",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *",
)

download_rocket_launches_and_print_stats = LaunchToGcsOperator(
    task_id="download_rocket_launches",
    provide_context=True,
    bucket='dag-exercise-connection',
    start_date="{{ds}}",
    end_date="{{tomorrow_ds}}",
    dag=dag,
)

download_rocket_launches_and_print_stats