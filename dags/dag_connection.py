import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


sql = '''SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = {{ds}} '''


with DAG(
        dag_id='dag_exercise_connection',
        default_args=args,
        schedule_interval=None,
) as dag:
    query_table = PostgresToGoogleCloudStorageOperator(task_id="query_table",
                                                       sql=sql,
                                                       postgres_conn_id="postgres_training",
                                                       bucket='dag-exercise-connection',
                                                       filename="test_table")


query_table
