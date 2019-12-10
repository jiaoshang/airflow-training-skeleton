import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

sql = '''SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ds}}' '''
http_endpoint = '​history?start_at={{yesterday_ds}}&end_at={{ds}}&symbols=EUR&base=GBP'

with DAG(
        dag_id='dag_exercise_connection',
        default_args=args,
        schedule_interval=None,
) as dag:
    query_table = PostgresToGoogleCloudStorageOperator(task_id="query_table",
                                                       sql=sql,
                                                       postgres_conn_id="postgres_training",
                                                       bucket='dag-exercise-connection',
                                                       filename="test_table/ {{ds}}.json")

    fetch_exchange_rate = HttpToGcsOperator(task_id="fetch_exchange_rate",
                                            http_conn_id="http_exchange",
                                            endpoint=http_endpoint,
                                            gcs_bucket="dag-exercise-connection",
                                            gcs_path="exchange_rate")

[query_table, fetch_exchange_rate]
