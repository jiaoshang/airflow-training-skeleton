import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='dag_exercise_2h_30m',
    default_args=args,
    schedule_interval='*/30 2 * * *',
) as dag:

    t1 = DummyOperator(task_id="task1")
    t2 = DummyOperator(task_id="task2")
    t3 = DummyOperator(task_id="task3")
    t4 = DummyOperator(task_id="task4")
    t5 = DummyOperator(task_id="task5")

t1 >> t2 >> [t3, t4] >> t5