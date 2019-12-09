import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

def _print_execution_date(execution_date, **kwargs):
    print(execution_date)

with DAG(
        dag_id='dag_exercise_2',
        default_args=args,
        schedule_interval=None,
) as dag:
    t1 = PythonOperator(task_id="print_execution_date", provide_context=True, python_callable=_print_execution_date,)
    t2 = BashOperator(task_id="wait_5", bash_command='sleep 5')
    t3 = BashOperator(task_id="wait_1", bash_command='sleep 1')
    t4 = BashOperator(task_id="wait_10", bash_command='sleep 10')
    t5 = DummyOperator(task_id="the_end")

    # for i in (1, 5, 10):
    #     wait = BashOperator(
    #         task_id=f"wait_{i}",
    #         bash_command=f"sleep {i}"
    #     )
    #
    #     t1 >> wait >> t5

t1 >> t2
t1 >> t3
t1 >> t4
[t2, t3, t4] >> t5