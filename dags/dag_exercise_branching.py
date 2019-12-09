import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(7),
}


def _print_weekday(execution_date, **kwargs):
    print(execution_date.weekday())


def branch_func(execution_date, **kwargs):
    if(execution_date.weekday() == 1):
        return 'email_bob'
    elif(execution_date.weekday() == 2):
        return 'email_alice'
    else:
        return 'email_joe'


with DAG(
        dag_id='dag_exercise_branching',
        default_args=args,
        schedule_interval=None,
) as dag:
    print_weekday = PythonOperator(task_id="print_execution_date", provide_context=True, python_callable=_print_weekday, )
    branching = BranchPythonOperator(task_id="branching", provide_context=True, python_callable=branch_func,)
    email_bob = DummyOperator(task_id="email_bob")
    email_alice = DummyOperator(task_id="email_alice")
    email_joe = DummyOperator(task_id="email_joe")
    final_task = BashOperator(task_id="final_task")

print_weekday >> branching >> final_task

