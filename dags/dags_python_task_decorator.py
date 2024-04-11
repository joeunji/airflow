from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    @task(task_id="python_task_2")
    def print_context2(some_input2):
        print(some_input2)
    
    python_task_1 = print_context('task_decorator 실행')
    python_task_2 = print_context2('task_decorator2 실행')

    python_task_1 >> python_task_2