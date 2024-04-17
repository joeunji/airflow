import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id="dags_time_sensor",
    start_date=pendulum.datetime(2024, 4, 17, 16, 2, 0).in_timezone("Asia/Seoul"),
    end_date=pendulum.datetime(2024, 4, 17, 16, 10, 0).in_timezone("Asia/Seoul"),
    schedule="*/2 * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensor(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=1) }}""",
    )