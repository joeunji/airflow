import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dags_time_sensor_with_async",
    start_date=pendulum.datetime(2024, 4, 17, 16, 0, 0),
    end_date=pendulum.datetime(2024, 4, 17, 16, 10, 0),
    schedule="*/2 * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=1) }}""",
    )