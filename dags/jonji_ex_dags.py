from operators.jonji_ex_operator import JonjiExOperator
from airflow import DAG
import pendulum


with DAG(
    dag_id='jonji_ex_dags',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False,
    tags=['jonji_ex']
) as dag:
    '''서울시 공공자전거 대여소 정보'''
    task_1 = JonjiExOperator(
        task_id='task_1',
        dataset_nm='tbCycleStationInfo',
        path='/opt/airflow/files/tbCycleStationInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tbCycleStationInfo.csv'
    )

    task_1