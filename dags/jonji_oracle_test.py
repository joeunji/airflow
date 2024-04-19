from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.decorators import task
import config.slack_block_builder as sb
from airflow import DAG
import pendulum
from contextlib import closing
import pandas as pd
import oracledb

# with DAG(
#     dag_id='jonji_oracle_test',
#     start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
#     schedule=None,
#     catchup=False
# ) as dag:
#     @task(task_id='get_emp_task')
#     def get_emp_task():
#         oracle_hook = OracleHook(oracle_conn_id='conn_db_oracle')
#         with closing(oracle_hook.get_conn()) as conn:
#             with closing(conn.cursor()) as cursor:
#                 with open('/opt/airflow/files/sqls/jonji_test.sql', 'r') as sql_file:
#                     sql = '\n'.join(sql_file.readlines())
#                     cursor.execute(sql)
#                     rslt = cursor.fetchall()
#                     rslt = pd.DataFrame(rslt)
#                     rslt.columns = ['EMP_NO', 'USER_ID', 'EMP_NAME']
#                     return_blocks = []

#                     return_blocks.append(sb.section_text("*사원정보*"))

#                     row_data = rslt.query()

#                     for idx, row in row_data.iterrows():
#                         return_blocks.append(sb.section_text(f"{row['EMP_NO']}"))
#                     return return_blocks

with DAG(
    dag_id='jonji_oracle_test',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    @task(task_id='get_emp_task')
    def get_emp_task():
        
        # oracledb.init_oracle_client()
        # con = oracledb.connect(user="ERPUSER", password="erpuser", dsn="118.32.191.131:1524/MISORA")

        oracle_hook = OracleHook(oracle_conn_id='conn_db_oracle', thick_mode=True)
        with closing(oracle_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/files/sqls/jonji_test.sql', 'r') as sql_file:
                    sql = '\n'.join(sql_file.readlines())
                    cursor.execute(sql)
                    rslt = cursor.fetchall()
                    rslt = pd.DataFrame(rslt)
                    rslt.columns = ['EMP_NO', 'USER_ID', 'EMP_NAME']
                    return_blocks = []

                    return_blocks.append(sb.section_text("*사원정보*"))

                    row_data = rslt.query()

                    for idx, row in row_data.iterrows():
                        return_blocks.append(sb.section_text(f"{row['EMP_NO']}"))
                    return return_blocks

    send_to_slack = SlackWebhookOperator(
        task_id='send_to_slack',
        slack_webhook_conn_id='conn_slack_airflow_bot',
        blocks='{{ ti.xcom_pull(task_ids="get_emp_task") }}'
    )

    get_emp_task() >> send_to_slack