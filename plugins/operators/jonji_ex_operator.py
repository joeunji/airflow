# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 


class JonjiExOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        # while True:
        #     self.log.info(f'시작:{start_row}')
        #     self.log.info(f'끝:{end_row}')
        #     # row_df = self._call_api(self.base_url, start_row, end_row)
        #     row_df = self.python_2()
        #     total_row_df = pd.concat([total_row_df, row_df])
        #     if len(row_df) < 1000:
        #         break
        #     else:
        #         start_row = end_row + 1
        #         end_row += 1000

        # if not os.path.exists(self.path):
        #     os.system(f'mkdir -p {self.path}')
        # total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

        self.test(self.python_2())

    def test(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids = 'python_2')

        from pprint import pprint

        pprint(result)
        

    


    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        import json 

        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        contents = json.loads(rslt.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        ti.xcom_push(key='result1', value={row_df})
        # return row_df
        # import json
        # from pprint import pprint

        # pprint(json.loads(rslt))
        
    # tb_cycle_station_info >> python_2()

