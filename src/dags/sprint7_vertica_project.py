'''
Dag последовательно выполняет все этапы ELT для истории действий пользователя в группах: 
- скачивает файл,
- выdодит 10 строк для проверки,
- создает таблицу  в слое staging
- сохраняет данные as is в слое staging,
- создает таблицы в слое dwh,
- наполняет таблицы слоя dwh
'''

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.func import get_files, load_data_to_stg, vertica_conn

DAG_ID = os.path.basename(__file__).split('.')[0]

args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

bash_command_tmpl = '''head -10 {{ params.files }}'''
bucket_files = ['group_log.csv']


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


dag = DAG(
    dag_id=DAG_ID,
    description='get data from s3',
    catchup=False,
    start_date=pendulum.datetime(2022, 8, 6, tz="UTC"),
    schedule_interval=None,
    template_searchpath='/lessons/dags/sql'
)

begin = DummyOperator(task_id='begin', dag=dag)

get_files = PythonOperator(
    task_id='get_files',
    python_callable=get_files,
    op_kwargs={'bucket_files': bucket_files},
    dag = dag
)

print_10_lines_of_each = BashOperator(
    task_id='print_10_lines_of_each',
    bash_command=bash_command_tmpl,
    params={'files': ' '.join([f'/data/{i}' for i in bucket_files])},
    dag=dag
)

create_stg_tables = SQLTemplatedPythonOperator(
    task_id='create_stg_tables',
    templates_dict={'stmt': 'create_stg.sql'},
    python_callable=vertica_conn,
    provide_context=True,
)

load_group_log_to_stg = PythonOperator(
    task_id='load_group_log_to_stg',
    python_callable=load_data_to_stg,
    op_kwargs={'table_name':'group_log'},
    dag=dag
    ) 

create_dwh_tables = SQLTemplatedPythonOperator(
    task_id='create_dwh_tables',
    templates_dict={'stmt': 'create_dwh.sql'},
    python_callable=vertica_conn,
    provide_context=True,
)

insert_dwh_tables = SQLTemplatedPythonOperator(
    task_id='insert_dwh_tables',
    templates_dict={'stmt': 'insert_dwh.sql'},
    python_callable=vertica_conn,
    provide_context=True,
)

end = DummyOperator(task_id='end', dag=dag)

begin >> get_files >> print_10_lines_of_each >> create_stg_tables >> load_group_log_to_stg >> create_dwh_tables >> insert_dwh_tables >> end

