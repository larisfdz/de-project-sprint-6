import logging

import boto3
import vertica_python
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

conn_info = {'host': '51.250.75.20',
             'port': '5433',
             'user': 'abaelardusyandexru',
             'password': '1vwTzMJ9PJpVChL',
             'database': 'dwh',
             'autocommit': True
             }


def fetch_s3_file(bucket: str, filename: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=filename,
        Filename=f'/data/{filename}'
    )


def get_files(bucket_files):
    bucket = 'sprint6'
    for filename in bucket_files:
        logging.info(f'filename: {filename}')
        fetch_s3_file(bucket, filename)


def vertica_conn(templates_dict):
    stmt = templates_dict['stmt']
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt)

        conn.commit()
    conn.close()


def load_data_to_stg(table_name):
    schema_name = conn_info['user']
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            logging.info(
                f"COPY {schema_name}__STAGING.{table_name} FROM local '/data/{table_name}.csv' DELIMITER ','")
            cur.execute(
                f"COPY {schema_name}__STAGING.{table_name} FROM local '/data/{table_name}.csv' DELIMITER ','")

        conn.commit()
    conn.close()
