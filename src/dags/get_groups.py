from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
import logging

import boto3
import vertica_python

#Для логирования
task_logger = logging.getLogger('airflow.task')

#Параметры AWS подключения
AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')


#Клиент для подключения к БД
def vertica_connect(database=Variable.get('VERTICA_DB'), 
                             user=Variable.get('VERTICA_USER'), 
                             password=Variable.get('VERTICA_SECRET_PASSWORD'), 
                             host=Variable.get('VERTICA_HOST'), 
                             port =5433, 
                             autocommit = True):

    conn_info = {'host': host, # Адрес сервера из инструкции
                 'port': port, # Порт для подключения
                 'user': user,       # Полученный логин
                 'password': password,# пароль. 
                 'database': database,
                 'autocommit': autocommit}
    
    try:
        server_conn = vertica_python.connect(**conn_info)
    except Exception as e:
        task_logger.error(f"Not managed to connect specified server: {str(e)}")    
    else:
        return server_conn

def get_file(name: str) -> None:

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=name,
        Filename=f'/data/{name}'
    )


def load_file(name: str, schema = 'ZHUKOVANANYANDEXRU__STAGING', client = vertica_connect()) -> None:

    with client as de_conn:
        with de_conn.cursor() as cur:
            cur.execute(f"COPY {schema}.{name} FROM LOCAL '/data/{name}.csv' DELIMITER ','")
            task_logger.info(f"COPY {schema}.{name} FROM LOCAL '/data/{name}.csv' DELIMITER ','")
        de_conn.commit()


def run_sql_command_vertica(file_name_path: str, client = vertica_connect()) -> None:
    with open(file_name_path, 'r') as sql_script:
        sqlCommands = sql_script.read().split(';')

    with client as de_conn:
        with de_conn.cursor() as cur:
            for command in sqlCommands:
                try:
                    cur.execute(command)
                except Exception as e:
                    task_logger.error(f'Not managed to run command {command} due to {str(e)}')
        de_conn.commit()



@task
def fetch_groups_log() -> None:
    get_file(name="group_log.csv")

@task
def load_file_group_log() -> None:
    load_file(name='group_log')

@task
def create_stg_group_log() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/create_stg_group_log.sql')

@task
def create_ddm_l_user_group_ativity() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/create_ddm_l_user_group_activity.sql')

@task
def create_ddm_s_auth_history() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/create_ddm_s_auth_history.sql')

@task
def upload_ddm_l_user_group_activity() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/upload_ddm_l_user_group_activity.sql')

@task
def upload_ddm_s_auth_history() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/upload_ddm_s_auth_history.sql')

@task
def create_view_count_top_conversion() -> None:
    run_sql_command_vertica(file_name_path='/de-project-5/src/sql/count_conversion.sql')

@dag(
    default_args={
        "owner": "student",
        "email": ["student@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5)},
    schedule_interval=None,
    start_date=datetime(2022, 7, 29),
    catchup=False,
    tags=["Yandex Praktikum"])

def etl_group_log():
    (create_stg_group_log()
    >> fetch_groups_log() 
    >> load_file_group_log() 
    >> create_ddm_l_user_group_ativity() 
    >> create_ddm_s_auth_history()
    >> upload_ddm_l_user_group_activity()
    >> upload_ddm_s_auth_history()
    >> create_view_count_top_conversion())


_ = etl_group_log()