import json
from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 4, 8)
}

def _processing_user():
    pass

with DAG('user_processing', schedule_interval='@daily', 
        default_args=default_args,
        catchup=False) as dag:
    # Define tasks/operators

    # 1. CREATING TABLE
    creating_table = SqliteOperator(
        task_id='creating_user_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
            '''
    )
    # DEVO CREARE LA CONNECTION AL DB DA AIRFLOW UI

    # 2. CHECK API AVAILABLE
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api', #specify the link/url of the api
        endpoint='api/' # the page that we are going to check, which is api/
    )
    # DEVO CREARE LA CONNECTION ALL'API DA AIRFLOW UI

    # 3. EXTRACTING USERS
    extractiong_user = SimpleHttpOperator( # this operator allows me to fetch the result of a given page
        task_id='extracting_users',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text), # using lambda fun: response is a param, 
        log_response=True
    )

    # 4. PROCESSING USERS
    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )