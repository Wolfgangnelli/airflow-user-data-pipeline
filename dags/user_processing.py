from email import header
import json
from operator import index
from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator, BashOperator
from datetime import datetime
from pandas import json_normalize

default_args = {
    'start_date': datetime(2022, 4, 8)
}

def _processing_user(ti): # ti (task instance)
    # xcom is used to share data between my tasks
    users = ti.xcom_pull(task_ids=['extracting_users'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    # convert this dict as json value (using pandas)
    precessed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    precessed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)
    # check if file is created: ls /tmp/
    # verify inside the file: cat /tmp/processed_user.csv

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

    # 5. STORING USER
    storing_user = 