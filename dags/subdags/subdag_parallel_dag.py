import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def subdag_parallel_dag(parent_dag_id, child_dag_id, default_args):
    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}', default_args=default_args) as dag:
        # put the tasks that we want to group together
        task_2 = PythonOperator(
        task_id='task_2',
        python_callable=lambda: time.sleep(3)
        )

        task_3 = BashOperator(
            task_id='task_3',
            bash_command="sleep 3"
        )

        return dag