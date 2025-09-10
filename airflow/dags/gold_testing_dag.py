import sys
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


PROJECT_ROOT_IN_AIRFLOW = '/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

SRC_PATH = os.path.join(os.path.dirname(__file__), '../../src')
SRC_PATH = os.path.abspath(SRC_PATH)
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)


with DAG(
    dag_id="test_gold_data_dag",
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='15 2 * * *',
    catchup=False,
    tags=['dbt', 'test_gold'],
) as gold_dag:
    gold_data = BashOperator(
        task_id='test_gold_data',
        bash_command='cd /opt/airflow/dbt_capstone && dbt test --select gold'
    )

