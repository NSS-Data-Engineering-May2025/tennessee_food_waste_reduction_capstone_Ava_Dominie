from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
import pendulum


AIRFLOW_PROJECT_ROOT = '/opt/airflow'
if AIRFLOW_PROJECT_ROOT not in sys.path:
    sys.path.append(AIRFLOW_PROJECT_ROOT)



from src.data_ingestion import main as data_ingestion_main


def _run_data_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running data_ingestion_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")
    data_ingestion_main()
    kwargs['ti'].log.info(f"data_ingestion_main completed.")




with DAG(
    dag_id='data_ingestion_pipeline',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='0 2 * * *',
    catchup=False,
    tags=['ingestion','bronze', 'api', 'minio']
) as dag:
    pull_api_data_to_minio_to_snowflake_task = PythonOperator(
        task_id='pull_api_data_to_minio_to_snowflake',
        python_callable=_run_data_ingestion_script
    )