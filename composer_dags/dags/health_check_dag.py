from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def health_check():
    return "DAG deployment successful"

with DAG(
    'health_check',
    default_args=default_args,
    description='Simple health check DAG',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['health'],
) as dag:
    
    check_task = PythonOperator(
        task_id='health_check',
        python_callable=health_check,
    )