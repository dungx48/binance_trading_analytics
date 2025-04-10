import sys
sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.service.fetch_kline_daily_service.fetch_service import run_daily_producer

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_data_binance_daily_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # Mỗi ngày 7h sáng
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['binance', 'fetch'],
) as dag:
    PythonOperator(
        task_id='fetch_binance_data',
        python_callable=run_daily_producer,
        
    )
