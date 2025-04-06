from airflow import DAG
from airflow.operators.python import PythonOperator

from dotenv import load_dotenv
from datetime import datetime, timedelta

from service.fetch_kline_daily_service.fetch_service import FetchBinanceKlinesDailyService
from service.get_coin_name import CoinInfoService

def run_daily_producer():
    load_dotenv()
    coin_info_service = CoinInfoService()
    coins_name = coin_info_service.get_all_coins()
    coins_usdt = {item + 'USDT' for item in coins_name}
    for symbol in coins_usdt:
        producer = FetchBinanceKlinesDailyService()
        producer.fetch_and_push(symbol)

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

    fetch_task = PythonOperator(
        task_id='fetch_binance_data',
        python_callable=run_daily_producer,
    )

    fetch_task