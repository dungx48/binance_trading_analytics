import json
import os
import requests
import datetime
import logging
from dotenv import load_dotenv
from typing import List
from tqdm import tqdm

from model.request.klines_request import KlinesRequest
from model.enums.time_enums import TimeEnums
from repository.redis_connection import RedisConnection
from service.get_coin_name import CoinInfoService
from constant.app_constant import AppConst
from utils.log_produce import setup_basic_logging

# ✅ Gọi logging setup ngay từ entry point
setup_basic_logging(level=logging.INFO)

# ✅ Logger chuẩn theo module
logger = logging.getLogger(__name__)

class BinanceKlinesFetcher:
    def __init__(self):
        redis_conn = RedisConnection()
        self.redis = redis_conn.connection()
        self.interval = AppConst.ONE_DAY
        self.yesterday = TimeEnums.YESTERDAY()
        self.yesterday_end = TimeEnums.YESTERDAY_END()
        self.start_time = TimeEnums.START_TIME()
        self.kline_url = os.environ.get("KLINE_URL")

    def fetch_and_push(self, symbol: str, mode: str = "daily"):
        start_time = self.yesterday if mode == "daily" else self.start_time

        request = KlinesRequest(
            symbol=symbol,
            interval=self.interval,
            start_time=start_time,
            end_time=self.yesterday_end
        )

        data = self.get_klines_data(request)
        if data:
            self.push_to_redis(symbol, data, mode)
        else:
            logger.warning(f"⚠️ No data returned for {symbol}")

    def get_klines_data(self, req: KlinesRequest) -> List:
        all_data = []
        current_time = req.start_time

        while current_time < req.end_time:
            logger.debug(f"⏳ Fetching data for {req.symbol} from {current_time} to {req.end_time}")

            params = {
                "symbol": req.symbol,
                "interval": req.interval,
                "startTime": current_time,
                "endTime": req.end_time,
                "limit": req.limit
            }

            try:
                response = requests.get(self.kline_url, params=params)
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.error(f"❌ Error fetching/parsing data for {req.symbol}: {e}")
                break

            if not data or not isinstance(data, list):
                logger.warning(f"⚠️ Invalid or empty data received for {req.symbol}")
                break

            all_data.extend(data)
            current_time = data[-1][0] + 1

        return all_data

    def push_to_redis(self, symbol: str, data: List, mode: str):
        for row in data:
            date_str = datetime.datetime.utcfromtimestamp(row[0] / 1000).strftime('%Y-%m-%d')
            row[0] = date_str

        message = {
            "type": mode,
            "symbol": symbol,
            "data": data
        }

        self.redis.rpush("klines_queue", json.dumps(message))
        logger.info(f"✅ Pushed {symbol} data to Redis queue")

def run_daily_producer():
    # load_dotenv()
    coin_service = CoinInfoService()
    symbols = {coin + "USDT" for coin in coin_service.get_all_coins()}

    fetcher = BinanceKlinesFetcher()
    for symbol in tqdm(symbols, desc="🚀 Fetching Klines"):
        fetcher.fetch_and_push(symbol, mode="daily")
        logging.info("="*80)

if __name__ == "__main__":
    run_daily_producer()
