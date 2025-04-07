import json
import requests
from dotenv import load_dotenv

from model.request.klines_request import KlinesRequest
from model.enums.time_enums import TimeEnums
from repository.redis_connection import RedisConnection
from service.get_coin_name import CoinInfoService

class FetchBinanceKlinesDailyService():
    def __init__(self):
        redis = RedisConnection()
        self.redis = redis.connection()
        self.interval = "1d"
        self.yesterday = TimeEnums.YESTERDAY
        self.start_time = TimeEnums.START_TIME
        self.current_time = TimeEnums.CURRENT_TIME

    def fetch_and_push(self, symbol):
        request = KlinesRequest(
            symbol=symbol,
            interval=self.interval,
            start_time=self.yesterday,
            end_time=self.current_time
        )
        data = self.get_klines_daily(request)
        self.push_to_queue(symbol, data)

    # Hàm lấy dữ liệu từ Binance API
    def get_klines_daily(self, req: KlinesRequest):
        url = "https://api.binance.com/api/v3/klines"
        all_data = []
        current_time = req.start_time

        while req.start_time < req.end_time:
            print(f"Bắt đầu lấy dữ liệu {req.symbol} - {req.start_time}")
            params = {
                "symbol": req.symbol,
                "interval": req.interval,
                "startTime": current_time,
                "endTime": req.end_time,
                "limit": req.limit
            }

            response = requests.get(url, params=params)
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print("❌ Lỗi khi parse JSON:", e)
                break

            if not data or not isinstance(data, list):
                print(f"⚠️ {req.symbol} Không có dữ liệu hoặc dữ liệu sai định dạng")
                break

            print(f"✅ Success {req.start_time}")
            all_data.extend(data)

            # Lấy timestamp của dòng cuối cùng làm mốc cho lần request tiếp theo
            # Tránh lỗi nếu data rỗng
            if data:
                current_time = data[-1][0] + 1
                # Tránh bị rate limit
                # time.sleep(1)
            else:
                break

        return all_data
    
    def push_to_queue(self, symbol, data):
        message = {
            "type": "daily",
            "symbol": symbol,
            "data": data
        }
        self.redis.rpush("klines_queue", json.dumps(message))
        print(f"✅ Pushed {symbol} data to Redis queue")
        
def run_daily_producer():
    load_dotenv()
    coin_info_service = CoinInfoService()
    coins_name = coin_info_service.get_all_coins()
    coins_usdt = {item + 'USDT' for item in coins_name}
    for symbol in coins_usdt:
        producer = FetchBinanceKlinesDailyService()
        producer.fetch_and_push(symbol)
if __name__ == "__main__":
    load_dotenv()
    coin_info_service = CoinInfoService()
    coins_name = coin_info_service.get_all_coins()
    coins_usdt = {item + 'USDT' for item in coins_name}
    for coin in coins_usdt:
        producer = FetchBinanceKlinesDailyService()
        producer.fetch_and_push(coin)