import json
from dotenv import load_dotenv

from repository.fetch_klines_repo import FetchKlinesRepository
from repository.redis_connection import RedisConnection


class ConsumeKlinesDailyService():
    def __init__(self):
        redis = RedisConnection()
        self.redis = redis.connection()
        self.repo = FetchKlinesRepository()
    def consume_and_push_to_db(self):
        print("🚀 Consumer đang chạy...")
        while True:
            _, raw_data = self.redis.blpop("klines_queue")
            try:
                payload = json.loads(raw_data)
                symbol = payload["symbol"]
                data = payload["data"]
                print(f"📥 Insert data của {symbol} vào DB...")
                self.repo.save_to_db(data, symbol)
            except Exception as e:
                print(f"❌ Lỗi khi xử lý message: {e}")

if __name__ == "__main__":
    load_dotenv()
    consumer = ConsumeKlinesDailyService()
    consumer.consume_and_push_to_db()