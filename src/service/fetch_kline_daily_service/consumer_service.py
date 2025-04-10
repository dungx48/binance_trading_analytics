import json
from dotenv import load_dotenv
from repository.fetch_klines_repo import FetchKlinesRepository
from repository.redis_connection import RedisConnection
from utils.log_consume import log_info, log_error

class ConsumeKlinesDailyService:
    def __init__(self):
        redis = RedisConnection()
        self.redis = redis.connection()
        self.repo = FetchKlinesRepository()

    def consume_and_push_to_db(self):
        log_info("🚀 Consumer đang chạy...")
        while True:
            try:
                _, raw_data = self.redis.blpop("klines_queue")
                payload = json.loads(raw_data)

                symbol = payload.get("symbol")
                data = payload.get("data")

                if not symbol or not isinstance(data, list):
                    log_error("❌ Payload không hợp lệ. Bỏ qua message.")
                    continue

                log_info(f"📥 Đang insert {len(data)} dòng dữ liệu của {symbol} vào DB...")
                self.repo.save_to_db(data, symbol)

            except json.JSONDecodeError as je:
                log_error(f"❌ JSON Decode Error: {je}")
            except Exception as e:
                log_error(f"❌ Lỗi không xác định khi xử lý message: {e}")

if __name__ == "__main__":
    # load_dotenv()
    consumer = ConsumeKlinesDailyService()
    consumer.consume_and_push_to_db()