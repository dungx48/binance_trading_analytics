import json
from repository.postgredb.fetch_klines_repo import FetchKlinesRepository
from repository.redis.redis_connection import RedisConnection
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

                base_symbol = payload.get("base_symbol")
                quote_symbol = payload.get("quote_symbol")
                data = payload.get("data")

                if not base_symbol or not isinstance(data, list):
                    log_error("❌ Payload không hợp lệ. Bỏ qua message.")
                    continue

                log_info(f"📥 Đang insert {len(data)} dòng dữ liệu của {base_symbol} vào DB...")
                self.repo.save_to_db(data, base_symbol, quote_symbol)

            except json.JSONDecodeError as je:
                log_error(f"❌ JSON Decode Error: {je}")
            except Exception as e:
                log_error(f"❌ Lỗi không xác định khi xử lý message: {e}")

if __name__ == "__main__":
    consumer = ConsumeKlinesDailyService()
    consumer.consume_and_push_to_db()