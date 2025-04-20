import time
import json
import logging

from repository.redis.redis_connection import RedisConnection
from constant.app_constant import AppConst
from repository.postgredb.db_connection import DatabaseConnection
from repository.postgredb.fetch_klines_repo import FetchKlinesRepository

# Lấy logger module
logger = logging.getLogger(__name__)

class ConsumeKlinesDailyService:
    """
    Consumer chạy liên tục đọc message từ Redis queue,
    đệm vào buffer sau đó flush xuống DB theo batch hoặc theo thời gian.
    """
    MAX_BATCH = 500
    FLUSH_INTERVAL = 60  # giây
    QUEUE_KEY = AppConst.REDIS_KLINES_QUEUE

    def __init__(self):
        self.redis = RedisConnection().connection()

    def consume_and_push_to_db(self):
        """
        Vòng lặp chính:
        - BLPOP với timeout 5s để không block vô hạn
        - Đọc payload JSON, gom vào buffer
        - Khi buffer đủ kích thước hoặc quá FLUSH_INTERVAL, gọi _flush_to_db
        - Tiếp tục loop vô hạn
        """
        logger.info("🚀 Consumer đang chạy liên tục...")
        buffer = []
        last_flush = time.time() # thời điểm flush cuối cùng

        while True:
            try:
                # BLPOP: nếu có message trong QUEUE_KEY thì trả về ngay,
                # nếu không sau 5s sẽ timeout (None) để loop tiếp
                item = self.redis.blpop(self.QUEUE_KEY, timeout=5)
                if item:
                    _, raw = item
                    try:
                        payload = json.loads(raw)
                        buffer.append(payload)
                    except json.JSONDecodeError as je:
                        logger.error(f"❌ JSON Decode Error: {je}")

                now = time.time()
                #    Điều kiện flush:
                #    - Đủ MAX_BATCH messages
                #    - Hoặc đã quá FLUSH_INTERVAL giây từ lần flush trước
                if buffer and (len(buffer) >= self.MAX_BATCH or now - last_flush >= self.FLUSH_INTERVAL):
                    self._flush_to_db(buffer)
                    buffer.clear()
                    last_flush = now

            except Exception as e:
                logger.error(f"❌ Lỗi không xác định: {e}")

    def _flush_to_db(self, buffer):
        """
        Mở connection 1 lần, dùng FetchKlinesRepository để insert tất cả payload trong buffer.
        """
        pending = len(buffer)
        logger.info(f"🍽 Flush {pending} messages vào DB...")

        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()
        repo = FetchKlinesRepository(conn, cursor)

        try:
            # Với mỗi payload, validate và gọi save_batches
            for payload in buffer:
                base = payload.get("base_symbol")
                quote = payload.get("quote_symbol")
                data = payload.get("data", [])
                if not base or not isinstance(data, list):
                    logger.error(f"❌ Payload không hợp lệ: {payload}")
                    continue
                logger.info(f"📥 Đang insert {len(data)} dòng của {base}...")
                repo.save_batches(data, base, quote)

            logger.info(f"🏁 Flush hoàn tất {pending} messages.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Lỗi khi flush batch: {e}")
        finally:
            repo.close()

if __name__ == "__main__":
    consumer = ConsumeKlinesDailyService()
    consumer.consume_and_push_to_db()