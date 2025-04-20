from service.fetch_kline_daily_service.consumer_service import ConsumeKlinesDailyService    
from service.fetch_kline_daily_service.producer_service import BinanceKlinesProducerService
import logging
import schedule
import time
import threading
from utils.logging_config import setup_logging
from constant.app_constant import MODE_RUN_JOB, AppConst

# ✅ Gọi logging setup ngay từ entry point
setup_logging(prefix="klines_job", level=logging.INFO)

# ✅ Logger chuẩn theo module
logger = logging.getLogger(__name__)

def job():
    logging.info("🚀 [Job] Bắt đầu producer + consumer song song")

    # Thread A: Producer chạy ngay
    def _producer():
        try:
            logging.info("🕖 [Producer] Start")
            producer = BinanceKlinesProducerService()
            producer.run_daily_producer(mode=MODE_RUN_JOB.DAILY)
            logging.info("✅ [Producer] Done")
        except Exception as e:
            logging.error(f"❌ [Producer] Lỗi: {e}")

    t_prod = threading.Thread(target=_producer, daemon=True)
    t_prod.start()

    # Thread B: Consumer đợi 2 phút rồi chạy
    def _consumer_delayed():
        time.sleep(120)
        try:
            logging.info("🚚 [Consumer] Start sau 2 phút")
            ConsumeKlinesDailyService().consume_and_push_to_db()
            logging.info("✅ [Consumer] Done")
        except Exception as e:
            logging.error(f"❌ [Consumer] Lỗi: {e}")

    t_cons = threading.Thread(target=_consumer_delayed, daemon=True)
    t_cons.start()

if __name__ == "__main__":
    # Lên lịch chạy job mỗi ngày lúc 07:00
    schedule.every().day.at(AppConst.SEVEN_AM).do(job)
    logging.info(f"📆 Scheduler khởi tạo – chờ job lúc {AppConst.SEVEN_AM} hằng ngày")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("⛔ Scheduler đã dừng (KeyboardInterrupt)")