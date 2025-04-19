from service.fetch_kline_daily_service.consumer_service import ConsumeKlinesDailyService    
from service.fetch_kline_daily_service.producer_service import run_daily_producer
import logging
import schedule
import time

logging.basicConfig(level=logging.INFO)

def job():
    logging.info("🕖 Running scheduled job at 7AM")
    run_daily_producer()


if __name__ == "__main__":
    schedule.every().day.at("07:00:00").do(job)
    logging.info("📆 Scheduler started – waiting for 7AM daily task...")

    while True:
        schedule.run_pending()
        time.sleep(60)  # kiểm tra mỗi phút
    # consumer = ConsumeKlinesDailyService()
    # consumer.consume_and_push_to_db()
    # run_daily_producer()