from dotenv import load_dotenv
from service.fetch_kline_daily_service.consumer_service import ConsumeKlinesDailyService    

if __name__ == "__main__":
    # Load biến môi trường từ .env
    load_dotenv()

    consumer = ConsumeKlinesDailyService()
    consumer.consume_and_push_to_db()