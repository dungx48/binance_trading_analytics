import asyncio
from service.get_btcusdt_ws import fetch_binance
from utils.logger import log_info
from dotenv import load_dotenv


if __name__ == "__main__":
    # Load biến môi trường từ .env
    load_dotenv()
    
    log_info("Starting Binance WebSocket...")
    asyncio.run(fetch_binance())
