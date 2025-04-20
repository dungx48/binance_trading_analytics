import websockets
import json
from datetime import datetime, timezone
import asyncio
import logging

from repository.postgredb.db_connection import DatabaseConnection
from repository.partition_manager import create_partition_if_not_exists

logger = logging.getLogger(__name__)

async def fetch_binance():
    """Nhận dữ liệu từ Binance và lưu vào DB"""
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"

    async with websockets.connect(url) as ws:
        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()

        while True:
            data = await ws.recv()
            data = json.loads(data)
            kline = data["k"]

            timestamp = datetime.fromtimestamp(kline["t"] / 1000.0, tz=timezone.utc)
            row = (
                timestamp,
                float(kline["o"]),  # Open
                float(kline["h"]),  # High
                float(kline["l"]),  # Low
                float(kline["c"]),  # Close
                float(kline["v"])   # Volume
            )

            # ✅ Kiểm tra và tạo partition nếu chưa có
            create_partition_if_not_exists(timestamp)

            # ✅ Chèn dữ liệu vào bảng chính
            cursor.execute("""
                INSERT INTO binance_prices (timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

            conn.commit()
            logger.info(f"Saved: {row}")

if __name__ == "__main__":
    
    logger.info("Starting Binance WebSocket...")
    asyncio.run(fetch_binance())
