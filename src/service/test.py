import asyncio
import websockets
import json
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# Load biến môi trường từ .env
load_dotenv()

# Kết nối PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# Hàm tạo partition nếu chưa tồn tại
def create_partition_if_not_exists(timestamp):
    partition_date = timestamp.strftime('%Y%m%d')  # Ví dụ: 20250330
    partition_name = f"binance_prices_{partition_date}"
    
    start_date = timestamp.strftime('%Y-%m-%d 00:00:00')
    end_date = timestamp.strftime('%Y-%m-%d 23:59:59')

    partition_sql = f"""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_tables WHERE tablename = '{partition_name}'
            ) THEN
                EXECUTE 'CREATE TABLE {partition_name} PARTITION OF binance_prices
                        FOR VALUES FROM (''{start_date}'') TO (''{end_date}'');';
            END IF;
        END $$;
    """

    # Mở kết nối riêng để tránh bị khóa bảng chính
    with psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    ) as partition_conn:
        with partition_conn.cursor() as partition_cursor:
            partition_cursor.execute(partition_sql)
            partition_conn.commit()

# Hàm nhận dữ liệu từ Binance WebSocket
async def fetch_binance():
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    async with websockets.connect(url) as ws:
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

            # Kiểm tra và tạo partition nếu chưa tồn tại
            create_partition_if_not_exists(timestamp)

            # Chèn dữ liệu vào bảng chính
            cursor.execute("""
                INSERT INTO binance_prices (timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

            conn.commit()
            print(f"Saved: {row}")

# Chạy WebSocket
asyncio.run(fetch_binance())
