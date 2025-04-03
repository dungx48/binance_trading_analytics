import requests
import pandas as pd
import time
from database.db_connection import DatabaseConnection

# Hàm lấy dữ liệu từ Binance API
def get_historical_klines(symbol, interval, start_time, end_time):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    limit = 1000  # Binance giới hạn 1000 dòng mỗi request

    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        
        response = requests.get(url, params=params)
        data = response.json()

        if not data:
            break

        all_data.extend(data)
        
        # Lấy timestamp của dòng cuối cùng làm mốc cho lần request tiếp theo
        start_time = data[-1][0] + 1  
        
        # Tránh bị rate limit
        time.sleep(1)

    return all_data

# Hàm lưu dữ liệu vào database
def save_to_db(data):
    db = DatabaseConnection()
    conn = db.connection
    cursor = conn.cursor()

    # Chuyển dữ liệu về dạng tuple để chèn vào DB
    formatted_data = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in data]

    # Chèn vào DB
    sql = """
    INSERT INTO binance_prices1 (timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (timestamp) DO NOTHING;
    """
    
    cursor.executemany(sql, formatted_data)
    conn.commit()
    
    print(f"✅ Đã lưu {len(formatted_data)} dòng vào database!")
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "1d"
    start_time = int(pd.Timestamp("2020-01-01").timestamp() * 1000)  # Dữ liệu từ 2020
    end_time = int(pd.Timestamp.now().timestamp() * 1000)

    # Lấy dữ liệu từ Binance API
    data = get_historical_klines(symbol, interval, start_time, end_time)

    if data:
        save_to_db(data)
    else:
        print("⚠️ Không có dữ liệu để lưu!")
