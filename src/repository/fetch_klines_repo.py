import time
import math
from repository.db_connection import DatabaseConnection

class FetchKlinesRepository():
    def __init__(self):
        pass

    def save_to_db(self, data, symbol):
        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()

        formatted_data = [(symbol, row[0], row[1], row[2], row[3], row[4], row[5]) for row in data]

        sql = """
        INSERT INTO binance_prices_daily (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
        """

        total = len(formatted_data)
        batch_size = max(1, math.ceil(total * 0.1))  # lấy 10% và làm tròn lên, tối thiểu là 1

        overall_start = time.time()

        for i in range(0, total, batch_size):
            batch_start = time.time()

            batch = formatted_data[i:i+batch_size]
            cursor.executemany(sql, batch)
            conn.commit()

            batch_end = time.time()
            inserted = i + len(batch)
            percent_done = (inserted / total) * 100

            print(f"✅ Đã insert {inserted}/{total} dòng ({percent_done:.2f}%) - thời gian batch: {batch_end - batch_start:.2f} giây")

        overall_end = time.time()
        print(f"🏁 Tổng thời gian insert: {overall_end - overall_start:.2f} giây")

        cursor.close()
        conn.close()