import time
import math
from repository.db_connection import DatabaseConnection
from utils.log_consume import log_info, log_error

class FetchKlinesRepository():
    def __init__(self):
        pass

    def save_to_db(self, data, symbol):
        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()

        try:
            formatted_data = [
                (
                    symbol,
                    row[0],  # trade_date
                    row[1],  # open_price
                    row[2],  # high_price
                    row[3],  # low_price
                    row[4],  # close_price
                    row[5],  # volume
                    row[8]   # trade_count
                )
                for row in data
            ]

            sql = """
            INSERT INTO daily_klines (
                symbol, trade_date, open_price, high_price,
                low_price, close_price, volume, trade_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) DO NOTHING;
            """

            total = len(formatted_data)
            batch_size = max(1, math.ceil(total * 0.1))  # 10%

            progress_bar_length = 20  # Độ dài thanh tiến trình
            overall_start = time.time()

            for i in range(0, total, batch_size):
                batch_start = time.time()

                batch = formatted_data[i:i+batch_size]
                cursor.executemany(sql, batch)
                conn.commit()

                batch_end = time.time()
                inserted = i + len(batch)
                percent_done = (inserted / total) * 100

                # Vẽ thanh tiến trình
                filled_length = int(progress_bar_length * inserted // total)
                bar = '█' * filled_length + '-' * (progress_bar_length - filled_length)

                log_info(
                    f"{symbol} ✅ [{bar}] {percent_done:.2f}% - {inserted}/{total} dòng "
                    f"- ⏱ {batch_end - batch_start:.2f} giây"
                )

            overall_end = time.time()
            log_info(f"🏁 {symbol} - Hoàn tất insert, tổng thời gian: {overall_end - overall_start:.2f} giây")

        except Exception as e:
            conn.rollback()
            log_error(f"❌ {symbol} - Lỗi khi insert dữ liệu vào DB: {e}")

        finally:
            cursor.close()
            conn.close()
            log_info(f"🔒 {symbol} - Đã đóng kết nối với DB")
