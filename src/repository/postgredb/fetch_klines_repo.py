import time
import math
import logging
from repository.postgredb.db_connection import DatabaseConnection

logger = logging.getLogger(__name__)

class FetchKlinesRepository:
    def __init__(self, conn=None, cursor=None):
        if conn and cursor:
            self.conn = conn
            self.cursor = cursor
            self._own_connection = False
        else:
            db = DatabaseConnection()
            self.conn = db.connection
            self.cursor = self.conn.cursor()
            self._own_connection = True

    def save_batches(self, data, base_symbol, quote_symbol, batch_size=None):
        """
        Chia nhỏ `data` thành các batch và insert vào DB.
        - `data`: list các kline (mỗi phần tử là tuple/row)
        - `base_symbol`, `quote_symbol`: dùng để insert kèm
        - `batch_size`: số dòng mỗi lần executemany (mặc định 10% tổng)
        """
        try:
            formatted = [
                (
                    base_symbol,
                    quote_symbol,
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
            # ON CONFLICT để tránh duplicate
            sql = """
            INSERT INTO fact_coin_metrics (
                base_symbol, quote_symbol, trade_date, open_price,
                high_price, low_price, close_price, volume, trade_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (base_symbol, trade_date) DO NOTHING;
            """

            total = len(formatted)
            if total == 0:
                logger.warning(f"⚠️ {base_symbol}: Không có dữ liệu để insert.")
                return

            batch_size = batch_size or max(1, math.ceil(total * 0.1))
            bar_len = 20
            start_all = time.time()

            for i in range(0, total, batch_size):
                start = time.time()
                chunk = formatted[i : i + batch_size]
                self.cursor.executemany(sql, chunk)
                self.conn.commit()
                # Ghi log thanh tiến trình + thời gian batch
                elapsed = time.time() - start
                done = i + len(chunk)
                pct = done / total * 100
                filled = int(bar_len * done // total)
                bar = '█' * filled + '-' * (bar_len - filled)

                logger.info(f"{base_symbol} ✅ [{bar}] {pct:.2f}% - {done}/{total} dòng - ⏱ {elapsed:.2f}s")
            # Ghi log tổng thời gian
            logger.info(f"🏁 {base_symbol} - Hoàn tất insert, tổng thời gian {time.time()-start_all:.2f}s")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ {base_symbol} - Lỗi khi insert dữ liệu: {e}")
            raise

    def close(self):
        """
        Đóng connection/cursor nếu chính repository tự tạo.
        Service truyền conn/cursor vào sẽ tự handle đóng.
        """
        if self._own_connection:
            self.cursor.close()
            self.conn.close()
            logger.info("🔒 Đã đóng kết nối với DB")