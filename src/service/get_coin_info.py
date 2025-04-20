import os
import logging
import requests
import psycopg2
from datetime import datetime
from tqdm import tqdm

from repository.postgredb.db_connection import DatabaseConnection

# Logger chuẩn cho module này
logger = logging.getLogger(__name__)

class CoinInfoSyncService:
    def __init__(self):
        # API endpoint của CoinPaprika
        self.api_url = "https://api.coinpaprika.com/v1/tickers"
        # Kết nối đến PostgreSQL qua DatabaseConnection
        db = DatabaseConnection()
        self.conn = db.connection
        # Mỗi câu lệnh sẽ tự động commit
        self.conn.autocommit = True

    def fetch_coin_data(self):
        logger.info("📡 Đang gọi API CoinPaprika...")
        response = requests.get(self.api_url)
        response.raise_for_status()
        return response.json()

    def save_to_db(self, coins):
        cur = self.conn.cursor()

        for coin in tqdm(coins, desc="💾 Sync coin info"):
            try:
                # Chuyển chuỗi ISO sang date (không lấy thời gian)
                first_data = self.parse_date(coin.get('first_data_at'))
                last_updated = self.parse_date(coin.get('last_updated'))

                cur.execute(
                    """
                    INSERT INTO dim_coin_info (
                        id, name, symbol, rank,
                        total_supply, max_supply, beta_value,
                        first_data_at, last_updated
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        symbol = EXCLUDED.symbol,
                        rank = EXCLUDED.rank,
                        total_supply = EXCLUDED.total_supply,
                        max_supply = EXCLUDED.max_supply,
                        beta_value = EXCLUDED.beta_value,
                        first_data_at = EXCLUDED.first_data_at,
                        last_updated = EXCLUDED.last_updated;
                    """,
                    (
                        coin.get('id'),
                        coin.get('name'),
                        coin.get('symbol'),
                        coin.get('rank'),
                        coin.get('total_supply'),
                        coin.get('max_supply'),
                        coin.get('beta_value'),
                        first_data,
                        last_updated
                    )
                )
            except Exception as e:
                # Bỏ qua coin lỗi, tiếp tục với coin kế tiếp
                logger.error(f"❌ Lỗi khi xử lý coin {coin.get('id')}: {e}")
                continue

        cur.close()
        self.conn.close()
        logger.info("✅ Hoàn tất insert/update dữ liệu coin info.")

    def parse_date(self, dt_str):
        """
        Chuyển chuỗi ISO 8601 (YYYY-MM-DDThh:mm:ssZ) về date (YYYY-MM-DD)
        Trả về None nếu dt_str không hợp lệ hoặc rỗng.
        """
        if not dt_str:
            return None
        try:
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").date()
        except Exception:
            return None

    def run_sync(self):
        coins = self.fetch_coin_data()
        self.save_to_db(coins)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    service = CoinInfoSyncService()
    service.run_sync()
