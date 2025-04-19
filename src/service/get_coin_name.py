from repository.postgredb.db_connection import DatabaseConnection
from utils.log_consume import log_info
import requests
import os
import time
import hmac
import hashlib

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY")

class CoinInfoService():
    def __init__(self):
        self.exchange_info_url = os.environ.get("EXCHANGE_INFO_URL")
    def get_all_coins(self):
        # Lấy dữ liệu từ API Binance
        response = requests.get(self.exchange_info_url)
        data = response.json()

        # Lấy danh sách các đồng coin từ các cặp giao dịch
        coins = set()  
        for symbol_info in data["symbols"]:
            base_asset = symbol_info["baseAsset"]
            coins.add(base_asset)
        return coins
    
    def get_coin_name(self):
        db = DatabaseConnection()
        conn = db.connection
        cursor = conn.cursor()
        
        """
            GET FULL NAME COIN
        """
        headers = {
            "X-MBX-APIKEY": BINANCE_API_KEY
        }
        BASE_URL = "https://api.binance.com"

        # Tạo timestamp
        timestamp = int(time.time() * 1000)

        # Tạo chuỗi query
        query_string = f"timestamp={timestamp}"

        # Tạo chữ ký HMAC SHA256
        signature = hmac.new(BINANCE_SECRET_KEY.encode(), query_string.encode(), hashlib.sha256).hexdigest()

        # Gửi request đến Binance
        url_infor = f"{BASE_URL}/sapi/v1/capital/config/getall?{query_string}&signature={signature}"
        response_info = requests.get(url_infor, headers=headers).json()

        bnb_dict = {coin["coin"].upper(): coin["name"] for coin in response_info}

        # Thêm dữ liệu vào PostgreSQL
        for symbol in self.get_all_coins():
            full_name = bnb_dict.get(symbol, symbol)  # Nếu không tìm thấy, giữ nguyên symbol
            sql = """
                INSERT INTO dim_coin (SYMBOL, COIN_NAME) 
                VALUES (%s, %s) 
                ON CONFLICT (SYMBOL) DO UPDATE 
                SET COIN_NAME = EXCLUDED.COIN_NAME;
            """
            values = (symbol, full_name)
            cursor.execute(sql, values)

        # Lưu thay đổi
        conn.commit()
        cursor.close()
        conn.close()

        log_info("Đã cập nhật danh sách coin vào PostgreSQL!")