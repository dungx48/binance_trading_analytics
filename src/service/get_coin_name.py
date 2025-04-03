from database.db_connection import get_db_connection
from utils.logger import log_info
import requests
import os
from dotenv import load_dotenv
import time
import hmac
import hashlib

load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")


def get_coin_name():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Lấy dữ liệu từ API Binance
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    # Lấy danh sách các đồng coin từ các cặp giao dịch
    coins = set()  
    for symbol_info in data["symbols"]:
        base_asset = symbol_info["baseAsset"]
        coins.add(base_asset)
    
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
    for symbol in coins:
        full_name = bnb_dict.get(symbol, symbol)  # Nếu không tìm thấy, giữ nguyên symbol
        sql = """
            INSERT INTO CRYPTO_COINS (COIN_CODE, COIN_NAME) 
            VALUES (%s, %s) 
            ON CONFLICT (COIN_CODE) DO UPDATE 
            SET COIN_NAME = EXCLUDED.COIN_NAME;
        """
        values = (symbol, full_name)
        cursor.execute(sql, values)

    # Lưu thay đổi
    conn.commit()
    cursor.close()
    conn.close()

    log_info("Đã cập nhật danh sách coin vào PostgreSQL!")


if __name__ == "__main__":
    get_coin_name()
