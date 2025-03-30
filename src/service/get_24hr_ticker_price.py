import requests
import time
import hashlib
import hmac
import configparser
import json 
import os
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

# Tạo timestamp
timestamp = int(time.time() * 1000)

# Tạo chuỗi query string
query_string = f"timestamp={timestamp}"

# Tạo chữ ký (signature) bằng HMAC-SHA256
signature = hmac.new(
    BINANCE_SECRET_KEY.encode('utf-8'), 
    query_string.encode('utf-8'), 
    hashlib.sha256
).hexdigest()

# Gửi request với chữ ký
headers = {
    "X-MBX-APIKEY": BINANCE_API_KEY
}

params = {
    "timestamp": timestamp,
    "signature": signature
}
# Endpoint Binance để lấy giá coin
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/24hr"

# Chọn cặp coin muốn lấy giá
symbol = "BTCUSDT"

# Gửi request
response = requests.get(BINANCE_API_URL, params={"symbol": symbol})

# Chuyển phản hồi sang JSON
data = response.json()

# Lưu vào file JSON
with open("coin_price.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)
