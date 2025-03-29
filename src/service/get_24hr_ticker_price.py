import requests
import time
import hashlib
import hmac
import configparser
import json 

# Đọc API Key từ file config
config = configparser.ConfigParser()
config.read("../../config.ini")

BINANCE_API_KEY = config["binance_keys"]["BINANCE_API_KEY"].strip()
BINANCE_SECRET_KEY = config["binance_keys"]["BINANCE_SECRET_KEY"].strip()

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

print("Dữ liệu giá coin đã được lưu vào coin_price.json")