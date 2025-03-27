import requests
import time
import hashlib
import hmac
import configparser
import os
import json  # Thêm thư viện json để lưu file

# Đọc API Key từ file config
config = configparser.ConfigParser()
config.read("config.ini")

BINANCE_API_KEY = config["keys"]["BINANCE_API_KEY"].strip()
BINANCE_SECRET_KEY = config["keys"]["BINANCE_SECRET_KEY"].strip()

# Endpoint Binance
BINANCE_API_URL = "https://api.binance.com/api/v3/account"

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

response = requests.get(BINANCE_API_URL, headers=headers, params=params)

# Chuyển phản hồi sang JSON
data = response.json()

# Lưu vào file JSON
with open("binance_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("Dữ liệu đã được lưu vào binance_response.json")
