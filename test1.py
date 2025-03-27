import requests
import time
import hashlib
import hmac
import configparser
import urllib.parse
import os

# Đọc API Key từ file config
config = configparser.ConfigParser()
config.read("config.ini")

# API_KEY = config["keys"]["api_key"].strip()
# SECRET_KEY = config["keys"]["secret_key"].strip()


API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# Endpoint Binance
BINANCE_API_URL = "https://api.binance.com/api/v3/account"

# Tạo timestamp
timestamp = int(time.time() * 1000)

# Tạo chuỗi query string
query_string = f"timestamp={timestamp}"

# Tạo chữ ký (signature) bằng HMAC-SHA256
signature = hmac.new(
    SECRET_KEY, 
    query_string.encode('utf-8'), 
    hashlib.sha256
).hexdigest()

# Gửi request với chữ ký
headers = {
    "X-MBX-APIKEY": API_KEY
}

params = {
    "timestamp": timestamp,
    "signature": signature
}

response = requests.get(BINANCE_API_URL, headers=headers, params=params)

print(response.json())
