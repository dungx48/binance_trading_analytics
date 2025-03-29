import requests
import json

# Endpoint Binance
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# Cấu hình tham số
params = {
    "symbol": "BTCUSDT",  # Cặp coin
    "interval": "1h",  # Khung thời gian (1m, 5m, 1h, 1d, 1w...)
    "limit": 100  # Số lượng nến lấy về
}

# Gửi request
response = requests.get(BINANCE_API_URL, params=params)

# Chuyển phản hồi sang JSON
data = response.json()

# Lưu vào file
with open("btc_ohlc_1h.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("Dữ liệu nến đã được lưu vào btc_ohlc_1h.json")
