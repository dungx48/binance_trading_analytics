import requests
import json

# Endpoint Binance để lấy giá coin
BINANCE_API_URL = "https://api.binance.com/fapi/v1/ticker/24hr"

# Chọn cặp coin muốn lấy giá
symbol = "BTCUSDT"

# Gửi request
response = requests.get(BINANCE_API_URL, params={"symbol": symbol})

# Chuyển phản hồi sang JSON
data = response.json()

# Lưu vào file JSON
with open("coin_price.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)