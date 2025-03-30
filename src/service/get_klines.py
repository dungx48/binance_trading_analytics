import requests
import pandas as pd
import json

# Gọi API Binance
url = "https://api.binance.com/api/v3/klines"
params = {
    "symbol": "BTCUSDT",
    "interval": "1h",
    "limit": 1000
}
response = requests.get(url, params=params)
data = response.json()

# Chuyển dữ liệu vào DataFrame
df = pd.DataFrame(data, columns=[
    "timestamp", "open", "high", "low", "close", "volume", 
    "close_time", "quote_volume", "trades", 
    "taker_base", "taker_quote", "ignore"
])

# Chuyển timestamp về datetime
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

# Giữ lại các cột quan trọng
df = df[["timestamp", "open", "high", "low", "close", "volume"]]

# Chuyển dữ liệu về dạng số
df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

print(df.head())
