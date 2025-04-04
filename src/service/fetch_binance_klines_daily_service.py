import requests
import pandas as pd
import time
from repository.fetch_klines_repo import FetchKlinesRepository
from model.request.klines_request import KlinesRequest


class FetchBinanceKlinesDailyService():
    def __init__(self):
        self.klines_daily_repo = FetchKlinesRepository()
        self.start_time = int(pd.Timestamp("2017-01-01").timestamp() * 1000)  # Dữ liệu từ 2017
        self.end_time = int(pd.Timestamp.now().timestamp() * 1000)
        self.interval = "1d"
        self.symbol = "BTCUSDT"
        
    # Hàm lấy dữ liệu từ Binance API
    def get_klines_daily(self, req: KlinesRequest):
        url = "https://api.binance.com/api/v3/klines"
        all_data = []
        current_time = req.start_time

        while req.start_time < req.end_time:
            params = {
                "symbol": req.symbol,
                "interval": req.interval,
                "startTime": current_time,
                "endTime": req.end_time,
                "limit": req.limit
            }

            response = requests.get(url, params=params)
            data = response.json()

            if not data:
                break

            all_data.extend(data)

            # Lấy timestamp của dòng cuối cùng làm mốc cho lần request tiếp theo
            current_time = data[-1][0] + 1  

            # Tránh bị rate limit
            time.sleep(1)

        return all_data
    def get_data_historical(self):
        KlinesRequest.symbol = self.symbol
        KlinesRequest.interval = self.interval
        KlinesRequest.start_time = self.start_time
        KlinesRequest.end_time = self.end_time
        
        self.get_klines_daily(KlinesRequest)
        



# if __name__ == "__main__":
#     symbol = "BTCUSDT"
#     interval = "1d"
#     start_time = int(pd.Timestamp("2017-01-01").timestamp() * 1000)  # Dữ liệu từ 2017
#     end_time = int(pd.Timestamp.now().timestamp() * 1000)

#     # Lấy dữ liệu từ Binance API
#     sv = FetchBinanceKlinesDailyService()
#     data = sv.get_historical_klines(symbol, interval, start_time, end_time)

#     if data:
#         db = FetchKlinesRepository()
#         db.save_to_db(data, symbol)
#     else:
#         print("⚠️ Không có dữ liệu để lưu!")