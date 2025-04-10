import pandas as pd

class TimeEnums:
    """
    Lớp chứa các mốc thời gian sử dụng cho việc lấy dữ liệu từ Binance.
    Tất cả được trả về dưới dạng int (milliseconds từ epoch).
    """

    @staticmethod
    def START_TIME() -> int:
        return _to_milliseconds(pd.Timestamp("2017-01-01"))

    @staticmethod
    def YESTERDAY() -> int:
        # Hôm qua lúc 00:00:00 UTC
        ts = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)
        return _to_milliseconds(ts)

    @staticmethod
    def YESTERDAY_END() -> int:
        # Hôm qua lúc 23:59:59.999 UTC
        ts = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)
        ts_end = ts + pd.Timedelta(hours=23, minutes=59, seconds=59, milliseconds=999)
        return _to_milliseconds(ts_end)

def _to_milliseconds(ts: pd.Timestamp) -> int:
    """
    Chuyển Timestamp thành số nguyên (milliseconds từ epoch)
    """
    return int(ts.timestamp() * 1000)
