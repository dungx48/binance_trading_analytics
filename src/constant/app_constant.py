from typing import Final

class AppConst:
    ONE_DAY: Final[str] = '1d'
    USDT: Final[str] = 'USDT'
    SEVEN_AM: Final[str] = '12:34'
    REDIS_KLINES_QUEUE = "klines_queue"

class MODE_RUN_JOB:
    DAILY: Final[str] = 'daily'
    ALL: Final[str] = 'all'