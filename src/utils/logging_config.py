import os, sys, logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def setup_logging(prefix: str, level: int = logging.INFO):
    """
    Cấu hình chung cho cả producer & consumer.
    prefix: phần tiền tố để đặt tên file (ví dụ 'produce_klines' / 'consume_klines')
    """
    log_dir = os.environ.get("CUSTOM_LOG_DIR", "/app/logs/app")
    os.makedirs(log_dir, exist_ok=True)

    path = os.path.join(log_dir, f"{prefix}.log")

    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    root = logging.getLogger()
    root.setLevel(level)
    if root.hasHandlers():
        root.handlers.clear()

    fh = TimedRotatingFileHandler(
        filename=path,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8",
        utc=True
    )
    fh.setFormatter(formatter)
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # Giảm bớt log thư viện thứ 3
    for lib in ("urllib3", "requests", "botocore"):
        logging.getLogger(lib).setLevel(logging.WARNING)
