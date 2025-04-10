import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from datetime import datetime

LOG_DIR = os.environ.get("CUSTOM_LOG_DIR", "/opt/airflow/logs/app")
LOG_FILE_BASE = 'consume_klines'

# Tạo thư mục logs nếu chưa tồn tại
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging():
    """Cấu hình logging: log file theo ngày ngay từ lần đầu tiên"""

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Xóa các handler cũ
    if logger.hasHandlers():
        logger.handlers.clear()

    # Tên file log có đuôi ngày luôn, ngay lần đầu tiên
    today_str = datetime.utcnow().strftime("%Y-%m-%d")  # dùng UTC để khớp với when='midnight'
    log_file_path = os.path.join(LOG_DIR, f"{LOG_FILE_BASE}.{today_str}.log")

    # File handler với xoay file hằng ngày
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8',
        utc=True
    )
    file_handler.setLevel(logging.INFO)
    file_handler.suffix = "%Y-%m-%d"

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Format log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Gắn handler
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Gọi khi file được import
setup_logging()

# Hàm tiện dụng
def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

def log_warning(message):
    logging.warning(message)
