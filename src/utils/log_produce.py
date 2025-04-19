from datetime import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler

DEFAULT_LOG_DIR = os.environ.get("CUSTOM_LOG_DIR",  "/opt/airflow/logs/app")

class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg.encode(stream.encoding or "utf-8", errors="replace").decode() + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_basic_logging(level=logging.INFO, log_dir=DEFAULT_LOG_DIR, log_prefix="produce_klines"):
    os.makedirs(log_dir, exist_ok=True)

    # Dùng định dạng ngày (không có giờ phút giây) để chỉ có 1 file log/ngày
    today_str = datetime.utcnow().strftime("%Y-%m-%d")  # dùng UTC để khớp với when='midnight'
    log_filename = os.path.join(DEFAULT_LOG_DIR, f"{log_prefix}.{today_str}.log")
    # log_path = os.path.join(log_dir, log_filename)

    log_format = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    file_handler = TimedRotatingFileHandler(
        log_filename, when="midnight", interval=1, backupCount=7, encoding="utf-8", utc=True
    )
    file_handler.setFormatter(log_format)
    root_logger.addHandler(file_handler)

    console_handler = SafeConsoleHandler()
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    # Loại bỏ log từ urllib3 và các thư viện bên thứ ba
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
