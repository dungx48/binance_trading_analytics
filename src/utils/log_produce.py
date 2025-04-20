from utils.logging_config import setup_logging
import logging

# khởi tạo khi import module
setup_logging(prefix="produce_klines", level=logging.INFO)
logger = logging.getLogger(__name__)
