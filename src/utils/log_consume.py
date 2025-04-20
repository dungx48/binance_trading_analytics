from utils.logging_config import setup_logging
import logging

setup_logging(prefix="consume_klines", level=logging.INFO)
logger = logging.getLogger(__name__)

def log_info(msg):  logger.info(msg)
def log_error(msg): logger.error(msg)
