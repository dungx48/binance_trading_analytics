import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Log ra console
    ]
)

def log_info(message):
    """Ghi log ở mức INFO"""
    logging.info(message)

def log_error(message):
    """Ghi log ở mức ERROR"""
    logging.error(message)
