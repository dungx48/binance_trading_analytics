import logging
from logging.handlers import RotatingFileHandler
import os

# Tạo thư mục logs nếu chưa tồn tại
if not os.path.exists('logs'):
    os.makedirs('logs')

# Cấu hình logging
def setup_logging():
    """Cấu hình logging để ghi log vào cả console và file với xoay vòng file"""
    
    # Tạo handler cho log file với RotatingFileHandler
    file_handler = RotatingFileHandler(
        'logs/consume_klines.log',       # Tên file log chính
        maxBytes=5*1024*1024,           # Kích thước tối đa của mỗi file log là 5MB
        backupCount=3                   # Giữ lại tối đa 3 file backup (file cũ)
    )
    file_handler.setLevel(logging.INFO)  # Mức độ log cho file

    # Tạo handler cho log ra console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Mức độ log cho console

    # Cấu hình định dạng log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Thêm handler vào logger chính
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().addHandler(console_handler)
    
    # Đặt mức độ log của logger chính
    logging.getLogger().setLevel(logging.INFO)

# Gọi hàm setup_logging() để cấu hình logging
setup_logging()

def log_info(message):
    """Ghi log ở mức INFO"""
    logging.info(message)

def log_error(message):
    """Ghi log ở mức ERROR"""
    logging.error(message)
