import os
import logging
import logging.handlers
from datetime import datetime, timedelta
from config.kafka_config import LOG_LEVEL

LOG_DIR = "Logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger(name="kafka", level=None):

    # get config from environment
    log_config = {
        'level': LOG_LEVEL,
        'max_file_size': 1024 * 1024 * 5,
        'backup_count': 5
    }

    if level is None:
        level = LOG_LEVEL

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File Handler - All logs
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, f'{name}.log'),
        maxBytes=log_config['max_file_size'],
        backupCount=log_config['backup_count'],
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    log_to_console = os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true'
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(file_formatter)
        logger.addHandler(console_handler)

    # File Handler riêng cho Error logs 
    if name != "error":  # Tránh duplicate cho error logger
        error_handler = logging.handlers.RotatingFileHandler(
            os.path.join(LOG_DIR, 'error.log'),
            maxBytes=log_config['max_file_size'] // 2,
            backupCount=log_config['backup_count'],
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        logger.addHandler(error_handler)
    
    return logger

def setup_mongodb_logger():
    """Cấu hình logger riêng cho MongoDB - sử dụng settings từ environment"""
    mongo_logger = logging.getLogger("mongodb")
    
    # Lấy config từ environment
    log_config = get_log_config()
    mongo_logger.setLevel(log_config['level'])
    
    if mongo_logger.handlers:
        return mongo_logger
    
    # Formatter đơn giản cho MongoDB
    mongo_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | MONGO | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # MongoDB file handler (sử dụng config từ env)
    mongo_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, 'mongodb.log'),
        maxBytes=log_config['max_file_size'] * 2, 
        backupCount=log_config['backup_count'],
        encoding='utf-8'
    )
    mongo_handler.setLevel(log_config['level'])
    mongo_handler.setFormatter(mongo_formatter)
    mongo_logger.addHandler(mongo_handler)
    
    return mongo_logger

def log_performance(operation, collection, count=None, duration=None):
    """Log hiệu suất - version đơn giản"""
    perf_logger = logging.getLogger("performance")
    
    message = f"📊 {operation} | {collection}"
    if count is not None:
        message += f" | Count: {count}"
    if duration is not None:
        message += f" | Time: {duration:.3f}s"
    
    perf_logger.info(message) 
