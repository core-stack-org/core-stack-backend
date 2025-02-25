import logging
import logging.handlers
import os
from datetime import datetime
from typing import Optional

LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure the logger
def setup_logger(name: str = 'nrm_app') -> logging.Logger:
    """
    Configure and return a global logger instance.

    logger.debug("Some debug message")  # Only appears in log file
    logger.info("Processing started")   # Appears in both terminal and log file
    logger.error("An error occurred")   # Appears in both terminal and log file
    
    console_handler.setLevel(logging.INFO) # set to DEBUG to see all messages

    Args:
        name (str): Name of the logger. Defaults to 'nrm_app'
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler (rotating file handler to manage log size)
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(LOGS_DIR, 'nrm_app.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# Create a global logger instance
logger = setup_logger()

# Convenience functions
def debug(message: str) -> None:
    """Log a debug message."""
    logger.debug(message)

def info(message: str) -> None:
    """Log an info message."""
    logger.info(message)

def warning(message: str) -> None:
    """Log a warning message."""
    logger.warning(message)

def error(message: str) -> None:
    """Log an error message."""
    logger.error(message)

def critical(message: str) -> None:
    """Log a critical message."""
    logger.critical(message)

def exception(message: str) -> None:
    """Log an exception message with traceback."""
    logger.exception(message)
