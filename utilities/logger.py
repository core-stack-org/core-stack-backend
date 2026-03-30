import logging


def setup_logger(name: str = "nrm_app") -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)

        logger.addHandler(console_handler)

    return logger


logger = setup_logger()


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
