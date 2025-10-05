"""
Logging utilities for India Stocks API
"""

import logging
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """
    Get logger instance with consistent formatting

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Set up formatter if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


def setup_logging(level: str = "INFO", to_file: bool = False, log_dir: str = "logs"):
    """
    Setup logging configuration

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        to_file: Whether to log to file
        log_dir: Directory for log files
    """
    # Set root logger level
    logging.getLogger().setLevel(getattr(logging, level.upper(), logging.INFO))

    # Clear existing handlers
    logging.getLogger().handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    logging.getLogger().addHandler(console_handler)

    # File handler (if requested)
    if to_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)

        file_handler = logging.FileHandler(log_path / "broker_operations.log")
        file_formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    get_logger(__name__).info(f"Logging configured: level={level}, to_file={to_file}")


# Initialize logging on import
setup_logging()
