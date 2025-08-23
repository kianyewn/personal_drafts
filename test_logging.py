# config/logging.py
from loguru import logger
import sys
import os
from pathlib import Path


def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    enable_console: bool = True,
    enable_file: bool = True,
    enable_json: bool = False,
):
    """Configure logging for the application"""

    # Remove default handler
    logger.remove()

    # Create log directory
    Path(log_dir).mkdir(exist_ok=True)

    # Console logging
    if enable_console:
        if log_level == 'DEBUG':
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level> - <yellow>{extra}</yellow>",
                level=log_level,
                colorize=True,
            )
        else:
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
                level=log_level,
                colorize=True,
            )

    # File logging
    if enable_file:
        logger.add(
            f"{log_dir}/app.log",
            rotation="100 MB",  # new file every 100 mb
            retention="30 days", 
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
        )

    # JSON logging for production
    if enable_json:
        logger.add(
            f"{log_dir}/app.json",
            format="{time} | {level} | {message} | {extra}",
            serialize=True,
            rotation="1 day",
        )

    logger.info("Logging configured", log_level=log_level, log_dir=log_dir)


# Usage in main.py
if __name__ == "__main__":
    setup_logging(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_dir=os.getenv("LOG_DIR", "logs"),
        enable_json=os.getenv("ENVIRONMENT") == "production",
    )
