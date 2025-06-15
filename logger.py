# logger.py

"""
Central logging module for rawprocessor.
- Loguru-based, per-site file rotation.
- Decorators/context managers for logging, error handling, and tracing.
- Production-grade, can be extended for external logging backends.
"""

import sys
from loguru import logger
from functools import wraps
from typing import Callable

def configure_logger(site: str, log_dir: str = "./logs") -> None:
    """
    Configure loguru to log to a per-site file and stderr with rotation.
    """
    logger.remove()
    log_path = f"{log_dir}/{site}.log"
    logger.add(log_path, rotation="10 MB", retention="7 days", backtrace=True, diagnose=True, level="INFO")
    logger.add(sys.stderr, level="INFO")

def log_exceptions(func: Callable) -> Callable:
    """
    Decorator to log exceptions with full stack trace.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.opt(exception=True).error(f"Exception in {func.__name__}: {str(e)}")
            raise
    return wrapper

def trace(func: Callable) -> Callable:
    """
    Decorator to log entry and exit of a function.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        logger.info(f"Entering: {func.__name__}")
        result = await func(*args, **kwargs)
        logger.info(f"Exiting: {func.__name__}")
        return result
    return wrapper
