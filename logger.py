"""
logger.py

Central logging module for rawprocessor.
- Loguru-based, per-site file rotation.
- Decorators/context managers for logging, error handling, and tracing.
- Production-grade, can be extended for external logging backends.

UPDATED: Now uses centralized configuration system for log settings.
"""

import sys
from loguru import logger
from functools import wraps
from typing import Callable

from config import config

def configure_logger(site: str, log_dir: str = "./logs") -> None:
    """
    Configure loguru to log to a per-site file and stderr with rotation.
    Uses configuration values for rotation and retention settings.
    """
    logger.remove()
    log_path = f"{log_dir}/{site}.log"
    
    # Use configured values for rotation and retention
    logger.add(
        log_path, 
        rotation=config.LOG_ROTATION_SIZE, 
        retention=config.LOG_RETENTION_DAYS, 
        backtrace=True, 
        diagnose=True, 
        level=config.LOG_LEVEL
    )
    
    # Add stderr logging with configured level
    logger.add(sys.stderr, level=config.LOG_LEVEL)
    
    logger.info(f"Logger configured for site '{site}' with rotation={config.LOG_ROTATION_SIZE}, retention={config.LOG_RETENTION_DAYS}, level={config.LOG_LEVEL}")

def configure_system_logger(log_dir: str = "./logs") -> None:
    """
    Configure logger for system-wide operations (not site-specific).
    """
    logger.remove()
    
    # System log file
    system_log_path = f"{log_dir}/system.log"
    logger.add(
        system_log_path,
        rotation=config.LOG_ROTATION_SIZE,
        retention=config.LOG_RETENTION_DAYS,
        backtrace=True,
        diagnose=True,
        level=config.LOG_LEVEL
    )
    
    # Console logging
    logger.add(sys.stderr, level=config.LOG_LEVEL)
    
    logger.info(f"System logger configured with rotation={config.LOG_ROTATION_SIZE}, retention={config.LOG_RETENTION_DAYS}, level={config.LOG_LEVEL}")

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
    Only logs if debug level is enabled to avoid spam.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if logger.level(config.LOG_LEVEL).no <= logger.level("DEBUG").no:
            logger.debug(f"Entering: {func.__name__}")
        
        result = await func(*args, **kwargs)
        
        if logger.level(config.LOG_LEVEL).no <= logger.level("DEBUG").no:
            logger.debug(f"Exiting: {func.__name__}")
        
        return result
    return wrapper

def performance_log(func: Callable) -> Callable:
    """
    Decorator to log function execution time.
    Only logs if performance monitoring is enabled.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if not config.ENABLE_PERFORMANCE_MONITORING:
            return await func(*args, **kwargs)
        
        import time
        start_time = time.time()
        
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            if execution_time > 1.0:  # Only log if execution takes more than 1 second
                logger.info(f"Performance: {func.__name__} took {execution_time:.2f}s")
            
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Performance: {func.__name__} failed after {execution_time:.2f}s")
            raise
    
    return wrapper

class LogContext:
    """
    Context manager for adding contextual information to logs.
    """
    
    def __init__(self, **context):
        self.context = context
        self.token = None
    
    def __enter__(self):
        # Add context to logger
        context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
        self.token = logger.contextualize(**self.context)
        logger.info(f"Context started: {context_str}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            logger.error(f"Context ended with exception: {context_str}")
        else:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            logger.info(f"Context completed: {context_str}")

def get_logger_config_summary() -> dict:
    """
    Get summary of current logging configuration.
    """
    return {
        "log_level": config.LOG_LEVEL,
        "rotation_size": config.LOG_ROTATION_SIZE,
        "retention_days": config.LOG_RETENTION_DAYS,
        "performance_monitoring": config.ENABLE_PERFORMANCE_MONITORING,
        "handlers": len(logger._core.handlers)
    }

def validate_log_configuration() -> dict:
    """
    Validate that logging configuration is sensible.
    """
    issues = []
    warnings = []
    
    # Check log level
    valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    if config.LOG_LEVEL not in valid_levels:
        issues.append(f"Invalid log level: {config.LOG_LEVEL}")
    
    # Check rotation size
    if not config.LOG_ROTATION_SIZE:
        issues.append("LOG_ROTATION_SIZE cannot be empty")
    elif config.LOG_ROTATION_SIZE.endswith("KB") and int(config.LOG_ROTATION_SIZE[:-2]) < 100:
        warnings.append("Very small rotation size may cause excessive file creation")
    
    # Check retention
    if not config.LOG_RETENTION_DAYS:
        issues.append("LOG_RETENTION_DAYS cannot be empty")
    elif config.LOG_RETENTION_DAYS.endswith("days"):
        try:
            days = int(config.LOG_RETENTION_DAYS.split()[0])
            if days < 1:
                issues.append("LOG_RETENTION_DAYS must be at least 1 day")
            elif days > 365:
                warnings.append("Very long retention period may use excessive disk space")
        except ValueError:
            issues.append(f"Invalid LOG_RETENTION_DAYS format: {config.LOG_RETENTION_DAYS}")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "configuration": get_logger_config_summary()
    }

# Auto-configure system logger on import if not already configured
try:
    if not logger._core.handlers:
        configure_system_logger()
except Exception as ex:
    print(f"Warning: Could not auto-configure logger: {ex}")