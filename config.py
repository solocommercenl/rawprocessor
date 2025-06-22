"""
config.py

Centralized configuration system for rawprocessor.
Loads all configuration from environment variables with sensible defaults.
Single source of truth for all configurable values.
"""

import os
from typing import Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_env_int(key: str, default: int) -> int:
    """Get integer environment variable with default."""
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

def get_env_float(key: str, default: float) -> float:
    """Get float environment variable with default."""
    try:
        return float(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

def get_env_bool(key: str, default: bool) -> bool:
    """Get boolean environment variable with default."""
    value = os.environ.get(key, str(default)).lower()
    return value in ('true', '1', 'yes', 'on')

def get_env_str(key: str, default: str) -> str:
    """Get string environment variable with default."""
    return os.environ.get(key, default)

class Config:
    """Centralized configuration for rawprocessor."""
    
    # === CORE DATABASE ===
    MONGO_URI = get_env_str("MONGO_URI", "")
    MONGO_DB = get_env_str("MONGO_DB", "autodex")
    
    # === DATABASE CONNECTION ===
    MONGO_MAX_POOL_SIZE = get_env_int("MONGO_MAX_POOL_SIZE", 50)
    MONGO_SERVER_SELECTION_TIMEOUT = get_env_int("MONGO_SERVER_SELECTION_TIMEOUT", 5000)
    MONGO_CONNECT_TIMEOUT = get_env_int("MONGO_CONNECT_TIMEOUT", 10000)
    MONGO_SOCKET_TIMEOUT = get_env_int("MONGO_SOCKET_TIMEOUT", 30000)
    
    # === PROCESSING QUEUE ===
    QUEUE_MAX_WORKERS = get_env_int("QUEUE_MAX_WORKERS", 5)
    QUEUE_BATCH_SIZE = get_env_int("QUEUE_BATCH_SIZE", 50)
    QUEUE_POLL_INTERVAL = get_env_float("QUEUE_POLL_INTERVAL", 1.0)
    QUEUE_MAX_PROCESSING_TIME = get_env_int("QUEUE_MAX_PROCESSING_TIME", 300)
    QUEUE_RETRY_LIMIT = get_env_int("QUEUE_RETRY_LIMIT", 3)
    QUEUE_RETRY_DELAY = get_env_int("QUEUE_RETRY_DELAY", 60)
    
    # === JOB TIMEOUTS ===
    QUEUE_TIMEOUT_SHORT = get_env_int("QUEUE_TIMEOUT_SHORT", 300)      # 5 minutes - single record
    QUEUE_TIMEOUT_MEDIUM = get_env_int("QUEUE_TIMEOUT_MEDIUM", 600)    # 10 minutes - batch jobs
    QUEUE_TIMEOUT_LONG = get_env_int("QUEUE_TIMEOUT_LONG", 1800)       # 30 minutes - full site
    
    # === MAINTENANCE SCHEDULES ===
    CLEANUP_INTERVAL_HOURS = get_env_int("CLEANUP_INTERVAL_HOURS", 8)
    MAINTENANCE_INTERVAL_HOURS = get_env_int("MAINTENANCE_INTERVAL_HOURS", 1)
    JOB_CLEANUP_RETENTION_HOURS = get_env_int("JOB_CLEANUP_RETENTION_HOURS", 24)
    
    # === DATA QUALITY RULES ===
    MIN_IMAGES_REQUIRED = get_env_int("MIN_IMAGES_REQUIRED", 4)
    EMISSIONS_ZERO_THRESHOLD = get_env_int("EMISSIONS_ZERO_THRESHOLD", 0)
    
    # === FINANCIAL CALCULATIONS ===
    VAT_RATE_NL = get_env_float("VAT_RATE_NL", 0.21)  # Dutch VAT 21%
    VAT_RATE_DE = get_env_float("VAT_RATE_DE", 0.19)  # German VAT 19%
    
    # BPM PHEV thresholds by registration period
    BPM_PHEV_THRESHOLD_OLD = get_env_int("BPM_PHEV_THRESHOLD_OLD", 50)  # Pre-2020 or 2020 H1
    BPM_PHEV_THRESHOLD_NEW = get_env_int("BPM_PHEV_THRESHOLD_NEW", 60)  # 2020 H2+
    
    # Default leasing percentages
    DEFAULT_DOWN_PAYMENT_PCT = get_env_float("DEFAULT_DOWN_PAYMENT_PCT", 0.10)      # 10%
    DEFAULT_REMAINING_DEBT_PCT = get_env_float("DEFAULT_REMAINING_DEBT_PCT", 0.20)  # 20%
    
    # === HEALTH CHECK SERVER ===
    HEALTH_HOST = get_env_str("HEALTH_HOST", "localhost")
    HEALTH_PORT = get_env_int("HEALTH_PORT", 8080)
    HEALTH_AUTH_USER = get_env_str("HEALTH_AUTH_USER", "")
    HEALTH_AUTH_PASS = get_env_str("HEALTH_AUTH_PASS", "")
    HEALTH_TIMEOUT = get_env_int("HEALTH_TIMEOUT", 5000)
    
    # === LOGGING ===
    LOG_ROTATION_SIZE = get_env_str("LOG_ROTATION_SIZE", "10 MB")
    LOG_RETENTION_DAYS = get_env_str("LOG_RETENTION_DAYS", "7 days")
    LOG_LEVEL = get_env_str("LOG_LEVEL", "INFO")
    
    # === MONITORING ===
    MONITOR_REFRESH_INTERVAL = get_env_int("MONITOR_REFRESH_INTERVAL", 5)
    MONITOR_DEFAULT_WORKERS = get_env_int("MONITOR_DEFAULT_WORKERS", 5)
    
    # === PERFORMANCE TUNING ===
    ENABLE_BATCH_PROCESSING = get_env_bool("ENABLE_BATCH_PROCESSING", True)
    MAX_CONCURRENT_SITES = get_env_int("MAX_CONCURRENT_SITES", 10)
    CHANGE_STREAM_BATCH_SIZE = get_env_int("CHANGE_STREAM_BATCH_SIZE", 1000)
    
    # === FEATURE FLAGS ===
    ENABLE_PERIODIC_CLEANUP = get_env_bool("ENABLE_PERIODIC_CLEANUP", True)
    ENABLE_HEALTH_SERVER = get_env_bool("ENABLE_HEALTH_SERVER", True)
    ENABLE_PERFORMANCE_MONITORING = get_env_bool("ENABLE_PERFORMANCE_MONITORING", True)
    
    @classmethod
    def validate(cls) -> bool:
        """
        Validate required configuration is present.
        Returns True if valid, raises ValueError if invalid.
        """
        if not cls.MONGO_URI:
            raise ValueError("MONGO_URI environment variable is required")
        
        if cls.QUEUE_MAX_WORKERS < 1:
            raise ValueError("QUEUE_MAX_WORKERS must be at least 1")
        
        if cls.QUEUE_BATCH_SIZE < 1:
            raise ValueError("QUEUE_BATCH_SIZE must be at least 1")
        
        if cls.MIN_IMAGES_REQUIRED < 1:
            raise ValueError("MIN_IMAGES_REQUIRED must be at least 1")
        
        if not (0 <= cls.VAT_RATE_NL <= 1):
            raise ValueError("VAT_RATE_NL must be between 0 and 1")
        
        return True
    
    @classmethod
    def summary(cls) -> dict:
        """Get configuration summary for logging/debugging."""
        return {
            "database": {
                "uri_configured": bool(cls.MONGO_URI),
                "database": cls.MONGO_DB,
                "pool_size": cls.MONGO_MAX_POOL_SIZE,
                "timeouts": {
                    "selection": cls.MONGO_SERVER_SELECTION_TIMEOUT,
                    "connect": cls.MONGO_CONNECT_TIMEOUT,
                    "socket": cls.MONGO_SOCKET_TIMEOUT
                }
            },
            "processing": {
                "max_workers": cls.QUEUE_MAX_WORKERS,
                "batch_size": cls.QUEUE_BATCH_SIZE,
                "poll_interval": cls.QUEUE_POLL_INTERVAL,
                "retry_limit": cls.QUEUE_RETRY_LIMIT
            },
            "timeouts": {
                "short": cls.QUEUE_TIMEOUT_SHORT,
                "medium": cls.QUEUE_TIMEOUT_MEDIUM,
                "long": cls.QUEUE_TIMEOUT_LONG
            },
            "maintenance": {
                "cleanup_hours": cls.CLEANUP_INTERVAL_HOURS,
                "maintenance_hours": cls.MAINTENANCE_INTERVAL_HOURS,
                "retention_hours": cls.JOB_CLEANUP_RETENTION_HOURS
            },
            "data_quality": {
                "min_images": cls.MIN_IMAGES_REQUIRED,
                "emissions_threshold": cls.EMISSIONS_ZERO_THRESHOLD
            },
            "financial": {
                "vat_nl": cls.VAT_RATE_NL,
                "vat_de": cls.VAT_RATE_DE,
                "down_payment": cls.DEFAULT_DOWN_PAYMENT_PCT,
                "remaining_debt": cls.DEFAULT_REMAINING_DEBT_PCT
            },
            "health_server": {
                "host": cls.HEALTH_HOST,
                "port": cls.HEALTH_PORT,
                "auth_enabled": bool(cls.HEALTH_AUTH_USER)
            },
            "features": {
                "batch_processing": cls.ENABLE_BATCH_PROCESSING,
                "periodic_cleanup": cls.ENABLE_PERIODIC_CLEANUP,
                "health_server": cls.ENABLE_HEALTH_SERVER,
                "performance_monitoring": cls.ENABLE_PERFORMANCE_MONITORING
            }
        }

# Create global config instance
config = Config()

# Validate configuration on import
try:
    config.validate()
except ValueError as ex:
    print(f"Configuration Error: {ex}")
    raise