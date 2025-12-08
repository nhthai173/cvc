"""
Centralized logging configuration for CIP MQTT project.
Provides structured logging with support for JSON, colored console, and file output.
"""

import logging
import logging.handlers
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
from .settings import settings, LoggingSettings


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for console output"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def format(self, record: logging.LogRecord) -> str:
        # Add color and icon
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        
        # Format level name with color
        record.levelname = f"{color}{levelname}{self.RESET}"
        
        # Add module name with bold
        record.name = f"{self.BOLD}{record.name}{self.RESET}"
        
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """Formatter for JSON log output (for production/log aggregation)"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra'):
            log_data['extra'] = record.extra
        
        return json.dumps(log_data)


class LoggerFactory:
    """Factory for creating and configuring loggers"""
    
    _configured = False
    _loggers = {}
    
    @classmethod
    def configure_root_logger(cls, config: Optional[LoggingSettings] = None):
        """Configure the root logger based on settings"""
        if cls._configured:
            return
        
        config = config or settings.logging
        
        # Create root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(config.get_log_level(config.level))
        
        # Remove existing handlers
        root_logger.handlers.clear()
        
        # Create formatter based on config
        if config.format == "json":
            formatter = JSONFormatter()
        elif config.format == "colored":
            formatter = ColoredFormatter(
                '%(levelname)s [%(name)s] %(message)s'
            )
        else:  # text
            formatter = logging.Formatter(
                '[%(levelname)s] %(asctime)s - %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        # Add console handler
        if config.output in ["console", "both"]:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
        
        # Add file handler
        if config.output in ["file", "both"]:
            # Create log directory if it doesn't exist
            log_file = Path(config.file_path)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Use rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                config.file_path,
                maxBytes=config.max_file_size,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
            
            # Always use JSON format for file output (easier to parse)
            file_handler.setFormatter(JSONFormatter())
            root_logger.addHandler(file_handler)
        
        cls._configured = True
        root_logger.info(f"Logging configured - Level: {config.level}, Format: {config.format}, Output: {config.output}")
    
    @classmethod
    def get_logger(cls, name: str, level: Optional[str] = None) -> logging.Logger:
        """Get or create a logger with specific configuration"""
        if not cls._configured:
            cls.configure_root_logger()
        
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        
        # Set specific level if provided
        if level:
            logger.setLevel(settings.logging.get_log_level(level))
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def get_db_logger(cls) -> logging.Logger:
        """Get logger for database module"""
        return cls.get_logger('cip.db', settings.logging.db_log_level)
    
    @classmethod
    def get_process_logger(cls) -> logging.Logger:
        """Get logger for processing module"""
        return cls.get_logger('cip.process', settings.logging.process_log_level)
    
    @classmethod
    def get_app_logger(cls) -> logging.Logger:
        """Get logger for application"""
        return cls.get_logger('cip.app', settings.logging.level)
    
    @classmethod
    def reset(cls):
        """Reset logger configuration (useful for testing)"""
        cls._configured = False
        cls._loggers.clear()
        logging.getLogger().handlers.clear()


# Initialize logging on module import
LoggerFactory.configure_root_logger()


# Convenience functions
def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get a configured logger"""
    return LoggerFactory.get_logger(name, level)


def get_db_logger() -> logging.Logger:
    """Get database logger"""
    return LoggerFactory.get_db_logger()


def get_process_logger() -> logging.Logger:
    """Get processing logger"""
    return LoggerFactory.get_process_logger()


def get_app_logger() -> logging.Logger:
    """Get application logger"""
    return LoggerFactory.get_app_logger()


if __name__ == "__main__":
    # Test logging
    logger = get_logger(__name__)
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    # Test module-specific loggers
    db_logger = get_db_logger()
    db_logger.info("Database logger test")
    
    process_logger = get_process_logger()
    process_logger.info("Process logger test")
