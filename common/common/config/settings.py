"""
Configuration settings for CIP MQTT Data Processing project.
Uses Pydantic for validation and environment variable management.
"""

from typing import Literal, Optional
from enum import Enum
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import logging


_env_path = (Path.cwd() / ".env"
                if (Path.cwd() / ".env").exists()
                else Path(__file__).resolve().parents[1] / ".env")

class Environment(str, Enum):
    """Application environment"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    
    # PostgreSQL settings
    host: str = Field(default="localhost", description="Database host")
    name: str = Field(default="cipdb", description="Database name")
    user: str = Field(default="cipuser", description="Database user")
    password: str = Field(default="", description="Database password")
    port: int = Field(default=5432, description="Database port")
    
    # Connection pool settings
    pool_min: int = Field(default=1, ge=1, description="Minimum connections in pool")
    pool_max: int = Field(default=10, ge=1, le=100, description="Maximum connections in pool")
    pool_timeout: int = Field(default=30, ge=5, description="Pool connection timeout (seconds)")
    
    # Query settings
    query_timeout: int = Field(default=30, ge=1, description="Query timeout (seconds)")
    
    model_config = SettingsConfigDict(
        env_prefix="DB_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @field_validator('pool_max')
    @classmethod
    def validate_pool_max(cls, v, info):
        pool_min = info.data.get('pool_min', 1)
        if v < pool_min:
            raise ValueError(f"pool_max ({v}) must be >= pool_min ({pool_min})")
        return v


class RedisSettings(BaseSettings):
    """Redis configuration settings"""
    
    host: str = Field(default="localhost", description="Redis server host")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis server port")
    password: Optional[str] = Field(default=None, description="Redis server password")
    db: int = Field(default=0, ge=0, description="Redis database index")
    
    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class WeconSettings(BaseSettings):
    """Wecon VNET account settings"""
    server: str = Field(default="", description="Wecon VNET server address")
    username: str = Field(default="", description="Wecon VNET username")
    password: str = Field(default="", description="Wecon VNET password")
    gateway_id: str = Field(default="", description="Wecon gateway ID")
    timeout: int = Field(default=30, description="Wecon request timeout (seconds)")
    verify_ssl: bool = Field(default=False, description="Verify SSL certificates")

    model_config = SettingsConfigDict(
        env_prefix="WECON_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class MQTTSettings(BaseSettings):
    broker: str = Field(default="localhost", description="MQTT broker address")
    port: int = Field(default=1883, ge=1, le=65535, description="MQTT broker port")
    client_id: str = Field(default="cip_mqtt_client", description="MQTT client ID")
    username: Optional[str] = Field(default=None, description="MQTT username")
    password: Optional[str] = Field(default=None, description="MQTT password")

    model_config = SettingsConfigDict(
        env_prefix="MQTT_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class QueueSettings(BaseSettings):
    """Queue processing configuration settings"""
    
    # Consumer settings
    num_workers: int = Field(default=1, ge=1, le=32, description="Number of consumer worker processes")
    receive_timeout: int = Field(default=120, ge=10, description="Timeout for auto-finish run (seconds)")
    dequeue_timeout: int = Field(default=1, ge=1, le=10, description="Timeout when dequeuing messages (seconds)")
    
    # Queue settings
    queue_key: str = Field(default="cip:mqtt:queue", description="Redis queue key for MQTT messages")
    max_queue_size_warning: int = Field(default=1000, ge=1, description="Queue size threshold for warnings")
    
    model_config = SettingsConfigDict(
        env_prefix="QUEUE_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# =======================================================================

class LoggingSettings(BaseSettings):
    """Logging configuration settings"""
    
    level: str = Field(default="INFO", description="Logging level")
    format: Literal["json", "text", "colored"] = Field(
        default="colored", 
        description="Log format"
    )
    output: Literal["console", "file", "both"] = Field(
        default="console",
        description="Log output destination"
    )
    file_path: str = Field(
        default="./logs/cip_app.log",
        description="Log file path (when output=file or both)"
    )
    max_file_size: int = Field(
        default=10485760,  # 10MB
        ge=1048576,  # 1MB minimum
        description="Maximum log file size in bytes"
    )
    backup_count: int = Field(
        default=5,
        ge=1,
        description="Number of backup log files to keep"
    )
    
    # Module-specific log levels
    db_log_level: str = Field(default="INFO", description="Database module log level")
    process_log_level: str = Field(default="INFO", description="Processing module log level")
    
    model_config = SettingsConfigDict(
        env_prefix="LOG_",
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @field_validator('level', 'db_log_level', 'process_log_level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}, got '{v}'")
        return v_upper
    
    def get_log_level(self, level_str: str) -> int:
        """Convert string log level to logging constant"""
        return getattr(logging, level_str.upper())



# =======================================================================
class Settings(BaseSettings):
    """Main settings class that combines all configuration sections"""
    
    env: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Application environment"
    )
    
    # Sub-settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    mqtt: MQTTSettings = Field(default_factory=MQTTSettings)
    queue: QueueSettings = Field(default_factory=QueueSettings)
    wecon: WeconSettings = Field(default_factory=WeconSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    
    
    model_config = SettingsConfigDict(
        env_file=_env_path,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @classmethod
    def load(cls, env_file: str = _env_path) -> "Settings":
        """Load settings from environment file"""
        return cls(
            db=DatabaseSettings(_env_file=env_file),
            redis=RedisSettings(_env_file=env_file),
            mqtt=MQTTSettings(_env_file=env_file),
            queue=QueueSettings(_env_file=env_file),
            wecon=WeconSettings(_env_file=env_file),
            logging=LoggingSettings(_env_file=env_file),
        )
    
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.env == Environment.DEVELOPMENT
    
    def is_staging(self) -> bool:
        """Check if running in staging environment"""
        return self.env == Environment.STAGING
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.env == Environment.PRODUCTION
    
    def display(self) -> str:
        """Display configuration (with sensitive data masked)"""
        return f"""
======== CIP Processing Configuration ========
Environment: {self.env}
Database:
    Host: {self.db.host}
    Name: {self.db.name}
    User: {self.db.user}
    Password: {'***' if self.db.password else '(not set)'}
    Port: {self.db.port}
Redis:
    Host: {self.redis.host}
    Port: {self.redis.port}
    Password: {'***' if self.redis.password else '(not set)'}
    DB: {self.redis.db}
Wecon VNET:
    Server: {self.wecon.server}
    Username: {self.wecon.username}
    Password: {'***' if self.wecon.password else '(not set)'}
    Gateway ID: {self.wecon.gateway_id}
MQTT:
    Broker: {self.mqtt.broker}
    Port: {self.mqtt.port}
    Client ID: {self.mqtt.client_id}
    Username: {self.mqtt.username if self.mqtt.username else '(not set)'}
    Password: {'***' if self.mqtt.password else '(not set)'}
Queue:
    Workers: {self.queue.num_workers}
    Receive Timeout: {self.queue.receive_timeout}s
    Dequeue Timeout: {self.queue.dequeue_timeout}s
    Queue Key: {self.queue.queue_key}
    Max Queue Size Warning: {self.queue.max_queue_size_warning}
Logging:
    Level: {self.logging.level}
    Format: {self.logging.format}
    Output: {self.logging.output}
    File Path: {self.logging.file_path}
    Max File Size: {self.logging.max_file_size} bytes
    Backup Count: {self.logging.backup_count}
=============================================
        """


# Global settings instance
settings = Settings.load()


if __name__ == "__main__":
    # Display current configuration
    print(settings.display())
    
    # Example: Access specific settings
    print(f"Database connection string: postgresql://{settings.db.user}:***@{settings.db.host}:{settings.db.port}/{settings.db.name}")
    print(f"Log level (as int): {settings.logging.get_log_level(settings.logging.level)}")
    print(f"Is production?: {settings.is_production()}")
