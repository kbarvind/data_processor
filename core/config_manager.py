"""
Configuration management for the data processing framework.
Handles loading and validation of configuration files and environment variables.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseSettings, Field, validator

from .exceptions import ConfigurationException, handle_exception
from .logging_config import get_logger
from .models import ToolConfig, ScheduleConfig

logger = get_logger(__name__)


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = Field(default="data_processing")
    username: str = Field(default="postgres")
    password: str = Field(default="password")
    ssl_mode: str = Field(default="prefer")
    pool_size: int = Field(default=5, ge=1, le=100)
    max_overflow: int = Field(default=10, ge=0, le=100)
    
    @property
    def url(self) -> str:
        """Generate database URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"
    
    class Config:
        env_prefix = "DB_"


class RedisConfig(BaseSettings):
    """Redis configuration settings."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=6379, ge=1, le=65535)
    password: Optional[str] = None
    database: int = Field(default=0, ge=0, le=15)
    ssl: bool = Field(default=False)
    
    @property
    def url(self) -> str:
        """Generate Redis URL."""
        scheme = "rediss" if self.ssl else "redis"
        auth = f":{self.password}@" if self.password else ""
        return f"{scheme}://{auth}{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_prefix = "REDIS_"


class AppConfig(BaseSettings):
    """Application configuration settings."""
    
    name: str = Field(default="Data Processing Framework")
    version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    debug: bool = Field(default=False)
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    workers: int = Field(default=1, ge=1, le=32)
    log_level: str = Field(default="INFO")
    
    @validator('environment')
    def validate_environment(cls, v):
        """Validate environment setting."""
        allowed = ['development', 'staging', 'production']
        if v not in allowed:
            raise ValueError(f'Environment must be one of {allowed}')
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        """Validate log level setting."""
        allowed = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in allowed:
            raise ValueError(f'Log level must be one of {allowed}')
        return v.upper()
    
    class Config:
        env_prefix = "APP_"


class ConfigManager:
    """Configuration manager for the data processing framework."""
    
    def __init__(self, config_dir: Union[str, Path] = "config"):
        """
        Initialize configuration manager.
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuration files
        self.planner_config_file = self.config_dir / "planner.yaml"
        self.tools_config_file = self.config_dir / "tools_config.yaml"
        self.database_config_file = self.config_dir / "database_config.yaml"
        
        # Loaded configurations
        self._app_config: Optional[AppConfig] = None
        self._database_config: Optional[DatabaseConfig] = None
        self._redis_config: Optional[RedisConfig] = None
        self._tools_config: Optional[Dict[str, ToolConfig]] = None
        self._schedules_config: Optional[Dict[str, ScheduleConfig]] = None
        
        logger.info("ConfigManager initialized", config_dir=str(self.config_dir))
    
    @handle_exception
    def load_yaml_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load and parse YAML configuration file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            ConfigurationException: If file cannot be loaded or parsed
        """
        file_path = Path(file_path)
        
        try:
            if not file_path.exists():
                raise ConfigurationException(
                    f"Configuration file not found: {file_path}",
                    config_file=str(file_path)
                )
            
            with file_path.open('r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            if config_data is None:
                config_data = {}
            
            logger.info("Configuration file loaded", file_path=str(file_path))
            return config_data
            
        except yaml.YAMLError as e:
            raise ConfigurationException(
                f"Invalid YAML in configuration file: {file_path}",
                config_file=str(file_path),
                details={"yaml_error": str(e)},
                original_exception=e
            )
        except Exception as e:
            raise ConfigurationException(
                f"Failed to load configuration file: {file_path}",
                config_file=str(file_path),
                original_exception=e
            )
    
    @handle_exception
    def save_yaml_file(self, file_path: Union[str, Path], data: Dict[str, Any]) -> None:
        """
        Save configuration data to YAML file.
        
        Args:
            file_path: Path to save YAML file
            data: Configuration data to save
            
        Raises:
            ConfigurationException: If file cannot be saved
        """
        file_path = Path(file_path)
        
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with file_path.open('w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, indent=2)
            
            logger.info("Configuration file saved", file_path=str(file_path))
            
        except Exception as e:
            raise ConfigurationException(
                f"Failed to save configuration file: {file_path}",
                config_file=str(file_path),
                original_exception=e
            )
    
    @property
    def app_config(self) -> AppConfig:
        """Get application configuration."""
        if self._app_config is None:
            self._app_config = AppConfig()
        return self._app_config
    
    @property
    def database_config(self) -> DatabaseConfig:
        """Get database configuration."""
        if self._database_config is None:
            self._database_config = DatabaseConfig()
        return self._database_config
    
    @property
    def redis_config(self) -> RedisConfig:
        """Get Redis configuration."""
        if self._redis_config is None:
            self._redis_config = RedisConfig()
        return self._redis_config
    
    @handle_exception
    def load_tools_config(self) -> Dict[str, ToolConfig]:
        """
        Load tools configuration from YAML file.
        
        Returns:
            Dictionary of tool configurations
            
        Raises:
            ConfigurationException: If configuration cannot be loaded or validated
        """
        if self._tools_config is None:
            try:
                config_data = self.load_yaml_file(self.tools_config_file)
                tools_config = {}
                
                for tool_name, tool_data in config_data.items():
                    try:
                        tool_config = ToolConfig(**tool_data)
                        tools_config[tool_name] = tool_config
                    except Exception as e:
                        raise ConfigurationException(
                            f"Invalid configuration for tool '{tool_name}'",
                            config_key=tool_name,
                            config_file=str(self.tools_config_file),
                            details={"validation_error": str(e)},
                            original_exception=e
                        )
                
                self._tools_config = tools_config
                logger.info("Tools configuration loaded", tools_count=len(tools_config))
                
            except ConfigurationException:
                raise
            except Exception as e:
                raise ConfigurationException(
                    "Failed to load tools configuration",
                    config_file=str(self.tools_config_file),
                    original_exception=e
                )
        
        return self._tools_config
    
    @handle_exception
    def load_schedules_config(self) -> Dict[str, ScheduleConfig]:
        """
        Load schedules configuration from YAML file.
        
        Returns:
            Dictionary of schedule configurations
            
        Raises:
            ConfigurationException: If configuration cannot be loaded or validated
        """
        if self._schedules_config is None:
            try:
                config_data = self.load_yaml_file(self.planner_config_file)
                schedules_data = config_data.get("schedules", {})
                schedules_config = {}
                
                for schedule_name, schedule_data in schedules_data.items():
                    try:
                        schedule_config = ScheduleConfig(name=schedule_name, **schedule_data)
                        schedules_config[schedule_name] = schedule_config
                    except Exception as e:
                        raise ConfigurationException(
                            f"Invalid configuration for schedule '{schedule_name}'",
                            config_key=schedule_name,
                            config_file=str(self.planner_config_file),
                            details={"validation_error": str(e)},
                            original_exception=e
                        )
                
                self._schedules_config = schedules_config
                logger.info("Schedules configuration loaded", schedules_count=len(schedules_config))
                
            except ConfigurationException:
                raise
            except Exception as e:
                raise ConfigurationException(
                    "Failed to load schedules configuration",
                    config_file=str(self.planner_config_file),
                    original_exception=e
                )
        
        return self._schedules_config
    
    @handle_exception
    def get_tool_config(self, tool_name: str) -> ToolConfig:
        """
        Get configuration for a specific tool.
        
        Args:
            tool_name: Name of the tool
            
        Returns:
            Tool configuration
            
        Raises:
            ConfigurationException: If tool configuration not found
        """
        tools_config = self.load_tools_config()
        
        if tool_name not in tools_config:
            raise ConfigurationException(
                f"Configuration not found for tool '{tool_name}'",
                config_key=tool_name,
                config_file=str(self.tools_config_file)
            )
        
        return tools_config[tool_name]
    
    @handle_exception
    def get_schedule_config(self, schedule_name: str) -> ScheduleConfig:
        """
        Get configuration for a specific schedule.
        
        Args:
            schedule_name: Name of the schedule
            
        Returns:
            Schedule configuration
            
        Raises:
            ConfigurationException: If schedule configuration not found
        """
        schedules_config = self.load_schedules_config()
        
        if schedule_name not in schedules_config:
            raise ConfigurationException(
                f"Configuration not found for schedule '{schedule_name}'",
                config_key=schedule_name,
                config_file=str(self.planner_config_file)
            )
        
        return schedules_config[schedule_name]
    
    @handle_exception
    def create_default_configs(self) -> None:
        """Create default configuration files if they don't exist."""
        # Default tools configuration
        if not self.tools_config_file.exists():
            default_tools = {
                "cequence": {
                    "name": "cequence",
                    "api_base_url": "https://api.cequence.ai",
                    "auth_type": "api_key",
                    "rate_limit": 100,
                    "timeout": 30,
                    "retry_attempts": 3,
                    "retry_delay": 1.0
                },
                "42crunch": {
                    "name": "42crunch",
                    "api_base_url": "https://api.42crunch.com",
                    "auth_type": "oauth",
                    "rate_limit": 50,
                    "timeout": 30,
                    "retry_attempts": 3,
                    "retry_delay": 1.0
                },
                "apigee": {
                    "name": "apigee",
                    "api_base_url": "https://apigee.googleapis.com",
                    "auth_type": "service_account",
                    "rate_limit": 200,
                    "timeout": 30,
                    "retry_attempts": 3,
                    "retry_delay": 1.0
                }
            }
            self.save_yaml_file(self.tools_config_file, default_tools)
        
        # Default planner configuration
        if not self.planner_config_file.exists():
            default_planner = {
                "schedules": {
                    "daily_collection": {
                        "cron_expression": "0 2 * * *",
                        "tools": ["cequence", "42crunch", "apigee"],
                        "job_type": "collection",
                        "enabled": True,
                        "max_runtime": 3600
                    },
                    "weekly_processing": {
                        "cron_expression": "0 4 * * 0",
                        "tools": ["cequence", "42crunch", "apigee"],
                        "job_type": "processing",
                        "enabled": True,
                        "max_runtime": 7200
                    },
                    "monthly_report": {
                        "cron_expression": "0 6 1 * *",
                        "tools": ["cequence", "42crunch", "apigee"],
                        "job_type": "report",
                        "enabled": True,
                        "max_runtime": 1800
                    }
                }
            }
            self.save_yaml_file(self.planner_config_file, default_planner)
        
        logger.info("Default configuration files created")
    
    @handle_exception
    def reload_configs(self) -> None:
        """Reload all configuration files."""
        self._tools_config = None
        self._schedules_config = None
        
        # Reload configurations
        self.load_tools_config()
        self.load_schedules_config()
        
        logger.info("All configurations reloaded")
    
    @handle_exception
    def load_config(self, config_file: Union[str, Path]) -> None:
        """
        Load configuration from a single YAML file.
        
        Args:
            config_file: Path to the configuration file
            
        Raises:
            ConfigurationException: If configuration cannot be loaded
        """
        config_data = self.load_yaml_file(config_file)
        
        # Extract and load different configuration sections
        if "tools" in config_data:
            self._tools_config = {}
            for tool_name, tool_data in config_data["tools"].items():
                try:
                    tool_config = ToolConfig(**tool_data)
                    self._tools_config[tool_name] = tool_config
                except Exception as e:
                    raise ConfigurationException(
                        f"Invalid configuration for tool '{tool_name}'",
                        config_key=tool_name,
                        config_file=str(config_file),
                        details={"validation_error": str(e)},
                        original_exception=e
                    )
        
        if "schedules" in config_data:
            self._schedules_config = {}
            for schedule_name, schedule_data in config_data["schedules"].items():
                try:
                    schedule_config = ScheduleConfig(name=schedule_name, **schedule_data)
                    self._schedules_config[schedule_name] = schedule_config
                except Exception as e:
                    raise ConfigurationException(
                        f"Invalid configuration for schedule '{schedule_name}'",
                        config_key=schedule_name,
                        config_file=str(config_file),
                        details={"validation_error": str(e)},
                        original_exception=e
                    )
        
        # Store the raw config data for get_config method
        self._raw_config = config_data
        
        logger.info("Configuration loaded successfully", config_file=str(config_file))
    
    @handle_exception
    def get_config(self) -> Dict[str, Any]:
        """
        Get the complete configuration dictionary.
        
        Returns:
            Complete configuration dictionary
        """
        if not hasattr(self, '_raw_config'):
            # Load default configuration if not loaded
            self.create_default_configs()
            default_config = {
                "tools": {},
                "schedules": {}
            }
            
            # Load tools config
            if self.tools_config_file.exists():
                tools_data = self.load_yaml_file(self.tools_config_file)
                default_config["tools"] = tools_data
            
            # Load planner config
            if self.planner_config_file.exists():
                planner_data = self.load_yaml_file(self.planner_config_file)
                default_config["schedules"] = planner_data.get("schedules", {})
            
            self._raw_config = default_config
        
        return self._raw_config
    
    @handle_exception
    def validate_configs(self) -> bool:
        """
        Validate all configuration files.
        
        Returns:
            True if all configurations are valid
            
        Raises:
            ConfigurationException: If any configuration is invalid
        """
        try:
            # Validate tools configuration
            tools_config = self.load_tools_config()
            if not tools_config:
                raise ConfigurationException(
                    "No tools configured",
                    config_file=str(self.tools_config_file)
                )
            
            # Validate schedules configuration
            schedules_config = self.load_schedules_config()
            if not schedules_config:
                raise ConfigurationException(
                    "No schedules configured",
                    config_file=str(self.planner_config_file)
                )
            
            # Validate schedule-tool references
            for schedule_name, schedule_config in schedules_config.items():
                for tool_name in schedule_config.tools:
                    if tool_name not in tools_config:
                        raise ConfigurationException(
                            f"Schedule '{schedule_name}' references unknown tool '{tool_name}'",
                            config_key=schedule_name,
                            config_file=str(self.planner_config_file)
                        )
            
            logger.info("All configurations validated successfully")
            return True
            
        except ConfigurationException:
            raise
        except Exception as e:
            raise ConfigurationException(
                "Configuration validation failed",
                original_exception=e
            )


# Global configuration manager instance
config_manager = ConfigManager() 