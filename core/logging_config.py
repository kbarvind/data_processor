"""
Logging configuration for the data processing framework.
Provides structured logging with consistent format across all components.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from loguru import logger
import structlog


class LoggingConfig:
    """Configuration for structured logging."""
    
    def __init__(self):
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_format = os.getenv("LOG_FORMAT", "json")
        self.log_dir = Path(os.getenv("LOG_DIR", "storage/logs"))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log files
        self.app_log_file = self.log_dir / "app.log"
        self.error_log_file = self.log_dir / "errors.log"
        self.audit_log_file = self.log_dir / "audit.log"
        
    def setup_loguru(self):
        """Setup loguru logger configuration."""
        # Remove default handler
        logger.remove()
        
        # Console handler with colors
        if self.log_format == "json":
            console_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}"
        else:
            console_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}:{function}:{line}</cyan> | <level>{message}</level> | {extra}"
        
        logger.add(
            sys.stdout,
            format=console_format,
            level=self.log_level,
            colorize=True,
            serialize=False,
        )
        
        # File handler for all logs
        logger.add(
            self.app_log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
            level="DEBUG",
            rotation="10 MB",
            retention="30 days",
            compression="gzip",
            serialize=True,
        )
        
        # Error file handler
        logger.add(
            self.error_log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
            level="ERROR",
            rotation="10 MB",
            retention="30 days",
            compression="gzip",
            serialize=True,
        )
        
        # Audit log handler
        logger.add(
            self.audit_log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message} | {extra}",
            level="INFO",
            rotation="10 MB",
            retention="90 days",
            compression="gzip",
            serialize=True,
            filter=lambda record: record["extra"].get("audit", False),
        )
        
    def setup_structlog(self):
        """Setup structlog for structured logging."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer() if self.log_format == "json" else structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )


# Global logging configuration instance
_logging_config = LoggingConfig()


def setup_logging(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_dir: Optional[str] = None
) -> None:
    """
    Setup logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log format (json, text)
        log_dir: Directory for log files
    """
    global _logging_config
    
    if log_level:
        _logging_config.log_level = log_level
    if log_format:
        _logging_config.log_format = log_format
    if log_dir:
        _logging_config.log_dir = Path(log_dir)
        _logging_config.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Update log file paths
        _logging_config.app_log_file = _logging_config.log_dir / "app.log"
        _logging_config.error_log_file = _logging_config.log_dir / "errors.log"
        _logging_config.audit_log_file = _logging_config.log_dir / "audit.log"
    
    # Setup loguru
    _logging_config.setup_loguru()
    
    # Setup structlog
    _logging_config.setup_structlog()
    
    logger.info("Logging configuration initialized", 
                level=_logging_config.log_level,
                format=_logging_config.log_format,
                log_dir=str(_logging_config.log_dir))


def get_logger(name: str) -> Any:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically module name)
        
    Returns:
        Logger instance
    """
    return logger.bind(name=name)


def get_structured_logger(name: str) -> Any:
    """
    Get a structured logger instance with the specified name.
    
    Args:
        name: Logger name (typically module name)
        
    Returns:
        Structured logger instance
    """
    return structlog.get_logger(name)


def log_api_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    response_status: Optional[int] = None,
    response_time: Optional[float] = None,
    error: Optional[str] = None
) -> None:
    """
    Log API request in standard format.
    
    Args:
        method: HTTP method
        url: Request URL
        headers: Request headers
        params: Request parameters
        response_status: Response status code
        response_time: Response time in seconds
        error: Error message if any
    """
    log_data = {
        "audit": True,
        "type": "api_request",
        "method": method,
        "url": url,
        "headers": headers,
        "params": params,
        "response_status": response_status,
        "response_time": response_time,
        "error": error,
    }
    
    if error:
        logger.error("API Request Failed", **log_data)
    else:
        logger.info("API Request", **log_data)


def log_data_collection(
    tool_name: str,
    collection_type: str,
    status: str,
    records_collected: Optional[int] = None,
    duration: Optional[float] = None,
    error: Optional[str] = None
) -> None:
    """
    Log data collection activity in standard format.
    
    Args:
        tool_name: Name of the tool (e.g., 'cequence', '42crunch')
        collection_type: Type of collection (e.g., 'scheduled', 'manual')
        status: Collection status ('success', 'failed', 'partial')
        records_collected: Number of records collected
        duration: Collection duration in seconds
        error: Error message if any
    """
    log_data = {
        "audit": True,
        "type": "data_collection",
        "tool_name": tool_name,
        "collection_type": collection_type,
        "status": status,
        "records_collected": records_collected,
        "duration": duration,
        "error": error,
    }
    
    if status == "failed":
        logger.error("Data Collection Failed", **log_data)
    elif status == "partial":
        logger.warning("Data Collection Partial", **log_data)
    else:
        logger.info("Data Collection Success", **log_data)


def log_data_processing(
    tool_name: str,
    processing_stage: str,
    status: str,
    records_processed: Optional[int] = None,
    duration: Optional[float] = None,
    error: Optional[str] = None
) -> None:
    """
    Log data processing activity in standard format.
    
    Args:
        tool_name: Name of the tool
        processing_stage: Stage of processing (e.g., 'validation', 'transformation')
        status: Processing status ('success', 'failed', 'partial')
        records_processed: Number of records processed
        duration: Processing duration in seconds
        error: Error message if any
    """
    log_data = {
        "audit": True,
        "type": "data_processing",
        "tool_name": tool_name,
        "processing_stage": processing_stage,
        "status": status,
        "records_processed": records_processed,
        "duration": duration,
        "error": error,
    }
    
    if status == "failed":
        logger.error("Data Processing Failed", **log_data)
    elif status == "partial":
        logger.warning("Data Processing Partial", **log_data)
    else:
        logger.info("Data Processing Success", **log_data)


def log_scheduler_event(
    job_id: str,
    job_type: str,
    event_type: str,
    status: str,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None
) -> None:
    """
    Log scheduler events in standard format.
    
    Args:
        job_id: Job identifier
        job_type: Type of job (e.g., 'collection', 'processing')
        event_type: Event type (e.g., 'scheduled', 'started', 'completed')
        status: Event status ('success', 'failed')
        details: Additional event details
        error: Error message if any
    """
    log_data = {
        "audit": True,
        "type": "scheduler_event",
        "job_id": job_id,
        "job_type": job_type,
        "event_type": event_type,
        "status": status,
        "details": details or {},
        "error": error,
    }
    
    if status == "failed":
        logger.error("Scheduler Event Failed", **log_data)
    else:
        logger.info("Scheduler Event", **log_data)


# Initialize logging on module import
setup_logging() 