"""
Standard exception handling for the data processing framework.
All exceptions are recorded in a consistent format with proper logging.
"""

import traceback
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from loguru import logger


class DataProcessingException(Exception):
    """Base exception for all data processing framework errors."""
    
    def __init__(
        self,
        message: str,
        error_code: str = "GENERAL_ERROR",
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.original_exception = original_exception
        self.timestamp = datetime.utcnow()
        self.error_id = str(uuid4())
        
        # Log the exception in standard format
        self._log_exception()
    
    def _log_exception(self):
        """Log the exception in standard format."""
        error_data = {
            "error_id": self.error_id,
            "error_code": self.error_code,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "traceback": traceback.format_exc(),
        }
        
        if self.original_exception:
            error_data["original_exception"] = {
                "type": type(self.original_exception).__name__,
                "message": str(self.original_exception),
            }
        
        logger.error("Data Processing Exception", **error_data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary format."""
        return {
            "error_id": self.error_id,
            "error_code": self.error_code,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "original_exception": str(self.original_exception) if self.original_exception else None,
        }


class APIClientException(DataProcessingException):
    """Exception raised for API client related errors."""
    
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "status_code": status_code,
            "response_data": response_data,
            "endpoint": endpoint,
            "method": method,
        })
        
        super().__init__(
            message=message,
            error_code="API_CLIENT_ERROR",
            details=details,
            **kwargs
        )


class ConfigurationException(DataProcessingException):
    """Exception raised for configuration related errors."""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_file: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "config_key": config_key,
            "config_file": config_file,
        })
        
        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            details=details,
            **kwargs
        )


class SchedulerException(DataProcessingException):
    """Exception raised for scheduler related errors."""
    
    def __init__(
        self,
        message: str,
        job_id: Optional[str] = None,
        job_type: Optional[str] = None,
        scheduler_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "job_id": job_id,
            "job_type": job_type,
            "scheduler_type": scheduler_type,
        })
        
        super().__init__(
            message=message,
            error_code="SCHEDULER_ERROR",
            details=details,
            **kwargs
        )


class ValidationException(DataProcessingException):
    """Exception raised for data validation errors."""
    
    def __init__(
        self,
        message: str,
        validation_errors: Optional[list] = None,
        field_name: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "validation_errors": validation_errors,
            "field_name": field_name,
        })
        
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=details,
            **kwargs
        )


class DataCollectionException(DataProcessingException):
    """Exception raised for data collection related errors."""
    
    def __init__(
        self,
        message: str,
        tool_name: Optional[str] = None,
        collection_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "tool_name": tool_name,
            "collection_type": collection_type,
        })
        
        super().__init__(
            message=message,
            error_code="DATA_COLLECTION_ERROR",
            details=details,
            **kwargs
        )


class DataProcessingError(DataProcessingException):
    """Exception raised for data processing related errors."""
    
    def __init__(
        self,
        message: str,
        processing_stage: Optional[str] = None,
        data_type: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.get("details", {})
        details.update({
            "processing_stage": processing_stage,
            "data_type": data_type,
        })
        
        super().__init__(
            message=message,
            error_code="DATA_PROCESSING_ERROR",
            details=details,
            **kwargs
        )


def handle_exception(func):
    """Decorator to handle exceptions and log them in standard format."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DataProcessingException:
            # Re-raise framework exceptions as they're already logged
            raise
        except Exception as e:
            # Wrap unknown exceptions in framework exception
            raise DataProcessingException(
                message=f"Unexpected error in {func.__name__}: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                details={"function": func.__name__},
                original_exception=e
            )
    
    return wrapper


async def handle_async_exception(func):
    """Async decorator to handle exceptions and log them in standard format."""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except DataProcessingException:
            # Re-raise framework exceptions as they're already logged
            raise
        except Exception as e:
            # Wrap unknown exceptions in framework exception
            raise DataProcessingException(
                message=f"Unexpected error in {func.__name__}: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                details={"function": func.__name__},
                original_exception=e
            )
    
    return wrapper 