"""
Core data models for the data processing framework.
Defines standard request/response models and data structures.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator


class BaseResponse(BaseModel):
    """Base response model for all API responses."""
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    

class ErrorResponse(BaseResponse):
    """Standard error response model."""
    
    success: bool = Field(default=False)
    error_code: str
    message: str
    details: Optional[Dict[str, Any]] = None
    error_id: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "success": False,
                "error_code": "API_CLIENT_ERROR",
                "message": "Failed to connect to external API",
                "details": {
                    "endpoint": "https://api.example.com/data",
                    "status_code": 500,
                    "response_data": {"error": "Internal server error"}
                },
                "error_id": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2024-01-01T12:00:00Z",
                "request_id": "req_123456789"
            }
        }


class SuccessResponse(BaseResponse):
    """Standard success response model."""
    
    success: bool = Field(default=True)
    message: str
    data: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Data collection completed successfully",
                "data": {
                    "records_collected": 150,
                    "duration": 30.5,
                    "tool_name": "cequence"
                },
                "timestamp": "2024-01-01T12:00:00Z",
                "request_id": "req_123456789"
            }
        }


class ToolConfig(BaseModel):
    """Configuration model for external tools."""
    
    name: str
    api_base_url: str
    auth_type: str = Field(..., regex="^(api_key|oauth|basic|bearer|service_account)$")
    rate_limit: int = Field(default=100, ge=1, le=10000)
    timeout: int = Field(default=30, ge=1, le=300)
    retry_attempts: int = Field(default=3, ge=0, le=10)
    retry_delay: float = Field(default=1.0, ge=0.1, le=60.0)
    
    class Config:
        schema_extra = {
            "example": {
                "name": "cequence",
                "api_base_url": "https://api.cequence.ai",
                "auth_type": "api_key",
                "rate_limit": 100,
                "timeout": 30,
                "retry_attempts": 3,
                "retry_delay": 1.0
            }
        }


class ScheduleConfig(BaseModel):
    """Configuration model for job scheduling."""
    
    name: str
    cron_expression: str
    tools: List[str]
    job_type: str = Field(..., regex="^(collection|processing|report)$")
    enabled: bool = Field(default=True)
    max_runtime: int = Field(default=3600, ge=60, le=86400)  # 1 hour default, max 24 hours
    
    @validator('cron_expression')
    def validate_cron(cls, v):
        """Basic cron expression validation."""
        parts = v.split()
        if len(parts) != 5:
            raise ValueError('Cron expression must have exactly 5 parts')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "name": "daily_collection",
                "cron_expression": "0 2 * * *",
                "tools": ["cequence", "42crunch", "apigee"],
                "job_type": "collection",
                "enabled": True,
                "max_runtime": 3600
            }
        }


class JobStatus(BaseModel):
    """Model for job status tracking."""
    
    job_id: str
    job_type: str
    tool_name: Optional[str] = None
    status: str = Field(..., regex="^(pending|running|completed|failed|cancelled)$")
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[float] = None
    records_processed: Optional[int] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "job_id": "job_123456789",
                "job_type": "collection",
                "tool_name": "cequence",
                "status": "completed",
                "started_at": "2024-01-01T12:00:00Z",
                "completed_at": "2024-01-01T12:30:00Z",
                "duration": 1800.0,
                "records_processed": 150,
                "error_message": None,
                "error_code": None
            }
        }


class DataRecord(BaseModel):
    """Base model for data records."""
    
    id: str = Field(default_factory=lambda: str(uuid4()))
    tool_name: str
    collection_timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "id": "rec_123456789",
                "tool_name": "cequence",
                "collection_timestamp": "2024-01-01T12:00:00Z",
                "data": {
                    "endpoint": "/api/users",
                    "method": "GET",
                    "response_time": 150,
                    "status_code": 200
                },
                "metadata": {
                    "collector_version": "1.0.0",
                    "collection_type": "scheduled"
                }
            }
        }


class CollectionResult(BaseModel):
    """Result model for data collection operations."""
    
    tool_name: str
    collection_type: str
    status: str = Field(..., regex="^(success|failed|partial)$")
    records_collected: int = Field(default=0, ge=0)
    duration: float = Field(default=0.0, ge=0.0)
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "tool_name": "cequence",
                "collection_type": "scheduled",
                "status": "success",
                "records_collected": 150,
                "duration": 30.5,
                "started_at": "2024-01-01T12:00:00Z",
                "completed_at": "2024-01-01T12:30:30Z",
                "error_message": None,
                "error_code": None
            }
        }


class ProcessingResult(BaseModel):
    """Result model for data processing operations."""
    
    tool_name: str
    processing_stage: str
    status: str = Field(..., regex="^(success|failed|partial)$")
    records_processed: int = Field(default=0, ge=0)
    records_valid: int = Field(default=0, ge=0)
    records_invalid: int = Field(default=0, ge=0)
    duration: float = Field(default=0.0, ge=0.0)
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "tool_name": "cequence",
                "processing_stage": "validation",
                "status": "success",
                "records_processed": 150,
                "records_valid": 147,
                "records_invalid": 3,
                "duration": 15.2,
                "started_at": "2024-01-01T12:30:00Z",
                "completed_at": "2024-01-01T12:30:15Z",
                "error_message": None,
                "error_code": None
            }
        }


class APIRequest(BaseModel):
    """Model for API request tracking."""
    
    method: str = Field(..., regex="^(GET|POST|PUT|DELETE|PATCH)$")
    url: str
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    timeout: int = Field(default=30, ge=1, le=300)
    
    class Config:
        schema_extra = {
            "example": {
                "method": "GET",
                "url": "https://api.example.com/data",
                "headers": {
                    "Authorization": "Bearer token123",
                    "Content-Type": "application/json"
                },
                "params": {
                    "page": 1,
                    "limit": 100
                },
                "body": None,
                "timeout": 30
            }
        }


class APIResponse(BaseModel):
    """Model for API response tracking."""
    
    status_code: int
    headers: Optional[Dict[str, str]] = None
    data: Optional[Union[Dict[str, Any], List[Any]]] = None
    response_time: float = Field(ge=0.0)
    error: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "status_code": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "X-RateLimit-Remaining": "99"
                },
                "data": {
                    "results": [{"id": 1, "name": "example"}],
                    "total": 1
                },
                "response_time": 0.25,
                "error": None
            }
        }


class HealthCheck(BaseModel):
    """Model for health check responses."""
    
    status: str = Field(..., regex="^(healthy|unhealthy|degraded)$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    services: Dict[str, str] = Field(default_factory=dict)
    version: str = Field(default="1.0.0")
    uptime: float = Field(default=0.0, ge=0.0)
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-01T12:00:00Z",
                "services": {
                    "database": "healthy",
                    "redis": "healthy",
                    "scheduler": "healthy"
                },
                "version": "1.0.0",
                "uptime": 3600.0
            }
        } 