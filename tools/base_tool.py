"""
Base tool classes for the data processing framework.
Provides abstract base classes for tool implementations.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..core.api_client import APIClient, APIClientFactory
from ..core.config_manager import config_manager
from ..core.exceptions import DataCollectionException, DataProcessingError, handle_exception
from ..core.logging_config import get_logger, log_data_collection, log_data_processing
from ..core.models import DataRecord, CollectionResult, ProcessingResult, ToolConfig


class BaseTool(ABC):
    """Base class for all tools."""
    
    def __init__(self, tool_name: str):
        """
        Initialize base tool.
        
        Args:
            tool_name: Name of the tool
        """
        self.tool_name = tool_name
        self.logger = get_logger(f"tools.{tool_name}")
        self.config: Optional[ToolConfig] = None
        self.credentials: Dict[str, Any] = {}
        self.storage_path = Path(f"storage/raw_data/{tool_name}")
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"{tool_name} tool initialized")
    
    @handle_exception
    def load_config(self) -> ToolConfig:
        """
        Load tool configuration.
        
        Returns:
            Tool configuration
        """
        if self.config is None:
            self.config = config_manager.get_tool_config(self.tool_name)
        return self.config
    
    @handle_exception
    def load_credentials(self, credentials: Dict[str, Any]) -> None:
        """
        Load tool credentials.
        
        Args:
            credentials: Tool credentials
        """
        self.credentials = credentials
        self.logger.info("Credentials loaded", auth_type=self.config.auth_type if self.config else "unknown")
    
    def get_api_client(self) -> APIClient:
        """
        Get API client for the tool.
        
        Returns:
            Configured API client
        """
        if not self.config:
            self.load_config()
        
        if not self.credentials:
            raise DataCollectionException(
                f"No credentials loaded for tool '{self.tool_name}'",
                tool_name=self.tool_name
            )
        
        return APIClientFactory.create_client(self.config, self.credentials)


class BaseCollector(BaseTool):
    """Base class for data collectors."""
    
    def __init__(self, tool_name: str):
        """
        Initialize base collector.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        self.collection_endpoints: Dict[str, str] = {}
        self.collection_params: Dict[str, Dict[str, Any]] = {}
    
    @abstractmethod
    async def collect_data(self, collection_type: str = "scheduled") -> CollectionResult:
        """
        Collect data from the tool.
        
        Args:
            collection_type: Type of collection (scheduled, manual)
            
        Returns:
            Collection result
        """
        pass
    
    @handle_exception
    async def _collect_from_endpoint(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Collect data from a specific endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            List of collected data records
        """
        with self.get_api_client() as client:
            response = client.get(endpoint, params=params)
            
            if response.data:
                if isinstance(response.data, dict):
                    # Handle paginated responses
                    if 'data' in response.data:
                        return response.data['data']
                    elif 'results' in response.data:
                        return response.data['results']
                    elif 'items' in response.data:
                        return response.data['items']
                    else:
                        return [response.data]
                elif isinstance(response.data, list):
                    return response.data
                else:
                    return [{"content": response.data}]
            
            return []
    
    @handle_exception
    def _save_raw_data(self, data: List[Dict[str, Any]], collection_type: str) -> Path:
        """
        Save raw data to file.
        
        Args:
            data: Raw data to save
            collection_type: Type of collection
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.tool_name}_{collection_type}_{timestamp}.json"
        file_path = self.storage_path / filename
        
        import json
        with file_path.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
        
        self.logger.info("Raw data saved", 
                        file_path=str(file_path),
                        record_count=len(data))
        
        return file_path
    
    @handle_exception
    def _convert_to_records(self, raw_data: List[Dict[str, Any]]) -> List[DataRecord]:
        """
        Convert raw data to DataRecord objects.
        
        Args:
            raw_data: Raw data from API
            
        Returns:
            List of DataRecord objects
        """
        records = []
        
        for item in raw_data:
            record = DataRecord(
                tool_name=self.tool_name,
                data=item,
                metadata={
                    "collector_version": "1.0.0",
                    "collection_timestamp": datetime.utcnow().isoformat()
                }
            )
            records.append(record)
        
        return records


class BaseProcessor(BaseTool):
    """Base class for data processors."""
    
    def __init__(self, tool_name: str):
        """
        Initialize base processor.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        self.processed_storage_path = Path(f"storage/processed_data/{tool_name}")
        self.processed_storage_path.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    async def process_data(
        self, 
        data: List[DataRecord], 
        processing_stage: str = "validation"
    ) -> ProcessingResult:
        """
        Process collected data.
        
        Args:
            data: Raw data records to process
            processing_stage: Stage of processing
            
        Returns:
            Processing result
        """
        pass
    
    @handle_exception
    def _validate_record(self, record: DataRecord) -> bool:
        """
        Validate a data record.
        
        Args:
            record: Data record to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Basic validation - override in subclasses
        if not record.data:
            return False
        
        if not isinstance(record.data, dict):
            return False
        
        return True
    
    @handle_exception
    def _transform_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Transform a data record.
        
        Args:
            record: Data record to transform
            
        Returns:
            Transformed record or None if should be filtered
        """
        # Default implementation - override in subclasses
        return record
    
    @handle_exception
    def _enrich_record(self, record: DataRecord) -> DataRecord:
        """
        Enrich a data record with additional metadata.
        
        Args:
            record: Data record to enrich
            
        Returns:
            Enriched record
        """
        # Add processing metadata
        if not record.metadata:
            record.metadata = {}
        
        record.metadata.update({
            "processed_at": datetime.utcnow().isoformat(),
            "processor_version": "1.0.0",
            "processing_tool": self.tool_name
        })
        
        return record
    
    @handle_exception
    def _save_processed_data(
        self, 
        records: List[DataRecord], 
        processing_stage: str
    ) -> Path:
        """
        Save processed data to file.
        
        Args:
            records: Processed data records
            processing_stage: Stage of processing
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.tool_name}_{processing_stage}_{timestamp}.json"
        file_path = self.processed_storage_path / filename
        
        # Convert records to dictionaries for JSON serialization
        data = [record.dict() for record in records]
        
        import json
        with file_path.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
        
        self.logger.info("Processed data saved", 
                        file_path=str(file_path),
                        record_count=len(records))
        
        return file_path
    
    @handle_exception
    async def validate_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Validate data records.
        
        Args:
            data: Data records to validate
            
        Returns:
            Validation result
        """
        started_at = datetime.utcnow()
        valid_records = []
        invalid_count = 0
        
        try:
            for record in data:
                if self._validate_record(record):
                    valid_records.append(record)
                else:
                    invalid_count += 1
            
            # Save valid records
            if valid_records:
                self._save_processed_data(valid_records, "validated")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="validation",
                status="success" if len(valid_records) > 0 else "failed",
                records_processed=len(data),
                records_valid=len(valid_records),
                records_invalid=invalid_count,
                duration=duration,
                started_at=started_at,
                completed_at=completed_at
            )
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="validation",
                status=result.status,
                records_processed=len(data),
                duration=duration
            )
            
            return result
            
        except Exception as e:
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            error_message = str(e)
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="validation",
                status="failed",
                records_processed=len(data),
                records_valid=len(valid_records),
                records_invalid=invalid_count,
                duration=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
                error_code="VALIDATION_ERROR"
            )
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="validation",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"Data validation failed: {error_message}",
                processing_stage="validation",
                data_type="records",
                original_exception=e
            )
    
    @handle_exception
    async def transform_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Transform data records.
        
        Args:
            data: Data records to transform
            
        Returns:
            Transformation result
        """
        started_at = datetime.utcnow()
        transformed_records = []
        
        try:
            for record in data:
                transformed_record = self._transform_record(record)
                if transformed_record:
                    transformed_records.append(transformed_record)
            
            # Save transformed records
            if transformed_records:
                self._save_processed_data(transformed_records, "transformed")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="transformation",
                status="success",
                records_processed=len(data),
                records_valid=len(transformed_records),
                records_invalid=len(data) - len(transformed_records),
                duration=duration,
                started_at=started_at,
                completed_at=completed_at
            )
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="transformation",
                status="success",
                records_processed=len(data),
                duration=duration
            )
            
            return result
            
        except Exception as e:
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            error_message = str(e)
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="transformation",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"Data transformation failed: {error_message}",
                processing_stage="transformation",
                data_type="records",
                original_exception=e
            ) 