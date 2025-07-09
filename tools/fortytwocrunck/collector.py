"""
42Crunch API Security data collector.
Collects security data from 42Crunch API Security platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataCollectionException
from ...core.logging_config import log_data_collection
from ...core.models import CollectionResult, DataRecord
from ..base_tool import BaseCollector


class FortyTwoCrunchCollector(BaseCollector):
    """42Crunch API Security data collector."""
    
    def __init__(self, tool_name: str):
        """
        Initialize 42Crunch collector.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # Configure collection endpoints
        self.collection_endpoints = {
            "security_audits": "/api/v1/security/audits",
            "conformance_scans": "/api/v1/conformance/scans",
            "runtime_protection": "/api/v1/runtime/protection",
            "api_definitions": "/api/v1/apis/definitions",
            "vulnerability_reports": "/api/v1/vulnerabilities/reports",
            "compliance_reports": "/api/v1/compliance/reports"
        }
        
        # Configure collection parameters
        self.collection_params = {
            "security_audits": {
                "status": "completed",
                "limit": 100,
                "include_details": True,
                "sort": "created_at",
                "order": "desc"
            },
            "conformance_scans": {
                "status": "completed,failed",
                "limit": 100,
                "include_remediation": True
            },
            "runtime_protection": {
                "time_range": "24h",
                "include_blocked": True,
                "include_alerts": True
            },
            "api_definitions": {
                "include_metadata": True,
                "include_endpoints": True,
                "validation_status": "all"
            },
            "vulnerability_reports": {
                "severity": "medium,high,critical",
                "status": "open,in_progress",
                "limit": 200
            },
            "compliance_reports": {
                "frameworks": "owasp,pci,gdpr",
                "status": "all",
                "include_recommendations": True
            }
        }
        
        self.logger.info("42Crunch collector initialized")
    
    async def collect_data(self, collection_type: str = "scheduled") -> CollectionResult:
        """
        Collect data from 42Crunch API Security platform.
        
        Args:
            collection_type: Type of collection (scheduled, manual)
            
        Returns:
            Collection result
        """
        started_at = datetime.utcnow()
        collected_records = []
        
        try:
            # Collect from all endpoints
            for endpoint_name, endpoint_url in self.collection_endpoints.items():
                try:
                    self.logger.info(f"Collecting from {endpoint_name}")
                    
                    # Get endpoint-specific parameters
                    params = self.collection_params.get(endpoint_name, {})
                    
                    # Add time range for time-sensitive endpoints
                    if endpoint_name in ["runtime_protection"]:
                        params["start_time"] = int((started_at.timestamp() - 86400) * 1000)
                        params["end_time"] = int(started_at.timestamp() * 1000)
                    
                    # Collect data from endpoint
                    raw_data = await self._collect_from_endpoint(endpoint_url, params)
                    
                    # Convert to records
                    records = self._convert_to_records(raw_data)
                    
                    # Add endpoint metadata
                    for record in records:
                        if record.metadata is None:
                            record.metadata = {}
                        record.metadata["endpoint"] = endpoint_name
                        record.metadata["collection_type"] = collection_type
                    
                    collected_records.extend(records)
                    
                    self.logger.info(f"Collected {len(records)} records from {endpoint_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to collect from {endpoint_name}: {str(e)}")
                    continue
            
            # Save raw data
            if collected_records:
                raw_data_for_save = [record.data for record in collected_records]
                self._save_raw_data(raw_data_for_save, collection_type)
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = CollectionResult(
                tool_name=self.tool_name,
                collection_type=collection_type,
                status="success" if collected_records else "no_data",
                records_collected=len(collected_records),
                duration=duration,
                started_at=started_at,
                completed_at=completed_at,
                endpoints_collected=list(self.collection_endpoints.keys())
            )
            
            log_data_collection(
                tool_name=self.tool_name,
                collection_type=collection_type,
                status=result.status,
                records_collected=len(collected_records),
                duration=duration
            )
            
            return result
            
        except Exception as e:
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            error_message = str(e)
            
            result = CollectionResult(
                tool_name=self.tool_name,
                collection_type=collection_type,
                status="failed",
                records_collected=len(collected_records),
                duration=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
                error_code="COLLECTION_ERROR"
            )
            
            log_data_collection(
                tool_name=self.tool_name,
                collection_type=collection_type,
                status="failed",
                records_collected=len(collected_records),
                duration=duration,
                error=error_message
            )
            
            raise DataCollectionException(
                f"42Crunch data collection failed: {error_message}",
                tool_name=self.tool_name,
                collection_type=collection_type,
                original_exception=e
            ) 