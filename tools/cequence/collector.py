"""
Cequence API Security data collector.
Collects security data from Cequence API Protection platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataCollectionException
from ...core.logging_config import log_data_collection
from ...core.models import CollectionResult, DataRecord
from ..base_tool import BaseCollector


class CequenceCollector(BaseCollector):
    """Cequence API Security data collector."""
    
    def __init__(self, tool_name: str):
        """
        Initialize Cequence collector.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # Configure collection endpoints
        self.collection_endpoints = {
            "api_inventory": "/api/v1/inventory",
            "security_events": "/api/v1/events",
            "vulnerability_scans": "/api/v1/vulnerabilities",
            "attack_analytics": "/api/v1/analytics/attacks",
            "policy_violations": "/api/v1/policy/violations",
            "threat_intelligence": "/api/v1/threats"
        }
        
        # Configure collection parameters
        self.collection_params = {
            "api_inventory": {
                "include_deprecated": True,
                "include_internal": True,
                "sort": "last_seen",
                "order": "desc"
            },
            "security_events": {
                "severity": "medium,high,critical",
                "limit": 1000,
                "sort": "timestamp",
                "order": "desc"
            },
            "vulnerability_scans": {
                "status": "active,resolved",
                "limit": 500,
                "include_details": True
            },
            "attack_analytics": {
                "time_range": "24h",
                "group_by": "attack_type",
                "include_patterns": True
            },
            "policy_violations": {
                "status": "active",
                "limit": 500,
                "include_context": True
            },
            "threat_intelligence": {
                "confidence": "medium,high",
                "limit": 200,
                "include_iocs": True
            }
        }
        
        self.logger.info("Cequence collector initialized")
    
    async def collect_data(self, collection_type: str = "scheduled") -> CollectionResult:
        """
        Collect data from Cequence API Security platform.
        
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
                    if endpoint_name in ["security_events", "attack_analytics"]:
                        params["start_time"] = (started_at.timestamp() - 86400) * 1000  # 24h ago
                        params["end_time"] = started_at.timestamp() * 1000
                    
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
                f"Cequence data collection failed: {error_message}",
                tool_name=self.tool_name,
                collection_type=collection_type,
                original_exception=e
            )
    
    def _validate_record(self, record: DataRecord) -> bool:
        """
        Validate Cequence-specific data record.
        
        Args:
            record: Data record to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not super()._validate_record(record):
            return False
        
        # Cequence-specific validation
        data = record.data
        
        # Check for required fields based on endpoint
        endpoint = record.metadata.get("endpoint") if record.metadata else None
        
        if endpoint == "api_inventory":
            required_fields = ["api_id", "name", "path", "method"]
        elif endpoint == "security_events":
            required_fields = ["event_id", "timestamp", "severity", "event_type"]
        elif endpoint == "vulnerability_scans":
            required_fields = ["vulnerability_id", "severity", "description", "endpoint"]
        elif endpoint == "attack_analytics":
            required_fields = ["attack_type", "timestamp", "source_ip", "target_endpoint"]
        elif endpoint == "policy_violations":
            required_fields = ["violation_id", "policy_name", "timestamp", "severity"]
        elif endpoint == "threat_intelligence":
            required_fields = ["threat_id", "type", "confidence", "description"]
        else:
            # Generic validation for unknown endpoints
            required_fields = ["id", "timestamp"]
        
        # Check if all required fields are present
        for field in required_fields:
            if field not in data:
                self.logger.warning(f"Missing required field '{field}' in {endpoint} record")
                return False
        
        return True 