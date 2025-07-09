"""
Apigee API Management data collector.
Collects data from Apigee API Management platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataCollectionException
from ...core.logging_config import log_data_collection
from ...core.models import CollectionResult, DataRecord
from ..base_tool import BaseCollector


class ApigeeCollector(BaseCollector):
    """Apigee API Management data collector."""
    
    def __init__(self, tool_name: str):
        """
        Initialize Apigee collector.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # Configure collection endpoints
        self.collection_endpoints = {
            "api_proxies": "/v1/organizations/{org}/apis",
            "api_products": "/v1/organizations/{org}/apiproducts",
            "apps": "/v1/organizations/{org}/apps",
            "developers": "/v1/organizations/{org}/developers",
            "analytics": "/v1/organizations/{org}/analytics",
            "security_reports": "/v1/organizations/{org}/security/reports",
            "environments": "/v1/organizations/{org}/environments",
            "virtual_hosts": "/v1/organizations/{org}/environments/{env}/virtualhosts",
            "policies": "/v1/organizations/{org}/apis/{api}/revisions/{rev}/policies",
            "deployments": "/v1/organizations/{org}/deployments"
        }
        
        # Configure collection parameters
        self.collection_params = {
            "api_proxies": {
                "includeMeta": "true",
                "includeRevisions": "true"
            },
            "api_products": {
                "expand": "true",
                "count": "1000"
            },
            "apps": {
                "expand": "true",
                "count": "1000",
                "includeCred": "true"
            },
            "developers": {
                "expand": "true",
                "count": "1000"
            },
            "analytics": {
                "select": "sum(message_count),sum(error_count),avg(total_response_time)",
                "timeRange": "last24hours",
                "timeUnit": "hour"
            },
            "security_reports": {
                "timeRange": "last24hours",
                "format": "json"
            },
            "environments": {
                "expand": "true"
            },
            "deployments": {
                "expand": "true"
            }
        }
        
        self.logger.info("Apigee collector initialized")
    
    async def collect_data(self, collection_type: str = "scheduled") -> CollectionResult:
        """
        Collect data from Apigee API Management platform.
        
        Args:
            collection_type: Type of collection (scheduled, manual)
            
        Returns:
            Collection result
        """
        started_at = datetime.utcnow()
        collected_records = []
        
        try:
            # Get organization from config
            org = self.config.base_url.split("/")[-1] if self.config else "default"
            
            # Collect from all endpoints
            for endpoint_name, endpoint_url in self.collection_endpoints.items():
                try:
                    self.logger.info(f"Collecting from {endpoint_name}")
                    
                    # Format endpoint URL with organization
                    formatted_url = endpoint_url.format(org=org, env="prod", api="", rev="")
                    
                    # Get endpoint-specific parameters
                    params = self.collection_params.get(endpoint_name, {})
                    
                    # Skip complex endpoints that require additional parameters
                    if "{" in formatted_url and endpoint_name in ["virtual_hosts", "policies"]:
                        self.logger.info(f"Skipping {endpoint_name} - requires additional parameters")
                        continue
                    
                    # Collect data from endpoint
                    raw_data = await self._collect_from_endpoint(formatted_url, params)
                    
                    # Convert to records
                    records = self._convert_to_records(raw_data)
                    
                    # Add endpoint metadata
                    for record in records:
                        if record.metadata is None:
                            record.metadata = {}
                        record.metadata["endpoint"] = endpoint_name
                        record.metadata["collection_type"] = collection_type
                        record.metadata["organization"] = org
                    
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
                f"Apigee data collection failed: {error_message}",
                tool_name=self.tool_name,
                collection_type=collection_type,
                original_exception=e
            ) 