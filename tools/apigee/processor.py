"""
Apigee API Management data processor.
Processes data from Apigee API Management platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataProcessingError
from ...core.logging_config import log_data_processing
from ...core.models import DataRecord, ProcessingResult
from ..base_tool import BaseProcessor


class ApigeeProcessor(BaseProcessor):
    """Apigee API Management data processor."""
    
    def __init__(self, tool_name: str):
        """
        Initialize Apigee processor.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # API health status mappings
        self.health_status_mapping = {
            "healthy": 1,
            "warning": 2,
            "critical": 3,
            "down": 4
        }
        
        # Performance thresholds
        self.performance_thresholds = {
            "response_time": {
                "excellent": 100,
                "good": 500,
                "fair": 1000,
                "poor": 2000
            },
            "error_rate": {
                "excellent": 0.01,
                "good": 0.05,
                "fair": 0.1,
                "poor": 0.2
            }
        }
        
        self.logger.info("Apigee processor initialized")
    
    async def process_data(
        self, 
        data: List[DataRecord], 
        processing_stage: str = "validation"
    ) -> ProcessingResult:
        """
        Process Apigee data records.
        
        Args:
            data: Raw data records to process
            processing_stage: Stage of processing
            
        Returns:
            Processing result
        """
        if processing_stage == "validation":
            return await self.validate_data(data)
        elif processing_stage == "enrichment":
            return await self.enrich_data(data)
        elif processing_stage == "analytics":
            return await self.analyze_performance(data)
        else:
            return await self.transform_data(data)
    
    async def enrich_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Enrich Apigee data with additional context.
        
        Args:
            data: Data records to enrich
            
        Returns:
            Enrichment result
        """
        started_at = datetime.utcnow()
        enriched_records = []
        
        try:
            for record in data:
                enriched_record = self._enrich_apigee_record(record)
                if enriched_record:
                    enriched_records.append(enriched_record)
            
            # Save enriched records
            if enriched_records:
                self._save_processed_data(enriched_records, "enriched")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="enrichment",
                status="success",
                records_processed=len(data),
                records_valid=len(enriched_records),
                records_invalid=len(data) - len(enriched_records),
                duration=duration,
                started_at=started_at,
                completed_at=completed_at
            )
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="enrichment",
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
                processing_stage="enrichment",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"Apigee data enrichment failed: {error_message}",
                processing_stage="enrichment",
                data_type="records",
                original_exception=e
            )
    
    async def analyze_performance(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Analyze Apigee data for performance insights.
        
        Args:
            data: Data records to analyze
            
        Returns:
            Performance analysis result
        """
        started_at = datetime.utcnow()
        analyzed_records = []
        
        try:
            for record in data:
                analyzed_record = self._analyze_performance_record(record)
                if analyzed_record:
                    analyzed_records.append(analyzed_record)
            
            # Save analyzed records
            if analyzed_records:
                self._save_processed_data(analyzed_records, "performance_analyzed")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="analytics",
                status="success",
                records_processed=len(data),
                records_valid=len(analyzed_records),
                records_invalid=len(data) - len(analyzed_records),
                duration=duration,
                started_at=started_at,
                completed_at=completed_at
            )
            
            log_data_processing(
                tool_name=self.tool_name,
                processing_stage="analytics",
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
                processing_stage="analytics",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"Apigee performance analysis failed: {error_message}",
                processing_stage="analytics",
                data_type="records",
                original_exception=e
            )
    
    def _enrich_apigee_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Enrich an Apigee record with additional context.
        
        Args:
            record: Data record to enrich
            
        Returns:
            Enriched record or None if enrichment failed
        """
        try:
            # Get endpoint type
            endpoint = record.metadata.get("endpoint") if record.metadata else None
            
            if endpoint == "api_proxies":
                record = self._enrich_api_proxy(record)
            elif endpoint == "api_products":
                record = self._enrich_api_product(record)
            elif endpoint == "apps":
                record = self._enrich_app(record)
            elif endpoint == "developers":
                record = self._enrich_developer(record)
            elif endpoint == "analytics":
                record = self._enrich_analytics(record)
            elif endpoint == "security_reports":
                record = self._enrich_security_report(record)
            elif endpoint == "environments":
                record = self._enrich_environment(record)
            elif endpoint == "deployments":
                record = self._enrich_deployment(record)
            
            # Add standard enrichment metadata
            record = self._enrich_record(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Record enrichment failed: {str(e)}")
            return None
    
    def _enrich_api_proxy(self, record: DataRecord) -> DataRecord:
        """Enrich API proxy record."""
        data = record.data
        
        # Add API proxy categorization
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["proxy_type"] = self._categorize_proxy(data)
        record.metadata["deployment_status"] = self._assess_deployment_status(data)
        record.metadata["complexity_score"] = self._calculate_complexity_score(data)
        
        return record
    
    def _enrich_api_product(self, record: DataRecord) -> DataRecord:
        """Enrich API product record."""
        data = record.data
        
        # Add product analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["product_category"] = self._categorize_product(data)
        record.metadata["access_level"] = self._assess_access_level(data)
        record.metadata["monetization_status"] = self._check_monetization(data)
        
        return record
    
    def _enrich_app(self, record: DataRecord) -> DataRecord:
        """Enrich app record."""
        data = record.data
        
        # Add app analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["app_type"] = self._categorize_app(data)
        record.metadata["credential_status"] = self._assess_credential_status(data)
        record.metadata["usage_pattern"] = self._analyze_usage_pattern(data)
        
        return record
    
    def _enrich_developer(self, record: DataRecord) -> DataRecord:
        """Enrich developer record."""
        data = record.data
        
        # Add developer analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["developer_tier"] = self._categorize_developer(data)
        record.metadata["activity_level"] = self._assess_activity_level(data)
        record.metadata["compliance_status"] = self._check_compliance_status(data)
        
        return record
    
    def _enrich_analytics(self, record: DataRecord) -> DataRecord:
        """Enrich analytics record."""
        data = record.data
        
        # Add analytics insights
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["performance_grade"] = self._calculate_performance_grade(data)
        record.metadata["trend_analysis"] = self._analyze_trends(data)
        record.metadata["anomaly_detection"] = self._detect_anomalies(data)
        
        return record
    
    def _enrich_security_report(self, record: DataRecord) -> DataRecord:
        """Enrich security report record."""
        data = record.data
        
        # Add security analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["security_score"] = self._calculate_security_score(data)
        record.metadata["threat_level"] = self._assess_threat_level(data)
        record.metadata["recommendations"] = self._generate_security_recommendations(data)
        
        return record
    
    def _enrich_environment(self, record: DataRecord) -> DataRecord:
        """Enrich environment record."""
        data = record.data
        
        # Add environment analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["environment_health"] = self._assess_environment_health(data)
        record.metadata["resource_utilization"] = self._analyze_resource_usage(data)
        
        return record
    
    def _enrich_deployment(self, record: DataRecord) -> DataRecord:
        """Enrich deployment record."""
        data = record.data
        
        # Add deployment analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["deployment_health"] = self._assess_deployment_health(data)
        record.metadata["version_analysis"] = self._analyze_version_distribution(data)
        
        return record
    
    def _analyze_performance_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Analyze a record for performance insights.
        
        Args:
            record: Data record to analyze
            
        Returns:
            Analyzed record or None if analysis failed
        """
        try:
            # Add performance analysis metadata
            if record.metadata is None:
                record.metadata = {}
            
            record.metadata["overall_performance"] = self._calculate_overall_performance(record)
            record.metadata["bottlenecks"] = self._identify_bottlenecks(record)
            record.metadata["optimization_opportunities"] = self._identify_optimizations(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Performance analysis failed: {str(e)}")
            return None
    
    def _categorize_proxy(self, data: Dict[str, Any]) -> str:
        """Categorize API proxy type."""
        name = data.get("name", "").lower()
        
        if "auth" in name:
            return "authentication"
        elif "payment" in name:
            return "payment"
        elif "user" in name:
            return "user_management"
        elif "data" in name:
            return "data_service"
        else:
            return "general"
    
    def _assess_deployment_status(self, data: Dict[str, Any]) -> str:
        """Assess deployment status."""
        deployments = data.get("deployments", [])
        
        if not deployments:
            return "not_deployed"
        
        active_deployments = [d for d in deployments if d.get("state") == "deployed"]
        
        if len(active_deployments) > 1:
            return "multi_environment"
        elif len(active_deployments) == 1:
            return "single_environment"
        else:
            return "inactive"
    
    def _calculate_complexity_score(self, data: Dict[str, Any]) -> int:
        """Calculate API proxy complexity score."""
        score = 0
        
        # Base score
        score += 1
        
        # Add score for policies
        policies = data.get("policies", [])
        score += len(policies)
        
        # Add score for resources
        resources = data.get("resources", [])
        score += len(resources)
        
        # Add score for targets
        targets = data.get("targets", [])
        score += len(targets) * 2
        
        return score
    
    def _categorize_product(self, data: Dict[str, Any]) -> str:
        """Categorize API product."""
        name = data.get("name", "").lower()
        
        if "premium" in name:
            return "premium"
        elif "basic" in name:
            return "basic"
        elif "enterprise" in name:
            return "enterprise"
        else:
            return "standard"
    
    def _assess_access_level(self, data: Dict[str, Any]) -> str:
        """Assess API product access level."""
        access = data.get("access", "").lower()
        
        if access == "public":
            return "public"
        elif access == "private":
            return "private"
        elif access == "internal":
            return "internal"
        else:
            return "unknown"
    
    def _check_monetization(self, data: Dict[str, Any]) -> bool:
        """Check if API product has monetization enabled."""
        return data.get("monetization", {}).get("enabled", False)
    
    def _categorize_app(self, data: Dict[str, Any]) -> str:
        """Categorize app type."""
        name = data.get("name", "").lower()
        
        if "mobile" in name:
            return "mobile"
        elif "web" in name:
            return "web"
        elif "iot" in name:
            return "iot"
        elif "backend" in name:
            return "backend"
        else:
            return "unknown"
    
    def _assess_credential_status(self, data: Dict[str, Any]) -> str:
        """Assess app credential status."""
        credentials = data.get("credentials", [])
        
        if not credentials:
            return "no_credentials"
        
        active_creds = [c for c in credentials if c.get("status") == "approved"]
        
        if active_creds:
            return "active"
        else:
            return "inactive"
    
    def _analyze_usage_pattern(self, data: Dict[str, Any]) -> str:
        """Analyze app usage pattern."""
        # This would typically analyze usage statistics
        # For now, return a placeholder
        return "normal"
    
    def _categorize_developer(self, data: Dict[str, Any]) -> str:
        """Categorize developer tier."""
        apps = data.get("apps", [])
        
        if len(apps) > 10:
            return "enterprise"
        elif len(apps) > 5:
            return "professional"
        elif len(apps) > 1:
            return "standard"
        else:
            return "basic"
    
    def _assess_activity_level(self, data: Dict[str, Any]) -> str:
        """Assess developer activity level."""
        # This would typically analyze recent activity
        # For now, return a placeholder
        return "active"
    
    def _check_compliance_status(self, data: Dict[str, Any]) -> str:
        """Check developer compliance status."""
        status = data.get("status", "").lower()
        
        if status == "active":
            return "compliant"
        elif status == "inactive":
            return "inactive"
        else:
            return "non_compliant"
    
    def _calculate_performance_grade(self, data: Dict[str, Any]) -> str:
        """Calculate performance grade based on metrics."""
        response_time = data.get("avg_response_time", 0)
        error_rate = data.get("error_rate", 0)
        
        if response_time <= self.performance_thresholds["response_time"]["excellent"] and \
           error_rate <= self.performance_thresholds["error_rate"]["excellent"]:
            return "A"
        elif response_time <= self.performance_thresholds["response_time"]["good"] and \
             error_rate <= self.performance_thresholds["error_rate"]["good"]:
            return "B"
        elif response_time <= self.performance_thresholds["response_time"]["fair"] and \
             error_rate <= self.performance_thresholds["error_rate"]["fair"]:
            return "C"
        else:
            return "D"
    
    def _analyze_trends(self, data: Dict[str, Any]) -> str:
        """Analyze performance trends."""
        # This would typically analyze time series data
        # For now, return a placeholder
        return "stable"
    
    def _detect_anomalies(self, data: Dict[str, Any]) -> List[str]:
        """Detect performance anomalies."""
        anomalies = []
        
        response_time = data.get("avg_response_time", 0)
        error_rate = data.get("error_rate", 0)
        
        if response_time > self.performance_thresholds["response_time"]["poor"]:
            anomalies.append("high_response_time")
        
        if error_rate > self.performance_thresholds["error_rate"]["poor"]:
            anomalies.append("high_error_rate")
        
        return anomalies
    
    def _calculate_security_score(self, data: Dict[str, Any]) -> int:
        """Calculate security score."""
        # This would typically analyze security metrics
        # For now, return a placeholder
        return 75
    
    def _assess_threat_level(self, data: Dict[str, Any]) -> str:
        """Assess threat level."""
        security_score = self._calculate_security_score(data)
        
        if security_score >= 90:
            return "low"
        elif security_score >= 70:
            return "medium"
        else:
            return "high"
    
    def _generate_security_recommendations(self, data: Dict[str, Any]) -> List[str]:
        """Generate security recommendations."""
        recommendations = []
        
        threat_level = self._assess_threat_level(data)
        
        if threat_level == "high":
            recommendations.append("Implement additional security policies")
            recommendations.append("Enable threat protection")
        elif threat_level == "medium":
            recommendations.append("Review access controls")
            recommendations.append("Monitor for suspicious activity")
        
        return recommendations
    
    def _assess_environment_health(self, data: Dict[str, Any]) -> str:
        """Assess environment health."""
        # This would typically analyze environment metrics
        # For now, return a placeholder
        return "healthy"
    
    def _analyze_resource_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource usage."""
        # This would typically analyze resource metrics
        # For now, return a placeholder
        return {
            "cpu_usage": 65,
            "memory_usage": 70,
            "disk_usage": 45
        }
    
    def _assess_deployment_health(self, data: Dict[str, Any]) -> str:
        """Assess deployment health."""
        # This would typically analyze deployment metrics
        # For now, return a placeholder
        return "healthy"
    
    def _analyze_version_distribution(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze version distribution."""
        # This would typically analyze version metrics
        # For now, return a placeholder
        return {
            "total_versions": 3,
            "active_versions": 2,
            "latest_version": "v1.2.0"
        }
    
    def _calculate_overall_performance(self, record: DataRecord) -> str:
        """Calculate overall performance rating."""
        data = record.data
        
        response_time = data.get("avg_response_time", 0)
        error_rate = data.get("error_rate", 0)
        
        if response_time <= 200 and error_rate <= 0.01:
            return "excellent"
        elif response_time <= 500 and error_rate <= 0.05:
            return "good"
        elif response_time <= 1000 and error_rate <= 0.1:
            return "fair"
        else:
            return "poor"
    
    def _identify_bottlenecks(self, record: DataRecord) -> List[str]:
        """Identify performance bottlenecks."""
        bottlenecks = []
        data = record.data
        
        response_time = data.get("avg_response_time", 0)
        error_rate = data.get("error_rate", 0)
        
        if response_time > 1000:
            bottlenecks.append("high_response_time")
        
        if error_rate > 0.1:
            bottlenecks.append("high_error_rate")
        
        return bottlenecks
    
    def _identify_optimizations(self, record: DataRecord) -> List[str]:
        """Identify optimization opportunities."""
        optimizations = []
        
        bottlenecks = self._identify_bottlenecks(record)
        
        if "high_response_time" in bottlenecks:
            optimizations.append("Implement caching")
            optimizations.append("Optimize database queries")
        
        if "high_error_rate" in bottlenecks:
            optimizations.append("Improve error handling")
            optimizations.append("Add input validation")
        
        return optimizations 