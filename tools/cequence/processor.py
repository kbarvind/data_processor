"""
Cequence API Security data processor.
Processes and enriches security data from Cequence API Protection platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataProcessingError
from ...core.logging_config import log_data_processing
from ...core.models import DataRecord, ProcessingResult
from ..base_tool import BaseProcessor


class CequenceProcessor(BaseProcessor):
    """Cequence API Security data processor."""
    
    def __init__(self, tool_name: str):
        """
        Initialize Cequence processor.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # Severity mappings for normalization
        self.severity_mappings = {
            "low": 1,
            "medium": 2,
            "high": 3,
            "critical": 4
        }
        
        # Risk score calculations
        self.risk_score_weights = {
            "vulnerability_severity": 0.4,
            "attack_frequency": 0.3,
            "asset_criticality": 0.2,
            "threat_intelligence": 0.1
        }
        
        self.logger.info("Cequence processor initialized")
    
    async def process_data(
        self, 
        data: List[DataRecord], 
        processing_stage: str = "validation"
    ) -> ProcessingResult:
        """
        Process Cequence data records.
        
        Args:
            data: Raw data records to process
            processing_stage: Stage of processing (validation, enrichment, analysis)
            
        Returns:
            Processing result
        """
        if processing_stage == "validation":
            return await self.validate_data(data)
        elif processing_stage == "enrichment":
            return await self.enrich_data(data)
        elif processing_stage == "analysis":
            return await self.analyze_data(data)
        else:
            return await self.transform_data(data)
    
    async def enrich_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Enrich Cequence data with additional context and metadata.
        
        Args:
            data: Data records to enrich
            
        Returns:
            Enrichment result
        """
        started_at = datetime.utcnow()
        enriched_records = []
        
        try:
            for record in data:
                enriched_record = self._enrich_cequence_record(record)
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
                f"Cequence data enrichment failed: {error_message}",
                processing_stage="enrichment",
                data_type="records",
                original_exception=e
            )
    
    async def analyze_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Analyze Cequence data for security insights.
        
        Args:
            data: Data records to analyze
            
        Returns:
            Analysis result
        """
        started_at = datetime.utcnow()
        analyzed_records = []
        
        try:
            for record in data:
                analyzed_record = self._analyze_cequence_record(record)
                if analyzed_record:
                    analyzed_records.append(analyzed_record)
            
            # Save analyzed records
            if analyzed_records:
                self._save_processed_data(analyzed_records, "analyzed")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="analysis",
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
                processing_stage="analysis",
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
                processing_stage="analysis",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"Cequence data analysis failed: {error_message}",
                processing_stage="analysis",
                data_type="records",
                original_exception=e
            )
    
    def _enrich_cequence_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Enrich a Cequence record with additional context.
        
        Args:
            record: Data record to enrich
            
        Returns:
            Enriched record or None if enrichment failed
        """
        try:
            # Get endpoint type
            endpoint = record.metadata.get("endpoint") if record.metadata else None
            
            if endpoint == "api_inventory":
                record = self._enrich_api_inventory(record)
            elif endpoint == "security_events":
                record = self._enrich_security_events(record)
            elif endpoint == "vulnerability_scans":
                record = self._enrich_vulnerabilities(record)
            elif endpoint == "attack_analytics":
                record = self._enrich_attack_analytics(record)
            elif endpoint == "policy_violations":
                record = self._enrich_policy_violations(record)
            elif endpoint == "threat_intelligence":
                record = self._enrich_threat_intelligence(record)
            
            # Add standard enrichment metadata
            record = self._enrich_record(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Record enrichment failed: {str(e)}")
            return None
    
    def _enrich_api_inventory(self, record: DataRecord) -> DataRecord:
        """Enrich API inventory record."""
        data = record.data
        
        # Add API categorization
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["api_category"] = self._categorize_api(data.get("path", ""))
        record.metadata["risk_level"] = self._calculate_api_risk(data)
        record.metadata["compliance_status"] = self._check_compliance(data)
        
        return record
    
    def _enrich_security_events(self, record: DataRecord) -> DataRecord:
        """Enrich security events record."""
        data = record.data
        
        # Normalize severity
        severity = data.get("severity", "").lower()
        data["severity_score"] = self.severity_mappings.get(severity, 0)
        
        # Add threat classification
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["threat_category"] = self._classify_threat(data)
        record.metadata["impact_score"] = self._calculate_impact_score(data)
        
        return record
    
    def _enrich_vulnerabilities(self, record: DataRecord) -> DataRecord:
        """Enrich vulnerability record."""
        data = record.data
        
        # Add CVSS score if available
        if "cvss_score" in data:
            data["cvss_category"] = self._categorize_cvss(data["cvss_score"])
        
        # Add remediation priority
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["remediation_priority"] = self._calculate_remediation_priority(data)
        record.metadata["exploitability"] = self._assess_exploitability(data)
        
        return record
    
    def _enrich_attack_analytics(self, record: DataRecord) -> DataRecord:
        """Enrich attack analytics record."""
        data = record.data
        
        # Add attack pattern analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["attack_pattern"] = self._identify_attack_pattern(data)
        record.metadata["sophistication_level"] = self._assess_sophistication(data)
        
        return record
    
    def _enrich_policy_violations(self, record: DataRecord) -> DataRecord:
        """Enrich policy violations record."""
        data = record.data
        
        # Add violation impact
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["violation_impact"] = self._assess_violation_impact(data)
        record.metadata["compliance_risk"] = self._calculate_compliance_risk(data)
        
        return record
    
    def _enrich_threat_intelligence(self, record: DataRecord) -> DataRecord:
        """Enrich threat intelligence record."""
        data = record.data
        
        # Add threat context
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["threat_context"] = self._analyze_threat_context(data)
        record.metadata["actionability"] = self._assess_actionability(data)
        
        return record
    
    def _analyze_cequence_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Analyze a Cequence record for security insights.
        
        Args:
            record: Data record to analyze
            
        Returns:
            Analyzed record or None if analysis failed
        """
        try:
            # Calculate overall risk score
            risk_score = self._calculate_risk_score(record)
            
            # Add analysis metadata
            if record.metadata is None:
                record.metadata = {}
            
            record.metadata["risk_score"] = risk_score
            record.metadata["risk_category"] = self._categorize_risk(risk_score)
            record.metadata["recommendations"] = self._generate_recommendations(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Record analysis failed: {str(e)}")
            return None
    
    def _categorize_api(self, path: str) -> str:
        """Categorize API based on path."""
        if "/admin" in path:
            return "administrative"
        elif "/auth" in path:
            return "authentication"
        elif "/payment" in path:
            return "financial"
        elif "/user" in path:
            return "user_management"
        else:
            return "general"
    
    def _calculate_api_risk(self, data: Dict[str, Any]) -> str:
        """Calculate API risk level."""
        risk_factors = 0
        
        # Check for sensitive paths
        path = data.get("path", "").lower()
        if any(keyword in path for keyword in ["admin", "payment", "auth"]):
            risk_factors += 2
        
        # Check for deprecated APIs
        if data.get("deprecated", False):
            risk_factors += 1
        
        # Check for public exposure
        if data.get("public", False):
            risk_factors += 1
        
        if risk_factors >= 3:
            return "high"
        elif risk_factors >= 2:
            return "medium"
        else:
            return "low"
    
    def _check_compliance(self, data: Dict[str, Any]) -> str:
        """Check compliance status."""
        # Basic compliance checks
        if data.get("tls_enabled", False) and data.get("auth_required", False):
            return "compliant"
        else:
            return "non_compliant"
    
    def _classify_threat(self, data: Dict[str, Any]) -> str:
        """Classify threat type."""
        event_type = data.get("event_type", "").lower()
        
        if "injection" in event_type:
            return "injection_attack"
        elif "ddos" in event_type:
            return "availability_attack"
        elif "auth" in event_type:
            return "authentication_attack"
        elif "data" in event_type:
            return "data_breach"
        else:
            return "unknown"
    
    def _calculate_impact_score(self, data: Dict[str, Any]) -> int:
        """Calculate impact score."""
        severity = data.get("severity", "").lower()
        severity_score = self.severity_mappings.get(severity, 0)
        
        # Factor in affected endpoints
        affected_endpoints = data.get("affected_endpoints", 0)
        endpoint_factor = min(affected_endpoints / 10, 1.0)
        
        return int(severity_score * 25 * (1 + endpoint_factor))
    
    def _categorize_cvss(self, cvss_score: float) -> str:
        """Categorize CVSS score."""
        if cvss_score >= 9.0:
            return "critical"
        elif cvss_score >= 7.0:
            return "high"
        elif cvss_score >= 4.0:
            return "medium"
        else:
            return "low"
    
    def _calculate_remediation_priority(self, data: Dict[str, Any]) -> int:
        """Calculate remediation priority (1-10)."""
        severity = data.get("severity", "").lower()
        severity_score = self.severity_mappings.get(severity, 0)
        
        # Factor in exploitability
        exploitable = data.get("exploitable", False)
        public_exploit = data.get("public_exploit", False)
        
        priority = severity_score * 2
        if exploitable:
            priority += 1
        if public_exploit:
            priority += 1
        
        return min(priority, 10)
    
    def _assess_exploitability(self, data: Dict[str, Any]) -> str:
        """Assess exploitability level."""
        if data.get("public_exploit", False):
            return "high"
        elif data.get("proof_of_concept", False):
            return "medium"
        elif data.get("exploitable", False):
            return "low"
        else:
            return "none"
    
    def _identify_attack_pattern(self, data: Dict[str, Any]) -> str:
        """Identify attack pattern."""
        attack_type = data.get("attack_type", "").lower()
        
        if "brute" in attack_type:
            return "brute_force"
        elif "scan" in attack_type:
            return "reconnaissance"
        elif "injection" in attack_type:
            return "injection"
        else:
            return "unknown"
    
    def _assess_sophistication(self, data: Dict[str, Any]) -> str:
        """Assess attack sophistication."""
        indicators = data.get("indicators", {})
        
        if indicators.get("advanced_evasion", False):
            return "high"
        elif indicators.get("automated", False):
            return "medium"
        else:
            return "low"
    
    def _assess_violation_impact(self, data: Dict[str, Any]) -> str:
        """Assess policy violation impact."""
        severity = data.get("severity", "").lower()
        
        if severity in ["high", "critical"]:
            return "high"
        elif severity == "medium":
            return "medium"
        else:
            return "low"
    
    def _calculate_compliance_risk(self, data: Dict[str, Any]) -> int:
        """Calculate compliance risk score."""
        severity = data.get("severity", "").lower()
        severity_score = self.severity_mappings.get(severity, 0)
        
        # Factor in policy type
        policy_type = data.get("policy_type", "").lower()
        if "regulatory" in policy_type:
            return severity_score * 30
        elif "security" in policy_type:
            return severity_score * 25
        else:
            return severity_score * 20
    
    def _analyze_threat_context(self, data: Dict[str, Any]) -> str:
        """Analyze threat context."""
        threat_type = data.get("type", "").lower()
        confidence = data.get("confidence", "").lower()
        
        if confidence == "high" and threat_type in ["malware", "apt"]:
            return "active_threat"
        elif confidence == "medium":
            return "potential_threat"
        else:
            return "informational"
    
    def _assess_actionability(self, data: Dict[str, Any]) -> str:
        """Assess threat intelligence actionability."""
        indicators = data.get("indicators", {})
        
        if indicators.get("iocs", []):
            return "high"
        elif indicators.get("patterns", []):
            return "medium"
        else:
            return "low"
    
    def _calculate_risk_score(self, record: DataRecord) -> int:
        """Calculate overall risk score."""
        # Base risk score calculation
        base_score = 50
        
        # Factor in severity if available
        if record.data.get("severity"):
            severity = record.data["severity"].lower()
            severity_score = self.severity_mappings.get(severity, 0)
            base_score += severity_score * 15
        
        # Factor in metadata
        if record.metadata:
            if record.metadata.get("threat_category") == "injection_attack":
                base_score += 20
            if record.metadata.get("exploitability") == "high":
                base_score += 15
        
        return min(base_score, 100)
    
    def _categorize_risk(self, risk_score: int) -> str:
        """Categorize risk level."""
        if risk_score >= 80:
            return "critical"
        elif risk_score >= 60:
            return "high"
        elif risk_score >= 40:
            return "medium"
        else:
            return "low"
    
    def _generate_recommendations(self, record: DataRecord) -> List[str]:
        """Generate security recommendations."""
        recommendations = []
        
        endpoint = record.metadata.get("endpoint") if record.metadata else None
        
        if endpoint == "vulnerability_scans":
            recommendations.append("Apply security patches immediately")
            recommendations.append("Implement additional access controls")
        elif endpoint == "security_events":
            recommendations.append("Review and update security policies")
            recommendations.append("Monitor for similar attack patterns")
        elif endpoint == "api_inventory":
            recommendations.append("Implement API rate limiting")
            recommendations.append("Add input validation")
        
        return recommendations 