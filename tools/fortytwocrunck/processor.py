"""
42Crunch API Security data processor.
Processes security data from 42Crunch API Security platform.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from ...core.exceptions import DataProcessingError
from ...core.logging_config import log_data_processing
from ...core.models import DataRecord, ProcessingResult
from ..base_tool import BaseProcessor


class FortyTwoCrunchProcessor(BaseProcessor):
    """42Crunch API Security data processor."""
    
    def __init__(self, tool_name: str):
        """
        Initialize 42Crunch processor.
        
        Args:
            tool_name: Name of the tool
        """
        super().__init__(tool_name)
        
        # Severity mappings for 42Crunch
        self.severity_mappings = {
            "info": 1,
            "low": 2,
            "medium": 3,
            "high": 4,
            "critical": 5
        }
        
        # Compliance framework mappings
        self.compliance_frameworks = {
            "owasp": "OWASP API Security Top 10",
            "pci": "PCI DSS",
            "gdpr": "GDPR",
            "hipaa": "HIPAA",
            "sox": "SOX"
        }
        
        self.logger.info("42Crunch processor initialized")
    
    async def process_data(
        self, 
        data: List[DataRecord], 
        processing_stage: str = "validation"
    ) -> ProcessingResult:
        """
        Process 42Crunch data records.
        
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
        elif processing_stage == "compliance":
            return await self.analyze_compliance(data)
        else:
            return await self.transform_data(data)
    
    async def enrich_data(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Enrich 42Crunch data with additional context.
        
        Args:
            data: Data records to enrich
            
        Returns:
            Enrichment result
        """
        started_at = datetime.utcnow()
        enriched_records = []
        
        try:
            for record in data:
                enriched_record = self._enrich_fortytwo_record(record)
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
                f"42Crunch data enrichment failed: {error_message}",
                processing_stage="enrichment",
                data_type="records",
                original_exception=e
            )
    
    async def analyze_compliance(self, data: List[DataRecord]) -> ProcessingResult:
        """
        Analyze 42Crunch data for compliance.
        
        Args:
            data: Data records to analyze
            
        Returns:
            Compliance analysis result
        """
        started_at = datetime.utcnow()
        analyzed_records = []
        
        try:
            for record in data:
                analyzed_record = self._analyze_compliance_record(record)
                if analyzed_record:
                    analyzed_records.append(analyzed_record)
            
            # Save analyzed records
            if analyzed_records:
                self._save_processed_data(analyzed_records, "compliance_analyzed")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()
            
            result = ProcessingResult(
                tool_name=self.tool_name,
                processing_stage="compliance",
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
                processing_stage="compliance",
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
                processing_stage="compliance",
                status="failed",
                records_processed=len(data),
                duration=duration,
                error=error_message
            )
            
            raise DataProcessingError(
                f"42Crunch compliance analysis failed: {error_message}",
                processing_stage="compliance",
                data_type="records",
                original_exception=e
            )
    
    def _enrich_fortytwo_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Enrich a 42Crunch record with additional context.
        
        Args:
            record: Data record to enrich
            
        Returns:
            Enriched record or None if enrichment failed
        """
        try:
            # Get endpoint type
            endpoint = record.metadata.get("endpoint") if record.metadata else None
            
            if endpoint == "security_audits":
                record = self._enrich_security_audit(record)
            elif endpoint == "conformance_scans":
                record = self._enrich_conformance_scan(record)
            elif endpoint == "runtime_protection":
                record = self._enrich_runtime_protection(record)
            elif endpoint == "api_definitions":
                record = self._enrich_api_definition(record)
            elif endpoint == "vulnerability_reports":
                record = self._enrich_vulnerability_report(record)
            elif endpoint == "compliance_reports":
                record = self._enrich_compliance_report(record)
            
            # Add standard enrichment metadata
            record = self._enrich_record(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Record enrichment failed: {str(e)}")
            return None
    
    def _enrich_security_audit(self, record: DataRecord) -> DataRecord:
        """Enrich security audit record."""
        data = record.data
        
        # Add audit categorization
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["audit_type"] = self._categorize_audit(data)
        record.metadata["severity_score"] = self._calculate_severity_score(data)
        record.metadata["remediation_effort"] = self._estimate_remediation_effort(data)
        
        return record
    
    def _enrich_conformance_scan(self, record: DataRecord) -> DataRecord:
        """Enrich conformance scan record."""
        data = record.data
        
        # Add conformance analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["conformance_level"] = self._assess_conformance_level(data)
        record.metadata["standard_compliance"] = self._check_standard_compliance(data)
        
        return record
    
    def _enrich_runtime_protection(self, record: DataRecord) -> DataRecord:
        """Enrich runtime protection record."""
        data = record.data
        
        # Add runtime analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["protection_effectiveness"] = self._assess_protection_effectiveness(data)
        record.metadata["attack_pattern"] = self._identify_attack_pattern(data)
        
        return record
    
    def _enrich_api_definition(self, record: DataRecord) -> DataRecord:
        """Enrich API definition record."""
        data = record.data
        
        # Add API analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["api_quality"] = self._assess_api_quality(data)
        record.metadata["security_posture"] = self._assess_security_posture(data)
        
        return record
    
    def _enrich_vulnerability_report(self, record: DataRecord) -> DataRecord:
        """Enrich vulnerability report record."""
        data = record.data
        
        # Add vulnerability analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["vulnerability_category"] = self._categorize_vulnerability(data)
        record.metadata["exploitability"] = self._assess_exploitability(data)
        record.metadata["business_impact"] = self._assess_business_impact(data)
        
        return record
    
    def _enrich_compliance_report(self, record: DataRecord) -> DataRecord:
        """Enrich compliance report record."""
        data = record.data
        
        # Add compliance analysis
        if record.metadata is None:
            record.metadata = {}
        
        record.metadata["compliance_score"] = self._calculate_compliance_score(data)
        record.metadata["framework_coverage"] = self._assess_framework_coverage(data)
        
        return record
    
    def _analyze_compliance_record(self, record: DataRecord) -> Optional[DataRecord]:
        """
        Analyze a record for compliance requirements.
        
        Args:
            record: Data record to analyze
            
        Returns:
            Analyzed record or None if analysis failed
        """
        try:
            # Add compliance analysis metadata
            if record.metadata is None:
                record.metadata = {}
            
            record.metadata["compliance_status"] = self._determine_compliance_status(record)
            record.metadata["required_actions"] = self._identify_required_actions(record)
            record.metadata["regulatory_impact"] = self._assess_regulatory_impact(record)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Compliance analysis failed: {str(e)}")
            return None
    
    def _categorize_audit(self, data: Dict[str, Any]) -> str:
        """Categorize security audit type."""
        audit_type = data.get("audit_type", "").lower()
        
        if "static" in audit_type:
            return "static_analysis"
        elif "dynamic" in audit_type:
            return "dynamic_analysis"
        elif "conformance" in audit_type:
            return "conformance_check"
        else:
            return "general_audit"
    
    def _calculate_severity_score(self, data: Dict[str, Any]) -> int:
        """Calculate severity score."""
        severity = data.get("severity", "").lower()
        return self.severity_mappings.get(severity, 0) * 20
    
    def _estimate_remediation_effort(self, data: Dict[str, Any]) -> str:
        """Estimate remediation effort."""
        issue_count = data.get("issue_count", 0)
        severity = data.get("severity", "").lower()
        
        if severity in ["critical", "high"] and issue_count > 10:
            return "high"
        elif severity == "medium" and issue_count > 5:
            return "medium"
        else:
            return "low"
    
    def _assess_conformance_level(self, data: Dict[str, Any]) -> str:
        """Assess conformance level."""
        conformance_score = data.get("conformance_score", 0)
        
        if conformance_score >= 90:
            return "excellent"
        elif conformance_score >= 70:
            return "good"
        elif conformance_score >= 50:
            return "fair"
        else:
            return "poor"
    
    def _check_standard_compliance(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Check compliance with various standards."""
        standards = data.get("standards", [])
        compliance = {}
        
        for standard in self.compliance_frameworks.keys():
            compliance[standard] = standard in standards
        
        return compliance
    
    def _assess_protection_effectiveness(self, data: Dict[str, Any]) -> str:
        """Assess runtime protection effectiveness."""
        blocked_attacks = data.get("blocked_attacks", 0)
        total_attacks = data.get("total_attacks", 0)
        
        if total_attacks == 0:
            return "no_attacks"
        
        effectiveness = blocked_attacks / total_attacks
        
        if effectiveness >= 0.95:
            return "excellent"
        elif effectiveness >= 0.85:
            return "good"
        elif effectiveness >= 0.70:
            return "fair"
        else:
            return "poor"
    
    def _identify_attack_pattern(self, data: Dict[str, Any]) -> str:
        """Identify attack pattern."""
        attack_type = data.get("attack_type", "").lower()
        
        if "injection" in attack_type:
            return "injection"
        elif "ddos" in attack_type:
            return "ddos"
        elif "auth" in attack_type:
            return "authentication"
        elif "xss" in attack_type:
            return "cross_site_scripting"
        else:
            return "unknown"
    
    def _assess_api_quality(self, data: Dict[str, Any]) -> str:
        """Assess API definition quality."""
        quality_score = data.get("quality_score", 0)
        
        if quality_score >= 90:
            return "excellent"
        elif quality_score >= 70:
            return "good"
        elif quality_score >= 50:
            return "fair"
        else:
            return "poor"
    
    def _assess_security_posture(self, data: Dict[str, Any]) -> str:
        """Assess API security posture."""
        security_score = data.get("security_score", 0)
        
        if security_score >= 90:
            return "strong"
        elif security_score >= 70:
            return "adequate"
        elif security_score >= 50:
            return "weak"
        else:
            return "poor"
    
    def _categorize_vulnerability(self, data: Dict[str, Any]) -> str:
        """Categorize vulnerability type."""
        vulnerability_type = data.get("vulnerability_type", "").lower()
        
        if "injection" in vulnerability_type:
            return "injection"
        elif "auth" in vulnerability_type:
            return "authentication"
        elif "access" in vulnerability_type:
            return "access_control"
        elif "crypto" in vulnerability_type:
            return "cryptographic"
        else:
            return "other"
    
    def _assess_exploitability(self, data: Dict[str, Any]) -> str:
        """Assess vulnerability exploitability."""
        exploitability_score = data.get("exploitability_score", 0)
        
        if exploitability_score >= 8:
            return "high"
        elif exploitability_score >= 5:
            return "medium"
        else:
            return "low"
    
    def _assess_business_impact(self, data: Dict[str, Any]) -> str:
        """Assess business impact of vulnerability."""
        impact_score = data.get("impact_score", 0)
        
        if impact_score >= 8:
            return "high"
        elif impact_score >= 5:
            return "medium"
        else:
            return "low"
    
    def _calculate_compliance_score(self, data: Dict[str, Any]) -> int:
        """Calculate compliance score."""
        return data.get("compliance_score", 0)
    
    def _assess_framework_coverage(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Assess framework coverage."""
        coverage = {}
        frameworks = data.get("frameworks", {})
        
        for framework in self.compliance_frameworks.keys():
            coverage[framework] = frameworks.get(framework, 0.0)
        
        return coverage
    
    def _determine_compliance_status(self, record: DataRecord) -> str:
        """Determine overall compliance status."""
        data = record.data
        compliance_score = data.get("compliance_score", 0)
        
        if compliance_score >= 95:
            return "fully_compliant"
        elif compliance_score >= 80:
            return "mostly_compliant"
        elif compliance_score >= 60:
            return "partially_compliant"
        else:
            return "non_compliant"
    
    def _identify_required_actions(self, record: DataRecord) -> List[str]:
        """Identify required compliance actions."""
        actions = []
        endpoint = record.metadata.get("endpoint") if record.metadata else None
        
        if endpoint == "vulnerability_reports":
            actions.append("Remediate high-severity vulnerabilities")
            actions.append("Implement additional security controls")
        elif endpoint == "security_audits":
            actions.append("Address audit findings")
            actions.append("Update security policies")
        elif endpoint == "conformance_scans":
            actions.append("Fix conformance issues")
            actions.append("Update API documentation")
        
        return actions
    
    def _assess_regulatory_impact(self, record: DataRecord) -> str:
        """Assess regulatory impact."""
        data = record.data
        severity = data.get("severity", "").lower()
        
        if severity in ["critical", "high"]:
            return "high"
        elif severity == "medium":
            return "medium"
        else:
            return "low" 