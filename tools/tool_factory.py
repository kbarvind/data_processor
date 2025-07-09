"""
Tool factory for creating tool instances.
Provides centralized tool instantiation and management.
"""

from typing import Dict, Type, Any

from ..core.exceptions import ConfigurationException
from ..core.logging_config import get_logger
from .base_tool import BaseCollector, BaseProcessor

# Import specific tool implementations
from .cequence.collector import CequenceCollector
from .cequence.processor import CequenceProcessor
from .fortytwocrunck.collector import FortyTwoCrunchCollector
from .fortytwocrunck.processor import FortyTwoCrunchProcessor
from .apigee.collector import ApigeeCollector
from .apigee.processor import ApigeeProcessor

logger = get_logger(__name__)


class ToolFactory:
    """Factory for creating tool instances."""
    
    # Registry of available collectors
    COLLECTORS: Dict[str, Type[BaseCollector]] = {
        "cequence": CequenceCollector,
        "42crunch": FortyTwoCrunchCollector,
        "apigee": ApigeeCollector,
    }
    
    # Registry of available processors
    PROCESSORS: Dict[str, Type[BaseProcessor]] = {
        "cequence": CequenceProcessor,
        "42crunch": FortyTwoCrunchProcessor,
        "apigee": ApigeeProcessor,
    }
    
    @classmethod
    def create_collector(
        self, 
        tool_name: str, 
        credentials: Dict[str, Any]
    ) -> BaseCollector:
        """
        Create a collector instance for the specified tool.
        
        Args:
            tool_name: Name of the tool
            credentials: Tool credentials
            
        Returns:
            Collector instance
            
        Raises:
            ConfigurationException: If tool is not supported
        """
        if tool_name not in self.COLLECTORS:
            available_tools = list(self.COLLECTORS.keys())
            raise ConfigurationException(
                f"Unsupported tool for collection: '{tool_name}'. Available tools: {available_tools}",
                config_key="tool_name",
                details={"available_tools": available_tools}
            )
        
        collector_class = self.COLLECTORS[tool_name]
        collector = collector_class(tool_name)
        collector.load_credentials(credentials)
        
        logger.info("Collector created", tool_name=tool_name, collector_class=collector_class.__name__)
        return collector
    
    @classmethod
    def create_processor(
        self, 
        tool_name: str, 
        credentials: Dict[str, Any]
    ) -> BaseProcessor:
        """
        Create a processor instance for the specified tool.
        
        Args:
            tool_name: Name of the tool
            credentials: Tool credentials
            
        Returns:
            Processor instance
            
        Raises:
            ConfigurationException: If tool is not supported
        """
        if tool_name not in self.PROCESSORS:
            available_tools = list(self.PROCESSORS.keys())
            raise ConfigurationException(
                f"Unsupported tool for processing: '{tool_name}'. Available tools: {available_tools}",
                config_key="tool_name",
                details={"available_tools": available_tools}
            )
        
        processor_class = self.PROCESSORS[tool_name]
        processor = processor_class(tool_name)
        processor.load_credentials(credentials)
        
        logger.info("Processor created", tool_name=tool_name, processor_class=processor_class.__name__)
        return processor
    
    @classmethod
    def get_supported_tools(cls) -> Dict[str, Dict[str, bool]]:
        """
        Get list of supported tools and their capabilities.
        
        Returns:
            Dictionary mapping tool names to their capabilities
        """
        supported_tools = {}
        
        all_tools = set(cls.COLLECTORS.keys()) | set(cls.PROCESSORS.keys())
        
        for tool_name in all_tools:
            supported_tools[tool_name] = {
                "collection": tool_name in cls.COLLECTORS,
                "processing": tool_name in cls.PROCESSORS
            }
        
        return supported_tools
    
    @classmethod
    def register_collector(cls, tool_name: str, collector_class: Type[BaseCollector]) -> None:
        """
        Register a new collector class.
        
        Args:
            tool_name: Name of the tool
            collector_class: Collector class to register
        """
        cls.COLLECTORS[tool_name] = collector_class
        logger.info("Collector registered", tool_name=tool_name, collector_class=collector_class.__name__)
    
    @classmethod
    def register_processor(cls, tool_name: str, processor_class: Type[BaseProcessor]) -> None:
        """
        Register a new processor class.
        
        Args:
            tool_name: Name of the tool
            processor_class: Processor class to register
        """
        cls.PROCESSORS[tool_name] = processor_class
        logger.info("Processor registered", tool_name=tool_name, processor_class=processor_class.__name__) 