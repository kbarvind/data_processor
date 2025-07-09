#!/usr/bin/env python3
"""
Basic usage example for the Data Processing Framework.
Demonstrates data collection, processing, and error handling.
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime

from core.config_manager import config_manager
from core.exceptions import DataCollectionException, DataProcessingError
from core.logging_config import get_logger, setup_logging
from core.scheduler import collection_scheduler, processing_scheduler
from tools.tool_factory import ToolFactory

# Setup logging
setup_logging()
logger = get_logger(__name__)


async def example_data_collection():
    """Example of manual data collection from all tools."""
    logger.info("Starting data collection example")
    
    try:
        # Load configuration
        config_manager.load_config('config.yaml')
        config_data = config_manager.get_config()
        
        # Get configured tools
        tools = config_data.get("tools", {})
        
        for tool_name, tool_config in tools.items():
            logger.info(f"Collecting data from {tool_name}")
            
            try:
                # Get credentials
                credentials = tool_config.get("credentials", {})
                
                # Create collector
                collector = ToolFactory.create_collector(tool_name, credentials)
                
                # Collect data
                result = await collector.collect_data("manual")
                
                logger.info(f"Collection completed for {tool_name}", 
                           records_collected=result.records_collected,
                           duration=result.duration,
                           status=result.status)
                
            except DataCollectionException as e:
                logger.error(f"Collection failed for {tool_name}: {e.message}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error collecting from {tool_name}: {str(e)}")
                continue
        
        logger.info("Data collection example completed")
        
    except Exception as e:
        logger.error(f"Collection example failed: {str(e)}")


async def example_data_processing():
    """Example of data processing for collected data."""
    logger.info("Starting data processing example")
    
    try:
        # Load configuration
        config_manager.load_config('config.yaml')
        config_data = config_manager.get_config()
        
        # Get configured tools
        tools = config_data.get("tools", {})
        
        for tool_name, tool_config in tools.items():
            logger.info(f"Processing data from {tool_name}")
            
            try:
                # Get credentials
                credentials = tool_config.get("credentials", {})
                
                # Create processor
                processor = ToolFactory.create_processor(tool_name, credentials)
                
                # Load sample data (in real usage, this would be loaded from storage)
                sample_data = []
                
                # Process data through different stages
                stages = ["validation", "enrichment", "analysis"]
                
                for stage in stages:
                    logger.info(f"Processing {tool_name} data - stage: {stage}")
                    
                    try:
                        result = await processor.process_data(sample_data, stage)
                        
                        logger.info(f"Processing completed for {tool_name} - {stage}", 
                                   records_processed=result.records_processed,
                                   records_valid=result.records_valid,
                                   duration=result.duration,
                                   status=result.status)
                        
                    except DataProcessingError as e:
                        logger.error(f"Processing failed for {tool_name} - {stage}: {e.message}")
                        continue
                
            except Exception as e:
                logger.error(f"Processing setup failed for {tool_name}: {str(e)}")
                continue
        
        logger.info("Data processing example completed")
        
    except Exception as e:
        logger.error(f"Processing example failed: {str(e)}")


async def example_scheduled_jobs():
    """Example of setting up and running scheduled jobs."""
    logger.info("Starting scheduled jobs example")
    
    try:
        # Load configuration
        config_manager.load_config('config.yaml')
        config_data = config_manager.get_config()
        
        # Start schedulers
        await collection_scheduler.start()
        await processing_scheduler.start()
        
        logger.info("Schedulers started successfully")
        
        # Add a manual job to the queue
        from core.models import DataRecord
        
        # Create a sample collection job
        async def sample_collection_job():
            """Sample collection job function."""
            logger.info("Executing sample collection job")
            
            # Simulate data collection
            await asyncio.sleep(2)
            
            return {
                "records_collected": 100,
                "status": "success",
                "duration": 2.0
            }
        
        # Add job to queue
        job_id = await collection_scheduler.add_manual_job(
            "collection",
            sample_collection_job,
            tool_name="sample_tool"
        )
        
        logger.info(f"Manual job queued with ID: {job_id}")
        
        # Wait for job completion
        await asyncio.sleep(5)
        
        # Check job status
        job_status = await collection_scheduler.get_job_status(job_id)
        if job_status:
            logger.info(f"Job status: {job_status.status}")
        
        # Get all jobs
        all_jobs = await collection_scheduler.get_all_jobs()
        logger.info(f"Total jobs in queue: {len(all_jobs)}")
        
    except Exception as e:
        logger.error(f"Scheduled jobs example failed: {str(e)}")
    
    finally:
        # Stop schedulers
        await collection_scheduler.stop()
        await processing_scheduler.stop()
        
        logger.info("Schedulers stopped")


async def example_error_handling():
    """Example of error handling and exception tracking."""
    logger.info("Starting error handling example")
    
    try:
        # Load configuration
        config_manager.load_config('config.yaml')
        
        # Simulate a collection error
        try:
            # This will fail due to invalid credentials
            collector = ToolFactory.create_collector("cequence", {})
            result = await collector.collect_data("manual")
            
        except DataCollectionException as e:
            logger.error(f"Expected collection error caught: {e.message}")
            logger.info(f"Error ID: {e.error_id}")
            logger.info(f"Error code: {e.error_code}")
            
        # Simulate a processing error
        try:
            # This will fail due to invalid data
            processor = ToolFactory.create_processor("cequence", {})
            result = await processor.process_data([], "invalid_stage")
            
        except DataProcessingError as e:
            logger.error(f"Expected processing error caught: {e.message}")
            logger.info(f"Error ID: {e.error_id}")
            logger.info(f"Error code: {e.error_code}")
        
        logger.info("Error handling example completed")
        
    except Exception as e:
        logger.error(f"Error handling example failed: {str(e)}")


async def example_configuration_management():
    """Example of configuration management."""
    logger.info("Starting configuration management example")
    
    try:
        # Load configuration
        config_manager.load_config('config.yaml')
        
        # Get full configuration
        config_data = config_manager.get_config()
        logger.info(f"Configuration loaded with {len(config_data.get('tools', {}))} tools")
        
        # Get tool-specific configuration
        try:
            cequence_config = config_manager.get_tool_config('cequence')
            logger.info(f"Cequence config: {cequence_config.name}")
            
        except Exception as e:
            logger.error(f"Failed to get tool config: {str(e)}")
        
        # Get schedule configuration
        try:
            schedules = config_manager.load_schedules_config()
            logger.info(f"Loaded {len(schedules)} schedules")
            
        except Exception as e:
            logger.error(f"Failed to load schedules: {str(e)}")
        
        # Validate configuration
        try:
            is_valid = config_manager.validate_configs()
            logger.info(f"Configuration validation: {'PASSED' if is_valid else 'FAILED'}")
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
        
        logger.info("Configuration management example completed")
        
    except Exception as e:
        logger.error(f"Configuration management example failed: {str(e)}")


async def example_tool_capabilities():
    """Example of checking tool capabilities."""
    logger.info("Starting tool capabilities example")
    
    try:
        # Get supported tools
        supported_tools = ToolFactory.get_supported_tools()
        
        logger.info("Supported tools and capabilities:")
        for tool_name, capabilities in supported_tools.items():
            logger.info(f"  {tool_name}:")
            logger.info(f"    - Collection: {'✓' if capabilities['collection'] else '✗'}")
            logger.info(f"    - Processing: {'✓' if capabilities['processing'] else '✗'}")
        
        logger.info("Tool capabilities example completed")
        
    except Exception as e:
        logger.error(f"Tool capabilities example failed: {str(e)}")


async def main():
    """Main function to run all examples."""
    logger.info("Starting Data Processing Framework Examples")
    
    # Run examples
    await example_tool_capabilities()
    await example_configuration_management()
    await example_error_handling()
    
    # Note: These require valid credentials to run successfully
    # await example_data_collection()
    # await example_data_processing()
    # await example_scheduled_jobs()
    
    logger.info("All examples completed")


if __name__ == "__main__":
    asyncio.run(main()) 