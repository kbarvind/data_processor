#!/usr/bin/env python3
"""
Data Processing Framework - Main Application Entry Point
Provides CLI interface and scheduler management.
"""

import asyncio
import json
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import click
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from core.config_manager import config_manager
from core.exceptions import ConfigurationException, DataCollectionException
from core.logging_config import get_logger, setup_logging
from core.scheduler import collection_scheduler, processing_scheduler
from tools.tool_factory import ToolFactory

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Global variables for graceful shutdown
shutdown_event = asyncio.Event()
running_tasks = []

# FastAPI application
app = FastAPI(
    title="Data Processing Framework",
    description="Multi-tool data collection and processing framework",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }


@app.get("/tools")
async def get_supported_tools():
    """Get supported tools and their capabilities."""
    return ToolFactory.get_supported_tools()


@app.get("/jobs")
async def get_all_jobs():
    """Get all job statuses."""
    jobs = await collection_scheduler.get_all_jobs()
    return {"jobs": [job.dict() for job in jobs]}


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get specific job status."""
    job = await collection_scheduler.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.dict()


@app.post("/jobs/collect/{tool_name}")
async def trigger_manual_collection(tool_name: str):
    """Trigger manual data collection for a tool."""
    try:
        # Load credentials from config
        config = config_manager.get_config()
        tool_credentials = config.get("tools", {}).get(tool_name, {}).get("credentials", {})
        
        if not tool_credentials:
            raise HTTPException(
                status_code=400, 
                detail=f"No credentials configured for tool: {tool_name}"
            )
        
        # Create collector
        collector = ToolFactory.create_collector(tool_name, tool_credentials)
        
        # Add manual collection job
        job_id = await collection_scheduler.add_manual_job(
            "collection",
            collector.collect_data,
            tool_name=tool_name,
            collection_type="manual"
        )
        
        return {"job_id": job_id, "status": "queued"}
        
    except Exception as e:
        logger.error(f"Failed to trigger collection for {tool_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/process/{tool_name}")
async def trigger_manual_processing(tool_name: str, processing_stage: str = "validation"):
    """Trigger manual data processing for a tool."""
    try:
        # Load credentials from config
        config = config_manager.get_config()
        tool_credentials = config.get("tools", {}).get(tool_name, {}).get("credentials", {})
        
        if not tool_credentials:
            raise HTTPException(
                status_code=400, 
                detail=f"No credentials configured for tool: {tool_name}"
            )
        
        # Create processor
        processor = ToolFactory.create_processor(tool_name, tool_credentials)
        
        # Add manual processing job
        job_id = await processing_scheduler.add_processing_job(
            processor.process_data,
            tool_name=tool_name,
            processing_stage=processing_stage,
            data=[],  # Would typically load from storage
        )
        
        return {"job_id": job_id, "status": "queued"}
        
    except Exception as e:
        logger.error(f"Failed to trigger processing for {tool_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def setup_schedulers():
    """Setup and start schedulers."""
    try:
        # Load configuration
        config = config_manager.get_config()
        
        # Setup collection schedules
        schedules = config.get("schedules", {})
        
        for schedule_name, schedule_config in schedules.items():
            if schedule_config.get("enabled", True):
                logger.info(f"Setting up schedule: {schedule_name}")
                
                # Create schedule config object
                from core.models import ScheduleConfig
                schedule_obj = ScheduleConfig(
                    name=schedule_name,
                    cron_expression=schedule_config.get("cron_expression", "0 */6 * * *"),
                    tools=schedule_config.get("tools", []),
                    job_type=schedule_config.get("job_type", "collection"),
                    enabled=schedule_config.get("enabled", True),
                    max_runtime=schedule_config.get("max_runtime", 3600)
                )
                
                # Create collection function
                async def collect_data_for_schedule(tool_name: str):
                    """Collection function for scheduled jobs."""
                    try:
                        # Load credentials
                        tool_credentials = config.get("tools", {}).get(tool_name, {}).get("credentials", {})
                        
                        if not tool_credentials:
                            logger.error(f"No credentials for tool: {tool_name}")
                            return
                        
                        # Create collector and collect data
                        collector = ToolFactory.create_collector(tool_name, tool_credentials)
                        result = await collector.collect_data("scheduled")
                        
                        logger.info(f"Scheduled collection completed for {tool_name}: {result.records_collected} records")
                        return result
                        
                    except Exception as e:
                        logger.error(f"Scheduled collection failed for {tool_name}: {str(e)}")
                        raise
                
                # Schedule the job
                collection_scheduler.schedule_job(schedule_obj, collect_data_for_schedule)
        
        # Start schedulers
        await collection_scheduler.start()
        await processing_scheduler.start()
        
        logger.info("Schedulers started successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup schedulers: {str(e)}")
        raise


async def shutdown_schedulers():
    """Shutdown schedulers gracefully."""
    try:
        logger.info("Shutting down schedulers...")
        await collection_scheduler.stop()
        await processing_scheduler.stop()
        
        # Close schedulers
        collection_scheduler.close()
        processing_scheduler.close()
        
        logger.info("Schedulers shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during scheduler shutdown: {str(e)}")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()


async def run_server():
    """Run the FastAPI server."""
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=8000,
        log_config=None,  # Use our custom logging
        access_log=False
    )
    
    server = uvicorn.Server(config)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Setup schedulers
        await setup_schedulers()
        
        # Start server
        logger.info("Starting Data Processing Framework server...")
        await server.serve()
        
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        raise
    finally:
        # Shutdown schedulers
        await shutdown_schedulers()


@click.group()
def cli():
    """Data Processing Framework CLI."""
    pass


@cli.command()
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def server(config):
    """Start the data processing server."""
    try:
        # Load configuration
        config_manager.load_config(config)
        
        # Run the server
        asyncio.run(run_server())
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed: {str(e)}")
        sys.exit(1)


@cli.command()
@click.argument('tool_name')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def collect(tool_name, config):
    """Manually trigger data collection for a tool."""
    async def run_collection():
        try:
            # Load configuration
            config_manager.load_config(config)
            config_data = config_manager.get_config()
            
            # Load credentials
            tool_credentials = config_data.get("tools", {}).get(tool_name, {}).get("credentials", {})
            
            if not tool_credentials:
                logger.error(f"No credentials configured for tool: {tool_name}")
                return
            
            # Create collector
            collector = ToolFactory.create_collector(tool_name, tool_credentials)
            
            # Collect data
            logger.info(f"Starting data collection for {tool_name}...")
            result = await collector.collect_data("manual")
            
            logger.info(f"Collection completed: {result.records_collected} records collected")
            
        except Exception as e:
            logger.error(f"Collection failed: {str(e)}")
            sys.exit(1)
    
    asyncio.run(run_collection())


@cli.command()
@click.argument('tool_name')
@click.option('--stage', '-s', default='validation', help='Processing stage')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def process(tool_name, stage, config):
    """Manually trigger data processing for a tool."""
    async def run_processing():
        try:
            # Load configuration
            config_manager.load_config(config)
            config_data = config_manager.get_config()
            
            # Load credentials
            tool_credentials = config_data.get("tools", {}).get(tool_name, {}).get("credentials", {})
            
            if not tool_credentials:
                logger.error(f"No credentials configured for tool: {tool_name}")
                return
            
            # Create processor
            processor = ToolFactory.create_processor(tool_name, tool_credentials)
            
            # Load data from storage (simplified - would load actual data)
            data = []
            
            # Process data
            logger.info(f"Starting data processing for {tool_name} (stage: {stage})...")
            result = await processor.process_data(data, stage)
            
            logger.info(f"Processing completed: {result.records_processed} records processed")
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            sys.exit(1)
    
    asyncio.run(run_processing())


@cli.command()
def tools():
    """List supported tools."""
    try:
        supported_tools = ToolFactory.get_supported_tools()
        
        print("Supported Tools:")
        print("-" * 50)
        
        for tool_name, capabilities in supported_tools.items():
            print(f"• {tool_name}")
            print(f"  - Collection: {'✓' if capabilities['collection'] else '✗'}")
            print(f"  - Processing: {'✓' if capabilities['processing'] else '✗'}")
            print()
            
    except Exception as e:
        logger.error(f"Failed to list tools: {str(e)}")
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def validate_config(config):
    """Validate configuration file."""
    try:
        config_manager.load_config(config)
        logger.info("Configuration is valid")
        
    except ConfigurationException as e:
        logger.error(f"Configuration validation failed: {e.message}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        sys.exit(1)


@cli.command()
@click.option('--max-age', '-a', default=24, help='Maximum age in hours')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def cleanup(max_age, config):
    """Clean up old job records."""
    async def run_cleanup():
        try:
            # Load configuration
            config_manager.load_config(config)
            
            # Start schedulers (needed for cleanup)
            await collection_scheduler.start()
            await processing_scheduler.start()
            
            # Clean up old jobs
            await collection_scheduler.cleanup_old_jobs(max_age)
            
            logger.info(f"Cleanup completed (max age: {max_age} hours)")
            
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")
            sys.exit(1)
        finally:
            # Stop schedulers
            await collection_scheduler.stop()
            await processing_scheduler.stop()
    
    asyncio.run(run_cleanup())


if __name__ == "__main__":
    cli() 