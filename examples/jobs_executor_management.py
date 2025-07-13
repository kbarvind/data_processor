#!/usr/bin/env python3
"""
JobsExecutor Management Examples
Demonstrates various ways to manage and monitor job execution in the data processing framework.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

from core.config_manager import config_manager
from core.scheduler import collection_scheduler, processing_scheduler, Job
from core.logging_config import get_logger, setup_logging
from tools.tool_factory import ToolFactory

# Setup logging
setup_logging()
logger = get_logger(__name__)


class JobsExecutorManager:
    """Comprehensive JobsExecutor management class."""
    
    def __init__(self):
        """Initialize the manager."""
        self.running = False
        self.monitoring_task = None
        
    async def start_executors(self, max_collection_jobs: int = 2, max_processing_jobs: int = 2):
        """Start the job executors with custom configuration."""
        logger.info("Starting JobsExecutor managers")
        
        # Configure schedulers with custom settings
        collection_scheduler.max_concurrent_jobs = max_collection_jobs
        processing_scheduler.max_concurrent_jobs = max_processing_jobs
        
        # Start schedulers (this starts the JobExecutors)
        await collection_scheduler.start()
        await processing_scheduler.start()
        
        self.running = True
        logger.info(f"JobsExecutor started with {max_collection_jobs} collection workers and {max_processing_jobs} processing workers")
    
    async def stop_executors(self):
        """Stop the job executors gracefully."""
        logger.info("Stopping JobsExecutor managers")
        
        self.running = False
        
        # Stop schedulers
        await collection_scheduler.stop()
        await processing_scheduler.stop()
        
        # Clean up resources
        collection_scheduler.close()
        processing_scheduler.close()
        
        logger.info("JobsExecutor managers stopped")
    
    async def add_collection_job(self, tool_name: str, collection_type: str = "manual") -> str:
        """Add a collection job to the queue."""
        try:
            # Load configuration
            config = config_manager.get_config()
            tool_credentials = config.get("tools", {}).get(tool_name, {}).get("credentials", {})
            
            if not tool_credentials:
                raise ValueError(f"No credentials configured for tool: {tool_name}")
            
            # Create collector
            collector = ToolFactory.create_collector(tool_name, tool_credentials)
            
            # Add job to queue
            job_id = await collection_scheduler.add_manual_job(
                "collection",
                collector.collect_data,
                tool_name=tool_name,
                collection_type=collection_type
            )
            
            logger.info(f"Collection job added: {job_id} for {tool_name}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to add collection job for {tool_name}: {str(e)}")
            raise
    
    async def add_processing_job(self, tool_name: str, processing_stage: str, data: List[Dict] = None) -> str:
        """Add a processing job to the queue."""
        try:
            # Load configuration
            config = config_manager.get_config()
            tool_credentials = config.get("tools", {}).get(tool_name, {}).get("credentials", {})
            
            if not tool_credentials:
                raise ValueError(f"No credentials configured for tool: {tool_name}")
            
            # Create processor
            processor = ToolFactory.create_processor(tool_name, tool_credentials)
            
            # Add job to queue
            job_data = data if data is not None else []
            job_id = await processing_scheduler.add_processing_job(
                processor.process_data,
                tool_name=tool_name,
                processing_stage=processing_stage,
                data=job_data
            )
            
            logger.info(f"Processing job added: {job_id} for {tool_name} - {processing_stage}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to add processing job for {tool_name}: {str(e)}")
            raise
    
    async def monitor_job_status(self, job_id: str, check_interval: int = 10) -> Dict[str, Any]:
        """Monitor a specific job until completion."""
        logger.info(f"Starting monitoring for job: {job_id}")
        
        while self.running:
            try:
                # Check collection scheduler first
                status = await collection_scheduler.get_job_status(job_id)
                if not status:
                    # Note: ProcessingScheduler doesn't have get_job_status method
                    # Jobs are tracked through the job queue
                    logger.warning(f"Job {job_id} not found in collection scheduler")
                    return {"error": "Job not found"}
                
                logger.info(f"Job {job_id}: {status.status}")
                
                # Return final status if job is complete
                if status.status in ["completed", "failed"]:
                    return {
                        "job_id": status.job_id,
                        "status": status.status,
                        "duration": status.duration,
                        "records_processed": status.records_processed,
                        "error_message": status.error_message,
                        "error_code": status.error_code
                    }
                
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error monitoring job {job_id}: {str(e)}")
                await asyncio.sleep(check_interval)
        
        return {"error": "Monitoring stopped"}
    
    async def get_executor_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics from both executors."""
        try:
            # Get all jobs from collection scheduler
            collection_jobs = await collection_scheduler.get_all_jobs()
            # Note: ProcessingScheduler doesn't have get_all_jobs method
            processing_jobs = []
            
            all_jobs = collection_jobs + processing_jobs
            
            # Calculate metrics
            metrics = {
                "total_jobs": len(all_jobs),
                "collection_jobs": len(collection_jobs),
                "processing_jobs": len(processing_jobs),
                "status_breakdown": {},
                "performance_metrics": {},
                "error_analysis": {}
            }
            
            # Status breakdown
            statuses = ["pending", "running", "completed", "failed"]
            for status in statuses:
                count = len([j for j in all_jobs if j.status == status])
                metrics["status_breakdown"][status] = count
            
            # Performance metrics
            completed_jobs = [j for j in all_jobs if j.status == "completed" and j.duration is not None]
            if completed_jobs:
                durations = [j.duration for j in completed_jobs if j.duration is not None]
                if durations:
                    metrics["performance_metrics"] = {
                        "avg_duration": sum(durations) / len(durations),
                        "min_duration": min(durations),
                        "max_duration": max(durations),
                        "total_duration": sum(durations)
                    }
            
            # Error analysis
            failed_jobs = [j for j in all_jobs if j.status == "failed"]
            if failed_jobs:
                error_codes = {}
                for job in failed_jobs:
                    error_code = job.error_code or "UNKNOWN"
                    error_codes[error_code] = error_codes.get(error_code, 0) + 1
                
                metrics["error_analysis"] = {
                    "total_failures": len(failed_jobs),
                    "error_codes": error_codes,
                    "failure_rate": len(failed_jobs) / len(all_jobs) if all_jobs else 0
                }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting executor metrics: {str(e)}")
            return {"error": str(e)}
    
    async def cleanup_old_jobs(self, max_age_hours: int = 24):
        """Clean up old completed and failed jobs."""
        logger.info(f"Cleaning up jobs older than {max_age_hours} hours")
        
        try:
            await collection_scheduler.cleanup_old_jobs(max_age_hours)
            # Note: ProcessingScheduler doesn't have cleanup_old_jobs method
            
            logger.info("Job cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during job cleanup: {str(e)}")
    
    async def start_monitoring(self, interval_seconds: int = 300):
        """Start continuous monitoring of job executors."""
        logger.info(f"Starting continuous monitoring (interval: {interval_seconds}s)")
        
        async def monitor_loop():
            while self.running:
                try:
                    metrics = await self.get_executor_metrics()
                    
                    # Log metrics
                    logger.info("Executor metrics", **metrics)
                    
                    # Check for issues
                    if metrics.get("error_analysis", {}).get("failure_rate", 0) > 0.5:
                        logger.warning("High failure rate detected!")
                    
                    # Check for stuck jobs
                    running_jobs = metrics["status_breakdown"].get("running", 0)
                    if running_jobs > 0:
                        logger.info(f"Currently running jobs: {running_jobs}")
                    
                    await asyncio.sleep(interval_seconds)
                    
                except Exception as e:
                    logger.error(f"Monitoring error: {str(e)}")
                    await asyncio.sleep(interval_seconds)
        
        self.monitoring_task = asyncio.create_task(monitor_loop())
    
    async def stop_monitoring(self):
        """Stop continuous monitoring."""
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            self.monitoring_task = None
            logger.info("Monitoring stopped")


async def example_basic_management():
    """Basic JobsExecutor management example."""
    logger.info("=== Basic JobsExecutor Management Example ===")
    
    manager = JobsExecutorManager()
    
    try:
        # Start executors
        await manager.start_executors(max_collection_jobs=2, max_processing_jobs=1)
        
        # Add some collection jobs
        job_ids = []
        for tool_name in ["cequence", "42crunch"]:
            try:
                job_id = await manager.add_collection_job(tool_name, "manual")
                job_ids.append(job_id)
            except Exception as e:
                logger.warning(f"Could not add job for {tool_name}: {str(e)}")
        
        # Monitor jobs
        for job_id in job_ids:
            result = await manager.monitor_job_status(job_id, check_interval=5)
            logger.info(f"Job {job_id} result: {result}")
        
        # Get metrics
        metrics = await manager.get_executor_metrics()
        logger.info("Final metrics:", **metrics)
        
    finally:
        await manager.stop_executors()


async def example_advanced_monitoring():
    """Advanced monitoring and management example."""
    logger.info("=== Advanced Monitoring Example ===")
    
    manager = JobsExecutorManager()
    
    try:
        # Start executors with monitoring
        await manager.start_executors(max_collection_jobs=3, max_processing_jobs=2)
        await manager.start_monitoring(interval_seconds=60)  # Monitor every minute
        
        # Add multiple jobs
        collection_jobs = []
        for i in range(3):
            try:
                job_id = await manager.add_collection_job("cequence", f"batch_{i}")
                collection_jobs.append(job_id)
            except Exception as e:
                logger.warning(f"Could not add collection job {i}: {str(e)}")
        
        # Wait for jobs to complete
        await asyncio.sleep(30)
        
        # Get detailed metrics
        metrics = await manager.get_executor_metrics()
        logger.info("Detailed metrics:", **metrics)
        
        # Clean up old jobs
        await manager.cleanup_old_jobs(max_age_hours=1)
        
    finally:
        await manager.stop_monitoring()
        await manager.stop_executors()


async def example_error_handling():
    """Error handling and recovery example."""
    logger.info("=== Error Handling Example ===")
    
    manager = JobsExecutorManager()
    
    try:
        await manager.start_executors()
        
        # Add a job that will likely fail (invalid tool)
        try:
            job_id = await manager.add_collection_job("invalid_tool", "manual")
            result = await manager.monitor_job_status(job_id)
            logger.info(f"Failed job result: {result}")
        except Exception as e:
            logger.info(f"Expected error: {str(e)}")
        
        # Get error analysis
        metrics = await manager.get_executor_metrics()
        error_analysis = metrics.get("error_analysis", {})
        logger.info("Error analysis:", **error_analysis)
        
    finally:
        await manager.stop_executors()


async def example_custom_job_management():
    """Custom job management with specific configurations."""
    logger.info("=== Custom Job Management Example ===")
    
    manager = JobsExecutorManager()
    
    try:
        await manager.start_executors()
        
        # Create a custom job with specific configuration
        async def custom_collection_function(tool_name: str, **kwargs):
            """Custom collection function with specific logic."""
            logger.info(f"Custom collection for {tool_name}")
            
            # Simulate work
            await asyncio.sleep(2)
            
            # Simulate success/failure based on tool
            if tool_name == "cequence":
                return {
                    "records_collected": 100,
                    "status": "success",
                    "custom_metric": "custom_value"
                }
            else:
                raise ValueError(f"Unsupported tool: {tool_name}")
        
        # Add custom job
        job_id = await collection_scheduler.add_manual_job(
            "collection",
            custom_collection_function,
            tool_name="cequence",
            custom_param="test_value"
        )
        
        # Monitor custom job
        result = await manager.monitor_job_status(job_id)
        logger.info(f"Custom job result: {result}")
        
    finally:
        await manager.stop_executors()


async def example_performance_optimization():
    """Performance optimization example."""
    logger.info("=== Performance Optimization Example ===")
    
    manager = JobsExecutorManager()
    
    try:
        # Start with conservative settings
        await manager.start_executors(max_collection_jobs=1, max_processing_jobs=1)
        
        # Add jobs and measure performance
        start_time = time.time()
        
        job_ids = []
        for i in range(3):
            try:
                job_id = await manager.add_collection_job("cequence", f"perf_test_{i}")
                job_ids.append(job_id)
            except Exception as e:
                logger.warning(f"Could not add job {i}: {str(e)}")
        
        # Monitor all jobs
        results = []
        for job_id in job_ids:
            result = await manager.monitor_job_status(job_id)
            results.append(result)
        
        total_time = time.time() - start_time
        
        # Analyze performance
        successful_jobs = [r for r in results if r.get("status") == "completed"]
        failed_jobs = [r for r in results if r.get("status") == "failed"]
        
        logger.info(f"Performance test results:")
        logger.info(f"  Total time: {total_time:.2f}s")
        logger.info(f"  Successful jobs: {len(successful_jobs)}")
        logger.info(f"  Failed jobs: {len(failed_jobs)}")
        logger.info(f"  Average job time: {total_time / len(job_ids):.2f}s")
        
        # Get detailed metrics
        metrics = await manager.get_executor_metrics()
        logger.info("Performance metrics:", **metrics)
        
    finally:
        await manager.stop_executors()


async def main():
    """Run all examples."""
    logger.info("Starting JobsExecutor Management Examples")
    
    # Load configuration
    try:
        config_manager.load_config('config.yaml')
    except Exception as e:
        logger.warning(f"Could not load config: {str(e)}")
    
    # Run examples
    examples = [
        example_basic_management,
        example_advanced_monitoring,
        example_error_handling,
        example_custom_job_management,
        example_performance_optimization
    ]
    
    for example in examples:
        try:
            await example()
            await asyncio.sleep(2)  # Brief pause between examples
        except Exception as e:
            logger.error(f"Example {example.__name__} failed: {str(e)}")
    
    logger.info("JobsExecutor Management Examples completed")


if __name__ == "__main__":
    asyncio.run(main()) 