"""
Scheduler module for the data processing framework.
Provides job scheduling, queue management, and parallel processing capabilities.
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import uuid4

import schedule
from celery import Celery
from croniter import croniter

from .exceptions import SchedulerException, handle_exception, handle_async_exception
from .logging_config import get_logger, log_scheduler_event
from .models import JobStatus, ScheduleConfig
from .config_manager import config_manager

logger = get_logger(__name__)


class Job:
    """Represents a scheduled job."""
    
    def __init__(
        self,
        job_id: str,
        job_type: str,
        function: Callable,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        tool_name: Optional[str] = None,
        max_runtime: int = 3600,
        retry_attempts: int = 3,
        retry_delay: float = 60.0
    ):
        """
        Initialize job.
        
        Args:
            job_id: Unique job identifier
            job_type: Type of job (collection, processing, report)
            function: Function to execute
            args: Function arguments
            kwargs: Function keyword arguments
            tool_name: Associated tool name
            max_runtime: Maximum runtime in seconds
            retry_attempts: Number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.job_id = job_id
        self.job_type = job_type
        self.function = function
        self.args = args
        self.kwargs = kwargs or {}
        self.tool_name = tool_name
        self.max_runtime = max_runtime
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Status tracking
        self.status = "pending"
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.duration: Optional[float] = None
        self.records_processed: Optional[int] = None
        self.error_message: Optional[str] = None
        self.error_code: Optional[str] = None
        self.current_attempt = 0
        
        logger.info("Job created", 
                   job_id=job_id, 
                   job_type=job_type, 
                   tool_name=tool_name)
    
    def to_status(self) -> JobStatus:
        """Convert to JobStatus model."""
        return JobStatus(
            job_id=self.job_id,
            job_type=self.job_type,
            tool_name=self.tool_name,
            status=self.status,
            started_at=self.started_at,
            completed_at=self.completed_at,
            duration=self.duration,
            records_processed=self.records_processed,
            error_message=self.error_message,
            error_code=self.error_code
        )
    
    def __repr__(self) -> str:
        return f"Job(id={self.job_id}, type={self.job_type}, status={self.status})"


class JobQueue:
    """Thread-safe job queue with priority support."""
    
    def __init__(self):
        """Initialize job queue."""
        self.jobs: Dict[str, Job] = {}
        self.pending_jobs: List[str] = []
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.completed_jobs: List[str] = []
        self.failed_jobs: List[str] = []
        self._lock = asyncio.Lock()
        
        logger.info("JobQueue initialized")
    
    async def add_job(self, job: Job) -> None:
        """Add job to queue."""
        async with self._lock:
            self.jobs[job.job_id] = job
            self.pending_jobs.append(job.job_id)
            
            log_scheduler_event(
                job_id=job.job_id,
                job_type=job.job_type,
                event_type="queued",
                status="success",
                details={"tool_name": job.tool_name}
            )
    
    async def get_next_job(self) -> Optional[Job]:
        """Get next pending job."""
        async with self._lock:
            if not self.pending_jobs:
                return None
            
            job_id = self.pending_jobs.pop(0)
            return self.jobs.get(job_id)
    
    async def mark_running(self, job: Job, task: asyncio.Task) -> None:
        """Mark job as running."""
        async with self._lock:
            job.status = "running"
            job.started_at = datetime.utcnow()
            self.running_jobs[job.job_id] = task
            
            log_scheduler_event(
                job_id=job.job_id,
                job_type=job.job_type,
                event_type="started",
                status="success",
                details={"tool_name": job.tool_name}
            )
    
    async def mark_completed(self, job: Job, records_processed: Optional[int] = None) -> None:
        """Mark job as completed."""
        async with self._lock:
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.records_processed = records_processed
            
            if job.started_at:
                job.duration = (job.completed_at - job.started_at).total_seconds()
            
            if job.job_id in self.running_jobs:
                del self.running_jobs[job.job_id]
            
            self.completed_jobs.append(job.job_id)
            
            log_scheduler_event(
                job_id=job.job_id,
                job_type=job.job_type,
                event_type="completed",
                status="success",
                details={
                    "tool_name": job.tool_name,
                    "duration": job.duration,
                    "records_processed": records_processed
                }
            )
    
    async def mark_failed(self, job: Job, error_message: str, error_code: str) -> None:
        """Mark job as failed."""
        async with self._lock:
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.error_message = error_message
            job.error_code = error_code
            
            if job.started_at:
                job.duration = (job.completed_at - job.started_at).total_seconds()
            
            if job.job_id in self.running_jobs:
                del self.running_jobs[job.job_id]
            
            self.failed_jobs.append(job.job_id)
            
            log_scheduler_event(
                job_id=job.job_id,
                job_type=job.job_type,
                event_type="failed",
                status="failed",
                details={
                    "tool_name": job.tool_name,
                    "error_message": error_message,
                    "error_code": error_code,
                    "duration": job.duration
                },
                error=error_message
            )
    
    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get job status."""
        async with self._lock:
            job = self.jobs.get(job_id)
            return job.to_status() if job else None
    
    async def get_all_jobs(self) -> List[JobStatus]:
        """Get all job statuses."""
        async with self._lock:
            return [job.to_status() for job in self.jobs.values()]
    
    async def cleanup_old_jobs(self, max_age_hours: int = 24) -> None:
        """Clean up old completed/failed jobs."""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        async with self._lock:
            jobs_to_remove = []
            
            for job_id, job in self.jobs.items():
                if (job.status in ["completed", "failed"] and 
                    job.completed_at and 
                    job.completed_at < cutoff_time):
                    jobs_to_remove.append(job_id)
            
            for job_id in jobs_to_remove:
                del self.jobs[job_id]
                if job_id in self.completed_jobs:
                    self.completed_jobs.remove(job_id)
                if job_id in self.failed_jobs:
                    self.failed_jobs.remove(job_id)
            
            logger.info("Old jobs cleaned up", removed_count=len(jobs_to_remove))


class JobExecutor:
    """Executes jobs with timeout and retry logic."""
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize job executor.
        
        Args:
            max_workers: Maximum number of concurrent workers
        """
        self.max_workers = max_workers
        self.thread_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.process_executor = ProcessPoolExecutor(max_workers=max_workers)
        
        logger.info("JobExecutor initialized", max_workers=max_workers)
    
    @handle_async_exception
    async def execute_job(self, job: Job) -> Any:
        """
        Execute a job with timeout and retry logic.
        
        Args:
            job: Job to execute
            
        Returns:
            Job execution result
            
        Raises:
            SchedulerException: If job execution fails
        """
        for attempt in range(job.retry_attempts + 1):
            job.current_attempt = attempt + 1
            
            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    self._execute_function(job),
                    timeout=job.max_runtime
                )
                
                logger.info("Job executed successfully", 
                           job_id=job.job_id,
                           attempt=job.current_attempt)
                
                return result
                
            except asyncio.TimeoutError:
                error_msg = f"Job timed out after {job.max_runtime} seconds"
                logger.error("Job timeout", 
                           job_id=job.job_id, 
                           attempt=job.current_attempt,
                           max_runtime=job.max_runtime)
                
                if attempt == job.retry_attempts:
                    raise SchedulerException(
                        error_msg,
                        job_id=job.job_id,
                        job_type=job.job_type,
                        details={"max_runtime": job.max_runtime}
                    )
                    
            except Exception as e:
                error_msg = f"Job execution failed: {str(e)}"
                logger.error("Job execution error", 
                           job_id=job.job_id,
                           attempt=job.current_attempt,
                           error=error_msg)
                
                if attempt == job.retry_attempts:
                    raise SchedulerException(
                        error_msg,
                        job_id=job.job_id,
                        job_type=job.job_type,
                        original_exception=e
                    )
                
                # Wait before retry
                if attempt < job.retry_attempts:
                    await asyncio.sleep(job.retry_delay * (attempt + 1))
        
        raise SchedulerException(
            f"Job failed after {job.retry_attempts + 1} attempts",
            job_id=job.job_id,
            job_type=job.job_type
        )
    
    async def _execute_function(self, job: Job) -> Any:
        """Execute job function."""
        if asyncio.iscoroutinefunction(job.function):
            # Async function
            return await job.function(*job.args, **job.kwargs)
        else:
            # Sync function - run in thread executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.thread_executor,
                lambda: job.function(*job.args, **job.kwargs)
            )
    
    def close(self):
        """Close executors."""
        self.thread_executor.shutdown(wait=True)
        self.process_executor.shutdown(wait=True)
        logger.info("JobExecutor closed")


class CollectionScheduler:
    """Collection scheduler for data collection jobs."""
    
    def __init__(self, max_concurrent_jobs: int = 2):
        """
        Initialize collection scheduler.
        
        Args:
            max_concurrent_jobs: Maximum concurrent collection jobs
        """
        self.max_concurrent_jobs = max_concurrent_jobs
        self.job_queue = JobQueue()
        self.job_executor = JobExecutor(max_workers=max_concurrent_jobs)
        self.scheduled_jobs: Dict[str, schedule.Job] = {}
        self.running = False
        self._worker_tasks: List[asyncio.Task] = []
        
        logger.info("CollectionScheduler initialized", 
                   max_concurrent_jobs=max_concurrent_jobs)
    
    @handle_exception
    def schedule_job(
        self,
        schedule_config: ScheduleConfig,
        collection_function: Callable,
        *args,
        **kwargs
    ) -> None:
        """
        Schedule a collection job.
        
        Args:
            schedule_config: Schedule configuration
            collection_function: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
        """
        if not schedule_config.enabled:
            logger.info("Schedule disabled, skipping", schedule_name=schedule_config.name)
            return
        
        # Parse cron expression and schedule
        try:
            cron = croniter(schedule_config.cron_expression)
            next_run = cron.get_next(datetime)
            
            def job_wrapper():
                """Wrapper to add job to queue."""
                asyncio.create_task(self._schedule_collection_job(
                    schedule_config, collection_function, *args, **kwargs
                ))
            
            # Use schedule library for cron-like scheduling
            scheduled_job = schedule.every().day.at("00:00").do(job_wrapper)
            self.scheduled_jobs[schedule_config.name] = scheduled_job
            
            logger.info("Job scheduled successfully", 
                       schedule_name=schedule_config.name,
                       cron_expression=schedule_config.cron_expression,
                       next_run=next_run)
            
        except Exception as e:
            raise SchedulerException(
                f"Failed to schedule job '{schedule_config.name}': {str(e)}",
                job_type="collection",
                details={"schedule_name": schedule_config.name},
                original_exception=e
            )
    
    async def _schedule_collection_job(
        self,
        schedule_config: ScheduleConfig,
        collection_function: Callable,
        *args,
        **kwargs
    ) -> None:
        """Schedule individual collection jobs for each tool."""
        for tool_name in schedule_config.tools:
            job_id = f"{schedule_config.name}_{tool_name}_{int(time.time())}"
            
            job = Job(
                job_id=job_id,
                job_type=schedule_config.job_type,
                function=collection_function,
                args=(tool_name,) + args,
                kwargs=kwargs,
                tool_name=tool_name,
                max_runtime=schedule_config.max_runtime
            )
            
            await self.job_queue.add_job(job)
    
    async def start(self) -> None:
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        self.running = True
        
        # Start worker tasks
        for i in range(self.max_concurrent_jobs):
            task = asyncio.create_task(self._worker())
            self._worker_tasks.append(task)
        
        # Start schedule checker
        schedule_task = asyncio.create_task(self._schedule_checker())
        self._worker_tasks.append(schedule_task)
        
        logger.info("CollectionScheduler started")
    
    async def stop(self) -> None:
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all worker tasks
        for task in self._worker_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        
        logger.info("CollectionScheduler stopped")
    
    async def _worker(self) -> None:
        """Worker to process jobs from queue."""
        while self.running:
            try:
                job = await self.job_queue.get_next_job()
                if not job:
                    await asyncio.sleep(1)
                    continue
                
                # Execute job
                task = asyncio.create_task(self._execute_job(job))
                await self.job_queue.mark_running(job, task)
                
                try:
                    result = await self.job_executor.execute_job(job)
                    
                    # Extract records processed if available
                    records_processed = None
                    if hasattr(result, 'records_collected'):
                        records_processed = result.records_collected
                    elif isinstance(result, dict) and 'records_collected' in result:
                        records_processed = result['records_collected']
                    
                    await self.job_queue.mark_completed(job, records_processed)
                    
                except SchedulerException as e:
                    await self.job_queue.mark_failed(job, e.message, e.error_code)
                except Exception as e:
                    await self.job_queue.mark_failed(
                        job, 
                        str(e), 
                        "UNEXPECTED_ERROR"
                    )
                
            except Exception as e:
                logger.error("Worker error", error=str(e))
                await asyncio.sleep(5)
    
    async def _execute_job(self, job: Job) -> Any:
        """Execute job wrapper."""
        return await self.job_executor.execute_job(job)
    
    async def _schedule_checker(self) -> None:
        """Check and run scheduled jobs."""
        while self.running:
            try:
                schedule.run_pending()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error("Schedule checker error", error=str(e))
                await asyncio.sleep(60)
    
    async def add_manual_job(
        self,
        job_type: str,
        function: Callable,
        tool_name: Optional[str] = None,
        *args,
        **kwargs
    ) -> str:
        """
        Add a manual job to the queue.
        
        Args:
            job_type: Type of job
            function: Function to execute
            tool_name: Associated tool name
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Job ID
        """
        job_id = f"manual_{job_type}_{int(time.time())}_{str(uuid4())[:8]}"
        
        job = Job(
            job_id=job_id,
            job_type=job_type,
            function=function,
            args=args,
            kwargs=kwargs,
            tool_name=tool_name
        )
        
        await self.job_queue.add_job(job)
        
        logger.info("Manual job added", job_id=job_id, job_type=job_type)
        return job_id
    
    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get job status."""
        return await self.job_queue.get_job_status(job_id)
    
    async def get_all_jobs(self) -> List[JobStatus]:
        """Get all job statuses."""
        return await self.job_queue.get_all_jobs()
    
    async def cleanup_old_jobs(self, max_age_hours: int = 24) -> None:
        """Clean up old jobs."""
        await self.job_queue.cleanup_old_jobs(max_age_hours)
    
    def close(self):
        """Close the scheduler."""
        self.job_executor.close()
        logger.info("CollectionScheduler closed")


class ProcessingScheduler:
    """Processing scheduler for data processing jobs."""
    
    def __init__(self, max_concurrent_jobs: int = 2):
        """
        Initialize processing scheduler.
        
        Args:
            max_concurrent_jobs: Maximum concurrent processing jobs
        """
        self.max_concurrent_jobs = max_concurrent_jobs
        self.job_queue = JobQueue()
        self.job_executor = JobExecutor(max_workers=max_concurrent_jobs)
        self.running = False
        self._worker_tasks: List[asyncio.Task] = []
        
        logger.info("ProcessingScheduler initialized", 
                   max_concurrent_jobs=max_concurrent_jobs)
    
    async def start(self) -> None:
        """Start the processing scheduler."""
        if self.running:
            logger.warning("Processing scheduler already running")
            return
        
        self.running = True
        
        # Start worker tasks
        for i in range(self.max_concurrent_jobs):
            task = asyncio.create_task(self._worker())
            self._worker_tasks.append(task)
        
        logger.info("ProcessingScheduler started")
    
    async def stop(self) -> None:
        """Stop the processing scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all worker tasks
        for task in self._worker_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        
        logger.info("ProcessingScheduler stopped")
    
    async def _worker(self) -> None:
        """Worker to process jobs from queue."""
        while self.running:
            try:
                job = await self.job_queue.get_next_job()
                if not job:
                    await asyncio.sleep(1)
                    continue
                
                # Execute job
                task = asyncio.create_task(self._execute_job(job))
                await self.job_queue.mark_running(job, task)
                
                try:
                    result = await self.job_executor.execute_job(job)
                    
                    # Extract records processed if available
                    records_processed = None
                    if hasattr(result, 'records_processed'):
                        records_processed = result.records_processed
                    elif isinstance(result, dict) and 'records_processed' in result:
                        records_processed = result['records_processed']
                    
                    await self.job_queue.mark_completed(job, records_processed)
                    
                except SchedulerException as e:
                    await self.job_queue.mark_failed(job, e.message, e.error_code)
                except Exception as e:
                    await self.job_queue.mark_failed(
                        job, 
                        str(e), 
                        "UNEXPECTED_ERROR"
                    )
                
            except Exception as e:
                logger.error("Processing worker error", error=str(e))
                await asyncio.sleep(5)
    
    async def _execute_job(self, job: Job) -> Any:
        """Execute job wrapper."""
        return await self.job_executor.execute_job(job)
    
    async def add_processing_job(
        self,
        function: Callable,
        tool_name: str,
        processing_stage: str,
        *args,
        **kwargs
    ) -> str:
        """
        Add a processing job to the queue.
        
        Args:
            function: Processing function to execute
            tool_name: Associated tool name
            processing_stage: Stage of processing
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Job ID
        """
        job_id = f"processing_{tool_name}_{processing_stage}_{int(time.time())}"
        
        job = Job(
            job_id=job_id,
            job_type="processing",
            function=function,
            args=args,
            kwargs=kwargs,
            tool_name=tool_name
        )
        
        await self.job_queue.add_job(job)
        
        logger.info("Processing job added", 
                   job_id=job_id, 
                   tool_name=tool_name,
                   processing_stage=processing_stage)
        return job_id
    
    def close(self):
        """Close the processing scheduler."""
        self.job_executor.close()
        logger.info("ProcessingScheduler closed")


# Global scheduler instances
collection_scheduler = CollectionScheduler()
processing_scheduler = ProcessingScheduler() 