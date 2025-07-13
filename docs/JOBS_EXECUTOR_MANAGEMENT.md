# JobsExecutor Management Guide

## Overview

The `JobsExecutor` is a core component of the data processing framework that handles the execution of jobs with timeout, retry logic, and parallel processing capabilities. It's used by both `CollectionScheduler` and `ProcessingScheduler` to execute data collection and processing tasks.

## Architecture

```
JobExecutor
├── ThreadPoolExecutor (for sync functions)
├── ProcessPoolExecutor (for CPU-intensive tasks)
├── Timeout management
├── Retry logic with exponential backoff
└── Error handling and logging
```

## Key Components

### 1. JobExecutor Class

The `JobExecutor` is responsible for:
- **Parallel execution** using thread and process pools
- **Timeout management** to prevent hanging jobs
- **Retry logic** with configurable attempts and delays
- **Error handling** with structured logging
- **Resource management** and cleanup

### 2. Job Class

Each job contains:
- **Job metadata** (ID, type, tool name)
- **Execution parameters** (function, args, kwargs)
- **Runtime configuration** (max_runtime, retry_attempts, retry_delay)
- **Status tracking** (pending, running, completed, failed)

### 3. JobQueue Class

Manages job lifecycle:
- **Thread-safe queue** operations
- **Job status tracking** (pending, running, completed, failed)
- **Job history** and cleanup

## Configuration

### 1. Basic Configuration

```python
from core.scheduler import collection_scheduler, processing_scheduler

# Configure max concurrent jobs
collection_scheduler = CollectionScheduler(max_concurrent_jobs=4)
processing_scheduler = ProcessingScheduler(max_concurrent_jobs=2)
```

### 2. Job-Level Configuration

```python
from core.scheduler import Job

job = Job(
    job_id="unique_job_id",
    job_type="collection",
    function=collect_data_function,
    tool_name="cequence",
    max_runtime=3600,        # 1 hour timeout
    retry_attempts=3,        # Retry 3 times
    retry_delay=60.0         # 60 seconds between retries
)
```

### 3. Configuration File Settings

```yaml
# config.yaml
processing:
  max_concurrent_jobs: 2
  job_timeout: 3600
  retry_attempts: 3
  retry_delay: 60.0
  batch_size: 1000

collection:
  max_concurrent_jobs: 2
  job_timeout: 3600
  retry_attempts: 3
  retry_delay: 60.0
  rate_limit_buffer: 0.1
```

## Usage Examples

### 1. Starting and Stopping Executors

```python
import asyncio
from core.scheduler import collection_scheduler, processing_scheduler

async def manage_executors():
    # Start schedulers (this starts the JobExecutors)
    await collection_scheduler.start()
    await processing_scheduler.start()
    
    try:
        # Your application logic here
        await asyncio.sleep(3600)  # Run for 1 hour
    finally:
        # Stop schedulers (this stops the JobExecutors)
        await collection_scheduler.stop()
        await processing_scheduler.stop()
        
        # Clean up resources
        collection_scheduler.close()
        processing_scheduler.close()
```

### 2. Adding Manual Jobs

```python
async def add_collection_job():
    # Add a manual collection job
    job_id = await collection_scheduler.add_manual_job(
        job_type="collection",
        function=collect_cequence_data,
        tool_name="cequence",
        collection_type="manual"
    )
    print(f"Job added with ID: {job_id}")
    return job_id

async def add_processing_job():
    # Add a manual processing job
    job_id = await processing_scheduler.add_processing_job(
        function=process_cequence_data,
        tool_name="cequence",
        processing_stage="validation",
        data=sample_data
    )
    print(f"Processing job added with ID: {job_id}")
    return job_id
```

### 3. Monitoring Job Status

```python
async def monitor_jobs():
    # Get status of specific job
    job_status = await collection_scheduler.get_job_status("job_id_123")
    if job_status:
        print(f"Job {job_status.job_id}: {job_status.status}")
        print(f"Duration: {job_status.duration}s")
        print(f"Records processed: {job_status.records_processed}")
    
    # Get all jobs
    all_jobs = await collection_scheduler.get_all_jobs()
    for job in all_jobs:
        print(f"{job.job_id}: {job.status} ({job.job_type})")
```

### 4. Custom Job Function

```python
async def custom_collection_function(tool_name: str, **kwargs):
    """Custom collection function for JobExecutor."""
    try:
        # Your collection logic here
        result = await collect_data_from_tool(tool_name)
        
        # Return result with records_collected for tracking
        return {
            "records_collected": len(result),
            "status": "success",
            "data": result
        }
    except Exception as e:
        # JobExecutor will handle retries
        raise e

# Add custom job
job_id = await collection_scheduler.add_manual_job(
    job_type="collection",
    function=custom_collection_function,
    tool_name="custom_tool",
    custom_param="value"
)
```

## Monitoring and Management

### 1. Job Status Tracking

```python
async def track_job_progress(job_id: str):
    """Track job progress with periodic status checks."""
    import time
    
    while True:
        status = await collection_scheduler.get_job_status(job_id)
        if not status:
            print(f"Job {job_id} not found")
            break
            
        print(f"Job {job_id}: {status.status}")
        
        if status.status in ["completed", "failed"]:
            print(f"Final status: {status.status}")
            if status.error_message:
                print(f"Error: {status.error_message}")
            break
            
        await asyncio.sleep(10)  # Check every 10 seconds
```

### 2. Performance Monitoring

```python
async def monitor_executor_performance():
    """Monitor JobExecutor performance metrics."""
    all_jobs = await collection_scheduler.get_all_jobs()
    
    # Calculate metrics
    total_jobs = len(all_jobs)
    completed_jobs = len([j for j in all_jobs if j.status == "completed"])
    failed_jobs = len([j for j in all_jobs if j.status == "failed"])
    running_jobs = len([j for j in all_jobs if j.status == "running"])
    
    success_rate = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0
    
    print(f"Total jobs: {total_jobs}")
    print(f"Completed: {completed_jobs}")
    print(f"Failed: {failed_jobs}")
    print(f"Running: {running_jobs}")
    print(f"Success rate: {success_rate:.2f}%")
    
    # Average duration
    completed_durations = [j.duration for j in all_jobs if j.duration]
    if completed_durations:
        avg_duration = sum(completed_durations) / len(completed_durations)
        print(f"Average duration: {avg_duration:.2f}s")
```

### 3. Error Handling and Recovery

```python
async def handle_job_failures():
    """Handle failed jobs and implement recovery strategies."""
    failed_jobs = [j for j in await collection_scheduler.get_all_jobs() 
                   if j.status == "failed"]
    
    for job in failed_jobs:
        print(f"Failed job: {job.job_id}")
        print(f"Error: {job.error_message}")
        print(f"Error code: {job.error_code}")
        
        # Implement recovery logic based on error type
        if job.error_code == "TIMEOUT_ERROR":
            # Retry with longer timeout
            await retry_with_longer_timeout(job)
        elif job.error_code == "API_ERROR":
            # Retry with exponential backoff
            await retry_with_backoff(job)
        elif job.error_code == "CONFIGURATION_ERROR":
            # Fix configuration and retry
            await fix_configuration_and_retry(job)

async def retry_with_longer_timeout(job):
    """Retry job with extended timeout."""
    # Create new job with longer timeout
    new_job = Job(
        job_id=f"{job.job_id}_retry",
        job_type=job.job_type,
        function=job.function,
        args=job.args,
        kwargs=job.kwargs,
        tool_name=job.tool_name,
        max_runtime=job.max_runtime * 2,  # Double the timeout
        retry_attempts=1  # Single retry attempt
    )
    
    await collection_scheduler.job_queue.add_job(new_job)
```

## Best Practices

### 1. Resource Management

```python
# Always close executors when done
async def cleanup_resources():
    try:
        # Stop schedulers
        await collection_scheduler.stop()
        await processing_scheduler.stop()
    finally:
        # Close executors to free resources
        collection_scheduler.close()
        processing_scheduler.close()
```

### 2. Job Design

```python
# Design jobs to be idempotent
async def idempotent_collection_job(tool_name: str, **kwargs):
    """Idempotent collection job that can be safely retried."""
    # Check if data already collected
    if await data_already_collected(tool_name, kwargs.get('collection_time')):
        return {"records_collected": 0, "status": "already_collected"}
    
    # Perform collection
    result = await collect_data(tool_name)
    await save_collection_metadata(tool_name, kwargs.get('collection_time'))
    
    return {"records_collected": len(result), "status": "success"}
```

### 3. Error Handling

```python
# Use specific error types for better handling
from core.exceptions import DataCollectionException

async def robust_collection_job(tool_name: str):
    """Robust collection job with proper error handling."""
    try:
        result = await collect_data(tool_name)
        return {"records_collected": len(result), "status": "success"}
    except ConnectionError as e:
        # Network errors - retry
        raise DataCollectionException(
            f"Connection failed for {tool_name}: {str(e)}",
            error_code="NETWORK_ERROR",
            tool_name=tool_name
        )
    except ValueError as e:
        # Data errors - don't retry
        raise DataCollectionException(
            f"Invalid data for {tool_name}: {str(e)}",
            error_code="DATA_ERROR",
            tool_name=tool_name
        )
```

### 4. Monitoring and Alerting

```python
async def setup_job_monitoring():
    """Setup comprehensive job monitoring."""
    import asyncio
    
    async def monitor_job_health():
        while True:
            try:
                # Check for stuck jobs
                running_jobs = [j for j in await collection_scheduler.get_all_jobs() 
                               if j.status == "running"]
                
                for job in running_jobs:
                    if job.started_at:
                        runtime = (datetime.utcnow() - job.started_at).total_seconds()
                        if runtime > 3600:  # 1 hour
                            logger.warning(f"Job {job.job_id} running for {runtime}s")
                
                # Check for high failure rates
                recent_jobs = [j for j in await collection_scheduler.get_all_jobs() 
                              if j.started_at and 
                              (datetime.utcnow() - j.started_at).total_seconds() < 3600]
                
                if recent_jobs:
                    failure_rate = len([j for j in recent_jobs if j.status == "failed"]) / len(recent_jobs)
                    if failure_rate > 0.5:  # 50% failure rate
                        logger.error(f"High failure rate detected: {failure_rate:.2%}")
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Monitoring error: {str(e)}")
                await asyncio.sleep(300)
    
    # Start monitoring task
    asyncio.create_task(monitor_job_health())
```

## Troubleshooting

### Common Issues

1. **Jobs timing out**: Increase `max_runtime` or optimize job performance
2. **High memory usage**: Reduce `max_concurrent_jobs` or implement memory-efficient processing
3. **Jobs stuck in running state**: Check for hanging network calls or infinite loops
4. **High failure rates**: Review error logs and implement proper error handling

### Debug Commands

```python
# Get detailed job information
async def debug_job(job_id: str):
    status = await collection_scheduler.get_job_status(job_id)
    if status:
        print(f"Job ID: {status.job_id}")
        print(f"Type: {status.job_type}")
        print(f"Status: {status.status}")
        print(f"Started: {status.started_at}")
        print(f"Completed: {status.completed_at}")
        print(f"Duration: {status.duration}")
        print(f"Records: {status.records_processed}")
        print(f"Error: {status.error_message}")
        print(f"Error Code: {status.error_code}")

# Clean up old jobs
await collection_scheduler.cleanup_old_jobs(max_age_hours=24)
```

## Advanced Configuration

### Custom JobExecutor

```python
class CustomJobExecutor(JobExecutor):
    """Custom JobExecutor with additional features."""
    
    def __init__(self, max_workers: int = 4, custom_config: dict = None):
        super().__init__(max_workers)
        self.custom_config = custom_config or {}
    
    async def execute_job(self, job: Job) -> Any:
        # Add custom pre-execution logic
        await self.pre_execution_hook(job)
        
        # Execute with custom timeout
        custom_timeout = self.custom_config.get('timeout_multiplier', 1.0) * job.max_runtime
        
        try:
            result = await asyncio.wait_for(
                self._execute_function(job),
                timeout=custom_timeout
            )
            
            # Add custom post-execution logic
            await self.post_execution_hook(job, result)
            
            return result
            
        except Exception as e:
            # Add custom error handling
            await self.error_hook(job, e)
            raise
    
    async def pre_execution_hook(self, job: Job):
        """Custom pre-execution logic."""
        logger.info(f"Pre-execution hook for job {job.job_id}")
    
    async def post_execution_hook(self, job: Job, result: Any):
        """Custom post-execution logic."""
        logger.info(f"Post-execution hook for job {job.job_id}")
    
    async def error_hook(self, job: Job, error: Exception):
        """Custom error handling logic."""
        logger.error(f"Error hook for job {job.job_id}: {str(error)}")
```

This comprehensive guide covers all aspects of managing the JobsExecutor in the data processing framework, from basic usage to advanced configuration and troubleshooting. 