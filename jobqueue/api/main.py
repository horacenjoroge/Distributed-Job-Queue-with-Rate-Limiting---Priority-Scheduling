"""
FastAPI application entry point.
"""
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import uvicorn

from jobqueue.core.queue import JobQueue
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter
from jobqueue.core.queue_config import queue_config_manager, QueueRateLimitConfig
from jobqueue.core.dead_letter_queue import dead_letter_queue
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.worker_monitor import WorkerMonitor
from jobqueue.core.task_deduplication import task_deduplication
from jobqueue.core.task_cancellation import task_cancellation, CancellationReason
from jobqueue.core.metrics import metrics_collector
from jobqueue.core.worker_pool import distributed_worker_manager, WorkerPool
from jobqueue.backend.result_backend import result_backend, TaskResult
from jobqueue.utils.logger import log
from config import settings


# Pydantic models for API requests/responses
class TaskSubmitRequest(BaseModel):
    """Request model for task submission."""
    task_name: str
    args: Optional[List[Any]] = None
    kwargs: Optional[Dict[str, Any]] = None
    priority: TaskPriority = TaskPriority.MEDIUM
    max_retries: Optional[int] = None
    timeout: Optional[int] = None
    depends_on: Optional[List[str]] = None
    queue_name: str = "default"
    unique: bool = False  # Enable deduplication


class TaskResponse(BaseModel):
    """Response model for task information."""
    id: str
    name: str
    priority: str
    status: str
    queue_name: str
    created_at: str
    result: Optional[Any] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    redis_connected: bool
    postgres_connected: bool


class RateLimitConfigRequest(BaseModel):
    """Request model for rate limit configuration."""
    max_tasks_per_minute: int
    burst_allowance: int = 0
    enabled: bool = True


class RateLimitStatsResponse(BaseModel):
    """Response model for rate limit stats."""
    queue: str
    window_size_seconds: int
    rate_limit_enabled: bool
    max_tasks_per_minute: int
    burst_allowance: int
    current_count: int
    burst_used: int
    remaining_capacity: int
    burst_remaining: int
    wait_time_seconds: float
    is_unlimited: bool


class TaskResultResponse(BaseModel):
    """Response model for task result."""
    task_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration: Optional[float] = None


# Initialize FastAPI app
app = FastAPI(
    title="Job Queue API",
    description="Distributed Job Queue with Rate Limiting & Priority Scheduling",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup."""
    try:
        log.info("Starting API server...")
        
        # Connect to Redis
        redis_broker.connect()
        log.info("Connected to Redis")
        
        # Connect to PostgreSQL
        postgres_backend.connect()
        log.info("Connected to PostgreSQL")
        
        # Initialize database schema
        postgres_backend.initialize_schema()
        log.info("Database schema initialized")
        
        log.info("API server started successfully")
    except Exception as e:
        log.error(f"Failed to start API server: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    log.info("Shutting down API server...")
    redis_broker.disconnect()
    postgres_backend.disconnect()
    log.info("API server shut down successfully")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint."""
    return {
        "name": "Job Queue API",
        "version": "0.1.0",
        "status": "running"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint."""
    redis_connected = redis_broker.is_connected()
    postgres_connected = postgres_backend.is_connected()
    
    status_str = "healthy" if (redis_connected and postgres_connected) else "unhealthy"
    
    return HealthResponse(
        status=status_str,
        redis_connected=redis_connected,
        postgres_connected=postgres_connected,
    )


@app.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED, tags=["Tasks"])
async def submit_task(request: TaskSubmitRequest):
    """
    Submit a new task to the queue.
    
    Args:
        request: Task submission request
        
    Returns:
        Created task information
    """
    try:
        queue = JobQueue(name=request.queue_name)
        
        task = queue.submit_task(
            task_name=request.task_name,
            args=request.args,
            kwargs=request.kwargs,
            priority=request.priority,
            max_retries=request.max_retries,
            timeout=request.timeout,
            depends_on=request.depends_on,
        )
        
        # Set unique flag if requested
        if request.unique:
            task.unique = True
        
        return TaskResponse(
            id=task.id,
            name=task.name,
            priority=task.priority,
            status=task.status,
            queue_name=task.queue_name,
            created_at=task.created_at.isoformat(),
        )
    except Exception as e:
        log.error(f"Error submitting task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/tasks/{task_id}", response_model=TaskResponse, tags=["Tasks"])
async def get_task(task_id: str):
    """
    Get task information by ID.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task information
    """
    try:
        queue = JobQueue()
        task = queue.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        return TaskResponse(
            id=task.id,
            name=task.name,
            priority=task.priority,
            status=task.status,
            queue_name=task.queue_name,
            created_at=task.created_at.isoformat(),
            result=task.result,
            error=task.error,
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error retrieving task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post("/tasks/{task_id}/cancel", tags=["Tasks"])
async def cancel_task(
    task_id: str,
    reason: Optional[str] = None,
    force: bool = False
):
    """
    Cancel a pending or running task.
    
    Args:
        task_id: Task ID to cancel
        reason: Cancellation reason (user_requested, timeout, etc.)
        force: If True, force kill running tasks
        
    Returns:
        Cancellation result
    """
    try:
        queue = JobQueue()
        task = queue.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        # Parse reason
        if reason:
            try:
                cancellation_reason = CancellationReason(reason)
            except ValueError:
                cancellation_reason = CancellationReason.USER_REQUESTED
        else:
            cancellation_reason = CancellationReason.USER_REQUESTED
        
        success = queue.cancel_task(task_id, reason=cancellation_reason, force=force)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Could not cancel task {task_id} (status: {task.status.value})"
            )
        
        return {
            "message": f"Task {task_id} cancelled",
            "task_id": task_id,
            "status": task.status.value,
            "reason": cancellation_reason.value,
            "force": force
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error cancelling task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/tasks/{task_id}/result", response_model=TaskResultResponse, tags=["Tasks"])
async def get_task_result(task_id: str):
    """
    Get task result by task ID.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task result information
    """
    try:
        result = result_backend.get_result(task_id)
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Result for task {task_id} not found or expired"
            )
        
        return TaskResultResponse(
            task_id=result.task_id,
            status=result.status.value if isinstance(result.status, TaskStatus) else result.status,
            result=result.result,
            error=result.error,
            started_at=result.started_at.isoformat() if result.started_at else None,
            completed_at=result.completed_at.isoformat() if result.completed_at else None,
            duration=result.duration
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error retrieving task result: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.delete("/tasks/{task_id}/result", status_code=status.HTTP_204_NO_CONTENT, tags=["Tasks"])
async def delete_task_result(task_id: str):
    """
    Delete task result from Redis.
    
    Args:
        task_id: Task ID
    """
    try:
        success = result_backend.delete_result(task_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Result for task {task_id} not found"
            )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting task result: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/tasks/{task_id}/result/ttl", tags=["Tasks"])
async def get_task_result_ttl(task_id: str):
    """
    Get remaining TTL for task result.
    
    Args:
        task_id: Task ID
        
    Returns:
        TTL information
    """
    try:
        ttl = result_backend.get_result_ttl(task_id)
        
        if ttl is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Result for task {task_id} not found"
            )
        
        return {
            "task_id": task_id,
            "ttl_seconds": ttl,
            "ttl_hours": round(ttl / 3600, 2) if ttl > 0 else None,
            "expired": ttl == -1 or ttl == 0
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting result TTL: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.put("/tasks/{task_id}/result/ttl", tags=["Tasks"])
async def extend_task_result_ttl(task_id: str, ttl_seconds: int):
    """
    Extend TTL for task result.
    
    Args:
        task_id: Task ID
        ttl_seconds: New TTL in seconds
        
    Returns:
        Success message
    """
    try:
        success = result_backend.extend_result_ttl(task_id, ttl_seconds)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Result for task {task_id} not found"
            )
        
        return {
            "message": f"TTL extended for task {task_id}",
            "ttl_seconds": ttl_seconds
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error extending result TTL: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/queues/{queue_name}/stats", tags=["Queues"])
async def get_queue_stats(queue_name: str):
    """
    Get statistics for a queue.
    
    Args:
        queue_name: Queue name
        
    Returns:
        Queue statistics
    """
    try:
        queue = JobQueue(name=queue_name)
        stats = queue.get_queue_stats()
        return stats
    except Exception as e:
        log.error(f"Error getting queue stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/queues", tags=["Queues"])
async def list_queues():
    """
    List all queues.
    
    Returns:
        List of queue names with their stats
    """
    try:
        # Query distinct queue names from database
        query = "SELECT DISTINCT queue_name FROM tasks"
        results = postgres_backend.execute_query(query, fetch_all=True)
        
        queues = []
        if results:
            for row in results:
                queue_name = row["queue_name"]
                queue = JobQueue(name=queue_name)
                stats = queue.get_queue_stats()
                queues.append(stats)
        
        return {"queues": queues}
    except Exception as e:
        log.error(f"Error listing queues: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/queues/{queue_name}/rate-limit", response_model=RateLimitStatsResponse, tags=["Rate Limiting"])
async def get_rate_limit_stats(queue_name: str):
    """
    Get rate limiting statistics for a queue.
    
    Args:
        queue_name: Queue name
        
    Returns:
        Rate limiting statistics
    """
    try:
        stats = distributed_rate_limiter.get_stats(queue_name)
        return RateLimitStatsResponse(**stats)
    except Exception as e:
        log.error(f"Error getting rate limit stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.put("/queues/{queue_name}/rate-limit", tags=["Rate Limiting"])
async def set_rate_limit(queue_name: str, config: RateLimitConfigRequest):
    """
    Set rate limit configuration for a queue.
    
    Args:
        queue_name: Queue name
        config: Rate limit configuration
        
    Returns:
        Updated configuration
    """
    try:
        queue_config_manager.set_rate_limit(
            queue_name=queue_name,
            max_tasks_per_minute=config.max_tasks_per_minute,
            burst_allowance=config.burst_allowance,
            enabled=config.enabled
        )
        
        log.info(
            f"Updated rate limit for queue {queue_name}",
            extra={
                "queue": queue_name,
                "limit": config.max_tasks_per_minute,
                "burst": config.burst_allowance,
                "enabled": config.enabled
            }
        )
        
        return {
            "message": f"Rate limit updated for queue {queue_name}",
            "queue": queue_name,
            "config": config.dict()
        }
    except Exception as e:
        log.error(f"Error setting rate limit: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.delete("/queues/{queue_name}/rate-limit", status_code=status.HTTP_204_NO_CONTENT, tags=["Rate Limiting"])
async def reset_rate_limit(queue_name: str):
    """
    Reset rate limit counters for a queue.
    
    Args:
        queue_name: Queue name
    """
    try:
        distributed_rate_limiter.reset(queue_name)
        
        log.info(f"Reset rate limit counters for queue {queue_name}")
    except Exception as e:
        log.error(f"Error resetting rate limit: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/rate-limits", tags=["Rate Limiting"])
async def list_all_rate_limits():
    """
    List rate limit stats for all configured queues.
    
    Returns:
        List of rate limit stats for all queues
    """
    try:
        all_queues = queue_config_manager.list_queues()
        
        rate_limits = []
        for queue_name in all_queues:
            stats = distributed_rate_limiter.get_stats(queue_name)
            rate_limits.append(stats)
        
        return {"rate_limits": rate_limits}
    except Exception as e:
        log.error(f"Error listing rate limits: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/dlq", tags=["Dead Letter Queue"])
async def get_dlq_tasks(limit: int = 100, offset: int = 0):
    """
    Get tasks from Dead Letter Queue.
    
    Args:
        limit: Maximum number of tasks to retrieve (default: 100)
        offset: Offset for pagination (default: 0)
        
    Returns:
        List of DLQ tasks with metadata
    """
    try:
        tasks = dead_letter_queue.get_tasks(limit=limit, offset=offset)
        stats = dead_letter_queue.get_stats()
        
        return {
            "tasks": tasks,
            "total": stats["size"],
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        log.error(f"Error getting DLQ tasks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/dlq/stats", tags=["Dead Letter Queue"])
async def get_dlq_stats():
    """
    Get Dead Letter Queue statistics.
    
    Returns:
        DLQ statistics including size, breakdowns, and alerts
    """
    try:
        stats = dead_letter_queue.get_stats()
        
        # Check alert threshold (default: 100)
        threshold = 100
        exceeds_threshold, current_size = dead_letter_queue.check_alert_threshold(threshold)
        
        stats["alert_threshold"] = threshold
        stats["exceeds_threshold"] = exceeds_threshold
        
        return stats
    except Exception as e:
        log.error(f"Error getting DLQ stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/dlq/{task_id}", tags=["Dead Letter Queue"])
async def get_dlq_task(task_id: str):
    """
    Get a specific task from Dead Letter Queue by ID.
    
    Args:
        task_id: Task ID
        
    Returns:
        DLQ task entry
    """
    try:
        entry = dead_letter_queue.get_task_by_id(task_id)
        
        if not entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found in Dead Letter Queue"
            )
        
        return entry
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting DLQ task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post("/dlq/{task_id}/retry", tags=["Dead Letter Queue"])
async def retry_dlq_task(task_id: str, reset_retry_count: bool = True):
    """
    Retry a task from Dead Letter Queue.
    
    Args:
        task_id: Task ID to retry
        reset_retry_count: Reset retry count to 0 (default: True)
        
    Returns:
        Retried task information
    """
    try:
        task = dead_letter_queue.retry_task(task_id, reset_retry_count=reset_retry_count)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found in Dead Letter Queue"
            )
        
        return {
            "message": f"Task {task_id} retried successfully",
            "task_id": task.id,
            "task_name": task.name,
            "reset_retry_count": reset_retry_count
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error retrying DLQ task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.delete("/dlq", tags=["Dead Letter Queue"])
async def purge_dlq():
    """
    Purge all tasks from Dead Letter Queue.
    
    Returns:
        Number of tasks purged
    """
    try:
        count = dead_letter_queue.purge()
        
        return {
            "message": f"Purged {count} tasks from Dead Letter Queue",
            "purged_count": count
        }
    except Exception as e:
        log.error(f"Error purging DLQ: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.delete("/dlq/{task_id}", tags=["Dead Letter Queue"])
async def remove_dlq_task(task_id: str):
    """
    Remove a specific task from Dead Letter Queue.
    
    Args:
        task_id: Task ID to remove
    """
    try:
        success = dead_letter_queue.remove_task(task_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found in Dead Letter Queue"
            )
        
        return {
            "message": f"Task {task_id} removed from Dead Letter Queue"
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error removing DLQ task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/dlq/alerts", tags=["Dead Letter Queue"])
async def check_dlq_alerts(threshold: int = 100):
    """
    Check if Dead Letter Queue exceeds threshold.
    
    Args:
        threshold: Alert threshold (default: 100)
        
    Returns:
        Alert status and current size
    """
    try:
        exceeds, size = dead_letter_queue.check_alert_threshold(threshold)
        
        return {
            "exceeds_threshold": exceeds,
            "current_size": size,
            "threshold": threshold,
            "alert": exceeds
        }
    except Exception as e:
        log.error(f"Error checking DLQ alerts: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/workers", tags=["Workers"])
async def list_workers():
    """
    List all workers with their status.
    
    Returns:
        List of worker information
    """
    try:
        workers_info = worker_heartbeat.get_all_workers_info()
        
        return {
            "workers": workers_info,
            "total": len(workers_info),
            "alive": len([w for w in workers_info if w["is_alive"]]),
            "dead": len([w for w in workers_info if not w["is_alive"]])
        }
    except Exception as e:
        log.error(f"Error listing workers: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/workers/{worker_id}", tags=["Workers"])
async def get_worker(worker_id: str):
    """
    Get information about a specific worker.
    
    Args:
        worker_id: Worker ID
        
    Returns:
        Worker information
    """
    try:
        worker_info = worker_heartbeat.get_worker_info(worker_id)
        
        if not worker_info.get("last_heartbeat"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Worker {worker_id} not found"
            )
        
        return worker_info
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting worker: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/workers/stats", tags=["Workers"])
async def get_worker_stats():
    """
    Get worker statistics and health summary.
    
    Returns:
        Worker statistics
    """
    try:
        all_workers = worker_heartbeat.get_all_workers()
        workers_info = worker_heartbeat.get_all_workers_info()
        
        alive_workers = [w for w in workers_info if w["is_alive"]]
        dead_workers = [w for w in workers_info if not w["is_alive"]]
        
        # Count by status
        by_status = {}
        for worker in workers_info:
            status = worker["status"]
            by_status[status] = by_status.get(status, 0) + 1
        
        # Count by queue
        by_queue = {}
        for worker in workers_info:
            queue = worker.get("metadata", {}).get("queue_name", "unknown")
            by_queue[queue] = by_queue.get(queue, 0) + 1
        
        return {
            "total_workers": len(all_workers),
            "alive_workers": len(alive_workers),
            "dead_workers": len(dead_workers),
            "by_status": by_status,
            "by_queue": by_queue,
            "workers": workers_info
        }
    except Exception as e:
        log.error(f"Error getting worker stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/workers/monitor/stats", tags=["Workers"])
async def get_monitor_stats():
    """
    Get worker monitor statistics.
    
    Returns:
        Monitor statistics
    """
    try:
        # Create temporary monitor instance to get stats
        monitor = WorkerMonitor()
        stats = monitor.get_stats()
        
        return stats
    except Exception as e:
        log.error(f"Error getting monitor stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.delete("/workers/{worker_id}", tags=["Workers"])
async def remove_worker(worker_id: str):
    """
    Remove worker from tracking.
    
    Args:
        worker_id: Worker ID to remove
    """
    try:
        worker_heartbeat.remove_worker(worker_id)
        
        return {
            "message": f"Worker {worker_id} removed from tracking"
        }
    except Exception as e:
        log.error(f"Error removing worker: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/tasks/{task_id}/duplicate", tags=["Tasks"])
async def check_duplicate_task(task_id: str):
    """
    Check if a task has duplicates.
    
    Args:
        task_id: Task ID to check
        
    Returns:
        Duplicate information if found
    """
    try:
        queue = JobQueue()
        task = queue.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        if not task.unique:
            return {
                "message": "Task is not marked as unique",
                "unique": False
            }
        
        if task.task_signature is None:
            task.compute_and_set_signature()
        
        duplicate_info = task_deduplication.get_duplicate_info(task.task_signature)
        
        if duplicate_info:
            return {
                "has_duplicate": True,
                "duplicate_task_id": duplicate_info["task_id"],
                "duplicate_status": duplicate_info["status"],
                "signature": duplicate_info["signature"]
            }
        
        return {
            "has_duplicate": False,
            "signature": task.task_signature
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error checking duplicate: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/metrics", tags=["Metrics"])
async def get_metrics(window_seconds: int = 3600):
    """
    Get performance metrics.
    
    Args:
        window_seconds: Time window in seconds (default: 3600 = 1 hour)
        
    Returns:
        Dictionary with all metrics
    """
    try:
        metrics = metrics_collector.get_all_metrics(window_seconds)
        
        return {
            "timestamp": metrics.get("timestamp"),
            "window_seconds": window_seconds,
            "tasks": {
                "enqueued_per_second": metrics.get("tasks_enqueued_per_second", 0.0),
                "completed_per_second": metrics.get("tasks_completed_per_second", 0.0)
            },
            "duration_percentiles": {
                "p50_ms": metrics.get("duration_percentiles", {}).get(0.5, 0.0),
                "p95_ms": metrics.get("duration_percentiles", {}).get(0.95, 0.0),
                "p99_ms": metrics.get("duration_percentiles", {}).get(0.99, 0.0)
            },
            "success_rate": metrics.get("success_rate", {}),
            "queue_size_per_priority": metrics.get("queue_size_per_priority", {}),
            "worker_utilization": metrics.get("worker_utilization", {})
        }
    except Exception as e:
        log.error(f"Error getting metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/metrics/aggregate", tags=["Metrics"])
async def get_aggregated_metrics(
    aggregation: str = "hourly",
    timestamp: Optional[float] = None
):
    """
    Get aggregated metrics for a time period.
    
    Args:
        aggregation: Aggregation type ("hourly" or "daily")
        timestamp: Timestamp to aggregate (defaults to current time)
        
    Returns:
        Aggregated metrics
    """
    try:
        if aggregation not in ["hourly", "daily"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="aggregation must be 'hourly' or 'daily'"
            )
        
        aggregated = metrics_collector.aggregate_metrics(aggregation, timestamp)
        
        return aggregated
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting aggregated metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


def main():
    """Main entry point for the API server."""
    log.info(
        f"Starting API server on {settings.api_host}:{settings.api_port}",
        extra={
            "host": settings.api_host,
            "port": settings.api_port,
            "workers": settings.api_workers,
        }
    )
    
    uvicorn.run(
        "jobqueue.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
