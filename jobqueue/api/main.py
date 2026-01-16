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


@app.delete("/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Tasks"])
async def cancel_task(task_id: str):
    """
    Cancel a task.
    
    Args:
        task_id: Task ID to cancel
    """
    try:
        queue = JobQueue()
        success = queue.cancel_task(task_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Could not cancel task {task_id}"
            )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error cancelling task: {e}")
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
