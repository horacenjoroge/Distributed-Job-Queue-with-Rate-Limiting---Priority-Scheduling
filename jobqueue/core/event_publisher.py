"""
Redis Pub/Sub event publisher for real-time updates.
"""
import json
from typing import Dict, Any, Optional
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class EventPublisher:
    """Publishes events to Redis Pub/Sub for real-time updates."""
    
    CHANNEL_PREFIX = "jobqueue:events:"
    
    # Event types
    TASK_ENQUEUED = "task_enqueued"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"
    TASK_RETRY = "task_retry"
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"
    WORKER_HEARTBEAT = "worker_heartbeat"
    QUEUE_UPDATED = "queue_updated"
    DLQ_UPDATED = "dlq_updated"
    
    def __init__(self):
        self.redis = None
    
    def connect(self):
        """Connect to Redis."""
        if not redis_broker.is_connected():
            redis_broker.connect()
        self.redis = redis_broker.get_connection()
    
    def publish(self, event_type: str, data: Dict[str, Any]):
        """
        Publish an event to Redis Pub/Sub.
        
        Args:
            event_type: Type of event (e.g., 'task_enqueued')
            data: Event data dictionary
        """
        if not self.redis:
            self.connect()
        
        try:
            event = {
                "type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "data": data
            }
            
            channel = f"{self.CHANNEL_PREFIX}{event_type}"
            message = json.dumps(event)
            
            self.redis.publish(channel, message)
            self.redis.publish(f"{self.CHANNEL_PREFIX}all", message)  # Also publish to 'all' channel
            
            log.debug(f"Published event: {event_type}", extra={"event_type": event_type, "data": data})
        except Exception as e:
            log.error(f"Error publishing event: {e}", extra={"event_type": event_type, "error": str(e)})
    
    def publish_task_enqueued(self, task_id: str, task_name: str, queue_name: str, priority: str):
        """Publish task_enqueued event."""
        self.publish(self.TASK_ENQUEUED, {
            "task_id": task_id,
            "task_name": task_name,
            "queue_name": queue_name,
            "priority": priority
        })
    
    def publish_task_started(self, task_id: str, worker_id: str):
        """Publish task_started event."""
        self.publish(self.TASK_STARTED, {
            "task_id": task_id,
            "worker_id": worker_id
        })
    
    def publish_task_completed(self, task_id: str, worker_id: str, duration: Optional[float] = None, result: Any = None):
        """Publish task_completed event."""
        self.publish(self.TASK_COMPLETED, {
            "task_id": task_id,
            "worker_id": worker_id,
            "duration": duration,
            "result": str(result) if result is not None else None
        })
    
    def publish_task_failed(self, task_id: str, worker_id: str, error: str, retry_count: int = 0):
        """Publish task_failed event."""
        self.publish(self.TASK_FAILED, {
            "task_id": task_id,
            "worker_id": worker_id,
            "error": error,
            "retry_count": retry_count
        })
    
    def publish_task_cancelled(self, task_id: str, reason: str):
        """Publish task_cancelled event."""
        self.publish(self.TASK_CANCELLED, {
            "task_id": task_id,
            "reason": reason
        })
    
    def publish_task_retry(self, task_id: str, retry_count: int):
        """Publish task_retry event."""
        self.publish(self.TASK_RETRY, {
            "task_id": task_id,
            "retry_count": retry_count
        })
    
    def publish_worker_started(self, worker_id: str, hostname: str, queue_name: str):
        """Publish worker_started event."""
        self.publish(self.WORKER_STARTED, {
            "worker_id": worker_id,
            "hostname": hostname,
            "queue_name": queue_name
        })
    
    def publish_worker_stopped(self, worker_id: str):
        """Publish worker_stopped event."""
        self.publish(self.WORKER_STOPPED, {
            "worker_id": worker_id
        })
    
    def publish_worker_heartbeat(self, worker_id: str, status: str, active_tasks: int):
        """Publish worker_heartbeat event."""
        self.publish(self.WORKER_HEARTBEAT, {
            "worker_id": worker_id,
            "status": status,
            "active_tasks": active_tasks
        })
    
    def publish_queue_updated(self, queue_name: str, size: int):
        """Publish queue_updated event."""
        self.publish(self.QUEUE_UPDATED, {
            "queue_name": queue_name,
            "size": size
        })
    
    def publish_dlq_updated(self, dlq_size: int):
        """Publish dlq_updated event."""
        self.publish(self.DLQ_UPDATED, {
            "dlq_size": dlq_size
        })


# Global event publisher instance
event_publisher = EventPublisher()
