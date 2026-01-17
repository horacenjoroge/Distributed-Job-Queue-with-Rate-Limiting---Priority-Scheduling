"""
Redis Pub/Sub event subscriber for forwarding events to WebSocket clients.
"""
import json
import asyncio
from typing import Optional
from redis import Redis
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.api.websocket_manager import websocket_manager
from jobqueue.utils.logger import log


class EventSubscriber:
    """Subscribes to Redis Pub/Sub events and forwards them to WebSocket clients."""
    
    CHANNEL_PREFIX = "jobqueue:events:"
    ALL_CHANNEL = f"{CHANNEL_PREFIX}all"
    
    def __init__(self):
        self.redis: Optional[Redis] = None
        self.pubsub = None
        self.running = False
        self.task: Optional[asyncio.Task] = None
    
    def connect(self):
        """Connect to Redis and subscribe to events."""
        if not redis_broker.is_connected():
            redis_broker.connect()
        self.redis = redis_broker.get_connection()
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(self.ALL_CHANNEL)
        log.info("Subscribed to Redis Pub/Sub events")
    
    async def start(self):
        """Start the event subscriber loop."""
        if self.running:
            return
        
        self.connect()
        self.running = True
        self.task = asyncio.create_task(self._listen_loop())
        log.info("Event subscriber started")
    
    async def stop(self):
        """Stop the event subscriber."""
        self.running = False
        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        log.info("Event subscriber stopped")
    
    async def _listen_loop(self):
        """Listen for Redis Pub/Sub messages and forward to WebSocket clients."""
        while self.running:
            try:
                # Get message from Redis Pub/Sub (non-blocking)
                message = self.pubsub.get_message(timeout=1.0)
                
                if message and message['type'] == 'message':
                    try:
                        event = json.loads(message['data'])
                        await self._handle_event(event)
                    except json.JSONDecodeError as e:
                        log.error(f"Error decoding event message: {e}")
                    except Exception as e:
                        log.error(f"Error handling event: {e}")
                
                # Small sleep to prevent tight loop
                await asyncio.sleep(0.1)
                
            except Exception as e:
                log.error(f"Error in event subscriber loop: {e}")
                await asyncio.sleep(1)  # Wait before retrying
    
    async def _handle_event(self, event: dict):
        """Handle a received event and forward to WebSocket clients."""
        event_type = event.get('type')
        data = event.get('data', {})
        
        # Forward to WebSocket clients based on event type
        if event_type == 'task_enqueued':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "name": data.get("task_name"),
                "status": "pending",
                "queue_name": data.get("queue_name"),
                "priority": data.get("priority"),
            })
            # Also broadcast queue update
            await websocket_manager.broadcast_queue_update(
                data.get("queue_name", "default"),
                -1  # Size will be updated separately
            )
        
        elif event_type == 'task_started':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "status": "running",
                "worker_id": data.get("worker_id"),
            })
        
        elif event_type == 'task_completed':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "status": "success",
                "worker_id": data.get("worker_id"),
                "duration": data.get("duration"),
            })
            # Broadcast queue update
            await websocket_manager.broadcast_queue_update("default", -1)
        
        elif event_type == 'task_failed':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "status": "failed",
                "worker_id": data.get("worker_id"),
                "error": data.get("error"),
            })
            # Broadcast DLQ update if max retries reached
            if data.get("retry_count", 0) >= 3:  # Assuming max_retries is 3
                await websocket_manager.broadcast_dlq_update(-1)
        
        elif event_type == 'task_cancelled':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "status": "cancelled",
            })
        
        elif event_type == 'task_retry':
            await websocket_manager.broadcast_task_update({
                "id": data.get("task_id"),
                "status": "retry",
                "retry_count": data.get("retry_count"),
            })
        
        elif event_type == 'worker_started':
            await websocket_manager.broadcast_worker_update({
                "id": data.get("worker_id"),
                "hostname": data.get("hostname"),
                "status": "active",
                "queue_name": data.get("queue_name"),
            })
        
        elif event_type == 'worker_stopped':
            await websocket_manager.broadcast_worker_update({
                "id": data.get("worker_id"),
                "status": "dead",
            })
        
        elif event_type == 'worker_heartbeat':
            await websocket_manager.broadcast_worker_update({
                "id": data.get("worker_id"),
                "status": data.get("status"),
                "active_tasks": data.get("active_tasks"),
            })
        
        elif event_type == 'queue_updated':
            await websocket_manager.broadcast_queue_update(
                data.get("queue_name", "default"),
                data.get("size", 0)
            )
        
        elif event_type == 'dlq_updated':
            await websocket_manager.broadcast_dlq_update(data.get("dlq_size", 0))
        
        # Broadcast raw event for any other handlers
        await websocket_manager.broadcast(event)


# Global event subscriber instance
event_subscriber = EventSubscriber()
