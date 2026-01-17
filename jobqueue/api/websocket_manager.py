"""
WebSocket manager for real-time updates.
"""
import asyncio
import json
from typing import Set, Dict, Any
from fastapi import WebSocket
from jobqueue.utils.logger import log


class WebSocketManager:
    """Manages WebSocket connections and broadcasts updates."""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.broadcast_queue: asyncio.Queue = asyncio.Queue()
        self.broadcast_task: asyncio.Task = None
    
    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        log.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        log.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket):
        """Send a message to a specific WebSocket connection."""
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            log.error(f"Error sending WebSocket message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast a message to all connected clients."""
        if not self.active_connections:
            return
        
        message_json = json.dumps(message)
        disconnected = set()
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                log.error(f"Error broadcasting to WebSocket: {e}")
                disconnected.add(connection)
        
        # Remove disconnected connections
        for connection in disconnected:
            self.disconnect(connection)
    
    async def broadcast_task_update(self, task: Dict[str, Any]):
        """Broadcast a task update."""
        await self.broadcast({
            "type": "task_updated",
            "task_id": task.get("id"),
            "task": task
        })
    
    async def broadcast_worker_update(self, worker: Dict[str, Any]):
        """Broadcast a worker update."""
        await self.broadcast({
            "type": "worker_updated",
            "worker_id": worker.get("id"),
            "worker": worker
        })
    
    async def broadcast_queue_update(self, queue_name: str, size: int):
        """Broadcast a queue update."""
        await self.broadcast({
            "type": "queue_updated",
            "queue_name": queue_name,
            "size": size
        })
    
    async def broadcast_dlq_update(self, dlq_size: int):
        """Broadcast a DLQ update."""
        await self.broadcast({
            "type": "dlq_updated",
            "dlq_size": dlq_size
        })
    
    async def broadcast_metrics_update(self, metrics: Dict[str, Any]):
        """Broadcast metrics update."""
        await self.broadcast({
            "type": "metrics_updated",
            "metrics": metrics
        })


# Global WebSocket manager instance
websocket_manager = WebSocketManager()
