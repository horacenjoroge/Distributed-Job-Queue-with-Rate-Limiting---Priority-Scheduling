"""
Metrics collection system for tracking performance metrics.
Uses Redis sorted sets for time-series data storage.
"""
import time
import math
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.utils.logger import log


class MetricsCollector:
    """
    Collects and stores performance metrics in Redis.
    Uses sorted sets for time-series data.
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        log.info("MetricsCollector initialized")
    
    def _get_timestamp(self) -> float:
        """Get current timestamp."""
        return time.time()
    
    def _get_metric_key(self, metric_name: str, aggregation: Optional[str] = None) -> str:
        """Get Redis key for metric."""
        if aggregation:
            return f"metrics:{metric_name}:{aggregation}"
        return f"metrics:{metric_name}"
    
    def _get_time_bucket(self, timestamp: float, bucket_size: int) -> int:
        """Get time bucket for aggregation."""
        return int(timestamp // bucket_size) * bucket_size
    
    def record_task_enqueued(
        self,
        queue_name: str,
        priority: Optional[TaskPriority] = None
    ) -> None:
        """
        Record a task enqueued event.
        
        Args:
            queue_name: Queue name
            priority: Task priority (optional)
        """
        try:
            timestamp = self._get_timestamp()
            
            # Record overall enqueued
            key = self._get_metric_key("tasks:enqueued")
            redis_broker.client.zadd(key, {f"{timestamp}": timestamp})
            redis_broker.client.expire(key, 86400 * 7)  # 7 days TTL
            
            # Record per queue
            queue_key = self._get_metric_key(f"tasks:enqueued:queue:{queue_name}")
            redis_broker.client.zadd(queue_key, {f"{timestamp}": timestamp})
            redis_broker.client.expire(queue_key, 86400 * 7)
            
            # Record per priority if provided
            if priority:
                priority_key = self._get_metric_key(f"tasks:enqueued:priority:{priority.value}")
                redis_broker.client.zadd(priority_key, {f"{timestamp}": timestamp})
                redis_broker.client.expire(priority_key, 86400 * 7)
            
        except Exception as e:
            log.error(f"Error recording task enqueued: {e}")
    
    def record_task_completed(
        self,
        task: Task,
        duration: float,
        success: bool
    ) -> None:
        """
        Record a task completed event.
        
        Args:
            task: Completed task
            duration: Task duration in seconds
            success: Whether task succeeded
        """
        try:
            timestamp = self._get_timestamp()
            duration_ms = int(duration * 1000)  # Convert to milliseconds
            
            # Record overall completed
            key = self._get_metric_key("tasks:completed")
            redis_broker.client.zadd(key, {f"{timestamp}": timestamp})
            redis_broker.client.expire(key, 86400 * 7)
            
            # Record success/failure
            status_key = self._get_metric_key("tasks:completed:success" if success else "tasks:completed:failure")
            redis_broker.client.zadd(status_key, {f"{timestamp}": timestamp})
            redis_broker.client.expire(status_key, 86400 * 7)
            
            # Record duration
            duration_key = self._get_metric_key("tasks:duration")
            redis_broker.client.zadd(duration_key, {f"{duration_ms}": timestamp})
            redis_broker.client.expire(duration_key, 86400 * 7)
            
            # Record per queue
            queue_key = self._get_metric_key(f"tasks:completed:queue:{task.queue_name}")
            redis_broker.client.zadd(queue_key, {f"{timestamp}": timestamp})
            redis_broker.client.expire(queue_key, 86400 * 7)
            
            # Record per task name
            task_name_key = self._get_metric_key(f"tasks:duration:name:{task.name}")
            redis_broker.client.zadd(task_name_key, {f"{duration_ms}": timestamp})
            redis_broker.client.expire(task_name_key, 86400 * 7)
            
        except Exception as e:
            log.error(f"Error recording task completed: {e}")
    
    def get_tasks_enqueued_per_second(self, window_seconds: int = 60) -> float:
        """
        Get tasks enqueued per second over a time window.
        
        Args:
            window_seconds: Time window in seconds
            
        Returns:
            Tasks enqueued per second
        """
        try:
            key = self._get_metric_key("tasks:enqueued")
            current_time = self._get_timestamp()
            window_start = current_time - window_seconds
            
            # Count entries in window
            count = redis_broker.client.zcount(key, window_start, current_time)
            
            return count / window_seconds if window_seconds > 0 else 0.0
            
        except Exception as e:
            log.error(f"Error getting tasks enqueued per second: {e}")
            return 0.0
    
    def get_tasks_completed_per_second(self, window_seconds: int = 60) -> float:
        """
        Get tasks completed per second over a time window.
        
        Args:
            window_seconds: Time window in seconds
            
        Returns:
            Tasks completed per second
        """
        try:
            key = self._get_metric_key("tasks:completed")
            current_time = self._get_timestamp()
            window_start = current_time - window_seconds
            
            # Count entries in window
            count = redis_broker.client.zcount(key, window_start, current_time)
            
            return count / window_seconds if window_seconds > 0 else 0.0
            
        except Exception as e:
            log.error(f"Error getting tasks completed per second: {e}")
            return 0.0
    
    def get_task_duration_percentiles(
        self,
        window_seconds: int = 3600,
        percentiles: List[float] = [0.5, 0.95, 0.99]
    ) -> Dict[float, float]:
        """
        Get task duration percentiles (p50, p95, p99).
        
        Args:
            window_seconds: Time window in seconds
            percentiles: List of percentiles to calculate (0.5 = p50, 0.95 = p95, etc.)
            
        Returns:
            Dictionary mapping percentile to duration in milliseconds
        """
        try:
            key = self._get_metric_key("tasks:duration")
            current_time = self._get_timestamp()
            window_start = current_time - window_seconds
            
            # Get all durations in window
            durations = redis_broker.client.zrangebyscore(
                key,
                window_start,
                current_time,
                withscores=False
            )
            
            if not durations:
                return {p: 0.0 for p in percentiles}
            
            # Parse durations (stored as strings in sorted set)
            duration_values = []
            for duration_str in durations:
                if isinstance(duration_str, bytes):
                    duration_str = duration_str.decode()
                try:
                    duration_values.append(float(duration_str))
                except ValueError:
                    continue
            
            if not duration_values:
                return {p: 0.0 for p in percentiles}
            
            # Sort durations
            duration_values.sort()
            
            # Calculate percentiles
            result = {}
            for percentile in percentiles:
                index = int((len(duration_values) - 1) * percentile)
                result[percentile] = duration_values[index]
            
            return result
            
        except Exception as e:
            log.error(f"Error getting task duration percentiles: {e}")
            return {p: 0.0 for p in percentiles}
    
    def get_success_rate(self, window_seconds: int = 3600) -> Dict[str, float]:
        """
        Get success vs failure rate.
        
        Args:
            window_seconds: Time window in seconds
            
        Returns:
            Dictionary with success_rate and failure_rate
        """
        try:
            current_time = self._get_timestamp()
            window_start = current_time - window_seconds
            
            # Count successes
            success_key = self._get_metric_key("tasks:completed:success")
            success_count = redis_broker.client.zcount(success_key, window_start, current_time)
            
            # Count failures
            failure_key = self._get_metric_key("tasks:completed:failure")
            failure_count = redis_broker.client.zcount(failure_key, window_start, current_time)
            
            total = success_count + failure_count
            
            if total == 0:
                return {"success_rate": 0.0, "failure_rate": 0.0, "total": 0}
            
            success_rate = success_count / total
            failure_rate = failure_count / total
            
            return {
                "success_rate": success_rate,
                "failure_rate": failure_rate,
                "total": total,
                "success_count": success_count,
                "failure_count": failure_count
            }
            
        except Exception as e:
            log.error(f"Error getting success rate: {e}")
            return {"success_rate": 0.0, "failure_rate": 0.0, "total": 0}
    
    def get_queue_size_per_priority(self) -> Dict[str, int]:
        """
        Get current queue size per priority from Redis queues.
        
        Returns:
            Dictionary mapping priority to queue size
        """
        try:
            result = {}
            
            # Get queue sizes from PriorityQueue (which uses sorted sets)
            try:
                queue = PriorityQueue("default")
                size_by_priority = queue.size_by_priority()
                for priority, count in size_by_priority.items():
                    result[priority.value] = count
            except Exception as e:
                log.debug(f"Error getting priority queue sizes: {e}")
            
            # Ensure all priorities are represented
            for priority in TaskPriority:
                if priority.value not in result:
                    result[priority.value] = 0
            
            return result
            
        except Exception as e:
            log.error(f"Error getting queue size per priority: {e}")
            return {p.value: 0 for p in TaskPriority}
    
    def get_task_counts_by_status(self) -> Dict[str, int]:
        """
        Get task counts by status from database.
        
        Returns:
            Dictionary mapping status to count
        """
        try:
            from jobqueue.backend.postgres_backend import postgres_backend
            
            query = """
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
            """
            results = postgres_backend.execute_query(query, fetch_all=True)
            
            counts = {}
            if results:
                for row in results:
                    counts[row["status"]] = row["count"]
            
            # Ensure all statuses are represented
            for status in TaskStatus:
                if status.value not in counts:
                    counts[status.value] = 0
            
            return counts
            
        except Exception as e:
            log.error(f"Error getting task counts by status: {e}")
            return {}
    
    def get_queue_info(self) -> Dict[str, Any]:
        """
        Get detailed queue information including pending tasks.
        
        Returns:
            Dictionary with queue information
        """
        try:
            from jobqueue.backend.postgres_backend import postgres_backend
            
            # Get queue sizes from Redis
            queue_sizes = self.get_queue_size_per_priority()
            
            # Get task counts by status from database
            status_counts = self.get_task_counts_by_status()
            
            # Get queue names and their sizes
            query = """
            SELECT queue_name, COUNT(*) as count
            FROM tasks
            WHERE status IN ('pending', 'queued')
            GROUP BY queue_name
            """
            results = postgres_backend.execute_query(query, fetch_all=True)
            
            queues = {}
            if results:
                for row in results:
                    queues[row["queue_name"]] = row["count"]
            
            return {
                "queue_sizes_by_priority": queue_sizes,
                "pending_tasks": status_counts.get("pending", 0) + status_counts.get("queued", 0),
                "running_tasks": status_counts.get("running", 0),
                "queues": queues,
                "status_counts": status_counts
            }
            
        except Exception as e:
            log.error(f"Error getting queue info: {e}")
            return {
                "queue_sizes_by_priority": {},
                "pending_tasks": 0,
                "running_tasks": 0,
                "queues": {},
                "status_counts": {}
            }
    
    def get_worker_utilization(self) -> Dict[str, Any]:
        """
        Get worker utilization metrics.
        
        Returns:
            Dictionary with worker utilization stats
        """
        try:
            workers_info = worker_heartbeat.get_all_workers_info()
            
            total_workers = len(workers_info)
            active_workers = sum(1 for w in workers_info if w.get("status") == WorkerStatus.ACTIVE.value)
            idle_workers = sum(1 for w in workers_info if w.get("status") == WorkerStatus.IDLE.value)
            dead_workers = sum(1 for w in workers_info if not w.get("is_alive", False))
            
            utilization = (active_workers / total_workers * 100) if total_workers > 0 else 0.0
            
            return {
                "total_workers": total_workers,
                "active_workers": active_workers,
                "idle_workers": idle_workers,
                "dead_workers": dead_workers,
                "utilization_percent": utilization
            }
            
        except Exception as e:
            log.error(f"Error getting worker utilization: {e}")
            return {
                "total_workers": 0,
                "active_workers": 0,
                "idle_workers": 0,
                "dead_workers": 0,
                "utilization_percent": 0.0
            }
    
    def aggregate_metrics(
        self,
        aggregation: str = "hourly",
        timestamp: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Aggregate metrics for a time period.
        
        Args:
            aggregation: Aggregation type ("hourly" or "daily")
            timestamp: Timestamp to aggregate (defaults to current time)
            
        Returns:
            Aggregated metrics
        """
        try:
            if timestamp is None:
                timestamp = self._get_timestamp()
            
            if aggregation == "hourly":
                bucket_size = 3600  # 1 hour
                window_start = timestamp - 3600
            elif aggregation == "daily":
                bucket_size = 86400  # 1 day
                window_start = timestamp - 86400
            else:
                raise ValueError(f"Unknown aggregation: {aggregation}")
            
            # Aggregate metrics
            aggregated = {
                "aggregation": aggregation,
                "timestamp": timestamp,
                "window_start": window_start,
                "window_end": timestamp,
                "tasks_enqueued_per_second": self.get_tasks_enqueued_per_second(
                    int(timestamp - window_start)
                ),
                "tasks_completed_per_second": self.get_tasks_completed_per_second(
                    int(timestamp - window_start)
                ),
                "duration_percentiles": self.get_task_duration_percentiles(
                    int(timestamp - window_start)
                ),
                "success_rate": self.get_success_rate(
                    int(timestamp - window_start)
                ),
                "queue_size_per_priority": self.get_queue_size_per_priority(),
                "worker_utilization": self.get_worker_utilization()
            }
            
            # Store aggregated metrics
            bucket = self._get_time_bucket(timestamp, bucket_size)
            agg_key = self._get_metric_key("aggregated", aggregation)
            
            import json
            redis_broker.client.zadd(
                agg_key,
                {json.dumps(aggregated): bucket}
            )
            redis_broker.client.expire(agg_key, 86400 * 30)  # 30 days TTL
            
            return aggregated
            
        except Exception as e:
            log.error(f"Error aggregating metrics: {e}")
            return {}
    
    def get_all_metrics(self, window_seconds: int = 3600) -> Dict[str, Any]:
        """
        Get all metrics for a time window.
        
        Args:
            window_seconds: Time window in seconds
            
        Returns:
            Dictionary with all metrics
        """
        try:
            queue_info = self.get_queue_info()
            
            return {
                "timestamp": self._get_timestamp(),
                "window_seconds": window_seconds,
                "tasks_enqueued_per_second": self.get_tasks_enqueued_per_second(window_seconds),
                "tasks_completed_per_second": self.get_tasks_completed_per_second(window_seconds),
                "duration_percentiles": self.get_task_duration_percentiles(window_seconds),
                "success_rate": self.get_success_rate(window_seconds),
                "queue_size_per_priority": queue_info.get("queue_sizes_by_priority", {}),
                "queue_info": queue_info,
                "worker_utilization": self.get_worker_utilization()
            }
            
        except Exception as e:
            log.error(f"Error getting all metrics: {e}")
            return {}


# Global metrics collector instance
metrics_collector = MetricsCollector()
