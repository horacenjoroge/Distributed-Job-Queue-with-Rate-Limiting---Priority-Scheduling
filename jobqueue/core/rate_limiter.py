"""
Rate limiter implementation for queue throttling.
"""
import time
from typing import Dict, Optional
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import TaskPriority
from jobqueue.utils.logger import log
from config import settings


class RateLimiter:
    """
    Token bucket rate limiter using Redis for distributed rate limiting.
    """
    
    def __init__(self):
        """Initialize rate limiter with configured limits."""
        self.limits = {
            TaskPriority.HIGH: settings.rate_limit_high,
            TaskPriority.MEDIUM: settings.rate_limit_medium,
            TaskPriority.LOW: settings.rate_limit_low,
        }
    
    def _get_rate_key(self, queue_name: str, priority: TaskPriority) -> str:
        """
        Get Redis key for rate limiting.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            Redis key for rate limiting
        """
        return f"rate_limit:{queue_name}:{priority}"
    
    def _get_window_key(self, queue_name: str, priority: TaskPriority) -> str:
        """
        Get Redis key for rate limiting window start.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            Redis key for window start
        """
        return f"rate_limit_window:{queue_name}:{priority}"
    
    def can_process(self, queue_name: str, priority: TaskPriority) -> bool:
        """
        Check if a task can be processed based on rate limits.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            True if task can be processed, False otherwise
        """
        limit = self.limits.get(priority)
        if limit is None or limit <= 0:
            return True
        
        rate_key = self._get_rate_key(queue_name, priority)
        window_key = self._get_window_key(queue_name, priority)
        
        current_time = int(time.time())
        window_start = redis_broker.get(window_key)
        
        # Initialize window if not exists
        if window_start is None:
            redis_broker.set_with_ttl(window_key, str(current_time), 60)
            redis_broker.set_with_ttl(rate_key, "0", 60)
            window_start = str(current_time)
        
        window_start = int(window_start)
        
        # Check if we're in a new minute window
        if current_time - window_start >= 60:
            # Reset counter for new window
            redis_broker.set_with_ttl(window_key, str(current_time), 60)
            redis_broker.set_with_ttl(rate_key, "0", 60)
            return True
        
        # Check current count
        count = redis_broker.get(rate_key)
        count = int(count) if count else 0
        
        if count < limit:
            return True
        
        log.warning(
            f"Rate limit exceeded for {queue_name}:{priority}",
            extra={
                "queue": queue_name,
                "priority": priority,
                "limit": limit,
                "count": count,
            }
        )
        return False
    
    def increment(self, queue_name: str, priority: TaskPriority) -> None:
        """
        Increment the task count for rate limiting.
        
        Args:
            queue_name: Queue name
            priority: Task priority
        """
        rate_key = self._get_rate_key(queue_name, priority)
        
        try:
            redis_broker.client.incr(rate_key)
        except Exception as e:
            log.error(f"Error incrementing rate limit counter: {e}")
    
    def get_current_count(self, queue_name: str, priority: TaskPriority) -> int:
        """
        Get current task count in the current window.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            Current count
        """
        rate_key = self._get_rate_key(queue_name, priority)
        count = redis_broker.get(rate_key)
        return int(count) if count else 0
    
    def get_remaining(self, queue_name: str, priority: TaskPriority) -> int:
        """
        Get remaining tasks that can be processed in current window.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            Remaining task count
        """
        limit = self.limits.get(priority, 0)
        current = self.get_current_count(queue_name, priority)
        return max(0, limit - current)
    
    def get_window_reset_time(self, queue_name: str, priority: TaskPriority) -> Optional[int]:
        """
        Get time when the rate limit window resets.
        
        Args:
            queue_name: Queue name
            priority: Task priority
            
        Returns:
            Unix timestamp when window resets, or None
        """
        window_key = self._get_window_key(queue_name, priority)
        window_start = redis_broker.get(window_key)
        
        if window_start:
            return int(window_start) + 60
        return None


# Global rate limiter instance
rate_limiter = RateLimiter()
