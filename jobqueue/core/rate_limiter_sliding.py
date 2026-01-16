"""
Sliding window rate limiter using Redis.
"""
import time
from typing import Optional, Dict
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter using Redis sorted sets.
    Tracks task execution timestamps for accurate rate limiting.
    """
    
    def __init__(self, window_size_seconds: int = 60):
        """
        Initialize sliding window rate limiter.
        
        Args:
            window_size_seconds: Size of the sliding window (default 60 = 1 minute)
            
        Example:
            limiter = SlidingWindowRateLimiter(window_size_seconds=60)
        """
        self.window_size = window_size_seconds
        
        log.info(
            f"Initialized sliding window rate limiter",
            extra={"window_size_seconds": window_size_seconds}
        )
    
    def _get_rate_limit_key(self, queue_name: str) -> str:
        """
        Get Redis key for rate limiting data.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Redis key
        """
        return f"ratelimit:{queue_name}:window"
    
    def check_rate_limit(
        self,
        queue_name: str,
        max_tasks_per_window: int
    ) -> tuple[bool, int]:
        """
        Check if rate limit is exceeded.
        
        Args:
            queue_name: Queue name
            max_tasks_per_window: Maximum tasks allowed in window
            
        Returns:
            Tuple of (can_proceed, current_count)
            
        Example:
            can_proceed, count = limiter.check_rate_limit("default", 100)
            if can_proceed:
                # Process task
                pass
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        window_start = current_time - self.window_size
        
        # Remove old entries outside the window
        redis_broker.client.zremrangebyscore(key, 0, window_start)
        
        # Count entries in current window
        current_count = redis_broker.client.zcard(key)
        
        # Check if under limit
        can_proceed = current_count < max_tasks_per_window
        
        log.debug(
            f"Rate limit check for {queue_name}",
            extra={
                "queue": queue_name,
                "current_count": current_count,
                "limit": max_tasks_per_window,
                "can_proceed": can_proceed
            }
        )
        
        return can_proceed, current_count
    
    def record_execution(self, queue_name: str) -> None:
        """
        Record a task execution in the sliding window.
        
        Args:
            queue_name: Queue name
            
        Example:
            limiter.record_execution("default")
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        
        # Add current timestamp to sorted set
        # Use timestamp as both score and member (with small random to avoid duplicates)
        member = f"{current_time}:{id(self)}"
        redis_broker.client.zadd(key, {member: current_time})
        
        # Set expiry on key to clean up automatically
        redis_broker.client.expire(key, self.window_size * 2)
        
        log.debug(
            f"Recorded execution for {queue_name}",
            extra={"queue": queue_name, "timestamp": current_time}
        )
    
    def get_current_rate(self, queue_name: str) -> int:
        """
        Get current execution count in the window.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Number of tasks executed in current window
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        window_start = current_time - self.window_size
        
        # Clean up old entries
        redis_broker.client.zremrangebyscore(key, 0, window_start)
        
        # Count current entries
        count = redis_broker.client.zcard(key)
        
        return count
    
    def get_remaining_capacity(
        self,
        queue_name: str,
        max_tasks_per_window: int
    ) -> int:
        """
        Get remaining capacity before hitting rate limit.
        
        Args:
            queue_name: Queue name
            max_tasks_per_window: Maximum tasks allowed
            
        Returns:
            Number of tasks that can still be executed
        """
        current = self.get_current_rate(queue_name)
        remaining = max(0, max_tasks_per_window - current)
        
        return remaining
    
    def wait_time_until_capacity(
        self,
        queue_name: str,
        max_tasks_per_window: int
    ) -> float:
        """
        Calculate wait time until capacity is available.
        
        Args:
            queue_name: Queue name
            max_tasks_per_window: Maximum tasks allowed
            
        Returns:
            Seconds to wait until at least one slot is available
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        
        # Check if we have capacity now
        can_proceed, current_count = self.check_rate_limit(queue_name, max_tasks_per_window)
        
        if can_proceed:
            return 0.0
        
        # Get the oldest entry in the window
        oldest_entries = redis_broker.client.zrange(key, 0, 0, withscores=True)
        
        if not oldest_entries:
            return 0.0
        
        _, oldest_timestamp = oldest_entries[0]
        
        # Calculate when the oldest entry will expire
        expiry_time = oldest_timestamp + self.window_size
        wait_time = max(0, expiry_time - current_time)
        
        log.debug(
            f"Wait time for {queue_name}: {wait_time}s",
            extra={
                "queue": queue_name,
                "wait_time": wait_time,
                "oldest_timestamp": oldest_timestamp
            }
        )
        
        return wait_time
    
    def reset(self, queue_name: str) -> None:
        """
        Reset rate limit counters for a queue.
        
        Args:
            queue_name: Queue name
        """
        key = self._get_rate_limit_key(queue_name)
        redis_broker.client.delete(key)
        
        log.info(f"Reset rate limit for queue {queue_name}")
    
    def get_stats(self, queue_name: str) -> Dict:
        """
        Get rate limiting statistics for a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Dictionary with rate stats
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        window_start = current_time - self.window_size
        
        # Clean old entries
        redis_broker.client.zremrangebyscore(key, 0, window_start)
        
        # Get all entries in window
        entries = redis_broker.client.zrange(key, 0, -1, withscores=True)
        
        if not entries:
            return {
                "queue": queue_name,
                "window_size_seconds": self.window_size,
                "current_count": 0,
                "rate_per_second": 0.0,
                "rate_per_minute": 0.0
            }
        
        count = len(entries)
        oldest_timestamp = entries[0][1]
        newest_timestamp = entries[-1][1]
        
        # Calculate actual time span
        time_span = newest_timestamp - oldest_timestamp
        
        if time_span > 0:
            rate_per_second = count / time_span
            rate_per_minute = rate_per_second * 60
        else:
            rate_per_second = 0.0
            rate_per_minute = 0.0
        
        return {
            "queue": queue_name,
            "window_size_seconds": self.window_size,
            "current_count": count,
            "rate_per_second": round(rate_per_second, 2),
            "rate_per_minute": round(rate_per_minute, 2),
            "oldest_timestamp": oldest_timestamp,
            "newest_timestamp": newest_timestamp
        }


# Global rate limiter instance
sliding_window_limiter = SlidingWindowRateLimiter()
