"""
Distributed rate limiter with burst support using Redis atomic operations.
Coordinates rate limiting across multiple workers.
"""
import time
from typing import Optional
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.queue_config import queue_config_manager
from jobqueue.utils.logger import log


class DistributedRateLimiter:
    """
    Distributed rate limiter supporting burst allowance.
    Uses Redis atomic operations to coordinate across multiple workers.
    """
    
    def __init__(self, window_size_seconds: int = 60):
        """
        Initialize distributed rate limiter.
        
        Args:
            window_size_seconds: Size of sliding window
        """
        self.window_size = window_size_seconds
        
        log.info(
            "Initialized distributed rate limiter",
            extra={"window_size_seconds": window_size_seconds}
        )
    
    def _get_rate_limit_key(self, queue_name: str) -> str:
        """Get Redis key for rate limiting data."""
        return f"ratelimit:{queue_name}:window"
    
    def _get_burst_key(self, queue_name: str) -> str:
        """Get Redis key for burst allowance tracking."""
        return f"ratelimit:{queue_name}:burst"
    
    def acquire(self, queue_name: str) -> bool:
        """
        Try to acquire permission to process a task (distributed-safe).
        
        This method is atomic and safe for concurrent access from multiple workers.
        
        Args:
            queue_name: Queue name
            
        Returns:
            True if permission granted, False otherwise
        """
        config = queue_config_manager.get_rate_limit(queue_name)
        
        # Check if rate limiting is disabled
        if config.is_unlimited:
            return True
        
        current_time = time.time()
        window_start = current_time - self.window_size
        
        key = self._get_rate_limit_key(queue_name)
        burst_key = self._get_burst_key(queue_name)
        
        # Use Lua script for atomic check-and-increment
        lua_script = """
        local key = KEYS[1]
        local burst_key = KEYS[2]
        local window_start = tonumber(ARGV[1])
        local current_time = tonumber(ARGV[2])
        local max_tasks = tonumber(ARGV[3])
        local burst_allowance = tonumber(ARGV[4])
        local window_size = tonumber(ARGV[5])
        local member = ARGV[6]
        
        -- Remove old entries
        redis.call('ZREMRANGEBYSCORE', key, 0, window_start)
        
        -- Count current entries
        local current_count = redis.call('ZCARD', key)
        
        -- Check regular limit
        if current_count < max_tasks then
            -- Under regular limit, add entry
            redis.call('ZADD', key, current_time, member)
            redis.call('EXPIRE', key, window_size * 2)
            return 1
        end
        
        -- Check burst allowance
        if burst_allowance > 0 then
            local burst_used = tonumber(redis.call('GET', burst_key) or 0)
            if burst_used < burst_allowance then
                -- Use burst capacity
                redis.call('ZADD', key, current_time, member)
                redis.call('EXPIRE', key, window_size * 2)
                redis.call('INCR', burst_key)
                redis.call('EXPIRE', burst_key, window_size)
                return 1
            end
        end
        
        -- Limit exceeded
        return 0
        """
        
        member = f"{current_time}:{id(self)}"
        
        result = redis_broker.client.eval(
            lua_script,
            2,  # Number of keys
            key,
            burst_key,
            window_start,
            current_time,
            config.max_tasks_per_minute,
            config.burst_allowance,
            self.window_size,
            member
        )
        
        acquired = bool(result)
        
        log.debug(
            f"Rate limit acquire for {queue_name}",
            extra={
                "queue": queue_name,
                "acquired": acquired,
                "timestamp": current_time
            }
        )
        
        return acquired
    
    def get_current_rate(self, queue_name: str) -> int:
        """
        Get current execution count in window.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Number of tasks in current window
        """
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        window_start = current_time - self.window_size
        
        # Clean old entries
        redis_broker.client.zremrangebyscore(key, 0, window_start)
        
        return redis_broker.client.zcard(key)
    
    def get_burst_usage(self, queue_name: str) -> int:
        """
        Get current burst capacity usage.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Number of burst slots used
        """
        burst_key = self._get_burst_key(queue_name)
        usage = redis_broker.client.get(burst_key)
        
        return int(usage) if usage else 0
    
    def wait_time_until_capacity(self, queue_name: str) -> float:
        """
        Calculate wait time until capacity available.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Seconds to wait
        """
        config = queue_config_manager.get_rate_limit(queue_name)
        
        if config.is_unlimited:
            return 0.0
        
        key = self._get_rate_limit_key(queue_name)
        current_time = time.time()
        
        # Get current count
        current_count = self.get_current_rate(queue_name)
        burst_used = self.get_burst_usage(queue_name)
        
        total_capacity = config.max_tasks_per_window
        
        # Check if we have capacity
        if current_count < config.max_tasks_per_minute:
            return 0.0
        
        if burst_used < config.burst_allowance:
            return 0.0
        
        # Get oldest entry
        oldest_entries = redis_broker.client.zrange(key, 0, 0, withscores=True)
        
        if not oldest_entries:
            return 0.0
        
        _, oldest_timestamp = oldest_entries[0]
        expiry_time = oldest_timestamp + self.window_size
        wait_time = max(0, expiry_time - current_time)
        
        return wait_time
    
    def reset(self, queue_name: str) -> None:
        """
        Reset rate limit counters.
        
        Args:
            queue_name: Queue name
        """
        key = self._get_rate_limit_key(queue_name)
        burst_key = self._get_burst_key(queue_name)
        
        redis_broker.client.delete(key)
        redis_broker.client.delete(burst_key)
        
        log.info(f"Reset distributed rate limit for {queue_name}")
    
    def get_stats(self, queue_name: str) -> dict:
        """
        Get rate limiting statistics.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Dictionary with stats
        """
        config = queue_config_manager.get_rate_limit(queue_name)
        current_count = self.get_current_rate(queue_name)
        burst_used = self.get_burst_usage(queue_name)
        
        remaining = max(0, config.max_tasks_per_minute - current_count)
        burst_remaining = max(0, config.burst_allowance - burst_used)
        
        wait_time = self.wait_time_until_capacity(queue_name)
        
        return {
            "queue": queue_name,
            "window_size_seconds": self.window_size,
            "rate_limit_enabled": config.enabled,
            "max_tasks_per_minute": config.max_tasks_per_minute,
            "burst_allowance": config.burst_allowance,
            "current_count": current_count,
            "burst_used": burst_used,
            "remaining_capacity": remaining,
            "burst_remaining": burst_remaining,
            "wait_time_seconds": round(wait_time, 2),
            "is_unlimited": config.is_unlimited
        }


# Global distributed rate limiter instance
distributed_rate_limiter = DistributedRateLimiter()
