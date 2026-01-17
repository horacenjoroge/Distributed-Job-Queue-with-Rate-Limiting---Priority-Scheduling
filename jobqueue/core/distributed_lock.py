"""
Distributed lock system using Redis SETNX to prevent concurrent execution of same task.
"""
import time
import uuid
from typing import Optional
from datetime import datetime, timedelta
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class DistributedLock:
    """
    Distributed lock implementation using Redis SETNX.
    Prevents concurrent execution of the same task.
    """
    
    def __init__(
        self,
        lock_key: str,
        ttl: int = 300,
        lock_value: Optional[str] = None
    ):
        """
        Initialize distributed lock.
        
        Args:
            lock_key: Redis key for the lock
            ttl: Time-to-live in seconds (default: 300)
            lock_value: Unique value for this lock instance (auto-generated if not provided)
        """
        self.lock_key = lock_key
        self.ttl = ttl
        self.lock_value = lock_value or str(uuid.uuid4())
        self.acquired_at: Optional[datetime] = None
        self.is_acquired = False
        
        log.debug(
            f"DistributedLock initialized",
            extra={
                "lock_key": lock_key,
                "ttl": ttl,
                "lock_value": self.lock_value[:8]  # First 8 chars for logging
            }
        )
    
    def acquire(self, timeout: int = 0, retry_interval: float = 0.1) -> bool:
        """
        Acquire the lock.
        
        Args:
            timeout: Maximum time to wait for lock acquisition (0 = no wait)
            retry_interval: Time to wait between retry attempts in seconds
            
        Returns:
            True if lock acquired, False otherwise
            
        Example:
            lock = DistributedLock("lock:task:123", ttl=60)
            if lock.acquire(timeout=5):
                try:
                    # Do work
                    pass
                finally:
                    lock.release()
        """
        start_time = time.time()
        
        while True:
            # Try to acquire lock using SETNX
            # SETNX key value - Set key to value if key does not exist
            # Returns 1 if key was set, 0 if key already exists
            result = redis_broker.client.set(
                self.lock_key,
                self.lock_value,
                nx=True,  # Only set if key does not exist
                ex=self.ttl  # Set expiration
            )
            
            if result:
                # Lock acquired
                self.is_acquired = True
                self.acquired_at = datetime.utcnow()
                
                log.debug(
                    f"Lock acquired",
                    extra={
                        "lock_key": self.lock_key,
                        "lock_value": self.lock_value[:8],
                        "ttl": self.ttl
                    }
                )
                
                return True
            
            # Lock not acquired, check if we should retry
            if timeout == 0:
                # No timeout, return immediately
                log.debug(
                    f"Lock not acquired (no timeout)",
                    extra={"lock_key": self.lock_key}
                )
                return False
            
            # Check if timeout exceeded
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                log.debug(
                    f"Lock acquisition timeout",
                    extra={
                        "lock_key": self.lock_key,
                        "timeout": timeout,
                        "elapsed": elapsed
                    }
                )
                return False
            
            # Wait before retry
            time.sleep(retry_interval)
    
    def release(self) -> bool:
        """
        Release the lock.
        Uses Lua script to ensure we only release our own lock.
        
        Returns:
            True if lock released, False otherwise
        """
        if not self.is_acquired:
            log.warning(
                f"Attempted to release lock that was not acquired",
                extra={"lock_key": self.lock_key}
            )
            return False
        
        # Lua script to atomically check and delete lock
        # Only delete if the value matches (prevents releasing someone else's lock)
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = redis_broker.client.eval(
                lua_script,
                1,  # Number of keys
                self.lock_key,
                self.lock_value
            )
            
            if result:
                self.is_acquired = False
                self.acquired_at = None
                
                log.debug(
                    f"Lock released",
                    extra={
                        "lock_key": self.lock_key,
                        "lock_value": self.lock_value[:8]
                    }
                )
                
                return True
            else:
                log.warning(
                    f"Lock release failed (lock value mismatch or already released)",
                    extra={
                        "lock_key": self.lock_key,
                        "lock_value": self.lock_value[:8]
                    }
                )
                return False
                
        except Exception as e:
            log.error(
                f"Error releasing lock: {e}",
                extra={
                    "lock_key": self.lock_key,
                    "error": str(e)
                }
            )
            return False
    
    def renew(self, additional_ttl: Optional[int] = None) -> bool:
        """
        Renew the lock TTL.
        Useful for long-running tasks.
        
        Args:
            additional_ttl: Additional TTL in seconds (default: use original TTL)
            
        Returns:
            True if lock renewed, False otherwise
        """
        if not self.is_acquired:
            log.warning(
                f"Attempted to renew lock that was not acquired",
                extra={"lock_key": self.lock_key}
            )
            return False
        
        ttl = additional_ttl or self.ttl
        
        # Lua script to atomically check and extend lock TTL
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        
        try:
            result = redis_broker.client.eval(
                lua_script,
                1,  # Number of keys
                self.lock_key,
                self.lock_value,
                str(ttl)
            )
            
            if result:
                log.debug(
                    f"Lock renewed",
                    extra={
                        "lock_key": self.lock_key,
                        "ttl": ttl,
                        "lock_value": self.lock_value[:8]
                    }
                )
                return True
            else:
                log.warning(
                    f"Lock renewal failed (lock value mismatch or expired)",
                    extra={
                        "lock_key": self.lock_key,
                        "lock_value": self.lock_value[:8]
                    }
                )
                return False
                
        except Exception as e:
            log.error(
                f"Error renewing lock: {e}",
                extra={
                    "lock_key": self.lock_key,
                    "error": str(e)
                }
            )
            return False
    
    def is_locked(self) -> bool:
        """
        Check if lock is currently held (by anyone).
        
        Returns:
            True if lock exists, False otherwise
        """
        try:
            exists = redis_broker.client.exists(self.lock_key)
            return exists == 1
        except Exception as e:
            log.error(f"Error checking lock status: {e}")
            return False
    
    def get_remaining_ttl(self) -> int:
        """
        Get remaining TTL of the lock.
        
        Returns:
            Remaining TTL in seconds, -1 if lock doesn't exist, -2 if no TTL
        """
        try:
            ttl = redis_broker.client.ttl(self.lock_key)
            return ttl
        except Exception as e:
            log.error(f"Error getting lock TTL: {e}")
            return -1
    
    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


class TaskLockManager:
    """
    Manager for task-specific distributed locks.
    Uses task signature to generate lock keys.
    """
    
    @staticmethod
    def get_lock_key(task_signature: str) -> str:
        """
        Generate lock key for a task.
        
        Args:
            task_signature: Task signature (hash of name + args + kwargs)
            
        Returns:
            Lock key in format: lock:{task_signature}
        """
        return f"lock:{task_signature}"
    
    @staticmethod
    def acquire_task_lock(
        task_signature: str,
        ttl: int = 300,
        timeout: int = 0,
        retry_interval: float = 0.1
    ) -> Optional[DistributedLock]:
        """
        Acquire a lock for a task.
        
        Args:
            task_signature: Task signature
            ttl: Lock TTL in seconds
            timeout: Maximum time to wait for lock (0 = no wait)
            retry_interval: Time between retry attempts
            
        Returns:
            DistributedLock instance if acquired, None otherwise
        """
        lock_key = TaskLockManager.get_lock_key(task_signature)
        lock = DistributedLock(lock_key, ttl=ttl)
        
        if lock.acquire(timeout=timeout, retry_interval=retry_interval):
            return lock
        
        return None
    
    @staticmethod
    def is_task_locked(task_signature: str) -> bool:
        """
        Check if a task is currently locked.
        
        Args:
            task_signature: Task signature
            
        Returns:
            True if task is locked, False otherwise
        """
        lock_key = TaskLockManager.get_lock_key(task_signature)
        lock = DistributedLock(lock_key)
        return lock.is_locked()
    
    @staticmethod
    def get_task_lock_ttl(task_signature: str) -> int:
        """
        Get remaining TTL for a task lock.
        
        Args:
            task_signature: Task signature
            
        Returns:
            Remaining TTL in seconds
        """
        lock_key = TaskLockManager.get_lock_key(task_signature)
        lock = DistributedLock(lock_key)
        return lock.get_remaining_ttl()


# Global task lock manager instance
task_lock_manager = TaskLockManager()
