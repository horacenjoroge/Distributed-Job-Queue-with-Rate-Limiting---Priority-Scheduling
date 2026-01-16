"""
Redis connection manager and broker implementation.
"""
import redis
from redis import ConnectionPool, Redis
from typing import Optional, Any
from contextlib import contextmanager
from config import settings
from jobqueue.utils.logger import log


class RedisBroker:
    """
    Redis connection manager with connection pooling.
    Handles message brokering for the job queue system.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        password: Optional[str] = None,
        max_connections: Optional[int] = None,
    ):
        """
        Initialize Redis broker with connection pooling.
        
        Args:
            host: Redis host address
            port: Redis port
            db: Redis database number
            password: Redis password (if required)
            max_connections: Maximum number of connections in pool
        """
        self.host = host or settings.redis_host
        self.port = port or settings.redis_port
        self.db = db or settings.redis_db
        self.password = password or settings.redis_password
        self.max_connections = max_connections or settings.redis_max_connections
        
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[Redis] = None
        
        log.info(
            f"Initializing Redis broker",
            extra={
                "host": self.host,
                "port": self.port,
                "db": self.db,
                "max_connections": self.max_connections,
            }
        )
    
    def connect(self) -> None:
        """Establish connection pool to Redis."""
        try:
            self._pool = ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                max_connections=self.max_connections,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
            )
            
            self._client = Redis(connection_pool=self._pool)
            
            # Test connection
            self._client.ping()
            
            log.info("Successfully connected to Redis")
            
        except redis.ConnectionError as e:
            log.error(f"Failed to connect to Redis: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error connecting to Redis: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close Redis connection pool."""
        if self._client:
            self._client.close()
            log.info("Closed Redis client connection")
        
        if self._pool:
            self._pool.disconnect()
            log.info("Disconnected Redis connection pool")
    
    @property
    def client(self) -> Redis:
        """
        Get Redis client instance.
        
        Returns:
            Redis client
            
        Raises:
            RuntimeError: If not connected
        """
        if self._client is None:
            raise RuntimeError("Redis broker not connected. Call connect() first.")
        return self._client
    
    def is_connected(self) -> bool:
        """
        Check if Redis connection is alive.
        
        Returns:
            True if connected and responsive, False otherwise
        """
        try:
            if self._client:
                self._client.ping()
                return True
        except (redis.ConnectionError, redis.TimeoutError):
            pass
        return False
    
    @contextmanager
    def pipeline(self, transaction: bool = True):
        """
        Context manager for Redis pipeline operations.
        
        Args:
            transaction: Whether to use transactions (MULTI/EXEC)
            
        Yields:
            Redis pipeline object
        """
        pipe = self.client.pipeline(transaction=transaction)
        try:
            yield pipe
            pipe.execute()
        except Exception as e:
            log.error(f"Pipeline error: {e}")
            raise
        finally:
            pipe.reset()
    
    def push_task(self, queue_name: str, task_data: str) -> int:
        """
        Push a task to a queue (list).
        
        Args:
            queue_name: Name of the queue
            task_data: Serialized task data
            
        Returns:
            Length of the queue after push
        """
        try:
            return self.client.lpush(queue_name, task_data)
        except Exception as e:
            log.error(f"Error pushing task to queue {queue_name}: {e}")
            raise
    
    def pop_task(self, queue_name: str, timeout: int = 0) -> Optional[str]:
        """
        Pop a task from a queue (blocking operation).
        
        Args:
            queue_name: Name of the queue
            timeout: Blocking timeout in seconds (0 = block indefinitely)
            
        Returns:
            Task data or None if timeout
        """
        try:
            result = self.client.brpop(queue_name, timeout=timeout)
            if result:
                _, task_data = result
                return task_data
            return None
        except Exception as e:
            log.error(f"Error popping task from queue {queue_name}: {e}")
            raise
    
    def get_queue_length(self, queue_name: str) -> int:
        """
        Get the current length of a queue.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            Number of tasks in the queue
        """
        try:
            return self.client.llen(queue_name)
        except Exception as e:
            log.error(f"Error getting queue length for {queue_name}: {e}")
            raise
    
    def set_with_ttl(self, key: str, value: Any, ttl: int) -> bool:
        """
        Set a key with time-to-live.
        
        Args:
            key: Redis key
            value: Value to store
            ttl: Time to live in seconds
            
        Returns:
            True if successful
        """
        try:
            return self.client.setex(key, ttl, value)
        except Exception as e:
            log.error(f"Error setting key {key} with TTL: {e}")
            raise
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for a key.
        
        Args:
            key: Redis key
            
        Returns:
            Value or None if not found
        """
        try:
            return self.client.get(key)
        except Exception as e:
            log.error(f"Error getting key {key}: {e}")
            raise
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.
        
        Args:
            keys: Keys to delete
            
        Returns:
            Number of keys deleted
        """
        try:
            return self.client.delete(*keys)
        except Exception as e:
            log.error(f"Error deleting keys: {e}")
            raise
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Args:
            key: Redis key
            
        Returns:
            True if key exists
        """
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            log.error(f"Error checking if key {key} exists: {e}")
            raise


# Global Redis broker instance
redis_broker = RedisBroker()
