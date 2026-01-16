"""
Queue configuration with rate limiting settings.
"""
from typing import Optional, Dict
from dataclasses import dataclass, field
from jobqueue.utils.logger import log


@dataclass
class QueueRateLimitConfig:
    """
    Rate limit configuration for a queue.
    """
    max_tasks_per_minute: int = 0  # 0 = unlimited
    burst_allowance: int = 0  # Extra tasks allowed in bursts (0 = no burst)
    window_size_seconds: int = 60  # Sliding window size
    enabled: bool = True
    
    def __post_init__(self):
        """Validate configuration."""
        if self.max_tasks_per_minute < 0:
            raise ValueError("max_tasks_per_minute must be >= 0")
        if self.burst_allowance < 0:
            raise ValueError("burst_allowance must be >= 0")
        if self.window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be > 0")
    
    @property
    def max_tasks_per_window(self) -> int:
        """
        Get total tasks allowed per window (limit + burst).
        """
        return self.max_tasks_per_minute + self.burst_allowance
    
    @property
    def is_unlimited(self) -> bool:
        """
        Check if rate limiting is disabled.
        """
        return not self.enabled or self.max_tasks_per_minute == 0


@dataclass
class QueueConfig:
    """
    Complete configuration for a queue.
    """
    name: str
    rate_limit: QueueRateLimitConfig = field(default_factory=QueueRateLimitConfig)
    priority: int = 0  # For priority queues
    max_queue_size: Optional[int] = None  # Max pending tasks (None = unlimited)
    
    def __post_init__(self):
        """Validate configuration."""
        if not self.name:
            raise ValueError("Queue name cannot be empty")


class QueueConfigManager:
    """
    Manages configurations for all queues.
    """
    
    def __init__(self):
        """Initialize queue config manager."""
        self._configs: Dict[str, QueueConfig] = {}
        
        # Set up default queue
        self.set_config(
            QueueConfig(
                name="default",
                rate_limit=QueueRateLimitConfig(
                    max_tasks_per_minute=0,  # Unlimited by default
                    enabled=False
                )
            )
        )
        
        log.info("Initialized queue config manager")
    
    def set_config(self, config: QueueConfig) -> None:
        """
        Set configuration for a queue.
        
        Args:
            config: Queue configuration
        """
        self._configs[config.name] = config
        
        log.info(
            f"Set config for queue {config.name}",
            extra={
                "queue": config.name,
                "rate_limit_enabled": config.rate_limit.enabled,
                "max_per_minute": config.rate_limit.max_tasks_per_minute,
                "burst": config.rate_limit.burst_allowance
            }
        )
    
    def get_config(self, queue_name: str) -> QueueConfig:
        """
        Get configuration for a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Queue configuration (uses default if not found)
        """
        if queue_name not in self._configs:
            # Create default config for new queue
            default_config = QueueConfig(
                name=queue_name,
                rate_limit=QueueRateLimitConfig(
                    max_tasks_per_minute=0,
                    enabled=False
                )
            )
            self._configs[queue_name] = default_config
            
            log.debug(f"Created default config for queue {queue_name}")
        
        return self._configs[queue_name]
    
    def set_rate_limit(
        self,
        queue_name: str,
        max_tasks_per_minute: int,
        burst_allowance: int = 0,
        enabled: bool = True
    ) -> None:
        """
        Set rate limit for a queue.
        
        Args:
            queue_name: Queue name
            max_tasks_per_minute: Max tasks per minute
            burst_allowance: Extra burst capacity
            enabled: Enable/disable rate limiting
        """
        config = self.get_config(queue_name)
        config.rate_limit = QueueRateLimitConfig(
            max_tasks_per_minute=max_tasks_per_minute,
            burst_allowance=burst_allowance,
            enabled=enabled
        )
        
        log.info(
            f"Updated rate limit for {queue_name}",
            extra={
                "queue": queue_name,
                "limit": max_tasks_per_minute,
                "burst": burst_allowance,
                "enabled": enabled
            }
        )
    
    def get_rate_limit(self, queue_name: str) -> QueueRateLimitConfig:
        """
        Get rate limit config for a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Rate limit configuration
        """
        return self.get_config(queue_name).rate_limit
    
    def remove_config(self, queue_name: str) -> None:
        """
        Remove configuration for a queue.
        
        Args:
            queue_name: Queue name
        """
        if queue_name in self._configs:
            del self._configs[queue_name]
            log.info(f"Removed config for queue {queue_name}")
    
    def list_queues(self) -> list[str]:
        """
        List all configured queues.
        
        Returns:
            List of queue names
        """
        return list(self._configs.keys())
    
    def get_all_configs(self) -> Dict[str, QueueConfig]:
        """
        Get all queue configurations.
        
        Returns:
            Dictionary of queue configs
        """
        return self._configs.copy()


# Global queue config manager instance
queue_config_manager = QueueConfigManager()
