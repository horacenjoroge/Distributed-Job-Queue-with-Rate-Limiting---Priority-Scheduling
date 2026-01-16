"""
Retry utilities with exponential backoff.
"""
import time
from typing import Optional
from jobqueue.utils.logger import log
from config import settings


def calculate_backoff_delay(retry_count: int, base: Optional[int] = None) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        retry_count: Current retry attempt (0-indexed)
        base: Backoff base multiplier
        
    Returns:
        Delay in seconds
        
    Example:
        retry_count=0 -> 2^0 = 1 second
        retry_count=1 -> 2^1 = 2 seconds
        retry_count=2 -> 2^2 = 4 seconds
        retry_count=3 -> 2^3 = 8 seconds
    """
    if base is None:
        base = settings.retry_backoff_base
    
    delay = base ** retry_count
    
    # Cap at 5 minutes max
    max_delay = 300
    delay = min(delay, max_delay)
    
    log.debug(
        f"Calculated backoff delay: {delay}s",
        extra={"retry_count": retry_count, "base": base}
    )
    
    return float(delay)


def wait_with_backoff(retry_count: int, base: Optional[int] = None) -> None:
    """
    Wait with exponential backoff.
    
    Args:
        retry_count: Current retry attempt
        base: Backoff base multiplier
    """
    delay = calculate_backoff_delay(retry_count, base)
    
    log.info(
        f"Waiting {delay}s before retry",
        extra={"retry_count": retry_count, "delay": delay}
    )
    
    time.sleep(delay)


def should_retry(retry_count: int, max_retries: Optional[int] = None) -> bool:
    """
    Check if task should be retried.
    
    Args:
        retry_count: Current retry count
        max_retries: Maximum retries allowed
        
    Returns:
        True if should retry, False otherwise
    """
    if max_retries is None:
        max_retries = settings.max_retries
    
    return retry_count < max_retries


class RetryStrategy:
    """
    Configurable retry strategy with exponential backoff.
    """
    
    def __init__(
        self,
        max_retries: Optional[int] = None,
        backoff_base: Optional[int] = None,
        max_delay: float = 300.0,
    ):
        """
        Initialize retry strategy.
        
        Args:
            max_retries: Maximum retry attempts
            backoff_base: Exponential backoff base
            max_delay: Maximum delay between retries in seconds
        """
        self.max_retries = max_retries or settings.max_retries
        self.backoff_base = backoff_base or settings.retry_backoff_base
        self.max_delay = max_delay
    
    def should_retry(self, retry_count: int) -> bool:
        """
        Check if should retry.
        
        Args:
            retry_count: Current retry count
            
        Returns:
            True if should retry
        """
        return retry_count < self.max_retries
    
    def get_delay(self, retry_count: int) -> float:
        """
        Get retry delay for current attempt.
        
        Args:
            retry_count: Current retry count
            
        Returns:
            Delay in seconds
        """
        delay = self.backoff_base ** retry_count
        return min(delay, self.max_delay)
    
    def wait(self, retry_count: int) -> None:
        """
        Wait before retry.
        
        Args:
            retry_count: Current retry count
        """
        delay = self.get_delay(retry_count)
        time.sleep(delay)
