"""
Exponential backoff calculator for task retries.
"""
import random
from typing import Optional
from jobqueue.utils.logger import log


class ExponentialBackoff:
    """
    Calculate exponential backoff delays for task retries.
    
    Formula: delay = base^retry_count
    With optional jitter and maximum cap.
    """
    
    def __init__(
        self,
        base: float = 2.0,
        max_delay: float = 300.0,
        jitter: bool = True
    ):
        """
        Initialize exponential backoff calculator.
        
        Args:
            base: Base for exponential calculation (default: 2.0)
            max_delay: Maximum delay cap in seconds (default: 300)
            jitter: Add random jitter to prevent thundering herd (default: True)
        """
        self.base = base
        self.max_delay = max_delay
        self.jitter = jitter
        
        log.debug(
            f"Initialized exponential backoff",
            extra={
                "base": base,
                "max_delay": max_delay,
                "jitter": jitter
            }
        )
    
    def calculate_delay(self, retry_count: int) -> float:
        """
        Calculate backoff delay for given retry attempt.
        
        Args:
            retry_count: Number of retry attempts so far
            
        Returns:
            Delay in seconds
            
        Example:
            backoff = ExponentialBackoff()
            delay = backoff.calculate_delay(0)  # 1s
            delay = backoff.calculate_delay(1)  # 2s
            delay = backoff.calculate_delay(2)  # 4s
            delay = backoff.calculate_delay(3)  # 8s
        """
        # Calculate exponential delay: base^retry_count
        delay = self.base ** retry_count
        
        # Apply maximum cap
        delay = min(delay, self.max_delay)
        
        # Add jitter if enabled (random between 0 and delay)
        if self.jitter and delay > 0:
            delay = delay * (0.5 + random.random() * 0.5)  # 50-100% of calculated delay
        
        log.debug(
            f"Calculated backoff delay",
            extra={
                "retry_count": retry_count,
                "delay": delay,
                "jittered": self.jitter
            }
        )
        
        return delay
    
    def get_delay_sequence(self, max_retries: int) -> list[float]:
        """
        Get sequence of delays for all retry attempts.
        
        Args:
            max_retries: Maximum number of retries
            
        Returns:
            List of delays in seconds
        """
        return [self.calculate_delay(i) for i in range(max_retries)]


class LinearBackoff:
    """
    Linear backoff calculator (constant delay between retries).
    """
    
    def __init__(self, delay: float = 5.0, max_delay: float = 300.0):
        """
        Initialize linear backoff.
        
        Args:
            delay: Constant delay between retries (default: 5s)
            max_delay: Maximum delay cap (default: 300s)
        """
        self.delay = delay
        self.max_delay = max_delay
    
    def calculate_delay(self, retry_count: int) -> float:
        """Calculate constant delay."""
        return min(self.delay, self.max_delay)


class FibonacciBackoff:
    """
    Fibonacci backoff calculator (Fibonacci sequence delays).
    """
    
    def __init__(self, base: float = 1.0, max_delay: float = 300.0):
        """
        Initialize Fibonacci backoff.
        
        Args:
            base: Base delay multiplier (default: 1.0)
            max_delay: Maximum delay cap (default: 300s)
        """
        self.base = base
        self.max_delay = max_delay
    
    def _fibonacci(self, n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return n
        
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a
    
    def calculate_delay(self, retry_count: int) -> float:
        """Calculate Fibonacci delay."""
        fib_value = self._fibonacci(retry_count + 1)
        delay = self.base * fib_value
        return min(delay, self.max_delay)


def calculate_exponential_backoff(
    retry_count: int,
    base: float = 2.0,
    max_delay: float = 300.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        retry_count: Number of retry attempts
        base: Exponential base (default: 2.0)
        max_delay: Maximum delay cap (default: 300s)
        jitter: Add random jitter (default: True)
        
    Returns:
        Delay in seconds
        
    Example:
        delay = calculate_exponential_backoff(0)  # ~1s
        delay = calculate_exponential_backoff(1)  # ~2s
        delay = calculate_exponential_backoff(2)  # ~4s
        delay = calculate_exponential_backoff(3)  # ~8s
        delay = calculate_exponential_backoff(4)  # ~16s
    """
    backoff = ExponentialBackoff(base=base, max_delay=max_delay, jitter=jitter)
    return backoff.calculate_delay(retry_count)


def get_retry_delays(
    max_retries: int,
    strategy: str = "exponential",
    **kwargs
) -> list[float]:
    """
    Get list of retry delays for a given strategy.
    
    Args:
        max_retries: Maximum number of retries
        strategy: Backoff strategy ('exponential', 'linear', 'fibonacci')
        **kwargs: Strategy-specific parameters
        
    Returns:
        List of delays in seconds
        
    Example:
        delays = get_retry_delays(5, strategy="exponential")
        # [1.0, 2.0, 4.0, 8.0, 16.0]
    """
    if strategy == "exponential":
        backoff = ExponentialBackoff(**kwargs)
    elif strategy == "linear":
        backoff = LinearBackoff(**kwargs)
    elif strategy == "fibonacci":
        backoff = FibonacciBackoff(**kwargs)
    else:
        raise ValueError(f"Unknown backoff strategy: {strategy}")
    
    return backoff.get_delay_sequence(max_retries)


# Global exponential backoff instance with default settings
default_backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=True)
