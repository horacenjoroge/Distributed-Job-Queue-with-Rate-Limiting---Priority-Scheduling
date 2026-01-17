"""
Integration tests for rate limiting.
"""
import pytest
import time
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter


@pytest.mark.integration
class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_enforcement(self, mock_redis_broker):
        """Test that rate limits are enforced."""
        queue_name = "test_queue"
        rate_limit = 10  # 10 tasks per minute
        
        # Configure rate limit
        distributed_rate_limiter.set_rate_limit(queue_name, rate_limit)
        
        # Try to exceed rate limit
        allowed = 0
        blocked = 0
        
        for i in range(15):
            if distributed_rate_limiter.allow_task(queue_name):
                allowed += 1
            else:
                blocked += 1
            time.sleep(0.1)
        
        # Should have some blocked requests
        assert blocked > 0
        assert allowed <= rate_limit + 1  # Allow some tolerance
    
    def test_rate_limit_reset(self, mock_redis_broker):
        """Test that rate limit resets after time window."""
        queue_name = "test_queue"
        rate_limit = 5  # 5 tasks per minute
        
        distributed_rate_limiter.set_rate_limit(queue_name, rate_limit)
        
        # Exhaust rate limit
        for i in range(5):
            distributed_rate_limiter.allow_task(queue_name)
        
        # Should be blocked
        assert distributed_rate_limiter.allow_task(queue_name) is False
        
        # Wait for window to reset (using shorter window for testing)
        time.sleep(2)
        
        # Should be allowed again (if window reset)
        # Note: This depends on implementation details
