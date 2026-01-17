"""
Integration tests for distributed locks.
"""
import pytest
import time
import threading
from jobqueue.core.distributed_lock import task_lock_manager


@pytest.mark.integration
@pytest.mark.race
class TestDistributedLocks:
    """Test distributed locking."""
    
    def test_lock_acquisition(self, mock_redis_broker):
        """Test acquiring a lock."""
        task_signature = "test_signature"
        ttl = 60
        
        lock = task_lock_manager.acquire_task_lock(task_signature, ttl=ttl)
        
        assert lock is not None
        assert task_lock_manager.is_task_locked(task_signature) is True
        
        lock.release()
        assert task_lock_manager.is_task_locked(task_signature) is False
    
    def test_lock_prevents_concurrent_execution(self, mock_redis_broker):
        """Test that lock prevents concurrent execution."""
        task_signature = "test_signature"
        ttl = 60
        
        # Acquire lock
        lock1 = task_lock_manager.acquire_task_lock(task_signature, ttl=ttl)
        assert lock1 is not None
        
        # Try to acquire same lock (should fail or wait)
        lock2 = task_lock_manager.acquire_task_lock(
            task_signature,
            ttl=ttl,
            timeout=1  # Short timeout
        )
        
        # Second lock should fail
        assert lock2 is None
        
        # Release first lock
        lock1.release()
        
        # Now should be able to acquire
        lock3 = task_lock_manager.acquire_task_lock(task_signature, ttl=ttl)
        assert lock3 is not None
        lock3.release()
    
    def test_lock_ttl_expiration(self, mock_redis_broker):
        """Test that lock expires after TTL."""
        task_signature = "test_signature"
        ttl = 1  # 1 second TTL
        
        lock = task_lock_manager.acquire_task_lock(task_signature, ttl=ttl)
        assert lock is not None
        
        # Wait for TTL to expire
        time.sleep(2)
        
        # Lock should be expired
        assert task_lock_manager.is_task_locked(task_signature) is False
    
    def test_concurrent_lock_attempts(self, mock_redis_broker):
        """Test multiple threads trying to acquire same lock."""
        task_signature = "test_signature"
        ttl = 60
        results = []
        
        def try_acquire_lock():
            lock = task_lock_manager.acquire_task_lock(
                task_signature,
                ttl=ttl,
                timeout=2
            )
            if lock:
                results.append("acquired")
                time.sleep(0.5)
                lock.release()
            else:
                results.append("failed")
        
        # Acquire lock first
        lock1 = task_lock_manager.acquire_task_lock(task_signature, ttl=ttl)
        
        # Start multiple threads trying to acquire
        threads = []
        for i in range(5):
            t = threading.Thread(target=try_acquire_lock)
            threads.append(t)
            t.start()
        
        # Release first lock
        time.sleep(0.1)
        lock1.release()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Only one should have acquired (or all should have failed initially)
        assert len(results) == 5
        # At least one should have acquired after release
        assert "acquired" in results
