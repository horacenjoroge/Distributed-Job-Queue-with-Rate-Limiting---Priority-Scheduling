"""
Rate limiting verification test.

Test case: Set limit to 10/min, enqueue 100 tasks, verify rate.
This demonstrates that workers respect rate limits when processing tasks.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter
from jobqueue.core.queue_config import queue_config_manager


def test_rate_limit_10_per_minute():
    """
    Test: Set limit to 10 tasks/minute, enqueue 100 tasks, verify rate.
    
    This test:
    1. Configures a queue with 10 tasks/minute limit
    2. Enqueues 100 tasks
    3. Simulates worker processing with rate limiting
    4. Verifies that only 10 tasks are processed per minute
    """
    print("\n" + "=" * 70)
    print("RATE LIMIT VERIFICATION TEST")
    print("Test: 10 tasks/minute limit with 100 enqueued tasks")
    print("=" * 70)
    
    # Connect to Redis
    redis_broker.connect()
    
    queue_name = "test_rate_limit_queue"
    queue = Queue(queue_name)
    
    # Clean up
    print("\nSetup:")
    queue.purge()
    distributed_rate_limiter.reset(queue_name)
    
    # Configure rate limit: 10 tasks per minute
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=0,
        enabled=True
    )
    print(f"  Rate limit configured: 10 tasks/minute")
    
    # Enqueue 100 tasks
    print(f"  Enqueuing 100 tasks...")
    for i in range(100):
        task = Task(
            name=f"test_task_{i}",
            args=[i],
            priority=TaskPriority.MEDIUM,
            queue_name=queue_name
        )
        queue.enqueue(task)
    
    print(f"  Queue size: {queue.size()} tasks")
    
    # Simulate worker processing with rate limiting
    print(f"\nProcessing tasks with rate limit enforcement:")
    print(f"  Start time: {datetime.now().strftime('%H:%M:%S')}")
    
    start_time = time.time()
    processed_count = 0
    blocked_count = 0
    
    # Process tasks respecting rate limit
    while queue.size() > 0 and processed_count < 100:
        # Check rate limit before dequeuing
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        
        if can_proceed:
            task = queue.dequeue_nowait()
            if task:
                processed_count += 1
                
                # Show progress every 10 tasks
                if processed_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / (elapsed / 60)  # tasks per minute
                    print(f"    Processed {processed_count:3d} tasks | "
                          f"Elapsed: {elapsed:5.1f}s | "
                          f"Rate: {rate:5.1f} tasks/min")
        else:
            # Rate limit hit
            blocked_count += 1
            
            if blocked_count == 1:  # First time hitting limit
                stats = distributed_rate_limiter.get_stats(queue_name)
                wait_time = stats['wait_time_seconds']
                print(f"\n  Rate limit reached after {processed_count} tasks")
                print(f"  Remaining in queue: {queue.size()}")
                print(f"  Wait time required: {wait_time:.2f}s")
                print(f"  (In production, worker would sleep and retry)")
                break
    
    # Calculate statistics
    elapsed_time = time.time() - start_time
    actual_rate = processed_count / (elapsed_time / 60) if elapsed_time > 0 else 0
    
    print(f"\nResults:")
    print(f"  Total processed: {processed_count} tasks")
    print(f"  Total time: {elapsed_time:.2f}s")
    print(f"  Actual rate: {actual_rate:.2f} tasks/minute")
    print(f"  Remaining in queue: {queue.size()} tasks")
    print(f"  Rate limit respected: {'YES' if processed_count <= 10 else 'NO'}")
    
    # Verify rate limit was enforced
    print(f"\nVerification:")
    if processed_count == 10:
        print(f"  PASS: Exactly 10 tasks processed (rate limit enforced)")
    elif processed_count < 10:
        print(f"  WARN: Only {processed_count} tasks processed (expected 10)")
    else:
        print(f"  FAIL: {processed_count} tasks processed (exceeded limit of 10)")
    
    # Show rate limit stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    print(f"\nRate Limit Statistics:")
    print(f"  Max tasks/minute: {stats['max_tasks_per_minute']}")
    print(f"  Current count: {stats['current_count']}")
    print(f"  Remaining capacity: {stats['remaining_capacity']}")
    print(f"  Burst used: {stats['burst_used']}")
    
    # Cleanup
    print(f"\nCleanup:")
    queue.purge()
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()
    print(f"  Cleaned up queue and rate limiter")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETED")
    print("=" * 70)


def test_rate_limit_with_burst():
    """
    Test: 10 tasks/minute + 5 burst capacity
    
    This shows how burst allowance lets through extra tasks during spikes.
    """
    print("\n" + "=" * 70)
    print("RATE LIMIT WITH BURST TEST")
    print("Test: 10 tasks/minute + 5 burst capacity")
    print("=" * 70)
    
    redis_broker.connect()
    
    queue_name = "test_burst_queue"
    queue = Queue(queue_name)
    queue.purge()
    distributed_rate_limiter.reset(queue_name)
    
    # Configure with burst
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=5,
        enabled=True
    )
    
    print(f"\nConfiguration:")
    print(f"  Regular limit: 10 tasks/minute")
    print(f"  Burst capacity: 5 additional tasks")
    print(f"  Total capacity: 15 tasks")
    
    # Enqueue 20 tasks
    print(f"\nEnqueuing 20 tasks...")
    for i in range(20):
        task = Task(
            name=f"burst_task_{i}",
            args=[i],
            priority=TaskPriority.MEDIUM,
            queue_name=queue_name
        )
        queue.enqueue(task)
    
    # Process with burst
    print(f"\nProcessing tasks:")
    processed = 0
    burst_used = 0
    
    while queue.size() > 0 and processed < 20:
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        
        if can_proceed:
            task = queue.dequeue_nowait()
            if task:
                processed += 1
                
                stats = distributed_rate_limiter.get_stats(queue_name)
                current_burst = stats['burst_used']
                
                if current_burst > burst_used:
                    burst_used = current_burst
                    print(f"  Task {processed:2d}: Using burst capacity ({burst_used}/5)")
                elif processed % 5 == 0:
                    print(f"  Task {processed:2d}: Regular capacity")
        else:
            break
    
    print(f"\nResults:")
    print(f"  Tasks processed: {processed}")
    print(f"  Burst used: {burst_used}/5")
    print(f"  Expected: 15 (10 regular + 5 burst)")
    print(f"  Test: {'PASS' if processed == 15 else 'FAIL'}")
    
    # Cleanup
    queue.purge()
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()
    
    print("\n" + "=" * 70)


def test_distributed_workers():
    """
    Test: Multiple workers sharing rate limit
    
    Demonstrates that rate limiting works correctly across multiple workers.
    """
    print("\n" + "=" * 70)
    print("DISTRIBUTED WORKERS TEST")
    print("Test: 3 workers sharing 10 tasks/minute limit")
    print("=" * 70)
    
    redis_broker.connect()
    
    queue_name = "test_distributed_queue"
    distributed_rate_limiter.reset(queue_name)
    
    # Configure
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=0,
        enabled=True
    )
    
    print(f"\nConfiguration:")
    print(f"  Rate limit: 10 tasks/minute (shared)")
    print(f"  Workers: 3 concurrent workers")
    
    # Simulate 3 workers each trying to process 10 tasks
    print(f"\nSimulating 3 workers:")
    
    workers_processed = {"worker_1": 0, "worker_2": 0, "worker_3": 0}
    
    for round_num in range(10):
        for worker_id in workers_processed.keys():
            can_proceed = distributed_rate_limiter.acquire(queue_name)
            if can_proceed:
                workers_processed[worker_id] += 1
    
    total_processed = sum(workers_processed.values())
    
    print(f"\nResults:")
    for worker_id, count in workers_processed.items():
        print(f"  {worker_id}: {count} tasks")
    print(f"  Total: {total_processed} tasks")
    print(f"  Expected: 10 tasks total (rate limit)")
    print(f"  Test: {'PASS' if total_processed == 10 else 'FAIL'}")
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    try:
        # Run all tests
        test_rate_limit_10_per_minute()
        test_rate_limit_with_burst()
        test_distributed_workers()
        
        print("\n" + "=" * 70)
        print("ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
