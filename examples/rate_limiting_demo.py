"""
Rate limiting demonstration.

This example shows how to configure and use rate limiting to control
task execution rates across distributed workers.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter
from jobqueue.core.queue_config import queue_config_manager
from jobqueue.utils.logger import log


def demo_basic_rate_limiting():
    """
    Demo 1: Basic rate limiting
    
    Configure a queue with rate limiting and observe behavior.
    """
    print("\n" + "=" * 60)
    print("Demo 1: Basic Rate Limiting")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "rate_limited_queue"
    queue = Queue(queue_name)
    queue.purge()
    
    # Configure rate limit: 10 tasks per minute
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=0,
        enabled=True
    )
    
    print(f"Rate limit set: 10 tasks/minute")
    
    # Try to acquire 15 tasks
    print("\nAttempting to acquire 15 tasks:")
    for i in range(15):
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        status = "ALLOWED" if can_proceed else "BLOCKED"
        print(f"  Task {i+1:2d}: {status}")
    
    # Show stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    print(f"\nRate Limit Stats:")
    print(f"  Current count: {stats['current_count']}")
    print(f"  Remaining capacity: {stats['remaining_capacity']}")
    print(f"  Wait time: {stats['wait_time_seconds']}s")
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def demo_burst_allowance():
    """
    Demo 2: Burst allowance
    
    Show how burst allowance allows temporary spikes in traffic.
    """
    print("\n" + "=" * 60)
    print("Demo 2: Burst Allowance")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "burst_queue"
    
    # Configure with burst: 5 normal + 3 burst = 8 total
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=5,
        burst_allowance=3,
        enabled=True
    )
    
    print(f"Rate limit: 5 tasks/minute with 3 burst capacity")
    print(f"Total capacity: 8 tasks")
    
    # Try to acquire 10 tasks
    print("\nAttempting to acquire 10 tasks:")
    for i in range(10):
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        
        stats = distributed_rate_limiter.get_stats(queue_name)
        burst_used = stats['burst_used']
        
        status = "ALLOWED"
        if can_proceed:
            if burst_used > 0 and i >= 5:
                status = "ALLOWED (BURST)"
        else:
            status = "BLOCKED"
        
        print(f"  Task {i+1:2d}: {status} (Burst used: {burst_used})")
    
    # Show final stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    print(f"\nFinal Stats:")
    print(f"  Regular count: {stats['current_count']}")
    print(f"  Burst used: {stats['burst_used']}")
    print(f"  Total allowed: {stats['current_count']}")
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def demo_sliding_window():
    """
    Demo 3: Sliding window behavior
    
    Show how the sliding window allows new tasks as old ones expire.
    """
    print("\n" + "=" * 60)
    print("Demo 3: Sliding Window")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "sliding_window_queue"
    
    # Use a short window for demo (5 seconds)
    distributed_rate_limiter.window_size = 5
    
    # Configure rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=3,
        burst_allowance=0,
        enabled=True
    )
    
    print(f"Rate limit: 3 tasks per 5-second window")
    
    # Acquire 3 tasks
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Acquiring 3 tasks:")
    for i in range(3):
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        print(f"  Task {i+1}: {'ALLOWED' if can_proceed else 'BLOCKED'}")
    
    # Try 4th task (should fail)
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Attempting 4th task:")
    can_proceed = distributed_rate_limiter.acquire(queue_name)
    print(f"  Task 4: {'ALLOWED' if can_proceed else 'BLOCKED'}")
    
    wait_time = distributed_rate_limiter.wait_time_until_capacity(queue_name)
    print(f"  Wait time until capacity: {wait_time:.2f}s")
    
    # Wait for window to slide
    print(f"\nWaiting for window to slide...")
    time.sleep(5.5)
    
    # Try again (should succeed)
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] After window slide:")
    can_proceed = distributed_rate_limiter.acquire(queue_name)
    print(f"  Task 5: {'ALLOWED' if can_proceed else 'BLOCKED'}")
    
    # Cleanup
    distributed_rate_limiter.window_size = 60  # Reset to default
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def demo_rate_limit_stats():
    """
    Demo 4: Rate limit statistics
    
    Show detailed statistics about rate limiting.
    """
    print("\n" + "=" * 60)
    print("Demo 4: Rate Limit Statistics")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "stats_queue"
    
    # Configure
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=20,
        burst_allowance=5,
        enabled=True
    )
    
    # Acquire some tasks
    print(f"Acquiring 12 tasks...")
    for i in range(12):
        distributed_rate_limiter.acquire(queue_name)
    
    # Get detailed stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    
    print(f"\nDetailed Statistics:")
    print(f"  Queue: {stats['queue']}")
    print(f"  Rate limit enabled: {stats['rate_limit_enabled']}")
    print(f"  Max tasks/minute: {stats['max_tasks_per_minute']}")
    print(f"  Burst allowance: {stats['burst_allowance']}")
    print(f"  Window size: {stats['window_size_seconds']}s")
    print(f"  Current count: {stats['current_count']}")
    print(f"  Burst used: {stats['burst_used']}")
    print(f"  Remaining capacity: {stats['remaining_capacity']}")
    print(f"  Burst remaining: {stats['burst_remaining']}")
    print(f"  Wait time: {stats['wait_time_seconds']}s")
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def demo_multiple_queues():
    """
    Demo 5: Multiple queues with different rate limits
    
    Show how different queues can have independent rate limits.
    """
    print("\n" + "=" * 60)
    print("Demo 5: Multiple Queues")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Configure different queues
    queue_configs = {
        "high_throughput": {"max": 100, "burst": 20},
        "medium_throughput": {"max": 50, "burst": 10},
        "low_throughput": {"max": 10, "burst": 2},
    }
    
    for queue_name, config in queue_configs.items():
        queue_config_manager.set_rate_limit(
            queue_name=queue_name,
            max_tasks_per_minute=config["max"],
            burst_allowance=config["burst"],
            enabled=True
        )
    
    print("Configured queues:")
    for queue_name, config in queue_configs.items():
        print(f"  {queue_name}: {config['max']}/min + {config['burst']} burst")
    
    # Simulate acquiring tasks from each queue
    print("\nAcquiring 30 tasks from each queue:")
    
    for queue_name in queue_configs.keys():
        allowed = 0
        blocked = 0
        
        for i in range(30):
            if distributed_rate_limiter.acquire(queue_name):
                allowed += 1
            else:
                blocked += 1
        
        print(f"  {queue_name}: {allowed} allowed, {blocked} blocked")
    
    # Show stats for all queues
    print("\nFinal statistics:")
    for queue_name in queue_configs.keys():
        stats = distributed_rate_limiter.get_stats(queue_name)
        print(f"  {queue_name}:")
        print(f"    Current: {stats['current_count']}")
        print(f"    Burst used: {stats['burst_used']}")
        print(f"    Remaining: {stats['remaining_capacity']}")
    
    # Cleanup
    for queue_name in queue_configs.keys():
        distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def demo_worker_integration():
    """
    Demo 6: Worker integration
    
    Show how workers check rate limits before processing tasks.
    """
    print("\n" + "=" * 60)
    print("Demo 6: Worker Integration")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "worker_queue"
    queue = Queue(queue_name)
    queue.purge()
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=5,
        burst_allowance=0,
        enabled=True
    )
    
    print(f"Rate limit: 5 tasks/minute")
    
    # Enqueue 10 tasks
    print(f"\nEnqueuing 10 tasks...")
    for i in range(10):
        task = Task(
            name=f"demo_task_{i}",
            args=[i],
            priority=TaskPriority.MEDIUM,
            queue_name=queue_name
        )
        queue.enqueue(task)
    
    print(f"Queue size: {queue.size()}")
    
    # Simulate worker processing with rate limiting
    print(f"\nSimulating worker processing with rate limit:")
    
    processed = 0
    rate_limited = 0
    
    while queue.size() > 0 and processed < 10:
        # Check rate limit
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        
        if can_proceed:
            task = queue.dequeue_nowait()
            if task:
                print(f"  Processing task: {task.name}")
                processed += 1
        else:
            wait_time = distributed_rate_limiter.wait_time_until_capacity(queue_name)
            print(f"  Rate limit hit! Would wait {wait_time:.2f}s")
            rate_limited += 1
            break  # In real scenario, worker would sleep here
    
    print(f"\nResults:")
    print(f"  Tasks processed: {processed}")
    print(f"  Tasks remaining: {queue.size()}")
    print(f"  Rate limit hits: {rate_limited}")
    
    # Cleanup
    queue.purge()
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def run_all_demos():
    """Run all rate limiting demonstrations."""
    print("\n" + "=" * 60)
    print("RATE LIMITING DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_basic_rate_limiting()
        demo_burst_allowance()
        demo_sliding_window()
        demo_rate_limit_stats()
        demo_multiple_queues()
        demo_worker_integration()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
