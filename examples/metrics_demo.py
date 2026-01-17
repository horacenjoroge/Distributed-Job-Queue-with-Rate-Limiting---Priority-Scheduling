"""
Metrics collection demonstration.

Shows how to track and view performance metrics.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.metrics import metrics_collector
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_metrics_task")
def demo_metrics_task(value):
    """Demo task function."""
    time.sleep(0.1)  # Simulate work
    return f"Processed: {value}"


def demo_record_metrics():
    """
    Demo 1: Recording metrics
    """
    print("\n" + "=" * 60)
    print("Demo 1: Recording Metrics")
    print("=" * 60)
    
    redis_broker.connect()
    
    print(f"\nRecording task enqueued events...")
    for i in range(5):
        metrics_collector.record_task_enqueued("demo_queue", TaskPriority.MEDIUM)
        time.sleep(0.1)
    print(f"  Recorded 5 enqueued events")
    
    print(f"\nRecording task completed events...")
    task = Task(name="demo_metrics_task", args=[1], queue_name="demo_queue")
    for i in range(5):
        metrics_collector.record_task_completed(task, 0.1 * (i + 1), success=True)
        time.sleep(0.1)
    print(f"  Recorded 5 completed events")
    
    redis_broker.disconnect()


def demo_tasks_per_second():
    """
    Demo 2: Tasks per second metrics
    """
    print("\n" + "=" * 60)
    print("Demo 2: Tasks Per Second Metrics")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Record some tasks
    print(f"\nRecording tasks over 5 seconds...")
    for i in range(20):
        metrics_collector.record_task_enqueued("demo_queue")
        metrics_collector.record_task_completed(
            Task(name="demo_task", args=[i], queue_name="demo_queue"),
            0.1,
            success=True
        )
        time.sleep(0.25)
    
    # Get rates
    enqueued_rate = metrics_collector.get_tasks_enqueued_per_second(window_seconds=60)
    completed_rate = metrics_collector.get_tasks_completed_per_second(window_seconds=60)
    
    print(f"\nMetrics:")
    print(f"  Tasks enqueued per second: {enqueued_rate:.2f}")
    print(f"  Tasks completed per second: {completed_rate:.2f}")
    
    redis_broker.disconnect()


def demo_duration_percentiles():
    """
    Demo 3: Task duration percentiles
    """
    print("\n" + "=" * 60)
    print("Demo 3: Task Duration Percentiles (p50, p95, p99)")
    print("=" * 60)
    
    redis_broker.connect()
    
    task = Task(name="demo_task", args=[1], queue_name="demo_queue")
    
    # Record tasks with varying durations
    print(f"\nRecording tasks with varying durations...")
    durations = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0]
    
    for duration in durations:
        metrics_collector.record_task_completed(task, duration, success=True)
    
    # Get percentiles
    percentiles = metrics_collector.get_task_duration_percentiles(
        window_seconds=3600,
        percentiles=[0.5, 0.95, 0.99]
    )
    
    print(f"\nDuration Percentiles:")
    print(f"  p50 (median): {percentiles.get(0.5, 0):.2f}ms")
    print(f"  p95: {percentiles.get(0.95, 0):.2f}ms")
    print(f"  p99: {percentiles.get(0.99, 0):.2f}ms")
    
    redis_broker.disconnect()


def demo_success_rate():
    """
    Demo 4: Success vs failure rate
    """
    print("\n" + "=" * 60)
    print("Demo 4: Success vs Failure Rate")
    print("=" * 60)
    
    redis_broker.connect()
    
    task = Task(name="demo_task", args=[1], queue_name="demo_queue")
    
    # Record successes and failures
    print(f"\nRecording task completions...")
    for i in range(10):
        success = i < 7  # 7 successes, 3 failures
        metrics_collector.record_task_completed(task, 0.1, success=success)
    
    # Get success rate
    rates = metrics_collector.get_success_rate(window_seconds=3600)
    
    print(f"\nSuccess Rate:")
    print(f"  Total tasks: {rates['total']}")
    print(f"  Successes: {rates['success_count']}")
    print(f"  Failures: {rates['failure_count']}")
    print(f"  Success rate: {rates['success_rate']:.2%}")
    print(f"  Failure rate: {rates['failure_rate']:.2%}")
    
    redis_broker.disconnect()


def demo_worker_utilization():
    """
    Demo 5: Worker utilization
    """
    print("\n" + "=" * 60)
    print("Demo 5: Worker Utilization")
    print("=" * 60)
    
    redis_broker.connect()
    
    utilization = metrics_collector.get_worker_utilization()
    
    print(f"\nWorker Utilization:")
    print(f"  Total workers: {utilization['total_workers']}")
    print(f"  Active workers: {utilization['active_workers']}")
    print(f"  Idle workers: {utilization['idle_workers']}")
    print(f"  Dead workers: {utilization['dead_workers']}")
    print(f"  Utilization: {utilization['utilization_percent']:.2f}%")
    
    redis_broker.disconnect()


def demo_queue_size_per_priority():
    """
    Demo 6: Queue size per priority
    """
    print("\n" + "=" * 60)
    print("Demo 6: Queue Size Per Priority")
    print("=" * 60)
    
    redis_broker.connect()
    
    sizes = metrics_collector.get_queue_size_per_priority()
    
    print(f"\nQueue Size Per Priority:")
    for priority, size in sizes.items():
        print(f"  {priority}: {size} tasks")
    
    redis_broker.disconnect()


def demo_aggregate_metrics():
    """
    Demo 7: Aggregate metrics
    """
    print("\n" + "=" * 60)
    print("Demo 7: Aggregate Metrics (Hourly, Daily)")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Record some metrics
    for i in range(10):
        metrics_collector.record_task_enqueued("demo_queue")
        task = Task(name="demo_task", args=[i], queue_name="demo_queue")
        metrics_collector.record_task_completed(task, 0.1, success=True)
    
    # Aggregate hourly
    print(f"\n1. Hourly Aggregation:")
    hourly = metrics_collector.aggregate_metrics("hourly")
    print(f"   Aggregation: {hourly['aggregation']}")
    print(f"   Tasks enqueued/sec: {hourly['tasks_enqueued_per_second']:.2f}")
    print(f"   Tasks completed/sec: {hourly['tasks_completed_per_second']:.2f}")
    
    # Aggregate daily
    print(f"\n2. Daily Aggregation:")
    daily = metrics_collector.aggregate_metrics("daily")
    print(f"   Aggregation: {daily['aggregation']}")
    print(f"   Tasks enqueued/sec: {daily['tasks_enqueued_per_second']:.2f}")
    print(f"   Tasks completed/sec: {daily['tasks_completed_per_second']:.2f}")
    
    redis_broker.disconnect()


def demo_generate_metrics_verify_accuracy():
    """
    Demo 8: Generate metrics, verify accuracy (Main test case)
    """
    print("\n" + "=" * 60)
    print("Demo 8: Generate Metrics, Verify Accuracy (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_metrics_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Enqueue tasks and record metrics")
    print(f"  2. Complete tasks with varying durations")
    print(f"  3. Generate metrics")
    print(f"  4. Verify accuracy\n")
    
    # Step 1: Enqueue tasks
    print(f"[Step 1] Enqueuing tasks...")
    enqueued_count = 20
    for i in range(enqueued_count):
        task = Task(
            name="demo_metrics_task",
            args=[i],
            queue_name="demo_metrics_queue"
        )
        queue.enqueue(task)
        time.sleep(0.05)
    
    print(f"  Enqueued: {enqueued_count} tasks")
    
    # Step 2: Complete tasks
    print(f"\n[Step 2] Completing tasks...")
    completed_count = 15
    successes = 12
    failures = 3
    durations = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                 1.1, 1.2, 1.3, 1.4, 1.5]
    
    for i in range(completed_count):
        task = Task(
            name="demo_metrics_task",
            args=[i],
            queue_name="demo_metrics_queue"
        )
        success = i < successes
        duration = durations[i] if i < len(durations) else 1.0
        metrics_collector.record_task_completed(task, duration, success=success)
        time.sleep(0.05)
    
    print(f"  Completed: {completed_count} tasks")
    print(f"  Successes: {successes}")
    print(f"  Failures: {failures}")
    
    # Step 3: Generate metrics
    print(f"\n[Step 3] Generating metrics...")
    time.sleep(0.5)  # Wait for metrics to settle
    
    all_metrics = metrics_collector.get_all_metrics(window_seconds=60)
    
    # Step 4: Verify accuracy
    print(f"\n[Step 4] Verifying accuracy...")
    
    print(f"\nMetrics Results:")
    print(f"  Tasks enqueued/sec: {all_metrics['tasks_enqueued_per_second']:.2f}")
    print(f"  Tasks completed/sec: {all_metrics['tasks_completed_per_second']:.2f}")
    
    percentiles = all_metrics['duration_percentiles']
    print(f"  Duration percentiles:")
    print(f"    p50: {percentiles.get(0.5, 0):.2f}ms")
    print(f"    p95: {percentiles.get(0.95, 0):.2f}ms")
    print(f"    p99: {percentiles.get(0.99, 0):.2f}ms")
    
    success_rate = all_metrics['success_rate']
    print(f"  Success rate:")
    print(f"    Total: {success_rate.get('total', 0)}")
    print(f"    Success: {success_rate.get('success_count', 0)}")
    print(f"    Failure: {success_rate.get('failure_count', 0)}")
    print(f"    Success rate: {success_rate.get('success_rate', 0):.2%}")
    
    # Verify
    assert all_metrics['tasks_enqueued_per_second'] > 0
    assert all_metrics['tasks_completed_per_second'] > 0
    assert success_rate['total'] == completed_count
    assert success_rate['success_count'] == successes
    assert success_rate['failure_count'] == failures
    
    print(f"\nVerification:")
    print(f"  ✓ Enqueued rate > 0")
    print(f"  ✓ Completed rate > 0")
    print(f"  ✓ Success rate accurate")
    print(f"  ✓ Percentiles calculated")
    
    print(f"\nTest: PASS - Metrics generated and verified for accuracy")
    
    queue.purge()
    redis_broker.disconnect()


def demo_all_metrics():
    """
    Demo 9: Get all metrics
    """
    print("\n" + "=" * 60)
    print("Demo 9: Get All Metrics")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Record some metrics
    for i in range(10):
        metrics_collector.record_task_enqueued("demo_queue")
        task = Task(name="demo_task", args=[i], queue_name="demo_queue")
        metrics_collector.record_task_completed(task, 0.1 * (i + 1), success=True)
    
    # Get all metrics
    all_metrics = metrics_collector.get_all_metrics(window_seconds=3600)
    
    print(f"\nAll Metrics:")
    print(f"  Timestamp: {all_metrics.get('timestamp')}")
    print(f"  Window: {all_metrics.get('window_seconds')} seconds")
    print(f"  Tasks enqueued/sec: {all_metrics.get('tasks_enqueued_per_second', 0):.2f}")
    print(f"  Tasks completed/sec: {all_metrics.get('tasks_completed_per_second', 0):.2f}")
    print(f"  Duration percentiles: {all_metrics.get('duration_percentiles', {})}")
    print(f"  Success rate: {all_metrics.get('success_rate', {})}")
    print(f"  Queue size per priority: {all_metrics.get('queue_size_per_priority', {})}")
    print(f"  Worker utilization: {all_metrics.get('worker_utilization', {})}")
    
    redis_broker.disconnect()


def run_all_demos():
    """Run all metrics demonstrations."""
    print("\n" + "=" * 60)
    print("METRICS COLLECTION DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_record_metrics()
        demo_tasks_per_second()
        demo_duration_percentiles()
        demo_success_rate()
        demo_worker_utilization()
        demo_queue_size_per_priority()
        demo_aggregate_metrics()
        demo_generate_metrics_verify_accuracy()  # Main test case
        demo_all_metrics()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
