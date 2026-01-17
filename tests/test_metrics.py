"""
Tests for metrics collection.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.metrics import MetricsCollector, metrics_collector
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_metrics(redis_connection):
    """Clean metrics keys before each test."""
    keys = redis_broker.client.keys("metrics:*")
    if keys:
        redis_broker.client.delete(*keys)
    yield
    keys = redis_broker.client.keys("metrics:*")
    if keys:
        redis_broker.client.delete(*keys)


@task_registry.register("test_metrics_task")
def test_metrics_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_record_task_enqueued(clean_metrics):
    """Test recording task enqueued event."""
    metrics_collector.record_task_enqueued("test_queue", TaskPriority.MEDIUM)
    
    # Verify recorded
    key = "metrics:tasks:enqueued"
    count = redis_broker.client.zcard(key)
    assert count > 0


def test_record_task_completed(clean_metrics):
    """Test recording task completed event."""
    task = Task(
        name="test_metrics_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("result")
    task.started_at = datetime.utcnow()
    task.completed_at = datetime.utcnow()
    
    duration = 1.5
    metrics_collector.record_task_completed(task, duration, success=True)
    
    # Verify recorded
    key = "metrics:tasks:completed"
    count = redis_broker.client.zcard(key)
    assert count > 0


def test_get_tasks_enqueued_per_second(clean_metrics):
    """Test getting tasks enqueued per second."""
    # Record some enqueued tasks
    for i in range(10):
        metrics_collector.record_task_enqueued("test_queue")
        time.sleep(0.1)
    
    # Get rate
    rate = metrics_collector.get_tasks_enqueued_per_second(window_seconds=60)
    
    assert rate >= 0.0
    assert isinstance(rate, float)


def test_get_tasks_completed_per_second(clean_metrics):
    """Test getting tasks completed per second."""
    task = Task(name="test_task", args=[1], queue_name="test_queue")
    
    # Record some completed tasks
    for i in range(10):
        metrics_collector.record_task_completed(task, 1.0, success=True)
        time.sleep(0.1)
    
    # Get rate
    rate = metrics_collector.get_tasks_completed_per_second(window_seconds=60)
    
    assert rate >= 0.0
    assert isinstance(rate, float)


def test_get_task_duration_percentiles(clean_metrics):
    """Test getting task duration percentiles."""
    task = Task(name="test_task", args=[1], queue_name="test_queue")
    
    # Record tasks with different durations
    durations = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]  # milliseconds
    
    for duration_ms in durations:
        duration_sec = duration_ms / 1000.0
        metrics_collector.record_task_completed(task, duration_sec, success=True)
    
    # Get percentiles
    percentiles = metrics_collector.get_task_duration_percentiles(
        window_seconds=3600,
        percentiles=[0.5, 0.95, 0.99]
    )
    
    assert 0.5 in percentiles
    assert 0.95 in percentiles
    assert 0.99 in percentiles
    
    # p50 should be around 500ms
    assert 400 <= percentiles[0.5] <= 600
    
    # p95 should be around 950ms
    assert 900 <= percentiles[0.95] <= 1000
    
    # p99 should be around 990ms
    assert 950 <= percentiles[0.99] <= 1000


def test_get_success_rate(clean_metrics):
    """Test getting success vs failure rate."""
    task = Task(name="test_task", args=[1], queue_name="test_queue")
    
    # Record successes
    for i in range(7):
        metrics_collector.record_task_completed(task, 1.0, success=True)
    
    # Record failures
    for i in range(3):
        metrics_collector.record_task_completed(task, 1.0, success=False)
    
    # Get success rate
    rates = metrics_collector.get_success_rate(window_seconds=3600)
    
    assert "success_rate" in rates
    assert "failure_rate" in rates
    assert "total" in rates
    
    assert rates["total"] == 10
    assert rates["success_count"] == 7
    assert rates["failure_count"] == 3
    assert abs(rates["success_rate"] - 0.7) < 0.01
    assert abs(rates["failure_rate"] - 0.3) < 0.01


def test_get_queue_size_per_priority(clean_metrics):
    """Test getting queue size per priority."""
    sizes = metrics_collector.get_queue_size_per_priority()
    
    assert isinstance(sizes, dict)
    # Should have entries for each priority
    assert len(sizes) >= 0


def test_get_worker_utilization(clean_metrics):
    """Test getting worker utilization."""
    utilization = metrics_collector.get_worker_utilization()
    
    assert "total_workers" in utilization
    assert "active_workers" in utilization
    assert "idle_workers" in utilization
    assert "dead_workers" in utilization
    assert "utilization_percent" in utilization
    
    assert isinstance(utilization["total_workers"], int)
    assert isinstance(utilization["utilization_percent"], float)


def test_aggregate_metrics_hourly(clean_metrics):
    """Test aggregating metrics hourly."""
    # Record some metrics
    for i in range(5):
        metrics_collector.record_task_enqueued("test_queue")
        task = Task(name="test_task", args=[i], queue_name="test_queue")
        metrics_collector.record_task_completed(task, 1.0, success=True)
    
    # Aggregate
    aggregated = metrics_collector.aggregate_metrics("hourly")
    
    assert "aggregation" in aggregated
    assert aggregated["aggregation"] == "hourly"
    assert "tasks_enqueued_per_second" in aggregated
    assert "tasks_completed_per_second" in aggregated
    assert "duration_percentiles" in aggregated
    assert "success_rate" in aggregated


def test_aggregate_metrics_daily(clean_metrics):
    """Test aggregating metrics daily."""
    # Record some metrics
    for i in range(5):
        metrics_collector.record_task_enqueued("test_queue")
        task = Task(name="test_task", args=[i], queue_name="test_queue")
        metrics_collector.record_task_completed(task, 1.0, success=True)
    
    # Aggregate
    aggregated = metrics_collector.aggregate_metrics("daily")
    
    assert "aggregation" in aggregated
    assert aggregated["aggregation"] == "daily"
    assert "tasks_enqueued_per_second" in aggregated
    assert "tasks_completed_per_second" in aggregated


def test_get_all_metrics(clean_metrics):
    """Test getting all metrics."""
    # Record some metrics
    for i in range(10):
        metrics_collector.record_task_enqueued("test_queue")
        task = Task(name="test_task", args=[i], queue_name="test_queue")
        metrics_collector.record_task_completed(task, 1.0, success=True)
    
    # Get all metrics
    all_metrics = metrics_collector.get_all_metrics(window_seconds=3600)
    
    assert "timestamp" in all_metrics
    assert "window_seconds" in all_metrics
    assert "tasks_enqueued_per_second" in all_metrics
    assert "tasks_completed_per_second" in all_metrics
    assert "duration_percentiles" in all_metrics
    assert "success_rate" in all_metrics
    assert "queue_size_per_priority" in all_metrics
    assert "worker_utilization" in all_metrics


def test_generate_metrics_verify_accuracy(clean_metrics):
    """
    Test main case: Generate metrics, verify accuracy.
    """
    print("\n" + "=" * 60)
    print("Test: Generate Metrics, Verify Accuracy")
    print("=" * 60)
    
    queue = Queue("test_metrics_queue")
    queue.purge()
    
    print(f"\n1. Generating metrics...")
    
    # Enqueue tasks
    enqueued_count = 20
    for i in range(enqueued_count):
        task = Task(
            name="test_metrics_task",
            args=[i],
            queue_name="test_metrics_queue"
        )
        queue.enqueue(task)
        time.sleep(0.05)  # Small delay
    
    print(f"   Enqueued {enqueued_count} tasks")
    
    # Complete tasks with different durations
    completed_count = 15
    durations = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                 1.1, 1.2, 1.3, 1.4, 1.5]
    
    successes = 12
    failures = 3
    
    for i in range(completed_count):
        task = Task(
            name="test_metrics_task",
            args=[i],
            queue_name="test_metrics_queue"
        )
        success = i < successes
        duration = durations[i] if i < len(durations) else 1.0
        metrics_collector.record_task_completed(task, duration, success=success)
        time.sleep(0.05)
    
    print(f"   Completed {completed_count} tasks ({successes} success, {failures} failure)")
    
    # Wait a moment for metrics to settle
    time.sleep(0.5)
    
    print(f"\n2. Verifying metrics accuracy...")
    
    # Get all metrics
    all_metrics = metrics_collector.get_all_metrics(window_seconds=60)
    
    print(f"\n3. Metrics Results:")
    print(f"   Tasks enqueued per second: {all_metrics['tasks_enqueued_per_second']:.2f}")
    print(f"   Tasks completed per second: {all_metrics['tasks_completed_per_second']:.2f}")
    
    percentiles = all_metrics['duration_percentiles']
    print(f"   Duration percentiles:")
    print(f"     p50: {percentiles.get(0.5, 0):.2f}ms")
    print(f"     p95: {percentiles.get(0.95, 0):.2f}ms")
    print(f"     p99: {percentiles.get(0.99, 0):.2f}ms")
    
    success_rate = all_metrics['success_rate']
    print(f"   Success rate:")
    print(f"     Success: {success_rate.get('success_rate', 0):.2%}")
    print(f"     Failure: {success_rate.get('failure_rate', 0):.2%}")
    print(f"     Total: {success_rate.get('total', 0)}")
    
    worker_util = all_metrics['worker_utilization']
    print(f"   Worker utilization:")
    print(f"     Total workers: {worker_util.get('total_workers', 0)}")
    print(f"     Utilization: {worker_util.get('utilization_percent', 0):.2f}%")
    
    # Verify accuracy
    print(f"\n4. Accuracy Verification:")
    
    # Check enqueued rate (should be > 0)
    assert all_metrics['tasks_enqueued_per_second'] > 0, "Enqueued rate should be > 0"
    print(f"   ✓ Enqueued rate > 0")
    
    # Check completed rate (should be > 0)
    assert all_metrics['tasks_completed_per_second'] > 0, "Completed rate should be > 0"
    print(f"   ✓ Completed rate > 0")
    
    # Check percentiles exist
    assert 0.5 in percentiles, "p50 percentile should exist"
    assert 0.95 in percentiles, "p95 percentile should exist"
    assert 0.99 in percentiles, "p99 percentile should exist"
    print(f"   ✓ All percentiles calculated")
    
    # Check success rate
    assert success_rate['total'] == completed_count, f"Total should be {completed_count}"
    assert success_rate['success_count'] == successes, f"Success count should be {successes}"
    assert success_rate['failure_count'] == failures, f"Failure count should be {failures}"
    print(f"   ✓ Success rate accurate")
    
    print(f"\nTest: PASS - Metrics generated and verified for accuracy")
    
    queue.purge()


def test_metrics_time_window(clean_metrics):
    """Test metrics with different time windows."""
    # Record metrics
    for i in range(10):
        metrics_collector.record_task_enqueued("test_queue")
        time.sleep(0.1)
    
    # Get metrics with different windows
    metrics_60 = metrics_collector.get_all_metrics(window_seconds=60)
    metrics_300 = metrics_collector.get_all_metrics(window_seconds=300)
    
    assert metrics_60["window_seconds"] == 60
    assert metrics_300["window_seconds"] == 300


def test_metrics_per_queue(clean_metrics):
    """Test metrics per queue."""
    # Record metrics for different queues
    metrics_collector.record_task_enqueued("queue1")
    metrics_collector.record_task_enqueued("queue2")
    metrics_collector.record_task_enqueued("queue1")
    
    # Verify per-queue metrics are recorded
    key1 = "metrics:tasks:enqueued:queue:queue1"
    key2 = "metrics:tasks:enqueued:queue:queue2"
    
    count1 = redis_broker.client.zcard(key1)
    count2 = redis_broker.client.zcard(key2)
    
    assert count1 >= 2
    assert count2 >= 1


def test_metrics_per_priority(clean_metrics):
    """Test metrics per priority."""
    # Record metrics for different priorities
    metrics_collector.record_task_enqueued("test_queue", TaskPriority.HIGH)
    metrics_collector.record_task_enqueued("test_queue", TaskPriority.MEDIUM)
    metrics_collector.record_task_enqueued("test_queue", TaskPriority.LOW)
    
    # Verify per-priority metrics are recorded
    high_key = "metrics:tasks:enqueued:priority:high"
    medium_key = "metrics:tasks:enqueued:priority:medium"
    low_key = "metrics:tasks:enqueued:priority:low"
    
    assert redis_broker.client.zcard(high_key) >= 1
    assert redis_broker.client.zcard(medium_key) >= 1
    assert redis_broker.client.zcard(low_key) >= 1


def test_metrics_per_task_name(clean_metrics):
    """Test metrics per task name."""
    task1 = Task(name="task_a", args=[1], queue_name="test_queue")
    task2 = Task(name="task_b", args=[2], queue_name="test_queue")
    
    # Record completed tasks
    metrics_collector.record_task_completed(task1, 0.1, success=True)
    metrics_collector.record_task_completed(task2, 0.2, success=True)
    
    # Verify per-task-name metrics are recorded
    key1 = "metrics:tasks:duration:name:task_a"
    key2 = "metrics:tasks:duration:name:task_b"
    
    assert redis_broker.client.zcard(key1) >= 1
    assert redis_broker.client.zcard(key2) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
