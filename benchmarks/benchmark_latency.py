"""
Benchmark latency (enqueue to start, start to complete).
"""
import time
import threading
import statistics
from typing import List, Dict
from datetime import datetime
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


def register_benchmark_task():
    """Register a simple task for benchmarking."""
    @task_registry.register("latency_task")
    def latency_task(x):
        """Simple task that returns input."""
        return x
    
    return "latency_task"


def benchmark_enqueue_to_start_latency(num_tasks: int = 100) -> Dict[str, float]:
    """
    Measure latency from enqueue to task start.
    
    Args:
        num_tasks: Number of tasks to measure
        
    Returns:
        Dictionary with latency statistics
    """
    print(f"\n=== Benchmark: Enqueue to Start Latency ({num_tasks} tasks) ===")
    
    register_benchmark_task()
    queue = JobQueue(name="latency_queue")
    
    # Start worker
    worker = SimpleWorker(queue_name="latency_queue")
    worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
    worker_thread.start()
    
    latencies = []
    
    for i in range(num_tasks):
        # Submit task
        enqueue_time = time.time()
        task = queue.submit_task("latency_task", args=[i])
        
        # Wait for task to start
        max_wait = 10
        start_time = None
        wait_start = time.time()
        
        while (time.time() - wait_start) < max_wait:
            updated = queue.get_task(task.id)
            if updated and updated.started_at:
                start_time = updated.started_at
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    start_time = start_time.timestamp()
                elif hasattr(start_time, 'timestamp'):
                    start_time = start_time.timestamp()
                break
            time.sleep(0.01)
        
        if start_time:
            latency = (start_time - enqueue_time) * 1000  # Convert to milliseconds
            latencies.append(latency)
        
        # Small delay between tasks
        time.sleep(0.1)
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2)
    
    if latencies:
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 1 else latencies[0]
        p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) > 1 else latencies[0]
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        print(f"Average:  {avg_latency:.2f} ms")
        print(f"Median:   {median_latency:.2f} ms")
        print(f"P95:      {p95_latency:.2f} ms")
        print(f"P99:      {p99_latency:.2f} ms")
        print(f"Min:      {min_latency:.2f} ms")
        print(f"Max:      {max_latency:.2f} ms")
        
        return {
            "num_tasks": num_tasks,
            "avg_latency_ms": avg_latency,
            "median_latency_ms": median_latency,
            "p95_latency_ms": p95_latency,
            "p99_latency_ms": p99_latency,
            "min_latency_ms": min_latency,
            "max_latency_ms": max_latency
        }
    else:
        print("No latency measurements collected")
        return {}


def benchmark_start_to_complete_latency(num_tasks: int = 100) -> Dict[str, float]:
    """
    Measure latency from task start to completion.
    
    Args:
        num_tasks: Number of tasks to measure
        
    Returns:
        Dictionary with latency statistics
    """
    print(f"\n=== Benchmark: Start to Complete Latency ({num_tasks} tasks) ===")
    
    register_benchmark_task()
    queue = JobQueue(name="latency_queue")
    
    # Start worker
    worker = SimpleWorker(queue_name="latency_queue")
    worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
    worker_thread.start()
    
    latencies = []
    
    for i in range(num_tasks):
        # Submit task
        task = queue.submit_task("latency_task", args=[i])
        
        # Wait for task to complete
        max_wait = 10
        wait_start = time.time()
        start_time = None
        complete_time = None
        
        while (time.time() - wait_start) < max_wait:
            updated = queue.get_task(task.id)
            if updated:
                if updated.started_at and not start_time:
                    start_time = updated.started_at
                    if isinstance(start_time, str):
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        start_time = start_time.timestamp()
                    elif hasattr(start_time, 'timestamp'):
                        start_time = start_time.timestamp()
                
                if updated.completed_at and updated.status.value == "success":
                    complete_time = updated.completed_at
                    if isinstance(complete_time, str):
                        complete_time = datetime.fromisoformat(complete_time.replace('Z', '+00:00'))
                        complete_time = complete_time.timestamp()
                    elif hasattr(complete_time, 'timestamp'):
                        complete_time = complete_time.timestamp()
                    break
            time.sleep(0.01)
        
        if start_time and complete_time:
            latency = (complete_time - start_time) * 1000  # Convert to milliseconds
            latencies.append(latency)
        
        # Small delay between tasks
        time.sleep(0.1)
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2)
    
    if latencies:
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 1 else latencies[0]
        p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) > 1 else latencies[0]
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        print(f"Average:  {avg_latency:.2f} ms")
        print(f"Median:   {median_latency:.2f} ms")
        print(f"P95:      {p95_latency:.2f} ms")
        print(f"P99:      {p99_latency:.2f} ms")
        print(f"Min:      {min_latency:.2f} ms")
        print(f"Max:      {max_latency:.2f} ms")
        
        return {
            "num_tasks": num_tasks,
            "avg_latency_ms": avg_latency,
            "median_latency_ms": median_latency,
            "p95_latency_ms": p95_latency,
            "p99_latency_ms": p99_latency,
            "min_latency_ms": min_latency,
            "max_latency_ms": max_latency
        }
    else:
        print("No latency measurements collected")
        return {}


if __name__ == "__main__":
    print("=" * 60)
    print("Job Queue Latency Benchmark")
    print("=" * 60)
    
    # Enqueue to start latency
    enqueue_results = benchmark_enqueue_to_start_latency(num_tasks=100)
    
    # Start to complete latency
    complete_results = benchmark_start_to_complete_latency(num_tasks=100)
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    if enqueue_results:
        print(f"Enqueue to Start: {enqueue_results['avg_latency_ms']:.2f} ms (avg)")
    if complete_results:
        print(f"Start to Complete: {complete_results['avg_latency_ms']:.2f} ms (avg)")
