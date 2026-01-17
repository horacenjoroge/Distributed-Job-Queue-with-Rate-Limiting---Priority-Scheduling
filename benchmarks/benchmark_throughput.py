"""
Benchmark throughput (tasks per second).
"""
import time
import threading
import statistics
from typing import List, Dict
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


def register_benchmark_task():
    """Register a simple task for benchmarking."""
    @task_registry.register("benchmark_task")
    def benchmark_task(x):
        """Simple task that returns input."""
        return x
    
    return "benchmark_task"


def benchmark_single_worker(num_tasks: int = 1000) -> Dict[str, float]:
    """
    Benchmark tasks per second with a single worker.
    
    Args:
        num_tasks: Number of tasks to process
        
    Returns:
        Dictionary with benchmark results
    """
    print(f"\n=== Benchmark: Single Worker ({num_tasks} tasks) ===")
    
    # Register task
    register_benchmark_task()
    
    queue = JobQueue(name="benchmark_queue")
    
    # Submit tasks
    print(f"Submitting {num_tasks} tasks...")
    submit_start = time.time()
    tasks = []
    for i in range(num_tasks):
        task = queue.submit_task("benchmark_task", args=[i])
        tasks.append(task)
    submit_time = time.time() - submit_start
    submit_rate = num_tasks / submit_time
    
    print(f"Submission: {submit_rate:.2f} tasks/sec ({submit_time:.2f}s)")
    
    # Start worker
    worker = SimpleWorker(queue_name="benchmark_queue")
    worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
    worker_thread.start()
    
    # Wait for processing
    print("Processing tasks...")
    process_start = time.time()
    
    # Monitor completion
    completed = 0
    check_interval = 0.5
    max_wait = 300  # 5 minutes max
    
    while completed < num_tasks and (time.time() - process_start) < max_wait:
        completed = 0
        for task in tasks:
            updated = queue.get_task(task.id)
            if updated and updated.status.value == "success":
                completed += 1
        
        if completed % 100 == 0 and completed > 0:
            elapsed = time.time() - process_start
            rate = completed / elapsed
            print(f"  Completed: {completed}/{num_tasks} ({rate:.2f} tasks/sec)")
        
        time.sleep(check_interval)
    
    process_time = time.time() - process_start
    process_rate = completed / process_time if process_time > 0 else 0
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2)
    
    print(f"Processing: {process_rate:.2f} tasks/sec ({process_time:.2f}s)")
    print(f"Completed: {completed}/{num_tasks} tasks")
    
    return {
        "num_tasks": num_tasks,
        "submit_rate": submit_rate,
        "process_rate": process_rate,
        "submit_time": submit_time,
        "process_time": process_time,
        "completed": completed
    }


def benchmark_multiple_workers(num_tasks: int = 1000, num_workers: int = 10) -> Dict[str, float]:
    """
    Benchmark tasks per second with multiple workers.
    
    Args:
        num_tasks: Number of tasks to process
        num_workers: Number of workers
        
    Returns:
        Dictionary with benchmark results
    """
    print(f"\n=== Benchmark: {num_workers} Workers ({num_tasks} tasks) ===")
    
    # Register task
    register_benchmark_task()
    
    queue = JobQueue(name="benchmark_queue")
    
    # Submit tasks
    print(f"Submitting {num_tasks} tasks...")
    submit_start = time.time()
    tasks = []
    for i in range(num_tasks):
        task = queue.submit_task("benchmark_task", args=[i])
        tasks.append(task)
    submit_time = time.time() - submit_start
    submit_rate = num_tasks / submit_time
    
    print(f"Submission: {submit_rate:.2f} tasks/sec ({submit_time:.2f}s)")
    
    # Start multiple workers
    workers = []
    worker_threads = []
    print(f"Starting {num_workers} workers...")
    
    for i in range(num_workers):
        worker = SimpleWorker(queue_name="benchmark_queue")
        workers.append(worker)
        thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_threads.append(thread)
        thread.start()
    
    # Wait for processing
    print("Processing tasks...")
    process_start = time.time()
    
    # Monitor completion
    completed = 0
    check_interval = 0.5
    max_wait = 300  # 5 minutes max
    
    while completed < num_tasks and (time.time() - process_start) < max_wait:
        completed = 0
        for task in tasks:
            updated = queue.get_task(task.id)
            if updated and updated.status.value == "success":
                completed += 1
        
        if completed % 100 == 0 and completed > 0:
            elapsed = time.time() - process_start
            rate = completed / elapsed
            print(f"  Completed: {completed}/{num_tasks} ({rate:.2f} tasks/sec)")
        
        time.sleep(check_interval)
    
    process_time = time.time() - process_start
    process_rate = completed / process_time if process_time > 0 else 0
    
    # Stop workers
    for worker in workers:
        worker.stop()
    for thread in worker_threads:
        thread.join(timeout=2)
    
    print(f"Processing: {process_rate:.2f} tasks/sec ({process_time:.2f}s)")
    print(f"Completed: {completed}/{num_tasks} tasks")
    
    return {
        "num_tasks": num_tasks,
        "num_workers": num_workers,
        "submit_rate": submit_rate,
        "process_rate": process_rate,
        "submit_time": submit_time,
        "process_time": process_time,
        "completed": completed
    }


if __name__ == "__main__":
    print("=" * 60)
    print("Job Queue Throughput Benchmark")
    print("=" * 60)
    
    # Single worker benchmark
    single_results = benchmark_single_worker(num_tasks=1000)
    
    # Multiple workers benchmark
    multi_results = benchmark_multiple_workers(num_tasks=1000, num_workers=10)
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Single Worker: {single_results['process_rate']:.2f} tasks/sec")
    print(f"10 Workers:    {multi_results['process_rate']:.2f} tasks/sec")
    print(f"Speedup:       {multi_results['process_rate'] / single_results['process_rate']:.2f}x")
