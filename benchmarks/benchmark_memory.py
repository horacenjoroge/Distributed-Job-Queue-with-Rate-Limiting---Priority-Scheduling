"""
Benchmark memory usage.
"""
import psutil
import os
import time
import threading
from typing import Dict
from jobqueue.core.queue import JobQueue
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry
from jobqueue.broker.redis_broker import redis_broker


def get_process_memory_mb(pid: int = None) -> float:
    """Get memory usage of current process in MB."""
    if pid is None:
        pid = os.getpid()
    
    process = psutil.Process(pid)
    memory_info = process.memory_info()
    return memory_info.rss / (1024 * 1024)  # Convert to MB


def get_redis_memory_mb() -> float:
    """Get Redis memory usage in MB."""
    try:
        if not redis_broker.is_connected():
            redis_broker.connect()
        
        redis_client = redis_broker.get_connection()
        info = redis_client.info('memory')
        used_memory = info.get('used_memory', 0)
        return used_memory / (1024 * 1024)  # Convert to MB
    except Exception as e:
        print(f"Error getting Redis memory: {e}")
        return 0.0


def benchmark_worker_memory(num_tasks: int = 1000) -> Dict[str, float]:
    """
    Measure memory usage per worker.
    
    Args:
        num_tasks: Number of tasks to process
        
    Returns:
        Dictionary with memory statistics
    """
    print(f"\n=== Benchmark: Worker Memory Usage ({num_tasks} tasks) ===")
    
    @task_registry.register("memory_task")
    def memory_task(x):
        return x
    
    queue = JobQueue(name="memory_queue")
    
    # Measure baseline memory
    baseline_memory = get_process_memory_mb()
    print(f"Baseline memory: {baseline_memory:.2f} MB")
    
    # Start worker
    worker = SimpleWorker(queue_name="memory_queue")
    worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
    worker_thread.start()
    
    # Wait for worker to start
    time.sleep(1)
    
    # Measure memory after worker start
    worker_memory = get_process_memory_mb()
    worker_overhead = worker_memory - baseline_memory
    print(f"Worker memory: {worker_memory:.2f} MB (overhead: {worker_overhead:.2f} MB)")
    
    # Submit and process tasks
    print(f"Submitting {num_tasks} tasks...")
    tasks = []
    for i in range(num_tasks):
        task = queue.submit_task("memory_task", args=[i])
        tasks.append(task)
    
    # Wait for processing
    print("Processing tasks...")
    time.sleep(5)
    
    # Measure memory during processing
    processing_memory = get_process_memory_mb()
    processing_overhead = processing_memory - baseline_memory
    print(f"Processing memory: {processing_memory:.2f} MB (overhead: {processing_overhead:.2f} MB)")
    
    # Wait for completion
    max_wait = 60
    start_time = time.time()
    while (time.time() - start_time) < max_wait:
        completed = sum(1 for t in tasks if queue.get_task(t.id) and queue.get_task(t.id).status.value == "success")
        if completed >= num_tasks * 0.9:  # 90% complete
            break
        time.sleep(1)
    
    # Measure memory after processing
    final_memory = get_process_memory_mb()
    final_overhead = final_memory - baseline_memory
    print(f"Final memory: {final_memory:.2f} MB (overhead: {final_overhead:.2f} MB)")
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2)
    
    return {
        "baseline_mb": baseline_memory,
        "worker_mb": worker_memory,
        "worker_overhead_mb": worker_overhead,
        "processing_mb": processing_memory,
        "processing_overhead_mb": processing_overhead,
        "final_mb": final_memory,
        "final_overhead_mb": final_overhead
    }


def benchmark_redis_memory(num_tasks: int = 10000) -> Dict[str, float]:
    """
    Measure Redis memory usage.
    
    Args:
        num_tasks: Number of tasks to store
        
    Returns:
        Dictionary with Redis memory statistics
    """
    print(f"\n=== Benchmark: Redis Memory Usage ({num_tasks} tasks) ===")
    
    # Get baseline Redis memory
    baseline_memory = get_redis_memory_mb()
    print(f"Baseline Redis memory: {baseline_memory:.2f} MB")
    
    queue = JobQueue(name="redis_memory_queue")
    
    # Submit tasks
    print(f"Submitting {num_tasks} tasks...")
    tasks = []
    for i in range(num_tasks):
        task = queue.submit_task("test_task", args=[i])
        tasks.append(task)
    
    # Measure memory after submission
    after_submit_memory = get_redis_memory_mb()
    submit_overhead = after_submit_memory - baseline_memory
    memory_per_task = submit_overhead / num_tasks if num_tasks > 0 else 0
    
    print(f"After submit Redis memory: {after_submit_memory:.2f} MB")
    print(f"Memory overhead: {submit_overhead:.2f} MB")
    print(f"Memory per task: {memory_per_task * 1024:.2f} KB")
    
    # Clean up
    if redis_broker.is_connected():
        redis_client = redis_broker.get_connection()
        redis_client.flushdb()
    
    return {
        "baseline_mb": baseline_memory,
        "after_submit_mb": after_submit_memory,
        "overhead_mb": submit_overhead,
        "memory_per_task_kb": memory_per_task * 1024,
        "num_tasks": num_tasks
    }


if __name__ == "__main__":
    print("=" * 60)
    print("Job Queue Memory Benchmark")
    print("=" * 60)
    
    # Worker memory benchmark
    worker_results = benchmark_worker_memory(num_tasks=1000)
    
    # Redis memory benchmark
    redis_results = benchmark_redis_memory(num_tasks=10000)
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Worker overhead: {worker_results['worker_overhead_mb']:.2f} MB")
    print(f"Redis memory per task: {redis_results['memory_per_task_kb']:.2f} KB")
