"""
Compare performance with Celery baseline.
Note: This requires Celery to be installed separately.
"""
import time
import threading
import statistics
from typing import Dict, Optional

try:
    from celery import Celery
    from celery.result import AsyncResult
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False
    print("Celery not installed. Install with: pip install celery redis")
    print("Skipping Celery comparison.")


def setup_celery_app():
    """Set up Celery app for benchmarking."""
    if not CELERY_AVAILABLE:
        return None
    
    app = Celery('benchmark')
    app.conf.broker_url = 'redis://localhost:6379/0'
    app.conf.result_backend = 'redis://localhost:6379/0'
    app.conf.task_serializer = 'json'
    app.conf.accept_content = ['json']
    app.conf.result_serializer = 'json'
    app.conf.timezone = 'UTC'
    app.conf.enable_utc = True
    
    @app.task
    def celery_benchmark_task(x):
        """Simple Celery task for benchmarking."""
        return x
    
    return app


def benchmark_celery_throughput(num_tasks: int = 1000, num_workers: int = 1) -> Optional[Dict[str, float]]:
    """
    Benchmark Celery throughput.
    
    Args:
        num_tasks: Number of tasks to process
        num_workers: Number of Celery workers (must be started separately)
        
    Returns:
        Dictionary with benchmark results or None if Celery unavailable
    """
    if not CELERY_AVAILABLE:
        return None
    
    print(f"\n=== Celery Benchmark: {num_workers} Worker(s) ({num_tasks} tasks) ===")
    print("Note: Ensure Celery workers are running: celery -A compare_celery worker")
    
    app = setup_celery_app()
    if not app:
        return None
    
    # Submit tasks
    print(f"Submitting {num_tasks} tasks...")
    submit_start = time.time()
    results = []
    for i in range(num_tasks):
        result = app.send_task('compare_celery.celery_benchmark_task', args=[i])
        results.append(result)
    submit_time = time.time() - submit_start
    submit_rate = num_tasks / submit_time
    
    print(f"Submission: {submit_rate:.2f} tasks/sec ({submit_time:.2f}s)")
    
    # Wait for results
    print("Processing tasks...")
    process_start = time.time()
    completed = 0
    
    for result in results:
        try:
            result.get(timeout=30)  # Wait up to 30 seconds per task
            completed += 1
        except Exception as e:
            print(f"Task failed: {e}")
    
    process_time = time.time() - process_start
    process_rate = completed / process_time if process_time > 0 else 0
    
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


def compare_with_celery():
    """Compare our implementation with Celery."""
    if not CELERY_AVAILABLE:
        print("\nCelery not available. Install with: pip install celery redis")
        print("Then start Celery worker: celery -A compare_celery worker")
        return
    
    print("=" * 80)
    print("Performance Comparison: Custom Job Queue vs Celery")
    print("=" * 80)
    
    # Note: This requires manual setup of Celery workers
    print("\nTo run Celery benchmarks:")
    print("1. Install Celery: pip install celery redis")
    print("2. Start Celery worker: celery -A compare_celery worker")
    print("3. Run this script again")
    
    # Run Celery benchmark
    celery_results = benchmark_celery_throughput(num_tasks=1000, num_workers=1)
    
    if celery_results:
        print("\n" + "=" * 80)
        print("Comparison Results")
        print("=" * 80)
        print(f"Celery (1 worker): {celery_results['process_rate']:.2f} tasks/sec")
        print("\nRun our benchmarks to compare:")
        print("  python benchmarks/benchmark_throughput.py")


if __name__ == "__main__":
    compare_with_celery()
