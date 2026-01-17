"""
Run all benchmarks and generate report.
"""
import json
import time
from datetime import datetime
from benchmark_throughput import benchmark_single_worker, benchmark_multiple_workers
from benchmark_latency import benchmark_enqueue_to_start_latency, benchmark_start_to_complete_latency
from benchmark_memory import benchmark_worker_memory, benchmark_redis_memory


def run_all_benchmarks():
    """Run all benchmarks and collect results."""
    print("=" * 80)
    print("Job Queue Comprehensive Benchmark Suite")
    print("=" * 80)
    print(f"Started at: {datetime.now().isoformat()}\n")
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "throughput": {},
        "latency": {},
        "memory": {}
    }
    
    try:
        # Throughput benchmarks
        print("\n" + "=" * 80)
        print("THROUGHPUT BENCHMARKS")
        print("=" * 80)
        
        single_worker = benchmark_single_worker(num_tasks=1000)
        results["throughput"]["single_worker"] = single_worker
        
        multi_worker = benchmark_multiple_workers(num_tasks=1000, num_workers=10)
        results["throughput"]["multi_worker"] = multi_worker
        
    except Exception as e:
        print(f"Error in throughput benchmarks: {e}")
        results["throughput"]["error"] = str(e)
    
    try:
        # Latency benchmarks
        print("\n" + "=" * 80)
        print("LATENCY BENCHMARKS")
        print("=" * 80)
        
        enqueue_latency = benchmark_enqueue_to_start_latency(num_tasks=100)
        results["latency"]["enqueue_to_start"] = enqueue_latency
        
        complete_latency = benchmark_start_to_complete_latency(num_tasks=100)
        results["latency"]["start_to_complete"] = complete_latency
        
    except Exception as e:
        print(f"Error in latency benchmarks: {e}")
        results["latency"]["error"] = str(e)
    
    try:
        # Memory benchmarks
        print("\n" + "=" * 80)
        print("MEMORY BENCHMARKS")
        print("=" * 80)
        
        worker_memory = benchmark_worker_memory(num_tasks=1000)
        results["memory"]["worker"] = worker_memory
        
        redis_memory = benchmark_redis_memory(num_tasks=10000)
        results["memory"]["redis"] = redis_memory
        
    except Exception as e:
        print(f"Error in memory benchmarks: {e}")
        results["memory"]["error"] = str(e)
    
    # Save results
    results_file = f"benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    
    if "single_worker" in results["throughput"]:
        print(f"\nThroughput (Single Worker): {results['throughput']['single_worker']['process_rate']:.2f} tasks/sec")
    
    if "multi_worker" in results["throughput"]:
        print(f"Throughput (10 Workers):     {results['throughput']['multi_worker']['process_rate']:.2f} tasks/sec")
    
    if "enqueue_to_start" in results["latency"] and results["latency"]["enqueue_to_start"]:
        print(f"\nLatency (Enqueue to Start): {results['latency']['enqueue_to_start']['avg_latency_ms']:.2f} ms (avg)")
    
    if "start_to_complete" in results["latency"] and results["latency"]["start_to_complete"]:
        print(f"Latency (Start to Complete): {results['latency']['start_to_complete']['avg_latency_ms']:.2f} ms (avg)")
    
    if "worker" in results["memory"]:
        print(f"\nWorker Memory Overhead: {results['memory']['worker']['worker_overhead_mb']:.2f} MB")
    
    if "redis" in results["memory"]:
        print(f"Redis Memory per Task: {results['memory']['redis']['memory_per_task_kb']:.2f} KB")
    
    print(f"\nResults saved to: {results_file}")
    print(f"Completed at: {datetime.now().isoformat()}")
    
    return results


if __name__ == "__main__":
    run_all_benchmarks()
