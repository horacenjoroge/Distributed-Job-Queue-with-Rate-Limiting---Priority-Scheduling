# Benchmarking Suite

Performance benchmarking tools for the distributed job queue system.

## Benchmarks

### Throughput Benchmarks
- **Single Worker**: Tasks per second with one worker
- **Multiple Workers**: Tasks per second with 10 workers
- Measures both submission rate and processing rate

### Latency Benchmarks
- **Enqueue to Start**: Time from task submission to execution start
- **Start to Complete**: Time from execution start to completion
- Includes percentiles (P95, P99) for tail latency analysis

### Memory Benchmarks
- **Worker Memory**: Memory usage per worker (baseline, idle, processing)
- **Redis Memory**: Memory usage in Redis per task
- Measures overhead and scalability

## Running Benchmarks

### Run Individual Benchmarks

```bash
# Throughput
python benchmarks/benchmark_throughput.py

# Latency
python benchmarks/benchmark_latency.py

# Memory
python benchmarks/benchmark_memory.py
```

### Run All Benchmarks

```bash
python benchmarks/run_all_benchmarks.py
```

This will:
1. Run all benchmark suites
2. Generate a JSON report with timestamp
3. Display summary statistics

## Benchmark Results

Results are saved as JSON files with timestamps:
- `benchmark_results_YYYYMMDD_HHMMSS.json`

### Example Results Structure

```json
{
  "timestamp": "2024-01-15T10:30:00",
  "throughput": {
    "single_worker": {
      "num_tasks": 1000,
      "process_rate": 150.5,
      "process_time": 6.64
    },
    "multi_worker": {
      "num_tasks": 1000,
      "num_workers": 10,
      "process_rate": 850.2,
      "process_time": 1.18
    }
  },
  "latency": {
    "enqueue_to_start": {
      "avg_latency_ms": 12.5,
      "p95_latency_ms": 25.0,
      "p99_latency_ms": 45.0
    }
  },
  "memory": {
    "worker": {
      "worker_overhead_mb": 45.2
    },
    "redis": {
      "memory_per_task_kb": 2.5
    }
  }
}
```

## Performance Targets

### Throughput
- Single Worker: > 100 tasks/sec
- 10 Workers: > 500 tasks/sec

### Latency
- Enqueue to Start: < 50ms (P95)
- Start to Complete: < 100ms (P95) for simple tasks

### Memory
- Worker Overhead: < 100 MB per worker
- Redis Memory: < 5 KB per task

## Comparison with Celery

To compare with Celery baseline:

1. Install Celery:
```bash
pip install celery redis
```

2. Run Celery benchmarks (create separate script)
3. Compare results side-by-side

## Notes

- Benchmarks use fake Redis (fakeredis) for isolation
- Results may vary based on system resources
- For production benchmarks, use real Redis and PostgreSQL
- Consider running benchmarks multiple times and averaging

## Profiling

For detailed profiling, use:

```bash
# Memory profiling
python -m memory_profiler benchmarks/benchmark_memory.py

# Time profiling
python -m cProfile -o profile.stats benchmarks/benchmark_throughput.py
```
