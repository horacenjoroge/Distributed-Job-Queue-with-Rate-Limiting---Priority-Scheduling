#!/usr/bin/env python
"""
Simulate task creation and processing.
Creates multiple tasks with different priorities and monitors their status.
"""
import requests
import time
import json
from typing import List, Dict

API_BASE_URL = "http://localhost:8000"

def check_server():
    """Check if API server is running."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        if response.status_code == 200:
            data = response.json()
            print("✓ API Server is running")
            print(f"  - Redis: {'✓' if data['redis_connected'] else '✗'}")
            print(f"  - PostgreSQL: {'✓' if data['postgres_connected'] else '✗'}")
            return True
    except requests.exceptions.ConnectionError:
        print("✗ API Server is not running!")
        print("\nPlease start the API server first:")
        print("  source venv/bin/activate")
        print("  python -m jobqueue.api.main")
        return False
    except Exception as e:
        print(f"✗ Error checking server: {e}")
        return False

def create_tasks(num_tasks: int = 10) -> List[str]:
    """Create multiple tasks with different priorities."""
    print(f"\n{'='*60}")
    print(f"Creating {num_tasks} tasks...")
    print(f"{'='*60}\n")
    
    task_ids = []
    priorities = ['high', 'medium', 'low']
    
    for i in range(num_tasks):
        priority = priorities[i % 3]
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/tasks",
                json={
                    "task_name": f"simulated_task_{i}",
                    "args": [i, f"test_arg_{i}"],
                    "kwargs": {"priority": priority, "index": i},
                    "priority": priority,
                    "queue_name": "default"
                },
                timeout=5
            )
            
            if response.status_code == 201:
                task = response.json()
                task_ids.append(task['id'])
                print(f"✓ Task {i+1:2d}: {task['id'][:8]}... (priority: {priority:6s}, status: {task['status']})")
            elif response.status_code == 429:
                print(f"⚠ Task {i+1:2d}: Rate limited, waiting...")
                time.sleep(2)
                continue
            else:
                print(f"✗ Task {i+1:2d}: Failed - {response.status_code}: {response.text[:50]}")
            
            # Small delay to avoid rate limits
            time.sleep(0.2)
            
        except Exception as e:
            print(f"✗ Task {i+1:2d}: Error - {str(e)[:50]}")
    
    print(f"\n✓ Created {len(task_ids)}/{num_tasks} tasks")
    return task_ids

def check_workers():
    """Check worker status."""
    print(f"\n{'='*60}")
    print("Worker Status")
    print(f"{'='*60}\n")
    
    try:
        response = requests.get(f"{API_BASE_URL}/workers", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"Total workers: {data['total']}")
            print(f"Alive workers: {data['alive']}")
            print(f"Dead workers: {data['dead']}")
            
            if data['workers']:
                print("\nWorker details:")
                for worker in data['workers'][:5]:
                    print(f"  - {worker['worker_id'][:30]}...")
                    print(f"    Status: {worker.get('status', 'unknown')}")
                    print(f"    Queue: {worker.get('queue_name', 'unknown')}")
            else:
                print("\n⚠ No workers running!")
                print("  Start a worker with:")
                print("    python -m jobqueue.worker.main")
    except Exception as e:
        print(f"✗ Error checking workers: {e}")

def monitor_tasks(task_ids: List[str], duration: int = 30):
    """Monitor task status for a period of time."""
    print(f"\n{'='*60}")
    print(f"Monitoring {len(task_ids)} tasks for {duration} seconds...")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    check_interval = 3
    
    while time.time() - start_time < duration:
        status_counts = {}
        
        # Check status of all tasks
        for task_id in task_ids[:10]:  # Limit to first 10 for display
            try:
                response = requests.get(f"{API_BASE_URL}/tasks/{task_id}", timeout=2)
                if response.status_code == 200:
                    task = response.json()
                    status = task['status']
                    status_counts[status] = status_counts.get(status, 0) + 1
            except:
                pass
        
        # Display status
        if status_counts:
            status_str = ", ".join([f"{status}: {count}" for status, count in status_counts.items()])
            elapsed = int(time.time() - start_time)
            print(f"[{elapsed:3d}s] {status_str}")
        
        time.sleep(check_interval)
    
    print("\n✓ Monitoring complete")

def get_system_stats():
    """Get overall system statistics."""
    print(f"\n{'='*60}")
    print("System Statistics")
    print(f"{'='*60}\n")
    
    # Get tasks by status
    print("Tasks by status:")
    for status in ['pending', 'queued', 'running', 'success', 'failed']:
        try:
            response = requests.get(
                f"{API_BASE_URL}/tasks",
                params={"status": status, "limit": 1},
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                print(f"  {status:10s}: {data['total']:5d} tasks")
        except:
            pass
    
    # Get metrics
    print("\nMetrics:")
    try:
        response = requests.get(f"{API_BASE_URL}/metrics", timeout=5)
        if response.status_code == 200:
            metrics = response.json()
            queue_info = metrics.get('queue_info', {})
            print(f"  Pending tasks: {queue_info.get('pending_tasks', 0)}")
            print(f"  Running tasks: {queue_info.get('running_tasks', 0)}")
            success_rate = metrics.get('success_rate', {})
            if success_rate.get('total', 0) > 0:
                print(f"  Success rate: {success_rate.get('success_rate', 0) * 100:.1f}%")
                print(f"  Total completed: {success_rate.get('total', 0)}")
    except Exception as e:
        print(f"  Error getting metrics: {e}")

def main():
    """Main function."""
    print("\n" + "="*60)
    print("Task Simulation Script")
    print("="*60 + "\n")
    
    # Check if server is running
    if not check_server():
        return
    
    # Create tasks
    task_ids = create_tasks(num_tasks=15)
    
    if not task_ids:
        print("\n⚠ No tasks were created. Check API server logs.")
        return
    
    # Check workers
    check_workers()
    
    # Get initial stats
    get_system_stats()
    
    # Monitor tasks
    monitor_tasks(task_ids, duration=20)
    
    # Final stats
    get_system_stats()
    
    print(f"\n{'='*60}")
    print("Simulation Complete!")
    print(f"{'='*60}\n")
    print(f"Created {len(task_ids)} tasks")
    print(f"Task IDs (first 5): {task_ids[:5]}")
    print("\nCheck the dashboard at http://localhost:5173 to see real-time updates!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Simulation interrupted by user")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
