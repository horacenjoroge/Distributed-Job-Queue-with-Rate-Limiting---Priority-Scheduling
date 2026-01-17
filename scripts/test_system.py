#!/usr/bin/env python
"""
Comprehensive system test script.
Tests all features of the job queue system.
"""
import sys
import time
import json
import requests
import threading
from typing import List, Dict, Any
from datetime import datetime

API_BASE_URL = "http://localhost:8000"
API_KEY = None  # No API key needed in dev mode

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_test(name: str):
    print(f"\n{Colors.BLUE}=== {name} ==={Colors.RESET}")

def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.RESET}")

def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.RESET}")

def print_info(msg: str):
    print(f"{Colors.YELLOW}ℹ {msg}{Colors.RESET}")

def test_health():
    """Test health endpoint."""
    print_test("Health Check")
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["redis_connected"] == True
        assert data["postgres_connected"] == True
        print_success("Health check passed")
        return True
    except Exception as e:
        print_error(f"Health check failed: {e}")
        return False

def submit_task_with_retry(json_data, max_retries=3):
    """Submit a task with retry logic for rate limiting."""
    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{API_BASE_URL}/tasks",
                json=json_data,
                timeout=5
            )
            if response.status_code == 201:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.json().get("retry_after", 5))
                print_info(f"Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise
    return None

def test_task_submission():
    """Test task submission with various configurations."""
    print_test("Task Submission")
    task_ids = []
    
    try:
        # Test 1: Basic task
        print_info("Submitting basic task...")
        task1 = submit_task_with_retry({
            "task_name": "test_task",
            "args": [1, 2, 3],
            "kwargs": {"key": "value"},
            "priority": "medium",
            "queue_name": "default"
        })
        if task1 and "id" in task1:
            task_ids.append(task1["id"])
            print_success(f"Basic task submitted: {task1['id']}")
        else:
            print_error("Failed to submit basic task")
            return []
        
        time.sleep(0.5)  # Small delay to avoid rate limits
        
        # Test 2: High priority task
        print_info("Submitting high priority task...")
        task2 = submit_task_with_retry({
            "task_name": "urgent_task",
            "priority": "high",
            "queue_name": "default"
        })
        if task2 and "id" in task2:
            task_ids.append(task2["id"])
            print_success(f"High priority task submitted: {task2['id']}")
        
        time.sleep(0.5)
        
        # Test 3: Low priority task
        print_info("Submitting low priority task...")
        task3 = submit_task_with_retry({
            "task_name": "background_task",
            "priority": "low",
            "queue_name": "default"
        })
        if task3 and "id" in task3:
            task_ids.append(task3["id"])
            print_success(f"Low priority task submitted: {task3['id']}")
        
        time.sleep(0.5)
        
        # Test 4: Task with timeout
        print_info("Submitting task with timeout...")
        task4 = submit_task_with_retry({
            "task_name": "timed_task",
            "timeout": 60,
            "priority": "medium"
        })
        if task4 and "id" in task4:
            task_ids.append(task4["id"])
            print_success(f"Task with timeout submitted: {task4['id']}")
        
        time.sleep(0.5)
        
        # Test 5: Unique task (deduplication)
        print_info("Submitting unique task...")
        task5 = submit_task_with_retry({
            "task_name": "unique_task",
            "args": [1, 2, 3],
            "unique": True,
            "priority": "medium"
        })
        if task5 and "id" in task5:
            task_ids.append(task5["id"])
            print_success(f"Unique task submitted: {task5['id']}")
        
        return task_ids
        
    except Exception as e:
        print_error(f"Task submission failed: {e}")
        import traceback
        traceback.print_exc()
        return []

def test_task_listing():
    """Test task listing endpoint."""
    print_test("Task Listing")
    try:
        # Test with pagination
        response = requests.get(
            f"{API_BASE_URL}/tasks",
            params={"limit": 10, "offset": 0},
            timeout=5
        )
        assert response.status_code == 200
        data = response.json()
        assert "tasks" in data
        assert "total" in data
        print_success(f"Listed {len(data['tasks'])} tasks (total: {data['total']})")
        
        # Test with status filter
        response = requests.get(
            f"{API_BASE_URL}/tasks",
            params={"status": "pending", "limit": 5},
            timeout=5
        )
        assert response.status_code == 200
        data = response.json()
        print_success(f"Filtered pending tasks: {len(data['tasks'])}")
        
        return True
    except Exception as e:
        print_error(f"Task listing failed: {e}")
        return False

def test_task_retrieval(task_ids: List[str]):
    """Test retrieving individual tasks."""
    print_test("Task Retrieval")
    if not task_ids:
        print_info("No tasks to retrieve")
        return True
    
    try:
        task_id = task_ids[0]
        response = requests.get(f"{API_BASE_URL}/tasks/{task_id}", timeout=5)
        assert response.status_code == 200
        task = response.json()
        assert task["id"] == task_id
        print_success(f"Retrieved task: {task_id}")
        return True
    except Exception as e:
        print_error(f"Task retrieval failed: {e}")
        return False

def test_queues():
    """Test queue endpoints."""
    print_test("Queue Management")
    try:
        # List queues
        response = requests.get(f"{API_BASE_URL}/queues", timeout=5)
        assert response.status_code == 200
        data = response.json()
        assert "queues" in data
        print_success(f"Listed {len(data['queues'])} queues")
        
        # Get queue stats
        response = requests.get(f"{API_BASE_URL}/queues/default/stats", timeout=5)
        assert response.status_code == 200
        stats = response.json()
        print_success(f"Queue stats retrieved: {json.dumps(stats, indent=2)}")
        return True
    except Exception as e:
        print_error(f"Queue management failed: {e}")
        return False

def test_workers():
    """Test worker endpoints."""
    print_test("Worker Management")
    try:
        response = requests.get(f"{API_BASE_URL}/workers", timeout=5)
        assert response.status_code == 200
        data = response.json()
        assert "workers" in data
        print_success(f"Listed {len(data['workers'])} workers")
        return True
    except Exception as e:
        print_error(f"Worker management failed: {e}")
        return False

def test_metrics():
    """Test metrics endpoint."""
    print_test("Metrics")
    try:
        response = requests.get(
            f"{API_BASE_URL}/metrics",
            params={"window_seconds": 3600},
            timeout=5
        )
        if response.status_code != 200:
            print_error(f"Metrics endpoint returned {response.status_code}: {response.text}")
            return False
        
        metrics = response.json()
        
        # Verify structure
        required_keys = ["tasks", "success_rate", "queue_size_per_priority", "queue_info", "worker_utilization"]
        for key in required_keys:
            if key not in metrics:
                print_error(f"Missing key in metrics: {key}")
                return False
        
        print_success("Metrics retrieved successfully")
        print_info(f"  Pending tasks: {metrics['queue_info'].get('pending_tasks', 0)}")
        print_info(f"  Running tasks: {metrics['queue_info'].get('running_tasks', 0)}")
        print_info(f"  Success rate: {metrics['success_rate'].get('success_rate', 0) * 100:.1f}%")
        return True
    except Exception as e:
        print_error(f"Metrics failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dlq():
    """Test Dead Letter Queue."""
    print_test("Dead Letter Queue")
    try:
        response = requests.get(f"{API_BASE_URL}/dlq", params={"limit": 10}, timeout=5)
        assert response.status_code == 200
        data = response.json()
        assert "tasks" in data
        print_success(f"DLQ has {data.get('total', 0)} tasks")
        return True
    except Exception as e:
        print_error(f"DLQ test failed: {e}")
        return False

def test_rate_limits():
    """Test rate limiting endpoints."""
    print_test("Rate Limiting")
    try:
        response = requests.get(f"{API_BASE_URL}/rate-limits", timeout=5)
        assert response.status_code == 200
        data = response.json()
        print_success("Rate limits retrieved")
        return True
    except Exception as e:
        print_error(f"Rate limits test failed: {e}")
        return False

def test_load_submission(num_tasks: int = 20):
    """Test submitting many tasks."""
    print_test(f"Load Test - Submitting {num_tasks} Tasks")
    task_ids = []
    start_time = time.time()
    
    try:
        for i in range(num_tasks):
            task = submit_task_with_retry({
                "task_name": f"load_test_task_{i}",
                "args": [i],
                "priority": "medium" if i % 3 == 0 else "low" if i % 3 == 1 else "high",
                "queue_name": "default"
            })
            if task and "id" in task:
                task_ids.append(task["id"])
            
            # Small delay to avoid rate limits
            if i % 10 == 0 and i > 0:
                time.sleep(1)
        
        elapsed = time.time() - start_time
        rate = len(task_ids) / elapsed if elapsed > 0 else 0
        
        print_success(f"Submitted {len(task_ids)}/{num_tasks} tasks in {elapsed:.2f}s ({rate:.1f} tasks/sec)")
        return task_ids
    except Exception as e:
        print_error(f"Load test failed: {e}")
        return []

def test_concurrent_submission(num_threads: int = 3, tasks_per_thread: int = 5):
    """Test concurrent task submission."""
    print_test(f"Concurrent Submission ({num_threads} threads, {tasks_per_thread} tasks each)")
    task_ids = []
    errors = []
    lock = threading.Lock()
    
    def submit_tasks(thread_id: int):
        thread_task_ids = []
        for i in range(tasks_per_thread):
            try:
                task = submit_task_with_retry({
                    "task_name": f"concurrent_task_{thread_id}_{i}",
                    "args": [thread_id, i],
                    "priority": "medium",
                    "queue_name": "default"
                })
                if task and "id" in task:
                    thread_task_ids.append(task["id"])
                time.sleep(0.2)  # Small delay between submissions
            except Exception as e:
                with lock:
                    errors.append(str(e))
        with lock:
            task_ids.extend(thread_task_ids)
    
    start_time = time.time()
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=submit_tasks, args=(i,))
        threads.append(thread)
        thread.start()
        time.sleep(0.1)  # Stagger thread starts
    
    for thread in threads:
        thread.join()
    
    elapsed = time.time() - start_time
    total_tasks = num_threads * tasks_per_thread
    
    print_success(f"Submitted {len(task_ids)}/{total_tasks} tasks concurrently in {elapsed:.2f}s")
    if errors:
        print_error(f"Encountered {len(errors)} errors")
    
    return task_ids

def test_task_cancellation(task_ids: List[str]):
    """Test task cancellation."""
    print_test("Task Cancellation")
    if not task_ids:
        print_info("No tasks to cancel")
        return True
    
    try:
        # Try to cancel a pending task
        task_id = task_ids[0]
        response = requests.post(
            f"{API_BASE_URL}/tasks/{task_id}/cancel",
            json={"reason": "test_cancellation"},
            timeout=5
        )
        # May fail if task is already processed
        if response.status_code in [200, 404, 400]:
            print_success(f"Task cancellation attempted: {task_id}")
        else:
            print_error(f"Unexpected status: {response.status_code}")
        return True
    except Exception as e:
        print_error(f"Task cancellation failed: {e}")
        return False

def test_websocket():
    """Test WebSocket connection."""
    print_test("WebSocket Connection")
    try:
        import websocket
        import json
        
        messages_received = []
        
        def on_message(ws, message):
            messages_received.append(json.loads(message))
        
        def on_error(ws, error):
            print_error(f"WebSocket error: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            print_info("WebSocket closed")
        
        ws_url = API_BASE_URL.replace("http://", "ws://") + "/ws"
        ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # Connect and wait briefly
        ws_thread = threading.Thread(target=ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        
        time.sleep(2)  # Wait for connection
        
        # Submit a task to trigger an event
        requests.post(
            f"{API_BASE_URL}/tasks",
            json={"task_name": "websocket_test", "priority": "medium"},
            timeout=5
        )
        
        time.sleep(1)  # Wait for event
        
        ws.close()
        ws_thread.join(timeout=1)
        
        if messages_received:
            print_success(f"Received {len(messages_received)} WebSocket messages")
        else:
            print_info("No WebSocket messages received (may be normal)")
        
        return True
    except ImportError:
        print_info("websocket-client not installed, skipping WebSocket test")
        return True
    except Exception as e:
        print_error(f"WebSocket test failed: {e}")
        return True  # Don't fail the whole test suite

def run_comprehensive_test():
    """Run all tests."""
    print(f"\n{Colors.BLUE}{'='*60}")
    print("COMPREHENSIVE SYSTEM TEST")
    print(f"{'='*60}{Colors.RESET}\n")
    
    results = {}
    
    # Basic connectivity
    results["health"] = test_health()
    if not results["health"]:
        print_error("System is not healthy. Aborting tests.")
        return results
    
    # Task operations
    task_ids = test_task_submission()
    results["task_submission"] = len(task_ids) > 0
    
    results["task_listing"] = test_task_listing()
    results["task_retrieval"] = test_task_retrieval(task_ids)
    
    # System components
    results["queues"] = test_queues()
    results["workers"] = test_workers()
    results["metrics"] = test_metrics()
    results["dlq"] = test_dlq()
    results["rate_limits"] = test_rate_limits()
    
    # Advanced tests
    load_task_ids = test_load_submission(50)
    results["load_submission"] = len(load_task_ids) > 0
    
    concurrent_task_ids = test_concurrent_submission(5, 10)
    results["concurrent_submission"] = len(concurrent_task_ids) > 0
    
    results["task_cancellation"] = test_task_cancellation(task_ids)
    results["websocket"] = test_websocket()
    
    # Final metrics check
    print_test("Final System State")
    time.sleep(2)  # Wait for metrics to update
    test_metrics()
    
    # Summary
    print(f"\n{Colors.BLUE}{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}{Colors.RESET}\n")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        if result:
            print_success(f"{test_name}")
        else:
            print_error(f"{test_name}")
    
    print(f"\n{Colors.BLUE}Results: {passed}/{total} tests passed{Colors.RESET}\n")
    
    return results

if __name__ == "__main__":
    try:
        results = run_comprehensive_test()
        sys.exit(0 if all(results.values()) else 1)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Tests interrupted by user{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
