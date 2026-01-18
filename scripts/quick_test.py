#!/usr/bin/env python
"""
Quick test script - tests basic functionality without hitting rate limits.
"""
import requests
import time

API_BASE_URL = "http://localhost:8000"

def test():
    print("Quick System Test\n" + "="*50)
    
    # Health check
    print("\n1. Health Check...")
    r = requests.get(f"{API_BASE_URL}/health", timeout=5)
    if r.status_code == 200:
        print("   ✓ API is healthy")
        print(f"   - Redis: {r.json()['redis_connected']}")
        print(f"   - PostgreSQL: {r.json()['postgres_connected']}")
    else:
        print(f"   ✗ Health check failed: {r.status_code}")
        return
    
    # Submit a task
    print("\n2. Task Submission...")
    time.sleep(0.5)  # Small delay
    r = requests.post(
        f"{API_BASE_URL}/tasks",
        json={"task_name": "test_task", "priority": "medium"},
        timeout=5
    )
    if r.status_code == 201:
        task = r.json()
        print(f"   ✓ Task submitted: {task['id']}")
        task_id = task['id']
    elif r.status_code == 429:
        print("   ⚠ Rate limited - wait a minute and try again")
        return
    else:
        print(f"   ✗ Task submission failed: {r.status_code} - {r.text}")
        return
    
    # Get task
    print("\n3. Get Task...")
    time.sleep(0.5)
    r = requests.get(f"{API_BASE_URL}/tasks/{task_id}", timeout=5)
    if r.status_code == 200:
        print(f"   ✓ Task retrieved: {r.json()['status']}")
    else:
        print(f"   ✗ Failed: {r.status_code}")
    
    # List tasks
    print("\n4. List Tasks...")
    time.sleep(0.5)
    r = requests.get(f"{API_BASE_URL}/tasks?limit=5", timeout=5)
    if r.status_code == 200:
        data = r.json()
        print(f"   ✓ Listed {len(data['tasks'])} tasks (total: {data['total']})")
    else:
        print(f"   ✗ Failed: {r.status_code}")
    
    # Metrics
    print("\n5. Metrics...")
    time.sleep(0.5)
    r = requests.get(f"{API_BASE_URL}/metrics", timeout=5)
    if r.status_code == 200:
        metrics = r.json()
        print("   ✓ Metrics retrieved")
        queue_info = metrics.get('queue_info', {})
        print(f"   - Pending tasks: {queue_info.get('pending_tasks', 0)}")
        print(f"   - Running tasks: {queue_info.get('running_tasks', 0)}")
    else:
        print(f"   ✗ Failed: {r.status_code}")
    
    # Queues
    print("\n6. Queues...")
    time.sleep(0.5)
    r = requests.get(f"{API_BASE_URL}/queues", timeout=5)
    if r.status_code == 200:
        data = r.json()
        print(f"   ✓ Listed {len(data.get('queues', []))} queues")
    else:
        print(f"   ✗ Failed: {r.status_code}")
    
    # Workers
    print("\n7. Workers...")
    time.sleep(0.5)
    r = requests.get(f"{API_BASE_URL}/workers", timeout=5)
    if r.status_code == 200:
        data = r.json()
        print(f"   ✓ Listed {len(data.get('workers', []))} workers")
    else:
        print(f"   ✗ Failed: {r.status_code}")
    
    print("\n" + "="*50)
    print("Quick test completed!")

if __name__ == "__main__":
    try:
        test()
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to API. Is the server running?")
        print("  Start with: python -m jobqueue.api.main")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
