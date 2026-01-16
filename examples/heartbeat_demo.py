"""
Worker heartbeat and monitoring demonstration.

Shows how workers send heartbeats and how dead workers are detected.
"""
import time
import threading
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.worker_monitor import WorkerMonitor
from jobqueue.core.task import Task
from jobqueue.core.redis_queue import Queue
from jobqueue.utils.logger import log


def demo_worker_sends_heartbeat():
    """
    Demo 1: Worker sends heartbeat every 10 seconds
    """
    print("\n" + "=" * 60)
    print("Demo 1: Worker Sends Heartbeat Every 10 Seconds")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "demo_worker_1"
    worker_heartbeat.remove_worker(worker_id)
    
    print(f"\nWorker ID: {worker_id}")
    print(f"Heartbeat interval: 10 seconds")
    print(f"Heartbeat TTL: 30 seconds")
    print(f"\nSending heartbeats...\n")
    
    for i in range(5):
        status = WorkerStatus.ACTIVE if i % 2 == 0 else WorkerStatus.IDLE
        
        worker_heartbeat.send_heartbeat(
            worker_id=worker_id,
            status=status,
            metadata={
                "hostname": "demo-host",
                "pid": 12345,
                "queue_name": "demo_queue"
            }
        )
        
        heartbeat = worker_heartbeat.get_heartbeat(worker_id)
        elapsed = time.time() - heartbeat if heartbeat else 0
        
        print(f"  [{i+1}] Heartbeat sent | Status: {status.value} | "
              f"Elapsed: {elapsed:.2f}s")
        
        time.sleep(2)
    
    # Get worker info
    info = worker_heartbeat.get_worker_info(worker_id)
    print(f"\nWorker Info:")
    print(f"  Status: {info['status']}")
    print(f"  Is Alive: {info['is_alive']}")
    print(f"  Last Heartbeat: {info['last_heartbeat_ago']:.2f}s ago")
    
    # Cleanup
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_worker_status_tracking():
    """
    Demo 2: Worker status tracking (ACTIVE, IDLE, DEAD)
    """
    print("\n" + "=" * 60)
    print("Demo 2: Worker Status Tracking")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "demo_status_worker"
    worker_heartbeat.remove_worker(worker_id)
    
    print("\nDemonstrating worker status changes:\n")
    
    # Start as IDLE
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    status = worker_heartbeat.get_worker_status(worker_id)
    print(f"  Initial: {status.value}")
    
    # Change to ACTIVE (processing task)
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    status = worker_heartbeat.get_worker_status(worker_id)
    print(f"  Processing task: {status.value}")
    
    # Back to IDLE
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    status = worker_heartbeat.get_worker_status(worker_id)
    print(f"  Task completed: {status.value}")
    
    # Stop sending heartbeats (simulate death)
    print(f"\n  Stopping heartbeats (simulating worker death)...")
    time.sleep(2)
    
    # Wait for stale threshold (using short threshold for demo)
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=3)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    print(f"  Waiting for stale threshold (3 seconds)...")
    time.sleep(3.5)
    
    # Check status
    is_alive = heartbeat_manager.is_worker_alive(worker_id)
    status = heartbeat_manager.get_worker_status(worker_id)
    
    print(f"  After threshold: Is Alive: {is_alive} | Status: {status.value}")
    
    # Cleanup
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_dead_worker_detection():
    """
    Demo 3: Dead worker detection
    """
    print("\n" + "=" * 60)
    print("Demo 3: Dead Worker Detection")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create heartbeat manager with short threshold
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=3)
    
    # Add workers
    worker1 = "alive_worker"
    worker2 = "dead_worker"
    
    print("\nAdding workers...")
    heartbeat_manager.send_heartbeat(worker1, WorkerStatus.IDLE)
    heartbeat_manager.send_heartbeat(worker2, WorkerStatus.IDLE)
    
    print(f"  Worker 1: {worker1} (alive)")
    print(f"  Worker 2: {worker2} (will die)")
    
    # Stop sending heartbeat for worker2
    print(f"\nWorker 2 stops sending heartbeats...")
    print(f"Waiting for stale threshold (3 seconds)...")
    
    time.sleep(3.5)
    
    # Check for stale workers
    stale_workers = heartbeat_manager.get_stale_workers()
    
    print(f"\nDead Workers Detected:")
    print(f"  {len(stale_workers)} worker(s) found")
    for worker_id in stale_workers:
        print(f"    - {worker_id}")
    
    # Cleanup
    worker_heartbeat.remove_worker(worker1)
    worker_heartbeat.remove_worker(worker2)
    redis_broker.disconnect()


def demo_kill_worker_mid_task_verify_requeued():
    """
    Demo 4: Kill worker mid-task, verify task re-queued
    Main test case from requirements
    """
    print("\n" + "=" * 60)
    print("Demo 4: Kill Worker Mid-Task, Verify Task Re-Queued (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "worker_killed_demo"
    queue_name = "demo_recovery_queue"
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    
    print(f"\nScenario:")
    print(f"  1. Worker starts processing a task")
    print(f"  2. Worker is killed mid-execution")
    print(f"  3. Monitor detects dead worker")
    print(f"  4. Task is re-queued for another worker\n")
    
    # Create task
    task = Task(
        name="demo_task",
        args=["test_data"],
        queue_name=queue_name
    )
    
    print(f"Task created: {task.id}")
    
    # Simulate worker processing task
    print(f"\nWorker {worker_id} starts processing task...")
    task.mark_running(worker_id)
    task.started_at = datetime.utcnow()
    
    # Store task as running
    key = f"task:running:{worker_id}:{task.id}"
    task_json = task.to_json()
    redis_broker.client.setex(key, 3600, task_json)
    
    # Send heartbeat (worker is alive)
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    print(f"  Worker heartbeat: ACTIVE")
    print(f"  Task status: RUNNING")
    print(f"  Queue size: {queue.size()}")
    
    # Simulate worker death (stop sending heartbeats)
    print(f"\nWorker {worker_id} is killed (stops sending heartbeats)...")
    
    # Wait for stale threshold
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=3)
    print(f"  Waiting for stale threshold (3 seconds)...")
    time.sleep(3.5)
    
    # Monitor detects dead worker
    print(f"\nMonitor checking for dead workers...")
    stale_workers = heartbeat_manager.get_stale_workers()
    
    if worker_id in stale_workers:
        print(f"  Dead worker detected: {worker_id}")
        
        # Recover tasks
        monitor = WorkerMonitor(stale_threshold=3)
        recovered = monitor._recover_worker_tasks(worker_id)
        
        print(f"\nTask Recovery:")
        print(f"  Tasks recovered: {recovered}")
        print(f"  Queue size: {queue.size()}")
        
        if queue.size() > 0:
            recovered_task = queue.dequeue_nowait()
            print(f"\nRecovered Task:")
            print(f"  Task ID: {recovered_task.id}")
            print(f"  Status: {recovered_task.status.value}")
            print(f"  Worker ID: {recovered_task.worker_id}")
            print(f"  Queue: {recovered_task.queue_name}")
            
            print(f"\nTest: PASS - Task successfully re-queued")
        else:
            print(f"\nTest: FAIL - Task not re-queued")
    else:
        print(f"  Worker not detected as dead")
    
    # Cleanup
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_worker_dashboard():
    """
    Demo 5: Worker dashboard showing live workers
    """
    print("\n" + "=" * 60)
    print("Demo 5: Worker Dashboard")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Add multiple workers
    workers = [
        ("worker_1", WorkerStatus.ACTIVE, "queue1"),
        ("worker_2", WorkerStatus.IDLE, "queue1"),
        ("worker_3", WorkerStatus.ACTIVE, "queue2"),
        ("worker_4", WorkerStatus.IDLE, "queue2"),
    ]
    
    print("\nRegistering workers...\n")
    
    for worker_id, status, queue_name in workers:
        worker_heartbeat.send_heartbeat(
            worker_id=worker_id,
            status=status,
            metadata={
                "hostname": "demo-host",
                "pid": 10000 + int(worker_id.split("_")[1]),
                "queue_name": queue_name
            }
        )
        print(f"  {worker_id}: {status.value} (queue: {queue_name})")
    
    # Get all workers info
    print(f"\nWorker Dashboard:\n")
    print(f"{'Worker ID':<20} {'Status':<10} {'Queue':<15} {'Alive':<10}")
    print("-" * 60)
    
    workers_info = worker_heartbeat.get_all_workers_info()
    
    for info in workers_info:
        queue = info.get("metadata", {}).get("queue_name", "unknown")
        print(f"{info['worker_id']:<20} {info['status']:<10} {queue:<15} {info['is_alive']:<10}")
    
    # Summary
    alive_count = len([w for w in workers_info if w["is_alive"]])
    dead_count = len([w for w in workers_info if not w["is_alive"]])
    
    print(f"\nSummary:")
    print(f"  Total Workers: {len(workers_info)}")
    print(f"  Alive: {alive_count}")
    print(f"  Dead: {dead_count}")
    
    # Cleanup
    for worker_id, _, _ in workers:
        worker_heartbeat.remove_worker(worker_id)
    
    redis_broker.disconnect()


def demo_worker_monitor():
    """
    Demo 6: Worker monitor process
    """
    print("\n" + "=" * 60)
    print("Demo 6: Worker Monitor Process")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create monitor
    monitor = WorkerMonitor(check_interval=2, stale_threshold=3)
    
    # Add workers
    worker1 = "monitor_worker_1"
    worker2 = "monitor_worker_2"
    
    worker_heartbeat.send_heartbeat(worker1, WorkerStatus.IDLE)
    worker_heartbeat.send_heartbeat(worker2, WorkerStatus.IDLE)
    
    print(f"\nMonitor started:")
    print(f"  Check interval: 2 seconds")
    print(f"  Stale threshold: 3 seconds")
    print(f"  Workers: {worker1}, {worker2}")
    
    print(f"\nSimulating worker death...")
    print(f"  {worker2} stops sending heartbeats")
    
    # Wait for stale
    time.sleep(3.5)
    
    # Check for stale workers
    stale = worker_heartbeat.get_stale_workers()
    
    print(f"\nMonitor Results:")
    print(f"  Dead workers detected: {len(stale)}")
    if stale:
        for worker_id in stale:
            print(f"    - {worker_id}")
    
    # Get monitor stats
    stats = monitor.get_stats()
    print(f"\nMonitor Statistics:")
    print(f"  Total workers: {stats['total_workers']}")
    print(f"  Alive: {stats['alive_workers']}")
    print(f"  Dead: {stats['dead_workers']}")
    print(f"  Dead workers detected: {stats['dead_workers_detected']}")
    print(f"  Tasks recovered: {stats['tasks_recovered']}")
    
    # Cleanup
    worker_heartbeat.remove_worker(worker1)
    worker_heartbeat.remove_worker(worker2)
    redis_broker.disconnect()


def demo_heartbeat_ttl():
    """
    Demo 7: Heartbeat TTL expiration
    """
    print("\n" + "=" * 60)
    print("Demo 7: Heartbeat TTL Expiration")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create heartbeat manager with short TTL
    heartbeat_manager = worker_heartbeat.__class__(heartbeat_ttl=3)
    
    worker_id = "ttl_test_worker"
    heartbeat_manager.remove_worker(worker_id)
    
    print(f"\nHeartbeat TTL: 3 seconds")
    print(f"Stale threshold: 30 seconds\n")
    
    # Send heartbeat
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.IDLE)
    print(f"  Heartbeat sent")
    
    # Check immediately
    heartbeat = heartbeat_manager.get_heartbeat(worker_id)
    print(f"  Heartbeat exists: {heartbeat is not None}")
    
    # Wait for TTL
    print(f"\n  Waiting for TTL to expire (3 seconds)...")
    time.sleep(3.5)
    
    # Check again
    heartbeat = heartbeat_manager.get_heartbeat(worker_id)
    print(f"  Heartbeat exists: {heartbeat is not None}")
    print(f"  (TTL expired, key removed from Redis)")
    
    # Cleanup
    heartbeat_manager.remove_worker(worker_id)
    redis_broker.disconnect()


def run_all_demos():
    """Run all heartbeat demonstrations."""
    print("\n" + "=" * 60)
    print("WORKER HEARTBEAT DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_worker_sends_heartbeat()
        demo_worker_status_tracking()
        demo_dead_worker_detection()
        demo_kill_worker_mid_task_verify_requeued()  # Main test case
        demo_worker_dashboard()
        demo_worker_monitor()
        demo_heartbeat_ttl()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
