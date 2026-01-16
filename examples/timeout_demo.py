"""
Task timeout demonstration.

Shows how tasks are killed when they exceed timeout limits.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_timeout import (
    TimeoutHandler,
    TimeoutManager,
    execute_with_timeout
)
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("slow_task")
def slow_task(duration: float):
    """
    Task that runs for a specified duration.
    
    Args:
        duration: Duration in seconds
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting slow task (duration: {duration}s)")
    
    for i in range(int(duration)):
        time.sleep(1)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Still running... {i+1}/{int(duration)}")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Task completed")
    return f"Completed after {duration}s"


@task_registry.register("quick_task")
def quick_task():
    """Task that completes quickly."""
    time.sleep(0.5)
    return "Quick success"


def demo_task_5s_timeout_runs_10s_killed():
    """
    Demo 1: Task with 5s timeout, runs for 10s → killed
    Main test case from requirements
    """
    print("\n" + "=" * 60)
    print("Demo 1: Task with 5s Timeout, Runs for 10s → Killed (Main Test Case)")
    print("=" * 60)
    
    print("\nCreating task with 5 second timeout...")
    print("Task will attempt to run for 10 seconds")
    print("Expected: Task killed after 5 seconds\n")
    
    # Create task with 5 second timeout
    task = Task(
        name="slow_task",
        args=[10.0],  # Task tries to run for 10 seconds
        timeout=5  # But timeout is 5 seconds
    )
    
    print(f"Task ID: {task.id}")
    print(f"Timeout: {task.timeout} seconds")
    print(f"Task duration: 10 seconds")
    print(f"\nExecuting task...\n")
    
    start_time = time.time()
    
    # Execute with timeout
    result, error = execute_with_timeout(
        slow_task,
        timeout_seconds=5,
        10.0
    )
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"Results:")
    print(f"  Elapsed Time: {elapsed_time:.2f} seconds")
    print(f"  Timeout Limit: 5 seconds")
    print(f"  Result: {result}")
    print(f"  Error: {error}")
    
    if error:
        print(f"  Status: TIMEOUT (as expected)")
        task.mark_timeout()
        print(f"  Task Status: {task.status.value}")
    else:
        print(f"  Status: SUCCESS (unexpected - should have timed out)")
    
    print(f"\nTest: {'PASS' if error and 'timeout' in error.lower() else 'FAIL'}")


def demo_soft_timeout_warning():
    """
    Demo 2: Soft timeout warning (80% threshold)
    """
    print("\n" + "=" * 60)
    print("Demo 2: Soft Timeout Warning")
    print("=" * 60)
    
    print("\nTask with 10 second timeout")
    print("Soft timeout at 80% = 8 seconds")
    print("Hard timeout at 10 seconds\n")
    
    manager = TimeoutManager(
        timeout_seconds=10,
        soft_timeout_ratio=0.8,
        enable_soft_timeout=True
    )
    
    manager.start()
    
    print("Monitoring timeout...\n")
    
    for i in range(12):
        is_timed_out, soft_warning = manager.check()
        elapsed = manager.get_elapsed_time()
        remaining = manager.get_remaining_time()
        
        status = "OK"
        if is_timed_out:
            status = "TIMED OUT"
        elif soft_warning:
            status = "SOFT TIMEOUT WARNING"
        
        print(f"  [{i+1:2d}s] Elapsed: {elapsed:5.2f}s | "
              f"Remaining: {remaining:5.2f}s | Status: {status}")
        
        if is_timed_out:
            break
        
        time.sleep(1)
    
    manager.stop()
    
    print(f"\nSoft timeout warning triggered at ~8 seconds")
    print(f"Hard timeout triggered at ~10 seconds")


def demo_task_completes_before_timeout():
    """
    Demo 3: Task completes before timeout
    """
    print("\n" + "=" * 60)
    print("Demo 3: Task Completes Before Timeout")
    print("=" * 60)
    
    print("\nTask with 10 second timeout")
    print("Task runs for 2 seconds\n")
    
    task = Task(
        name="slow_task",
        args=[2.0],  # Runs for 2 seconds
        timeout=10  # Timeout at 10 seconds
    )
    
    print(f"Executing task...\n")
    
    start_time = time.time()
    
    result, error = execute_with_timeout(
        slow_task,
        timeout_seconds=10,
        2.0
    )
    
    elapsed_time = time.time() - start_time
    
    print(f"\nResults:")
    print(f"  Elapsed Time: {elapsed_time:.2f} seconds")
    print(f"  Timeout Limit: 10 seconds")
    print(f"  Result: {result}")
    print(f"  Error: {error}")
    print(f"  Status: {'SUCCESS' if result else 'FAILED'}")
    
    if result:
        task.mark_success(result)
        print(f"  Task Status: {task.status.value}")


def demo_timeout_handler():
    """
    Demo 4: Timeout Handler Usage
    """
    print("\n" + "=" * 60)
    print("Demo 4: Timeout Handler")
    print("=" * 60)
    
    handler = TimeoutHandler(timeout_seconds=5)
    handler.start()
    
    print("\nTimeout Handler started (5 second timeout)\n")
    
    for i in range(7):
        elapsed = handler.elapsed_time()
        remaining = handler.remaining_time()
        is_timed_out = handler.is_timed_out()
        
        status = "OK" if not is_timed_out else "TIMED OUT"
        
        print(f"  [{i+1}s] Elapsed: {elapsed:.2f}s | "
              f"Remaining: {remaining:.2f}s | Status: {status}")
        
        if is_timed_out:
            break
        
        time.sleep(1)
    
    handler.stop()
    
    print(f"\nTimeout handler stopped")


def demo_timeout_manager_features():
    """
    Demo 5: Timeout Manager Features
    """
    print("\n" + "=" * 60)
    print("Demo 5: Timeout Manager Features")
    print("=" * 60)
    
    manager = TimeoutManager(
        timeout_seconds=8,
        soft_timeout_ratio=0.75,  # 75% = 6 seconds
        enable_soft_timeout=True
    )
    
    manager.start()
    
    print("\nTimeout Manager:")
    print(f"  Hard Timeout: 8 seconds")
    print(f"  Soft Timeout: 6 seconds (75%)")
    print(f"  Monitoring...\n")
    
    check_count = 0
    
    while True:
        is_timed_out, soft_warning = manager.check()
        elapsed = manager.get_elapsed_time()
        remaining = manager.get_remaining_time()
        
        check_count += 1
        
        status_parts = []
        if soft_warning:
            status_parts.append("SOFT WARNING")
        if is_timed_out:
            status_parts.append("HARD TIMEOUT")
        if not status_parts:
            status_parts.append("OK")
        
        status = " | ".join(status_parts)
        
        print(f"  Check {check_count}: Elapsed={elapsed:.1f}s | "
              f"Remaining={remaining:.1f}s | {status}")
        
        if is_timed_out:
            break
        
        time.sleep(1)
        
        if check_count >= 10:
            break
    
    manager.stop()
    
    print(f"\nTimeout manager stopped")


def demo_different_timeout_ratios():
    """
    Demo 6: Different Soft Timeout Ratios
    """
    print("\n" + "=" * 60)
    print("Demo 6: Different Soft Timeout Ratios")
    print("=" * 60)
    
    ratios = [0.5, 0.7, 0.8, 0.9]
    timeout = 10
    
    print(f"\nTesting different soft timeout ratios (hard timeout: {timeout}s)\n")
    
    for ratio in ratios:
        handler = TimeoutHandler(timeout_seconds=timeout)
        handler.start()
        
        soft_threshold = timeout * ratio
        
        print(f"  Ratio: {ratio*100:.0f}% (soft timeout at {soft_threshold:.1f}s)")
        
        # Wait for soft timeout
        time.sleep(soft_threshold + 0.1)
        
        triggered = handler.check_soft_timeout(soft_timeout_ratio=ratio)
        print(f"    Soft timeout triggered: {triggered}")
        
        handler.stop()
        print()


def demo_timeout_disabled():
    """
    Demo 7: Timeout Disabled (timeout=0)
    """
    print("\n" + "=" * 60)
    print("Demo 7: Timeout Disabled (timeout=0)")
    print("=" * 60)
    
    print("\nTask with timeout=0 (disabled)\n")
    
    task = Task(
        name="slow_task",
        args=[3.0],  # Runs for 3 seconds
        timeout=0  # Timeout disabled
    )
    
    print(f"Executing task with timeout disabled...\n")
    
    start_time = time.time()
    
    # Execute without timeout enforcement
    result, error = execute_with_timeout(
        slow_task,
        timeout_seconds=0,  # 0 = disabled
        3.0
    )
    
    elapsed_time = time.time() - start_time
    
    print(f"\nResults:")
    print(f"  Elapsed Time: {elapsed_time:.2f} seconds")
    print(f"  Timeout: Disabled (0)")
    print(f"  Result: {result}")
    print(f"  Error: {error}")
    print(f"  Status: {'SUCCESS' if result else 'FAILED'}")


def run_all_demos():
    """Run all timeout demonstrations."""
    print("\n" + "=" * 60)
    print("TASK TIMEOUT DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_task_5s_timeout_runs_10s_killed()  # Main test case
        demo_soft_timeout_warning()
        demo_task_completes_before_timeout()
        demo_timeout_handler()
        demo_timeout_manager_features()
        demo_different_timeout_ratios()
        demo_timeout_disabled()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
