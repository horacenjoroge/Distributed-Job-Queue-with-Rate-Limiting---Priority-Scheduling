"""
Retry logic with exponential backoff demonstration.

Shows how tasks are automatically retried with increasing delays.
"""
import time
from datetime import datetime
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.retry_backoff import (
    ExponentialBackoff,
    calculate_exponential_backoff,
    get_retry_delays
)
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


# Counter to track task attempts
attempt_counter = {}


@task_registry.register("unstable_api_call")
def unstable_api_call(task_id: str, fail_count: int = 2):
    """
    Simulates an unstable API that fails a few times then succeeds.
    
    Args:
        task_id: Unique task ID
        fail_count: Number of times to fail before succeeding
    """
    if task_id not in attempt_counter:
        attempt_counter[task_id] = 0
    
    attempt_counter[task_id] += 1
    attempt = attempt_counter[task_id]
    
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] API Call - Attempt {attempt}")
    
    if attempt <= fail_count:
        raise Exception(f"API Error: Connection timeout (attempt {attempt})")
    
    return {"status": "success", "attempts": attempt}


@task_registry.register("flaky_database_query")
def flaky_database_query(query_id: str):
    """Simulates a database query that occasionally fails."""
    if query_id not in attempt_counter:
        attempt_counter[query_id] = 0
    
    attempt_counter[query_id] += 1
    attempt = attempt_counter[query_id]
    
    print(f"Database Query - Attempt {attempt}")
    
    # Fail first 3 attempts
    if attempt <= 3:
        raise Exception(f"Database connection lost (attempt {attempt})")
    
    return {"result": "data", "attempts": attempt}


def demo_exponential_backoff_calculation():
    """
    Demo 1: Exponential backoff calculation
    """
    print("\n" + "=" * 60)
    print("Demo 1: Exponential Backoff Calculation")
    print("=" * 60)
    
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=False)
    
    print("\nBackoff delays (base=2, max=300s):\n")
    
    for retry_count in range(10):
        delay = backoff.calculate_delay(retry_count)
        print(f"  Retry {retry_count}: {delay:6.1f}s (2^{retry_count} = {2**retry_count})")
    
    print("\nNote: After retry 8, delays are capped at 300s")


def demo_backoff_with_jitter():
    """
    Demo 2: Backoff with jitter
    """
    print("\n" + "=" * 60)
    print("Demo 2: Backoff with Jitter")
    print("=" * 60)
    
    backoff_no_jitter = ExponentialBackoff(base=2.0, jitter=False)
    backoff_with_jitter = ExponentialBackoff(base=2.0, jitter=True)
    
    print("\nRetry 3 delays (without jitter):")
    for i in range(5):
        delay = backoff_no_jitter.calculate_delay(3)
        print(f"  Attempt {i+1}: {delay:.2f}s")
    
    print("\nRetry 3 delays (with jitter):")
    for i in range(5):
        delay = backoff_with_jitter.calculate_delay(3)
        print(f"  Attempt {i+1}: {delay:.2f}s (varies!)")
    
    print("\nJitter prevents thundering herd problem")
    print("(Multiple tasks won't retry at exact same time)")


def demo_retry_delay_sequence():
    """
    Demo 3: Retry delay sequences
    """
    print("\n" + "=" * 60)
    print("Demo 3: Retry Delay Sequences")
    print("=" * 60)
    
    # Exponential
    exp_delays = get_retry_delays(5, strategy="exponential", base=2.0, jitter=False)
    print("\nExponential (base=2):")
    for i, delay in enumerate(exp_delays):
        print(f"  Retry {i}: {delay:.1f}s")
    
    # Linear
    linear_delays = get_retry_delays(5, strategy="linear", delay=5.0)
    print("\nLinear (5s constant):")
    for i, delay in enumerate(linear_delays):
        print(f"  Retry {i}: {delay:.1f}s")
    
    # Fibonacci
    fib_delays = get_retry_delays(5, strategy="fibonacci", base=1.0)
    print("\nFibonacci (base=1.0):")
    for i, delay in enumerate(fib_delays):
        print(f"  Retry {i}: {delay:.1f}s")


def demo_task_fails_twice_then_succeeds():
    """
    Demo 4: Task fails 2 times then succeeds
    Main test case from requirements
    """
    print("\n" + "=" * 60)
    print("Demo 4: Task Fails Twice Then Succeeds (Main Test Case)")
    print("=" * 60)
    
    task_id = "api_call_demo"
    attempt_counter[task_id] = 0
    
    task = Task(
        name="unstable_api_call",
        args=[task_id, 2],  # Fail 2 times
        max_retries=3,
        queue_name="demo_retry"
    )
    
    print(f"\nTask created: {task.id}")
    print(f"Max retries: {task.max_retries}")
    print(f"Will fail 2 times, then succeed")
    
    # Simulate task execution with retries
    print(f"\n{'Attempt':<10} {'Result':<15} {'Backoff':<15} {'Status'}")
    print("-" * 60)
    
    for attempt in range(4):
        try:
            # Try to execute task
            result = unstable_api_call(task_id, 2)
            
            # Success!
            task.mark_success(result)
            print(f"{attempt+1:<10} {'Success':<15} {'-':<15} {task.status.value}")
            break
            
        except Exception as e:
            # Failure
            if task.can_retry():
                # Calculate backoff
                backoff = calculate_exponential_backoff(task.retry_count, jitter=False)
                
                # Record retry
                task.record_retry_attempt(str(e), backoff)
                task.increment_retry()
                
                print(f"{attempt+1:<10} {'Failed':<15} {f'{backoff:.1f}s':<15} Retry {task.retry_count}")
                
                # Wait for backoff (simulated)
                time.sleep(min(backoff, 0.5))  # Cap at 0.5s for demo
            else:
                print(f"{attempt+1:<10} {'Failed':<15} {'-':<15} Max retries")
                break
    
    # Show retry history
    print(f"\nRetry History:")
    for i, record in enumerate(task.retry_history):
        print(f"  Attempt {record['attempt']}: {record['error']}")
        print(f"    Backoff: {record['backoff_seconds']}s")
        print(f"    Timestamp: {record['timestamp']}")
    
    print(f"\nFinal Status: {task.status.value}")
    print(f"Total Attempts: {attempt_counter[task_id]}")
    print(f"Result: {task.result}")


def demo_retry_with_max_backoff_cap():
    """
    Demo 5: Maximum backoff cap
    """
    print("\n" + "=" * 60)
    print("Demo 5: Maximum Backoff Cap")
    print("=" * 60)
    
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=False)
    
    print("\nRetry delays with 300s cap:\n")
    
    for retry_count in [0, 5, 8, 9, 10, 15, 20]:
        delay = backoff.calculate_delay(retry_count)
        calculated = 2 ** retry_count
        capped = "(capped)" if calculated > 300 else ""
        
        print(f"  Retry {retry_count:2d}: {delay:6.1f}s  "
              f"(2^{retry_count} = {calculated:6d}) {capped}")
    
    print("\nAll delays above 300s are capped at 300s")


def demo_retry_history_tracking():
    """
    Demo 6: Retry history tracking
    """
    print("\n" + "=" * 60)
    print("Demo 6: Retry History Tracking")
    print("=" * 60)
    
    task = Task(
        name="demo_task",
        args=["test"],
        max_retries=4
    )
    
    print("\nSimulating 4 retry attempts:\n")
    
    errors = [
        "Connection timeout",
        "Server error 500",
        "Rate limit exceeded",
        "Network unreachable"
    ]
    
    for i, error in enumerate(errors):
        backoff = calculate_exponential_backoff(i, jitter=False)
        task.record_retry_attempt(error, backoff)
        
        print(f"Attempt {i+1}: {error}")
        print(f"  Backoff: {backoff:.1f}s")
        
        if i < len(errors) - 1:
            task.increment_retry()
    
    print(f"\nRetry History Summary:")
    print(f"  Total attempts: {len(task.retry_history)}")
    print(f"  Current retry count: {task.retry_count}")
    
    print(f"\nDetailed History:")
    for record in task.retry_history:
        print(f"\n  Attempt {record['attempt']}:")
        print(f"    Error: {record['error']}")
        print(f"    Backoff: {record['backoff_seconds']}s")
        print(f"    Time: {record['timestamp']}")


def demo_multiple_tasks_retry():
    """
    Demo 7: Multiple tasks with independent retry histories
    """
    print("\n" + "=" * 60)
    print("Demo 7: Multiple Tasks with Independent Retries")
    print("=" * 60)
    
    # Create multiple tasks
    task1 = Task(name="api_call_1", args=[1], max_retries=3)
    task2 = Task(name="api_call_2", args=[2], max_retries=3)
    task3 = Task(name="api_call_3", args=[3], max_retries=3)
    
    print("\nTask 1 fails twice:")
    for i in range(2):
        backoff = calculate_exponential_backoff(i, jitter=False)
        task1.record_retry_attempt(f"Error {i+1}", backoff)
        task1.increment_retry()
        print(f"  Retry {i+1}: {backoff:.1f}s backoff")
    
    print("\nTask 2 fails once:")
    backoff = calculate_exponential_backoff(0, jitter=False)
    task2.record_retry_attempt("Error 1", backoff)
    task2.increment_retry()
    print(f"  Retry 1: {backoff:.1f}s backoff")
    
    print("\nTask 3 fails three times:")
    for i in range(3):
        backoff = calculate_exponential_backoff(i, jitter=False)
        task3.record_retry_attempt(f"Error {i+1}", backoff)
        task3.increment_retry()
        print(f"  Retry {i+1}: {backoff:.1f}s backoff")
    
    print(f"\nSummary:")
    print(f"  Task 1: {task1.retry_count} retries, {len(task1.retry_history)} history entries")
    print(f"  Task 2: {task2.retry_count} retries, {len(task2.retry_history)} history entries")
    print(f"  Task 3: {task3.retry_count} retries, {len(task3.retry_history)} history entries")


def demo_backoff_strategies_comparison():
    """
    Demo 8: Compare different backoff strategies
    """
    print("\n" + "=" * 60)
    print("Demo 8: Backoff Strategies Comparison")
    print("=" * 60)
    
    print("\nComparison of retry delays (first 6 retries):\n")
    print(f"{'Retry':<10} {'Exponential':<15} {'Linear':<15} {'Fibonacci':<15}")
    print("-" * 60)
    
    for i in range(6):
        exp = get_retry_delays(i+1, strategy="exponential", base=2.0, jitter=False)[i]
        lin = get_retry_delays(i+1, strategy="linear", delay=5.0)[i]
        fib = get_retry_delays(i+1, strategy="fibonacci", base=1.0)[i]
        
        print(f"{i:<10} {f'{exp:.1f}s':<15} {f'{lin:.1f}s':<15} {f'{fib:.1f}s':<15}")
    
    print("\nExponential: Best for most cases (balances retry speed & server load)")
    print("Linear: Predictable, but may retry too fast or too slow")
    print("Fibonacci: Gentler growth than exponential")


def run_all_demos():
    """Run all retry demonstrations."""
    print("\n" + "=" * 60)
    print("RETRY LOGIC WITH EXPONENTIAL BACKOFF DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_exponential_backoff_calculation()
        demo_backoff_with_jitter()
        demo_retry_delay_sequence()
        demo_task_fails_twice_then_succeeds()  # Main test case
        demo_retry_with_max_backoff_cap()
        demo_retry_history_tracking()
        demo_multiple_tasks_retry()
        demo_backoff_strategies_comparison()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
