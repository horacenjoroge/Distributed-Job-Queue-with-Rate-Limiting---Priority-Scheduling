"""
Task result backend demonstration.

Shows how to store and retrieve task results.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.backend.result_backend import TaskResult, ResultBackend, result_backend
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_task")
def demo_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_task_result_creation():
    """
    Demo 1: Creating TaskResult instances
    """
    print("\n" + "=" * 60)
    print("Demo 1: Creating TaskResult Instances")
    print("=" * 60)
    
    # Create result for successful task
    result = TaskResult(
        task_id="task_1",
        status=TaskStatus.SUCCESS,
        result="Task completed successfully",
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        duration=1.5
    )
    
    print(f"\nSuccessful Task Result:")
    print(f"  Task ID: {result.task_id}")
    print(f"  Status: {result.status.value}")
    print(f"  Result: {result.result}")
    print(f"  Duration: {result.duration}s")
    
    # Create result for failed task
    failed_result = TaskResult(
        task_id="task_2",
        status=TaskStatus.FAILED,
        error="Task execution failed",
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        duration=0.5
    )
    
    print(f"\nFailed Task Result:")
    print(f"  Task ID: {failed_result.task_id}")
    print(f"  Status: {failed_result.status.value}")
    print(f"  Error: {failed_result.error}")
    print(f"  Duration: {failed_result.duration}s")


def demo_store_and_retrieve_result():
    """
    Demo 2: Store and retrieve results
    """
    print("\n" + "=" * 60)
    print("Demo 2: Store and Retrieve Results")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create and execute task
    task = Task(
        name="demo_task",
        args=["test_value"],
        queue_name="demo_queue"
    )
    
    print(f"\nTask created: {task.id}")
    
    # Simulate execution
    task.mark_running("worker_1")
    task.started_at = datetime.utcnow()
    time.sleep(0.1)
    task.mark_success("Processed: test_value")
    
    print(f"Task executed successfully")
    print(f"Result: {task.result}")
    
    # Store result
    print(f"\nStoring result in Redis...")
    success = result_backend.store_result(task)
    print(f"  Stored: {success}")
    print(f"  Key: result:{task.id}")
    
    # Retrieve result
    print(f"\nRetrieving result from Redis...")
    result = result_backend.get_result(task.id)
    
    if result:
        print(f"  Result found:")
        print(f"    Task ID: {result.task_id}")
        print(f"    Status: {result.status.value}")
        print(f"    Result: {result.result}")
        print(f"    Duration: {result.duration:.2f}s")
    else:
        print(f"  Result not found")
    
    # Cleanup
    result_backend.delete_result(task.id)
    redis_broker.disconnect()


def demo_result_ttl():
    """
    Demo 3: Result TTL (Time To Live)
    """
    print("\n" + "=" * 60)
    print("Demo 3: Result TTL (Time To Live)")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create task
    task = Task(
        name="demo_task",
        args=["ttl_test"],
        queue_name="demo_queue"
    )
    task.mark_success("TTL test result")
    
    # Store with custom TTL (60 seconds)
    print(f"\nStoring result with 60 second TTL...")
    result_backend.store_result(task, ttl=60)
    
    # Check TTL
    ttl = result_backend.get_result_ttl(task.id)
    print(f"  Remaining TTL: {ttl} seconds")
    print(f"  Remaining TTL: {ttl / 60:.2f} minutes")
    
    # Extend TTL
    print(f"\nExtending TTL to 3600 seconds (1 hour)...")
    result_backend.extend_result_ttl(task.id, 3600)
    
    new_ttl = result_backend.get_result_ttl(task.id)
    print(f"  New TTL: {new_ttl} seconds")
    print(f"  New TTL: {new_ttl / 3600:.2f} hours")
    
    # Cleanup
    result_backend.delete_result(task.id)
    redis_broker.disconnect()


def demo_result_expiration():
    """
    Demo 4: Result expiration
    """
    print("\n" + "=" * 60)
    print("Demo 4: Result Expiration")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create task
    task = Task(
        name="demo_task",
        args=["expiration_test"],
        queue_name="demo_queue"
    )
    task.mark_success("This will expire")
    
    # Store with very short TTL (2 seconds)
    print(f"\nStoring result with 2 second TTL...")
    result_backend.store_result(task, ttl=2)
    
    # Check immediately
    exists = result_backend.result_exists(task.id)
    print(f"  Result exists: {exists}")
    
    # Wait for expiration
    print(f"\nWaiting for expiration (2 seconds)...")
    time.sleep(2.5)
    
    # Check after expiration
    exists = result_backend.result_exists(task.id)
    result = result_backend.get_result(task.id)
    
    print(f"  Result exists: {exists}")
    print(f"  Result retrieved: {result is not None}")
    print(f"  Result expired: {result is None}")
    
    redis_broker.disconnect()


def demo_result_from_task():
    """
    Demo 5: Create result from task
    """
    print("\n" + "=" * 60)
    print("Demo 5: Create Result from Task")
    print("=" * 60)
    
    # Create and execute task
    task = Task(
        name="demo_task",
        args=["from_task_test"],
        queue_name="demo_queue"
    )
    
    task.mark_running("worker_1")
    task.started_at = datetime.utcnow()
    time.sleep(0.1)
    task.mark_success("Result from task")
    
    print(f"\nTask executed:")
    print(f"  Task ID: {task.id}")
    print(f"  Status: {task.status.value}")
    print(f"  Result: {task.result}")
    print(f"  Duration: {task.execution_time():.2f}s")
    
    # Create result from task
    result = TaskResult.from_task(task)
    
    print(f"\nTaskResult created from task:")
    print(f"  Task ID: {result.task_id}")
    print(f"  Status: {result.status.value}")
    print(f"  Result: {result.result}")
    print(f"  Duration: {result.duration:.2f}s")
    print(f"  Started at: {result.started_at}")
    print(f"  Completed at: {result.completed_at}")


def demo_result_with_complex_data():
    """
    Demo 6: Results with complex data structures
    """
    print("\n" + "=" * 60)
    print("Demo 6: Results with Complex Data")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create task with complex result
    task = Task(
        name="demo_task",
        args=[1],
        queue_name="demo_queue"
    )
    
    complex_result = {
        "user_id": 12345,
        "data": {
            "items": [1, 2, 3, 4, 5],
            "metadata": {
                "processed": True,
                "count": 5
            }
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    task.mark_success(complex_result)
    
    print(f"\nStoring complex result...")
    result_backend.store_result(task)
    
    # Retrieve and verify
    result = result_backend.get_result(task.id)
    
    print(f"\nRetrieved result:")
    print(f"  Type: {type(result.result)}")
    print(f"  User ID: {result.result['user_id']}")
    print(f"  Items count: {len(result.result['data']['items'])}")
    print(f"  Processed: {result.result['data']['metadata']['processed']}")
    
    # Cleanup
    result_backend.delete_result(task.id)
    redis_broker.disconnect()


def demo_execute_task_fetch_result():
    """
    Demo 7: Execute task, fetch result (Main test case)
    """
    print("\n" + "=" * 60)
    print("Demo 7: Execute Task, Fetch Result (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    print(f"\nScenario:")
    print(f"  1. Create and execute a task")
    print(f"  2. Store result in Redis")
    print(f"  3. Fetch result by task_id")
    print(f"  4. Verify result data\n")
    
    # Step 1: Create task
    task = Task(
        name="demo_task",
        args=["fetch_test"],
        queue_name="demo_queue"
    )
    
    print(f"[Step 1] Task created:")
    print(f"  Task ID: {task.id}")
    print(f"  Task name: {task.name}")
    print(f"  Args: {task.args}")
    
    # Step 2: Execute task
    print(f"\n[Step 2] Executing task...")
    task.mark_running("worker_1")
    task.started_at = datetime.utcnow()
    
    # Simulate work
    time.sleep(0.2)
    
    # Task completes
    result_value = "Processed: fetch_test"
    task.mark_success(result_value)
    
    print(f"  Task completed successfully")
    print(f"  Result: {result_value}")
    print(f"  Duration: {task.execution_time():.2f}s")
    
    # Step 3: Store result
    print(f"\n[Step 3] Storing result in Redis...")
    success = result_backend.store_result(task)
    
    result_key = f"result:{task.id}"
    print(f"  Result key: {result_key}")
    print(f"  Stored: {success}")
    
    # Step 4: Fetch result
    print(f"\n[Step 4] Fetching result by task_id...")
    result = result_backend.get_result(task.id)
    
    if result:
        print(f"\nResult Retrieved:")
        print(f"  Task ID: {result.task_id}")
        print(f"  Status: {result.status.value}")
        print(f"  Result: {result.result}")
        print(f"  Duration: {result.duration:.2f}s")
        print(f"  Started at: {result.started_at}")
        print(f"  Completed at: {result.completed_at}")
        
        # Verify
        assert result.task_id == task.id
        assert result.status == TaskStatus.SUCCESS
        assert result.result == result_value
        
        print(f"\nTest: PASS - Task executed and result fetched successfully")
    else:
        print(f"\nTest: FAIL - Result not found")
    
    # Cleanup
    result_backend.delete_result(task.id)
    redis_broker.disconnect()


def demo_result_backend_api():
    """
    Demo 8: Using result backend API
    """
    print("\n" + "=" * 60)
    print("Demo 8: Result Backend API Usage")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create task
    task = Task(
        name="demo_task",
        args=["api_test"],
        queue_name="demo_queue"
    )
    task.mark_success("API test result")
    
    print(f"\nAvailable methods:")
    print(f"  - store_result(task, ttl=None)")
    print(f"  - get_result(task_id)")
    print(f"  - delete_result(task_id)")
    print(f"  - result_exists(task_id)")
    print(f"  - get_result_ttl(task_id)")
    print(f"  - extend_result_ttl(task_id, ttl)")
    
    # Store
    print(f"\n1. Storing result...")
    result_backend.store_result(task)
    print(f"   Stored: {result_backend.result_exists(task.id)}")
    
    # Check existence
    print(f"\n2. Checking existence...")
    exists = result_backend.result_exists(task.id)
    print(f"   Exists: {exists}")
    
    # Get TTL
    print(f"\n3. Getting TTL...")
    ttl = result_backend.get_result_ttl(task.id)
    print(f"   TTL: {ttl} seconds ({ttl / 3600:.2f} hours)")
    
    # Extend TTL
    print(f"\n4. Extending TTL...")
    result_backend.extend_result_ttl(task.id, 7200)
    new_ttl = result_backend.get_result_ttl(task.id)
    print(f"   New TTL: {new_ttl} seconds ({new_ttl / 3600:.2f} hours)")
    
    # Get result
    print(f"\n5. Getting result...")
    result = result_backend.get_result(task.id)
    print(f"   Result: {result.result if result else None}")
    
    # Delete
    print(f"\n6. Deleting result...")
    deleted = result_backend.delete_result(task.id)
    print(f"   Deleted: {deleted}")
    print(f"   Exists: {result_backend.result_exists(task.id)}")
    
    redis_broker.disconnect()


def run_all_demos():
    """Run all result backend demonstrations."""
    print("\n" + "=" * 60)
    print("TASK RESULT BACKEND DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_task_result_creation()
        demo_store_and_retrieve_result()
        demo_result_ttl()
        demo_result_expiration()
        demo_result_from_task()
        demo_result_with_complex_data()
        demo_execute_task_fetch_result()  # Main test case
        demo_result_backend_api()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
