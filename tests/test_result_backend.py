"""
Tests for task result backend.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.result_backend import TaskResult, ResultBackend, result_backend
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_results(redis_connection):
    """Clean result keys before each test."""
    # Clean any existing results
    keys = redis_broker.client.keys("result:*")
    if keys:
        redis_broker.client.delete(*keys)
    yield
    # Cleanup
    keys = redis_broker.client.keys("result:*")
    if keys:
        redis_broker.client.delete(*keys)


@task_registry.register("test_result_task")
def test_result_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_task_result_creation():
    """Test creating TaskResult instance."""
    result = TaskResult(
        task_id="test_1",
        status=TaskStatus.SUCCESS,
        result="test_result",
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        duration=1.5
    )
    
    assert result.task_id == "test_1"
    assert result.status == TaskStatus.SUCCESS
    assert result.result == "test_result"
    assert result.duration == 1.5


def test_task_result_to_dict(clean_results):
    """Test converting TaskResult to dictionary."""
    started = datetime.utcnow()
    completed = datetime.utcnow()
    
    result = TaskResult(
        task_id="test_2",
        status=TaskStatus.SUCCESS,
        result={"key": "value"},
        started_at=started,
        completed_at=completed,
        duration=2.0
    )
    
    data = result.to_dict()
    
    assert data["task_id"] == "test_2"
    assert data["status"] == TaskStatus.SUCCESS.value
    assert data["result"] == {"key": "value"}
    assert data["duration"] == 2.0
    assert "started_at" in data
    assert "completed_at" in data


def test_task_result_from_dict(clean_results):
    """Test creating TaskResult from dictionary."""
    data = {
        "task_id": "test_3",
        "status": TaskStatus.SUCCESS.value,
        "result": "test_result",
        "error": None,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": datetime.utcnow().isoformat(),
        "duration": 1.5
    }
    
    result = TaskResult.from_dict(data)
    
    assert result.task_id == "test_3"
    assert result.status == TaskStatus.SUCCESS
    assert result.result == "test_result"
    assert result.duration == 1.5


def test_task_result_from_task(clean_results):
    """Test creating TaskResult from Task."""
    task = Task(
        name="test_task",
        args=[1, 2],
        queue_name="test_queue"
    )
    
    task.mark_running("worker_1")
    task.started_at = datetime.utcnow()
    time.sleep(0.1)
    task.mark_success("result_value")
    
    result = TaskResult.from_task(task)
    
    assert result.task_id == task.id
    assert result.status == TaskStatus.SUCCESS
    assert result.result == "result_value"
    assert result.duration is not None
    assert result.duration > 0


def test_store_result(clean_results):
    """Test storing result in Redis."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    success = result_backend.store_result(task)
    
    assert success
    
    # Verify stored
    result_key = f"result:{task.id}"
    stored = redis_broker.client.get(result_key)
    assert stored is not None


def test_get_result(clean_results):
    """Test retrieving result from Redis."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store result
    result_backend.store_result(task)
    
    # Retrieve result
    result = result_backend.get_result(task.id)
    
    assert result is not None
    assert result.task_id == task.id
    assert result.status == TaskStatus.SUCCESS
    assert result.result == "test_result"


def test_get_result_not_found(clean_results):
    """Test retrieving non-existent result."""
    result = result_backend.get_result("non_existent_task")
    assert result is None


def test_delete_result(clean_results):
    """Test deleting result from Redis."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store result
    result_backend.store_result(task)
    
    # Verify exists
    assert result_backend.result_exists(task.id)
    
    # Delete result
    success = result_backend.delete_result(task.id)
    assert success
    
    # Verify deleted
    assert not result_backend.result_exists(task.id)


def test_result_exists(clean_results):
    """Test checking if result exists."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Should not exist yet
    assert not result_backend.result_exists(task.id)
    
    # Store result
    result_backend.store_result(task)
    
    # Should exist now
    assert result_backend.result_exists(task.id)


def test_result_ttl(clean_results):
    """Test result TTL functionality."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store with custom TTL
    result_backend.store_result(task, ttl=60)
    
    # Get TTL
    ttl = result_backend.get_result_ttl(task.id)
    assert ttl is not None
    assert ttl > 0
    assert ttl <= 60


def test_extend_result_ttl(clean_results):
    """Test extending result TTL."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store with short TTL
    result_backend.store_result(task, ttl=10)
    
    # Extend TTL
    success = result_backend.extend_result_ttl(task.id, 3600)
    assert success
    
    # Verify extended
    ttl = result_backend.get_result_ttl(task.id)
    assert ttl is not None
    assert ttl > 10
    assert ttl <= 3600


def test_result_expiration(clean_results):
    """Test result expiration with TTL."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store with very short TTL
    result_backend.store_result(task, ttl=1)
    
    # Should exist immediately
    assert result_backend.result_exists(task.id)
    
    # Wait for expiration
    time.sleep(1.5)
    
    # Should be expired
    result = result_backend.get_result(task.id)
    assert result is None


def test_result_with_error(clean_results):
    """Test storing result with error."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_failed("Test error message")
    
    # Store result
    result_backend.store_result(task)
    
    # Retrieve result
    result = result_backend.get_result(task.id)
    
    assert result is not None
    assert result.status == TaskStatus.FAILED
    assert result.error == "Test error message"
    assert result.result is None


def test_result_with_complex_data(clean_results):
    """Test storing result with complex data structures."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    
    complex_result = {
        "nested": {
            "list": [1, 2, 3],
            "dict": {"key": "value"}
        },
        "items": ["a", "b", "c"]
    }
    
    task.mark_success(complex_result)
    
    # Store result
    result_backend.store_result(task)
    
    # Retrieve result
    result = result_backend.get_result(task.id)
    
    assert result is not None
    assert result.result == complex_result


def test_execute_task_fetch_result(clean_results):
    """
    Test main case: Execute task, fetch result.
    """
    print("\n" + "=" * 60)
    print("Test: Execute task, fetch result")
    print("=" * 60)
    
    # Create task
    task = Task(
        name="test_result_task",
        args=["test_value"],
        queue_name="test_queue"
    )
    
    print(f"\n1. Task created: {task.id}")
    print(f"   Task name: {task.name}")
    print(f"   Args: {task.args}")
    
    # Simulate task execution
    print(f"\n2. Executing task...")
    task.mark_running("worker_1")
    task.started_at = datetime.utcnow()
    
    # Simulate work
    time.sleep(0.1)
    
    # Task completes successfully
    result_value = "Processed: test_value"
    task.mark_success(result_value)
    
    print(f"   Task completed successfully")
    print(f"   Result: {result_value}")
    print(f"   Duration: {task.execution_time():.2f}s")
    
    # Store result
    print(f"\n3. Storing result in Redis...")
    success = result_backend.store_result(task)
    assert success
    
    result_key = f"result:{task.id}"
    print(f"   Result key: {result_key}")
    print(f"   Stored: {success}")
    
    # Fetch result
    print(f"\n4. Fetching result from Redis...")
    result = result_backend.get_result(task.id)
    
    assert result is not None
    print(f"   Result found: {result is not None}")
    print(f"   Task ID: {result.task_id}")
    print(f"   Status: {result.status.value}")
    print(f"   Result: {result.result}")
    print(f"   Duration: {result.duration:.2f}s")
    
    # Verify result
    assert result.task_id == task.id
    assert result.status == TaskStatus.SUCCESS
    assert result.result == result_value
    assert result.duration is not None
    
    print(f"\nTest: PASS - Task executed and result fetched successfully")
    
    # Test TTL
    print(f"\n5. Checking result TTL...")
    ttl = result_backend.get_result_ttl(task.id)
    print(f"   TTL: {ttl} seconds")
    assert ttl is not None
    assert ttl > 0


def test_result_backend_default_ttl(clean_results):
    """Test default TTL is used when not specified."""
    backend = ResultBackend(default_ttl=120)
    
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store without specifying TTL
    backend.store_result(task)
    
    # Check TTL
    ttl = backend.get_result_ttl(task.id)
    assert ttl is not None
    assert ttl > 0
    assert ttl <= 120


def test_result_backend_custom_ttl(clean_results):
    """Test custom TTL can be specified."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success("test_result")
    
    # Store with custom TTL
    result_backend.store_result(task, ttl=300)
    
    # Check TTL
    ttl = result_backend.get_result_ttl(task.id)
    assert ttl is not None
    assert ttl > 0
    assert ttl <= 300


def test_result_serialization(clean_results):
    """Test result serialization to/from JSON."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue"
    )
    task.mark_success({"key": "value", "number": 42})
    
    # Store result
    result_backend.store_result(task)
    
    # Retrieve and verify
    result = result_backend.get_result(task.id)
    
    assert result is not None
    assert isinstance(result.result, dict)
    assert result.result["key"] == "value"
    assert result.result["number"] == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
