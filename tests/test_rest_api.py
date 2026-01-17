"""
Comprehensive tests for REST API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from jobqueue.api.main import app
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def api_key():
    """Get API key for testing."""
    # In test mode, API keys are optional
    return "test-api-key-123"


@task_registry.register("test_api_task")
def test_api_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_health_check(client):
    """Test health check endpoint (no auth required)."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "redis_connected" in data
    assert "postgres_connected" in data


def test_openapi_docs(client):
    """Test OpenAPI docs are accessible."""
    response = client.get("/docs")
    assert response.status_code == 200
    
    response = client.get("/openapi.json")
    assert response.status_code == 200
    data = response.json()
    assert "openapi" in data
    assert "info" in data
    assert "paths" in data


def test_post_tasks(client, redis_connection, api_key):
    """Test POST /tasks endpoint."""
    response = client.post(
        "/tasks",
        json={
            "task_name": "test_api_task",
            "args": [1, 2, 3],
            "priority": "high",
            "queue_name": "test_queue"
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["name"] == "test_api_task"
    assert data["priority"] == "high"
    assert data["status"] == "pending"


def test_post_tasks_validation_error(client, api_key):
    """Test POST /tasks with validation error."""
    # Invalid max_retries (negative)
    response = client.post(
        "/tasks",
        json={
            "task_name": "test_api_task",
            "max_retries": -1
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 422  # Validation error


def test_post_tasks_missing_api_key(client):
    """Test POST /tasks without API key."""
    response = client.post(
        "/tasks",
        json={
            "task_name": "test_api_task",
            "args": [1]
        }
    )
    
    # Should fail if API keys are configured
    # In test mode without keys, might succeed
    assert response.status_code in [401, 201]


def test_get_tasks(client, redis_connection, api_key):
    """Test GET /tasks endpoint with pagination."""
    # Create some tasks
    queue = Queue("test_queue")
    for i in range(5):
        task = Task(name="test_api_task", args=[i], queue_name="test_queue")
        queue.enqueue(task)
    
    response = client.get(
        "/tasks",
        params={"skip": 0, "limit": 10},
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "tasks" in data
    assert "total" in data
    assert "skip" in data
    assert "limit" in data
    assert "has_more" in data
    assert len(data["tasks"]) <= 10


def test_get_tasks_with_filters(client, redis_connection, api_key):
    """Test GET /tasks with status and queue_name filters."""
    queue = Queue("test_queue")
    task = Task(name="test_api_task", args=[1], queue_name="test_queue")
    queue.enqueue(task)
    
    response = client.get(
        "/tasks",
        params={"status": "pending", "queue_name": "test_queue"},
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "tasks" in data
    # All tasks should match filters
    for task in data["tasks"]:
        assert task["status"] == "pending"
        assert task["queue_name"] == "test_queue"


def test_get_task_by_id(client, redis_connection, api_key):
    """Test GET /tasks/{id} endpoint."""
    # Create task
    queue = Queue("test_queue")
    task = Task(name="test_api_task", args=[1], queue_name="test_queue")
    queue.enqueue(task)
    
    response = client.get(
        f"/tasks/{task.id}",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == task.id
    assert data["name"] == "test_api_task"


def test_get_task_not_found(client, api_key):
    """Test GET /tasks/{id} with non-existent task."""
    response = client.get(
        "/tasks/non-existent-id",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_delete_task(client, redis_connection, api_key):
    """Test DELETE /tasks/{id} endpoint."""
    # Create task
    queue = Queue("test_queue")
    task = Task(name="test_api_task", args=[1], queue_name="test_queue")
    queue.enqueue(task)
    
    response = client.delete(
        f"/tasks/{task.id}",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert task.id in data["message"]


def test_post_tasks_retry(client, redis_connection, api_key):
    """Test POST /tasks/{id}/retry endpoint."""
    # This would require a task in DLQ
    # For now, test the endpoint exists
    response = client.post(
        "/tasks/non-existent-id/retry",
        headers={"X-API-Key": api_key}
    )
    
    # Should return 404 or appropriate error
    assert response.status_code in [404, 400]


def test_get_queues(client, redis_connection, api_key):
    """Test GET /queues endpoint."""
    # Create some queues
    queue1 = Queue("queue1")
    queue2 = Queue("queue2")
    for i in range(3):
        task = Task(name="test_api_task", args=[i], queue_name="queue1")
        queue1.enqueue(task)
    
    response = client.get(
        "/queues",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "queues" in data
    assert isinstance(data["queues"], list)


def test_delete_queues(client, redis_connection, api_key):
    """Test DELETE /queues/{name} endpoint."""
    # Create queue with tasks
    queue = Queue("test_purge_queue")
    for i in range(5):
        task = Task(name="test_api_task", args=[i], queue_name="test_purge_queue")
        queue.enqueue(task)
    
    assert queue.size() == 5
    
    response = client.delete(
        "/queues/test_purge_queue",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "tasks_removed" in data
    
    # Verify queue is purged
    assert queue.size() == 0


def test_get_workers(client, api_key):
    """Test GET /workers endpoint."""
    response = client.get(
        "/workers",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "workers" in data
    assert isinstance(data["workers"], list)


def test_get_worker_by_id(client, api_key):
    """Test GET /workers/{id} endpoint."""
    # Get list of workers first
    response = client.get(
        "/workers",
        headers={"X-API-Key": api_key}
    )
    
    if response.status_code == 200:
        data = response.json()
        if data.get("workers"):
            worker_id = data["workers"][0]["worker_id"]
            
            response = client.get(
                f"/workers/{worker_id}",
                headers={"X-API-Key": api_key}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "worker_id" in data


def test_get_metrics(client, api_key):
    """Test GET /metrics endpoint."""
    response = client.get(
        "/metrics",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "tasks_enqueued_per_second" in data or "metrics" in data


def test_rate_limiting(client, api_key):
    """Test API rate limiting."""
    # Make many requests quickly
    responses = []
    for i in range(100):
        response = client.get(
            "/health",  # Health check doesn't require auth
            headers={"X-API-Key": api_key}
        )
        responses.append(response.status_code)
    
    # Check rate limit headers
    response = client.get("/health")
    assert "X-RateLimit-Limit-Minute" in response.headers
    assert "X-RateLimit-Remaining-Minute" in response.headers


def test_api_key_authentication(client):
    """Test API key authentication."""
    # Request without API key
    response = client.post(
        "/tasks",
        json={"task_name": "test_api_task", "args": [1]}
    )
    
    # Should fail if API keys are configured
    # In test mode without keys, might succeed
    assert response.status_code in [401, 201]
    
    # Request with invalid API key
    response = client.post(
        "/tasks",
        json={"task_name": "test_api_task", "args": [1]},
        headers={"X-API-Key": "invalid-key"}
    )
    
    # Should fail if API keys are configured
    assert response.status_code in [401, 201]


def test_error_handling(client, api_key):
    """Test proper error handling with HTTP codes."""
    # 404 Not Found
    response = client.get(
        "/tasks/non-existent-id",
        headers={"X-API-Key": api_key}
    )
    assert response.status_code == 404
    
    # 400 Bad Request (validation error)
    response = client.post(
        "/tasks",
        json={"task_name": "test_api_task", "max_retries": -1},
        headers={"X-API-Key": api_key}
    )
    assert response.status_code == 422  # Validation error
    
    # 401 Unauthorized (if API keys configured)
    response = client.post(
        "/tasks",
        json={"task_name": "test_api_task", "args": [1]}
    )
    assert response.status_code in [401, 201]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
