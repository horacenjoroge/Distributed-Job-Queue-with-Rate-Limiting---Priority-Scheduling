"""
Pytest configuration and fixtures for testing.
"""
import pytest
import fakeredis
from typing import Generator
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.core.event_publisher import event_publisher


@pytest.fixture(scope="function")
def fake_redis():
    """Create a fake Redis instance for testing."""
    server = fakeredis.FakeStrictRedis()
    yield server
    server.flushall()


@pytest.fixture(scope="function")
def mock_redis_broker(monkeypatch, fake_redis):
    """Mock Redis broker to use fake Redis."""
    original_connect = redis_broker.connect
    original_get_connection = redis_broker.get_connection
    
    def mock_connect():
        redis_broker._connection = fake_redis
        redis_broker._connected = True
    
    def mock_get_connection():
        return fake_redis
    
    monkeypatch.setattr(redis_broker, "connect", mock_connect)
    monkeypatch.setattr(redis_broker, "get_connection", mock_get_connection)
    monkeypatch.setattr(redis_broker, "is_connected", lambda: True)
    
    mock_connect()
    
    yield redis_broker
    
    # Restore original methods
    monkeypatch.setattr(redis_broker, "connect", original_connect)
    monkeypatch.setattr(redis_broker, "get_connection", original_get_connection)


@pytest.fixture(scope="function")
def mock_postgres_backend(monkeypatch):
    """Mock PostgreSQL backend for testing."""
    class MockPostgresBackend:
        def __init__(self):
            self.tasks = {}
            self._connected = True
        
        def connect(self):
            self._connected = True
        
        def disconnect(self):
            self._connected = False
        
        def is_connected(self):
            return self._connected
        
        def initialize_schema(self):
            pass
        
        def execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
            if "INSERT INTO tasks" in query:
                task_id = params[0]
                self.tasks[task_id] = {
                    "id": params[0],
                    "name": params[1],
                    "priority": params[2],
                    "status": params[3],
                    "queue_name": params[4],
                    "args": params[5],
                    "kwargs": params[6],
                    "retry_count": params[7],
                    "max_retries": params[8],
                    "timeout": params[9],
                    "created_at": params[10],
                    "parent_task_id": params[11] if len(params) > 11 else None,
                    "depends_on": params[12] if len(params) > 12 else [],
                }
                return None
            elif "SELECT * FROM tasks WHERE id" in query:
                task_id = params[0]
                if task_id in self.tasks:
                    from types import SimpleNamespace
                    return SimpleNamespace(**self.tasks[task_id])
                return None
            elif "UPDATE tasks SET" in query:
                task_id = params[-1]
                if task_id in self.tasks:
                    self.tasks[task_id].update({
                        "status": params[0],
                        "result": params[1],
                        "error": params[2],
                        "retry_count": params[3],
                        "started_at": params[4],
                        "completed_at": params[5],
                        "worker_id": params[6],
                    })
                return None
            elif "SELECT status, COUNT" in query:
                return []
            return None
    
    mock_backend = MockPostgresBackend()
    monkeypatch.setattr(postgres_backend, "connect", mock_backend.connect)
    monkeypatch.setattr(postgres_backend, "disconnect", mock_backend.disconnect)
    monkeypatch.setattr(postgres_backend, "is_connected", mock_backend.is_connected)
    monkeypatch.setattr(postgres_backend, "initialize_schema", mock_backend.initialize_schema)
    monkeypatch.setattr(postgres_backend, "execute_query", mock_backend.execute_query)
    
    yield mock_backend


@pytest.fixture(scope="function")
def mock_event_publisher(monkeypatch):
    """Mock event publisher to capture events."""
    events = []
    
    def mock_publish(event_type, data):
        events.append({"type": event_type, "data": data})
    
    original_publish = event_publisher.publish
    monkeypatch.setattr(event_publisher, "publish", mock_publish)
    
    yield events
    
    monkeypatch.setattr(event_publisher, "publish", original_publish)


@pytest.fixture(scope="function")
def clean_redis(mock_redis_broker):
    """Ensure Redis is clean before each test."""
    redis_broker.get_connection().flushall()
    yield
    redis_broker.get_connection().flushall()
