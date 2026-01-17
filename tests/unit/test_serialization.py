"""
Unit tests for task serialization.
"""
import pytest
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.serialization import serialize_task, deserialize_task


@pytest.mark.unit
class TestSerialization:
    """Test task serialization."""
    
    def test_serialize_simple_task(self):
        """Test serializing a simple task."""
        task = Task(name="test_task", args=[1, 2, 3])
        serialized = serialize_task(task)
        
        assert isinstance(serialized, str)
        assert "test_task" in serialized
    
    def test_deserialize_simple_task(self):
        """Test deserializing a simple task."""
        task = Task(name="test_task", args=[1, 2, 3])
        serialized = serialize_task(task)
        deserialized = deserialize_task(serialized)
        
        assert deserialized.name == task.name
        assert deserialized.args == task.args
    
    def test_serialize_complex_args(self):
        """Test serializing task with complex arguments."""
        task = Task(
            name="test_task",
            args=[1, "string", [1, 2, 3], {"nested": "dict"}],
            kwargs={"key": "value", "number": 42}
        )
        serialized = serialize_task(task)
        deserialized = deserialize_task(serialized)
        
        assert deserialized.args == task.args
        assert deserialized.kwargs == task.kwargs
    
    def test_serialize_with_priority(self):
        """Test serializing task with priority."""
        task = Task(name="test_task", priority=TaskPriority.HIGH)
        serialized = serialize_task(task)
        deserialized = deserialize_task(serialized)
        
        assert deserialized.priority == TaskPriority.HIGH
    
    def test_serialize_round_trip(self):
        """Test serialization round trip."""
        original = Task(
            name="test_task",
            args=[1, 2, 3],
            kwargs={"key": "value"},
            priority=TaskPriority.LOW,
            max_retries=5,
            timeout=600
        )
        
        serialized = serialize_task(original)
        deserialized = deserialize_task(serialized)
        
        assert deserialized.name == original.name
        assert deserialized.args == original.args
        assert deserialized.kwargs == original.kwargs
        assert deserialized.priority == original.priority
        assert deserialized.max_retries == original.max_retries
        assert deserialized.timeout == original.timeout
