"""
Tests for task serialization functionality.
"""
import pytest
from datetime import datetime, date
from decimal import Decimal
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.serialization import (
    task_serializer,
    detect_circular_reference,
    is_serializable,
    SerializationError
)
from jobqueue.core.validation import task_validator, ValidationError


class TestTaskSerialization:
    """Test task serialization with various data types."""
    
    def test_simple_task_json_serialization(self):
        """Test basic JSON serialization."""
        task = Task(
            name="test_task",
            args=[1, 2, 3],
            kwargs={"key": "value"}
        )
        
        # Serialize to JSON
        json_str = task.to_json()
        
        # Deserialize
        restored_task = Task.from_json(json_str)
        
        assert restored_task.name == task.name
        assert restored_task.args == task.args
        assert restored_task.kwargs == task.kwargs
    
    def test_complex_objects_serialization(self):
        """Test serialization with complex Python objects."""
        now = datetime.now()
        today = date.today()
        decimal_val = Decimal("123.45")
        
        task = Task(
            name="complex_task",
            args=[now, today, decimal_val],
            kwargs={
                "datetime": now,
                "date": today,
                "decimal": decimal_val,
                "set": {1, 2, 3},
                "bytes": b"hello"
            }
        )
        
        # Use advanced serializer
        serialized = task.serialize_with_format("json")
        restored_task = Task.deserialize_with_format(serialized, "json")
        
        assert restored_task.name == task.name
    
    def test_pickle_serialization(self):
        """Test pickle serialization for non-JSON objects."""
        task = Task(
            name="pickle_task",
            args=[1, 2, 3],
            kwargs={"key": "value"}
        )
        
        # Serialize with pickle
        pickled = task.serialize_with_format("pickle")
        restored_task = Task.deserialize_with_format(pickled, "pickle")
        
        assert restored_task.name == task.name
        assert restored_task.args == task.args
    
    def test_task_signature_generation(self):
        """Test task signature for deduplication."""
        task1 = Task(
            name="test",
            args=[1, 2],
            kwargs={"x": 10}
        )
        
        task2 = Task(
            name="test",
            args=[1, 2],
            kwargs={"x": 10}
        )
        
        task3 = Task(
            name="test",
            args=[1, 3],
            kwargs={"x": 10}
        )
        
        task1.compute_and_set_signature()
        task2.compute_and_set_signature()
        task3.compute_and_set_signature()
        
        # Same args/kwargs should produce same signature
        assert task1.task_signature == task2.task_signature
        
        # Different args should produce different signature
        assert task1.task_signature != task3.task_signature
    
    def test_task_deduplication(self):
        """Test task deduplication detection."""
        task1 = Task(name="test", args=[1, 2])
        task2 = Task(name="test", args=[1, 2])
        task3 = Task(name="test", args=[3, 4])
        
        assert task1.is_duplicate_of(task2)
        assert not task1.is_duplicate_of(task3)
    
    def test_circular_reference_detection(self):
        """Test detection of circular references."""
        # Create circular reference
        list_with_circular = [1, 2, 3]
        list_with_circular.append(list_with_circular)
        
        assert detect_circular_reference(list_with_circular)
        
        # Normal list should not have circular reference
        normal_list = [1, 2, 3, [4, 5]]
        assert not detect_circular_reference(normal_list)
    
    def test_task_versioning(self):
        """Test task version tracking."""
        task = Task(name="test", version="1.0")
        assert task.version == "1.0"
        
        # Test serialization preserves version
        json_str = task.to_json()
        restored = Task.from_json(json_str)
        assert restored.version == "1.0"


class TestTaskValidation:
    """Test task signature validation."""
    
    def test_signature_validation(self):
        """Test function signature validation."""
        def sample_func(a: int, b: int, c: int = 10):
            return a + b + c
        
        # Valid arguments
        assert task_validator.validate_task_signature(
            sample_func,
            (1, 2),
            {}
        )
        
        # Valid with kwargs
        assert task_validator.validate_task_signature(
            sample_func,
            (1,),
            {"b": 2, "c": 3}
        )
    
    def test_signature_validation_failure(self):
        """Test signature validation failure."""
        def sample_func(a: int, b: int):
            return a + b
        
        # Missing required argument
        with pytest.raises(ValidationError):
            task_validator.validate_task_signature(
                sample_func,
                (1,),
                {}
            )
    
    def test_required_args_check(self):
        """Test required arguments checking."""
        def sample_func(a, b, c=10):
            return a + b + c
        
        # Should pass with required args
        assert task_validator.check_required_args(
            sample_func,
            (1, 2),
            {}
        )
        
        # Should fail without required args
        with pytest.raises(ValidationError):
            task_validator.check_required_args(
                sample_func,
                (1,),
                {}
            )
    
    def test_get_signature_info(self):
        """Test getting function signature information."""
        def sample_func(a: int, b: str = "default"):
            return f"{a}: {b}"
        
        info = task_validator.get_signature_info(sample_func)
        
        assert "a" in info["parameters"]
        assert "b" in info["parameters"]
        assert info["parameters"]["b"]["default"] == "default"


class TestSerializationEdgeCases:
    """Test edge cases in serialization."""
    
    def test_empty_args_kwargs(self):
        """Test task with empty args and kwargs."""
        task = Task(name="empty_task")
        
        json_str = task.to_json()
        restored = Task.from_json(json_str)
        
        assert restored.args == []
        assert restored.kwargs == {}
    
    def test_nested_structures(self):
        """Test deeply nested data structures."""
        nested_data = {
            "level1": {
                "level2": {
                    "level3": [1, 2, {"level4": "value"}]
                }
            }
        }
        
        task = Task(
            name="nested_task",
            kwargs=nested_data
        )
        
        json_str = task.to_json()
        restored = Task.from_json(json_str)
        
        assert restored.kwargs == nested_data
    
    def test_special_characters(self):
        """Test serialization with special characters."""
        task = Task(
            name="special_chars",
            args=["hello\nworld", "tab\there"],
            kwargs={"emoji": "test", "unicode": "café"}
        )
        
        json_str = task.to_json()
        restored = Task.from_json(json_str)
        
        assert restored.args == task.args
        assert restored.kwargs == task.kwargs
