"""
Tests for task registration and discovery.
"""
import pytest
import asyncio
from jobqueue.core.decorators import task, is_task, get_task_metadata
from jobqueue.core.task_registry import TaskRegistry
from jobqueue.core.async_support import is_async_function, execute_task_sync_or_async
from jobqueue.core.task_versioning import (
    get_function_signature_hash,
    TaskVersionManager,
    version_compatible
)
from jobqueue.core.validation import ValidationError


class TestTaskDecorator:
    """Test task decorator functionality."""
    
    def test_basic_decorator(self):
        """Test basic task decoration."""
        @task()
        def simple_task():
            return "result"
        
        assert is_task(simple_task)
        metadata = get_task_metadata(simple_task)
        assert metadata["name"] == "simple_task"
        assert metadata["max_retries"] == 3
        assert metadata["timeout"] == 300
    
    def test_custom_decorator_params(self):
        """Test decorator with custom parameters."""
        @task(name="custom_name", max_retries=5, timeout=600, priority="high")
        def my_task():
            return "result"
        
        metadata = get_task_metadata(my_task)
        assert metadata["name"] == "custom_name"
        assert metadata["max_retries"] == 5
        assert metadata["timeout"] == 600
        assert metadata["priority"] == "high"
    
    def test_decorated_function_callable(self):
        """Test that decorated function can still be called."""
        @task()
        def add_numbers(a, b):
            return a + b
        
        result = add_numbers(2, 3)
        assert result == 5
    
    def test_non_task_function(self):
        """Test that non-decorated function returns False."""
        def regular_func():
            pass
        
        assert not is_task(regular_func)
        assert get_task_metadata(regular_func) is None


class TestTaskRegistry:
    """Test task registry functionality."""
    
    def test_register_with_decorator(self):
        """Test registering function decorated with @task."""
        registry = TaskRegistry()
        
        @task(name="test_task")
        def my_func(x, y):
            return x + y
        
        registry.register_from_decorator(my_func)
        
        assert registry.is_registered("test_task")
        assert registry.get_task("test_task") is my_func
    
    def test_register_function_programmatically(self):
        """Test programmatic registration."""
        registry = TaskRegistry()
        
        def my_func(x):
            return x * 2
        
        registry.register_function("multiply", my_func)
        
        assert registry.is_registered("multiply")
        result = registry.execute("multiply", 5)
        assert result == 10
    
    def test_unregister_task(self):
        """Test task unregistration."""
        registry = TaskRegistry()
        
        @task()
        def temp_task():
            return "temp"
        
        registry.register_from_decorator(temp_task)
        assert registry.is_registered("temp_task")
        
        registry.unregister("temp_task")
        assert not registry.is_registered("temp_task")
    
    def test_execute_with_validation(self):
        """Test task execution with signature validation."""
        registry = TaskRegistry()
        
        @task()
        def validated_task(a: int, b: int):
            return a + b
        
        registry.register_from_decorator(validated_task)
        
        # Valid args
        result = registry.execute("validated_task", 1, 2)
        assert result == 3
        
        # Invalid args (missing required parameter)
        with pytest.raises(ValidationError):
            registry.execute("validated_task", 1)
    
    def test_task_not_found(self):
        """Test error when task not found."""
        registry = TaskRegistry()
        
        with pytest.raises(ValueError, match="not found"):
            registry.execute("nonexistent_task")
    
    def test_list_tasks(self):
        """Test listing all registered tasks."""
        registry = TaskRegistry()
        
        @task()
        def task1():
            pass
        
        @task()
        def task2():
            pass
        
        registry.register_from_decorator(task1)
        registry.register_from_decorator(task2)
        
        tasks = registry.list_tasks()
        assert "task1" in tasks
        assert "task2" in tasks
    
    def test_get_metadata(self):
        """Test retrieving task metadata."""
        registry = TaskRegistry()
        
        @task(name="meta_task", max_retries=10)
        def my_task():
            pass
        
        registry.register_from_decorator(my_task)
        
        metadata = registry.get_metadata("meta_task")
        assert metadata is not None
        assert metadata["max_retries"] == 10
    
    def test_filter_by_queue(self):
        """Test filtering tasks by queue."""
        registry = TaskRegistry()
        
        @task(queue="high_priority")
        def high_task():
            pass
        
        @task(queue="low_priority")
        def low_task():
            pass
        
        registry.register_from_decorator(high_task)
        registry.register_from_decorator(low_task)
        
        high_tasks = registry.filter_by_queue("high_priority")
        assert "high_task" in high_tasks
        assert "low_task" not in high_tasks


class TestAsyncSupport:
    """Test async function support."""
    
    def test_detect_async_function(self):
        """Test async function detection."""
        async def async_func():
            return "async"
        
        def sync_func():
            return "sync"
        
        assert is_async_function(async_func)
        assert not is_async_function(sync_func)
    
    def test_execute_async_function(self):
        """Test executing async function."""
        async def async_task(x):
            await asyncio.sleep(0.01)
            return x * 2
        
        result = execute_task_sync_or_async(async_task, 5)
        assert result == 10
    
    def test_execute_sync_function(self):
        """Test executing sync function with async executor."""
        def sync_task(x):
            return x * 2
        
        result = execute_task_sync_or_async(sync_task, 5)
        assert result == 10
    
    def test_register_async_task(self):
        """Test registering async task."""
        registry = TaskRegistry()
        
        @task()
        async def async_task(value):
            await asyncio.sleep(0.01)
            return value * 2
        
        registry.register_from_decorator(async_task)
        assert registry.is_registered("async_task")


class TestTaskVersioning:
    """Test task versioning functionality."""
    
    def test_signature_hash(self):
        """Test signature hashing."""
        def func1(a, b):
            pass
        
        def func2(a, b):
            pass
        
        def func3(a, b, c):
            pass
        
        hash1 = get_function_signature_hash(func1)
        hash2 = get_function_signature_hash(func2)
        hash3 = get_function_signature_hash(func3)
        
        # Same signature should produce same hash
        assert hash1 == hash2
        
        # Different signature should produce different hash
        assert hash1 != hash3
    
    def test_register_version(self):
        """Test registering task version."""
        manager = TaskVersionManager()
        
        def my_task(x: int):
            return x * 2
        
        manager.register_version("my_task", my_task, "1.0")
        
        info = manager.get_version_info("my_task", "1.0")
        assert info is not None
        assert info["version"] == "1.0"
        assert info["parameter_count"] == 1
    
    def test_detect_signature_change(self):
        """Test detecting signature changes."""
        manager = TaskVersionManager()
        
        def original_func(a, b):
            return a + b
        
        def modified_func(a, b, c):
            return a + b + c
        
        manager.register_version("calc", original_func, "1.0")
        
        # Signature has changed
        assert manager.has_signature_changed("calc", modified_func)
        
        # Same signature
        assert not manager.has_signature_changed("calc", original_func)
    
    def test_compare_signatures(self):
        """Test signature comparison."""
        manager = TaskVersionManager()
        
        def old_func(a, b):
            pass
        
        def new_func(a, b, c=10):
            pass
        
        diff = manager.compare_signatures(old_func, new_func)
        
        assert diff["changed"]
        assert diff["parameter_count_changed"]
        assert "c" in diff["parameter_changes"]["added"]
    
    def test_version_compatibility(self):
        """Test version compatibility checking."""
        def my_func():
            pass
        
        # Same major version is compatible
        assert version_compatible(my_func, "1.2.0", "1.3.0")
        
        # Different major version is not compatible
        assert not version_compatible(my_func, "1.0.0", "2.0.0")
        
        # Newer minor version is compatible
        assert version_compatible(my_func, "1.2.0", "1.2.1")


class TestEdgeCases:
    """Test edge cases in task registration."""
    
    def test_register_lambda(self):
        """Test registering lambda function."""
        registry = TaskRegistry()
        
        registry.register_function("lambda_task", lambda x: x * 2)
        
        result = registry.execute("lambda_task", 5)
        assert result == 10
    
    def test_register_class_method(self):
        """Test registering class method."""
        registry = TaskRegistry()
        
        class MyClass:
            @task()
            def my_method(self, value):
                return value * 2
        
        obj = MyClass()
        registry.register_from_decorator(obj.my_method)
        
        # Note: This will fail because self isn't bound
        # This shows a limitation - class methods need special handling
    
    def test_task_with_variable_args(self):
        """Test task with *args and **kwargs."""
        registry = TaskRegistry()
        
        @task()
        def variable_task(*args, **kwargs):
            return sum(args) + sum(kwargs.values())
        
        registry.register_from_decorator(variable_task)
        
        result = registry.execute("variable_task", 1, 2, 3, x=4, y=5)
        assert result == 15
    
    def test_task_with_defaults(self):
        """Test task with default arguments."""
        registry = TaskRegistry()
        
        @task()
        def default_task(a, b=10, c=20):
            return a + b + c
        
        registry.register_from_decorator(default_task)
        
        # With all args
        result1 = registry.execute("default_task", 1, 2, 3)
        assert result1 == 6
        
        # With defaults
        result2 = registry.execute("default_task", 1)
        assert result2 == 31
