"""
Task registry for registering and executing tasks.
"""
from typing import Callable, Dict, Any, Optional
from jobqueue.utils.logger import log


class TaskRegistry:
    """
    Registry for task functions that can be executed by workers.
    """
    
    def __init__(self):
        """Initialize task registry."""
        self._tasks: Dict[str, Callable] = {}
    
    def register(self, name: Optional[str] = None):
        """
        Decorator to register a task function.
        
        Args:
            name: Optional custom name for the task
            
        Returns:
            Decorator function
            
        Example:
            @task_registry.register()
            def process_data(x, y):
                return x + y
                
            @task_registry.register("custom_name")
            def another_task():
                pass
        """
        def decorator(func: Callable) -> Callable:
            task_name = name or func.__name__
            self._tasks[task_name] = func
            log.info(f"Registered task: {task_name}")
            return func
        return decorator
    
    def register_function(self, name: str, func: Callable) -> None:
        """
        Register a task function programmatically.
        
        Args:
            name: Task name
            func: Task function
        """
        self._tasks[name] = func
        log.info(f"Registered task: {name}")
    
    def unregister(self, name: str) -> None:
        """
        Unregister a task.
        
        Args:
            name: Task name to unregister
        """
        if name in self._tasks:
            del self._tasks[name]
            log.info(f"Unregistered task: {name}")
    
    def get_task(self, name: str) -> Optional[Callable]:
        """
        Get a registered task function.
        
        Args:
            name: Task name
            
        Returns:
            Task function or None if not found
        """
        return self._tasks.get(name)
    
    def execute(self, name: str, *args, **kwargs) -> Any:
        """
        Execute a registered task.
        
        Args:
            name: Task name
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Task result
            
        Raises:
            ValueError: If task not found
            Exception: Any exception raised by the task
        """
        task_func = self.get_task(name)
        
        if task_func is None:
            raise ValueError(f"Task '{name}' not found in registry")
        
        log.debug(
            f"Executing task: {name}",
            extra={"args": args, "kwargs": kwargs}
        )
        
        return task_func(*args, **kwargs)
    
    def list_tasks(self) -> list:
        """
        List all registered tasks.
        
        Returns:
            List of task names
        """
        return list(self._tasks.keys())
    
    def is_registered(self, name: str) -> bool:
        """
        Check if a task is registered.
        
        Args:
            name: Task name
            
        Returns:
            True if registered, False otherwise
        """
        return name in self._tasks
    
    def clear(self) -> None:
        """Clear all registered tasks."""
        self._tasks.clear()
        log.info("Cleared all registered tasks")


# Global task registry instance
task_registry = TaskRegistry()


# Register some default example tasks
@task_registry.register()
def example_task(x: int, y: int, multiplier: int = 1) -> int:
    """Example task that adds and multiplies numbers."""
    return (x + y) * multiplier


@task_registry.register()
def process_data(data: list, multiplier: int = 2) -> list:
    """Example task that processes data."""
    return [item * multiplier for item in data]


@task_registry.register()
def fetch_data(url: str = "https://example.com") -> dict:
    """Example task that simulates data fetching."""
    return {"url": url, "status": "success"}


@task_registry.register()
def long_running_task(duration: int = 10) -> str:
    """Example task that simulates long running operation."""
    import time
    time.sleep(min(duration, 30))  # Cap at 30 seconds for safety
    return f"Task completed after {duration} seconds"
