"""
Task decorator for registering functions as tasks.
"""
from typing import Callable, Optional
from functools import wraps
from jobqueue.utils.logger import log


def task(
    name: Optional[str] = None,
    max_retries: int = 3,
    timeout: int = 300,
    priority: str = "medium",
    queue: str = "default"
):
    """
    Decorator to register a function as a task.
    
    Args:
        name: Custom task name (defaults to function name)
        max_retries: Maximum retry attempts
        timeout: Task timeout in seconds
        priority: Task priority (high, medium, low)
        queue: Queue name
        
    Returns:
        Decorated function
        
    Example:
        @task(name="send_email", max_retries=3)
        def send_email(to: str, subject: str, body: str):
            # Send email logic
            pass
            
        @task()
        def process_data(data: list):
            return [x * 2 for x in data]
    """
    def decorator(func: Callable) -> Callable:
        # Use function name if no custom name provided
        task_name = name or func.__name__
        
        # Store task metadata on the function
        func._task_metadata = {
            "name": task_name,
            "max_retries": max_retries,
            "timeout": timeout,
            "priority": priority,
            "queue": queue,
            "original_name": func.__name__,
            "module": func.__module__,
        }
        
        # Mark as registered task
        func._is_task = True
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Function can still be called directly
            return func(*args, **kwargs)
        
        # Preserve metadata on wrapper
        wrapper._task_metadata = func._task_metadata
        wrapper._is_task = True
        
        log.debug(
            f"Decorated function '{func.__name__}' as task '{task_name}'",
            extra={"function": func.__name__, "task_name": task_name}
        )
        
        return wrapper
    
    return decorator


def get_task_metadata(func: Callable) -> Optional[dict]:
    """
    Get task metadata from a decorated function.
    
    Args:
        func: Function to inspect
        
    Returns:
        Task metadata dict or None if not a task
        
    Example:
        @task(name="my_task")
        def my_func():
            pass
            
        metadata = get_task_metadata(my_func)
        print(metadata["name"])  # "my_task"
    """
    if hasattr(func, "_task_metadata"):
        return func._task_metadata
    return None


def is_task(func: Callable) -> bool:
    """
    Check if a function is decorated as a task.
    
    Args:
        func: Function to check
        
    Returns:
        True if function is a task
        
    Example:
        @task()
        def my_task():
            pass
            
        print(is_task(my_task))  # True
        print(is_task(lambda: None))  # False
    """
    return hasattr(func, "_is_task") and func._is_task
