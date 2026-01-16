"""
Support for async task execution.
"""
import asyncio
import inspect
from typing import Callable, Any
from jobqueue.utils.logger import log


def is_async_function(func: Callable) -> bool:
    """
    Check if a function is async.
    
    Args:
        func: Function to check
        
    Returns:
        True if function is async (coroutine function)
        
    Example:
        async def async_task():
            pass
            
        def sync_task():
            pass
            
        print(is_async_function(async_task))  # True
        print(is_async_function(sync_task))   # False
    """
    return asyncio.iscoroutinefunction(func)


def run_async_task(func: Callable, *args, **kwargs) -> Any:
    """
    Run an async function synchronously.
    Creates a new event loop if needed.
    
    Args:
        func: Async function to run
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Result of the async function
        
    Example:
        async def fetch_data(url: str):
            # Async logic here
            return "data"
            
        result = run_async_task(fetch_data, "https://example.com")
    """
    if not is_async_function(func):
        raise ValueError(f"Function {func.__name__} is not async")
    
    log.debug(f"Running async task: {func.__name__}")
    
    # Try to get existing event loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(func(*args, **kwargs))
        finally:
            loop.close()
        return result
    else:
        # Already in an event loop, run in executor
        return loop.run_until_complete(func(*args, **kwargs))


async def execute_async(func: Callable, *args, **kwargs) -> Any:
    """
    Execute a function asynchronously.
    Works with both sync and async functions.
    
    Args:
        func: Function to execute
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Result of the function
        
    Example:
        async def process():
            result = await execute_async(some_func, arg1, arg2)
            return result
    """
    if is_async_function(func):
        return await func(*args, **kwargs)
    else:
        # Run sync function in executor to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


def execute_task_sync_or_async(func: Callable, *args, **kwargs) -> Any:
    """
    Execute a task whether it's sync or async.
    Automatically detects function type and executes appropriately.
    
    Args:
        func: Function to execute (sync or async)
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Result of the function
        
    Example:
        # Works with sync functions
        def sync_task(x):
            return x * 2
            
        result = execute_task_sync_or_async(sync_task, 5)  # Returns 10
        
        # Also works with async functions
        async def async_task(x):
            await asyncio.sleep(0.1)
            return x * 2
            
        result = execute_task_sync_or_async(async_task, 5)  # Returns 10
    """
    if is_async_function(func):
        # Run async function
        return run_async_task(func, *args, **kwargs)
    else:
        # Run sync function normally
        return func(*args, **kwargs)


class AsyncTaskExecutor:
    """
    Executor for running both sync and async tasks.
    """
    
    def __init__(self):
        """Initialize async task executor."""
        self._loop = None
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a task (sync or async).
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Task result
        """
        if is_async_function(func):
            log.debug(f"Executing async task: {func.__name__}")
            return self._run_async(func, *args, **kwargs)
        else:
            log.debug(f"Executing sync task: {func.__name__}")
            return func(*args, **kwargs)
    
    def _run_async(self, func: Callable, *args, **kwargs) -> Any:
        """
        Run async function in event loop.
        
        Args:
            func: Async function
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
        """
        try:
            loop = asyncio.get_running_loop()
            # We're already in a loop, can't use run_until_complete
            raise RuntimeError("Cannot run async task in existing event loop")
        except RuntimeError:
            # No running loop, create new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(func(*args, **kwargs))
            finally:
                loop.close()
    
    def is_async(self, func: Callable) -> bool:
        """
        Check if function is async.
        
        Args:
            func: Function to check
            
        Returns:
            True if async
        """
        return is_async_function(func)


# Global async executor instance
async_executor = AsyncTaskExecutor()
