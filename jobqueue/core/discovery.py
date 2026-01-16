"""
Auto-discovery of task functions from modules.
"""
import os
import sys
import importlib
import inspect
from pathlib import Path
from typing import List, Callable, Optional
from jobqueue.core.decorators import is_task
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


def discover_tasks_in_module(module_name: str) -> List[Callable]:
    """
    Discover all tasks in a module.
    
    Args:
        module_name: Module name to import (e.g., 'my_app.tasks')
        
    Returns:
        List of task functions found
        
    Example:
        tasks = discover_tasks_in_module('my_app.tasks')
        for task in tasks:
            print(f"Found task: {task.__name__}")
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        log.error(f"Failed to import module {module_name}: {e}")
        return []
    
    tasks = []
    
    # Inspect all members of the module
    for name, obj in inspect.getmembers(module):
        # Check if it's a function and has task decorator
        if inspect.isfunction(obj) and is_task(obj):
            tasks.append(obj)
            log.debug(f"Discovered task: {name} in {module_name}")
    
    log.info(f"Discovered {len(tasks)} tasks in module {module_name}")
    return tasks


def discover_tasks_in_directory(directory: str, pattern: str = "tasks.py") -> List[Callable]:
    """
    Discover all tasks in Python files within a directory.
    
    Args:
        directory: Directory path to search
        pattern: File name pattern to match (default: tasks.py)
        
    Returns:
        List of task functions found
        
    Example:
        tasks = discover_tasks_in_directory('my_app', pattern='*_tasks.py')
    """
    directory_path = Path(directory)
    
    if not directory_path.exists():
        log.error(f"Directory not found: {directory}")
        return []
    
    all_tasks = []
    
    # Find all matching Python files
    if '*' in pattern:
        # Pattern with wildcard
        python_files = list(directory_path.rglob(pattern))
    else:
        # Exact filename
        python_files = list(directory_path.rglob(pattern))
    
    log.info(f"Found {len(python_files)} files matching '{pattern}' in {directory}")
    
    # Add directory to Python path if not already there
    dir_str = str(directory_path.absolute())
    if dir_str not in sys.path:
        sys.path.insert(0, dir_str)
    
    for file_path in python_files:
        # Convert file path to module name
        relative_path = file_path.relative_to(directory_path)
        module_parts = list(relative_path.parts[:-1]) + [relative_path.stem]
        module_name = '.'.join(module_parts)
        
        tasks = discover_tasks_in_module(module_name)
        all_tasks.extend(tasks)
    
    return all_tasks


def auto_discover_and_register(
    modules: Optional[List[str]] = None,
    directories: Optional[List[str]] = None,
    pattern: str = "tasks.py"
) -> int:
    """
    Auto-discover tasks and register them.
    
    Args:
        modules: List of module names to search (e.g., ['app.tasks', 'workers.tasks'])
        directories: List of directory paths to search
        pattern: File name pattern for directory search
        
    Returns:
        Number of tasks registered
        
    Example:
        # Discover from specific modules
        count = auto_discover_and_register(modules=['app.tasks', 'workers.tasks'])
        
        # Discover from directories
        count = auto_discover_and_register(directories=['./tasks'], pattern='*_tasks.py')
    """
    discovered_tasks = []
    
    # Discover from modules
    if modules:
        for module_name in modules:
            tasks = discover_tasks_in_module(module_name)
            discovered_tasks.extend(tasks)
    
    # Discover from directories
    if directories:
        for directory in directories:
            tasks = discover_tasks_in_directory(directory, pattern)
            discovered_tasks.extend(tasks)
    
    # Register all discovered tasks
    registered_count = 0
    for task_func in discovered_tasks:
        try:
            task_registry.register_from_decorator(task_func)
            registered_count += 1
        except Exception as e:
            log.error(f"Failed to register task {task_func.__name__}: {e}")
    
    log.info(f"Auto-discovered and registered {registered_count} tasks")
    return registered_count


def discover_tasks_in_package(package_name: str, recursive: bool = True) -> List[Callable]:
    """
    Discover all tasks in a package.
    
    Args:
        package_name: Package name (e.g., 'myapp.tasks')
        recursive: Whether to search subpackages recursively
        
    Returns:
        List of task functions found
        
    Example:
        tasks = discover_tasks_in_package('myapp.tasks', recursive=True)
    """
    try:
        package = importlib.import_module(package_name)
    except ImportError as e:
        log.error(f"Failed to import package {package_name}: {e}")
        return []
    
    all_tasks = []
    
    # Get package path
    if hasattr(package, '__path__'):
        package_path = Path(package.__path__[0])
        
        # Find all Python files in package
        if recursive:
            python_files = list(package_path.rglob('*.py'))
        else:
            python_files = list(package_path.glob('*.py'))
        
        for file_path in python_files:
            if file_path.name == '__init__.py':
                continue
            
            # Build module name
            relative_path = file_path.relative_to(package_path.parent)
            module_parts = list(relative_path.parts[:-1]) + [relative_path.stem]
            module_name = '.'.join(module_parts)
            
            tasks = discover_tasks_in_module(module_name)
            all_tasks.extend(tasks)
    
    return all_tasks


class TaskDiscovery:
    """
    Task discovery manager.
    """
    
    def __init__(self):
        """Initialize task discovery manager."""
        self.discovered_modules = set()
    
    def discover_from_module(self, module_name: str, register: bool = True) -> List[Callable]:
        """
        Discover tasks from a module.
        
        Args:
            module_name: Module to discover from
            register: Whether to automatically register discovered tasks
            
        Returns:
            List of discovered tasks
        """
        tasks = discover_tasks_in_module(module_name)
        self.discovered_modules.add(module_name)
        
        if register:
            for task in tasks:
                try:
                    task_registry.register_from_decorator(task)
                except Exception as e:
                    log.error(f"Failed to register task: {e}")
        
        return tasks
    
    def discover_from_directory(
        self,
        directory: str,
        pattern: str = "tasks.py",
        register: bool = True
    ) -> List[Callable]:
        """
        Discover tasks from a directory.
        
        Args:
            directory: Directory to search
            pattern: File pattern to match
            register: Whether to automatically register discovered tasks
            
        Returns:
            List of discovered tasks
        """
        tasks = discover_tasks_in_directory(directory, pattern)
        
        if register:
            for task in tasks:
                try:
                    task_registry.register_from_decorator(task)
                except Exception as e:
                    log.error(f"Failed to register task: {e}")
        
        return tasks
    
    def get_discovered_modules(self) -> List[str]:
        """
        Get list of discovered modules.
        
        Returns:
            List of module names
        """
        return list(self.discovered_modules)


# Global task discovery instance
task_discovery = TaskDiscovery()
