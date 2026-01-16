"""
Task reloading without restarting workers.
"""
import importlib
import sys
from typing import List, Optional
from jobqueue.core.task_registry import task_registry
from jobqueue.core.discovery import discover_tasks_in_module
from jobqueue.utils.logger import log


class TaskReloader:
    """
    Reloads task modules without restarting the worker process.
    """
    
    def __init__(self):
        """Initialize task reloader."""
        self._loaded_modules = {}
    
    def reload_module(self, module_name: str) -> bool:
        """
        Reload a module and re-register its tasks.
        
        Args:
            module_name: Module to reload (e.g., 'app.tasks')
            
        Returns:
            True if reload successful
            
        Example:
            reloader = TaskReloader()
            success = reloader.reload_module('my_app.tasks')
        """
        try:
            # Check if module is loaded
            if module_name not in sys.modules:
                log.warning(f"Module {module_name} not loaded, importing instead")
                discover_tasks_in_module(module_name)
                self._loaded_modules[module_name] = True
                return True
            
            # Get the module
            module = sys.modules[module_name]
            
            # Before reloading, get list of existing tasks from this module
            old_tasks = []
            for task_name, func in task_registry._tasks.items():
                if hasattr(func, '__module__') and func.__module__ == module_name:
                    old_tasks.append(task_name)
            
            # Unregister old tasks from this module
            for task_name in old_tasks:
                task_registry.unregister(task_name)
                log.debug(f"Unregistered old task: {task_name}")
            
            # Reload the module
            importlib.reload(module)
            log.info(f"Reloaded module: {module_name}")
            
            # Discover and register new tasks
            new_tasks = discover_tasks_in_module(module_name)
            for task_func in new_tasks:
                task_registry.register_from_decorator(task_func)
            
            self._loaded_modules[module_name] = True
            
            log.info(
                f"Successfully reloaded {module_name}: {len(new_tasks)} tasks registered",
                extra={"module": module_name, "task_count": len(new_tasks)}
            )
            
            return True
            
        except Exception as e:
            log.error(f"Failed to reload module {module_name}: {e}")
            return False
    
    def reload_all(self, module_names: List[str]) -> int:
        """
        Reload multiple modules.
        
        Args:
            module_names: List of modules to reload
            
        Returns:
            Number of successfully reloaded modules
            
        Example:
            reloader = TaskReloader()
            count = reloader.reload_all(['app.tasks', 'workers.tasks'])
        """
        success_count = 0
        
        for module_name in module_names:
            if self.reload_module(module_name):
                success_count += 1
        
        log.info(f"Reloaded {success_count}/{len(module_names)} modules")
        return success_count
    
    def get_loaded_modules(self) -> List[str]:
        """
        Get list of loaded module names.
        
        Returns:
            List of module names
        """
        return list(self._loaded_modules.keys())
    
    def is_module_loaded(self, module_name: str) -> bool:
        """
        Check if a module has been loaded.
        
        Args:
            module_name: Module name to check
            
        Returns:
            True if module has been loaded
        """
        return module_name in self._loaded_modules


class HotReloadManager:
    """
    Manager for hot-reloading tasks in running workers.
    """
    
    def __init__(self):
        """Initialize hot reload manager."""
        self.reloader = TaskReloader()
        self.watch_modules = []
    
    def watch(self, module_name: str) -> None:
        """
        Add a module to the watch list for hot reloading.
        
        Args:
            module_name: Module to watch
        """
        if module_name not in self.watch_modules:
            self.watch_modules.append(module_name)
            log.info(f"Watching module for hot reload: {module_name}")
    
    def unwatch(self, module_name: str) -> None:
        """
        Remove a module from the watch list.
        
        Args:
            module_name: Module to stop watching
        """
        if module_name in self.watch_modules:
            self.watch_modules.remove(module_name)
            log.info(f"Stopped watching module: {module_name}")
    
    def reload_watched_modules(self) -> int:
        """
        Reload all watched modules.
        
        Returns:
            Number of successfully reloaded modules
        """
        if not self.watch_modules:
            log.warning("No modules are being watched")
            return 0
        
        return self.reloader.reload_all(self.watch_modules)
    
    def reload_module(self, module_name: str) -> bool:
        """
        Reload a specific module.
        
        Args:
            module_name: Module to reload
            
        Returns:
            True if successful
        """
        return self.reloader.reload_module(module_name)
    
    def get_watched_modules(self) -> List[str]:
        """
        Get list of watched modules.
        
        Returns:
            List of module names being watched
        """
        return self.watch_modules.copy()


def reload_task_module(module_name: str) -> bool:
    """
    Convenience function to reload a task module.
    
    Args:
        module_name: Module to reload
        
    Returns:
        True if successful
        
    Example:
        # In a running worker, reload tasks
        reload_task_module('app.tasks')
    """
    reloader = TaskReloader()
    return reloader.reload_module(module_name)


# Global hot reload manager
hot_reload_manager = HotReloadManager()
