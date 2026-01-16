"""
Task versioning for handling signature changes.
"""
import hashlib
import inspect
from typing import Callable, Optional, Dict
from jobqueue.utils.logger import log


def get_function_signature_hash(func: Callable) -> str:
    """
    Generate a hash of function signature for versioning.
    
    Args:
        func: Function to hash
        
    Returns:
        SHA256 hash of the function signature
        
    Example:
        def my_func(a: int, b: str = "default"):
            pass
            
        sig_hash = get_function_signature_hash(my_func)
    """
    sig = inspect.signature(func)
    sig_str = str(sig)
    
    # Include function name in hash
    full_sig = f"{func.__name__}{sig_str}"
    
    return hashlib.sha256(full_sig.encode()).hexdigest()


def get_function_version_info(func: Callable) -> Dict:
    """
    Get complete version information for a function.
    
    Args:
        func: Function to inspect
        
    Returns:
        Dictionary with version information
        
    Example:
        info = get_function_version_info(my_func)
        print(info["signature_hash"])
        print(info["parameter_count"])
    """
    sig = inspect.signature(func)
    params = sig.parameters
    
    return {
        "name": func.__name__,
        "module": func.__module__,
        "signature": str(sig),
        "signature_hash": get_function_signature_hash(func),
        "parameter_count": len(params),
        "parameters": {
            name: {
                "kind": str(param.kind),
                "default": param.default if param.default != inspect.Parameter.empty else None,
                "annotation": str(param.annotation) if param.annotation != inspect.Parameter.empty else None
            }
            for name, param in params.items()
        }
    }


class TaskVersionManager:
    """
    Manages task versions and signature changes.
    """
    
    def __init__(self):
        """Initialize task version manager."""
        self._versions: Dict[str, Dict] = {}
        self._signature_hashes: Dict[str, str] = {}
    
    def register_version(self, task_name: str, func: Callable, version: str = "1.0") -> None:
        """
        Register a task version.
        
        Args:
            task_name: Task name
            func: Task function
            version: Version string
            
        Example:
            manager = TaskVersionManager()
            manager.register_version("process_data", process_data_func, "1.0")
        """
        version_info = get_function_version_info(func)
        version_info["version"] = version
        
        key = f"{task_name}:{version}"
        self._versions[key] = version_info
        self._signature_hashes[task_name] = version_info["signature_hash"]
        
        log.info(
            f"Registered task version: {task_name} v{version}",
            extra={
                "task": task_name,
                "version": version,
                "signature_hash": version_info["signature_hash"]
            }
        )
    
    def get_version_info(self, task_name: str, version: str) -> Optional[Dict]:
        """
        Get version information for a task.
        
        Args:
            task_name: Task name
            version: Version string
            
        Returns:
            Version information or None if not found
        """
        key = f"{task_name}:{version}"
        return self._versions.get(key)
    
    def has_signature_changed(self, task_name: str, func: Callable) -> bool:
        """
        Check if task signature has changed.
        
        Args:
            task_name: Task name
            func: Current function
            
        Returns:
            True if signature has changed
            
        Example:
            if manager.has_signature_changed("process_data", new_func):
                print("Signature changed! Need to update version.")
        """
        if task_name not in self._signature_hashes:
            # No previous version, assume changed
            return True
        
        old_hash = self._signature_hashes[task_name]
        new_hash = get_function_signature_hash(func)
        
        return old_hash != new_hash
    
    def update_version(self, task_name: str, func: Callable, new_version: str) -> None:
        """
        Update task to a new version.
        
        Args:
            task_name: Task name
            func: New function
            new_version: New version string
        """
        self.register_version(task_name, func, new_version)
        
        log.info(
            f"Updated task {task_name} to version {new_version}",
            extra={"task": task_name, "version": new_version}
        )
    
    def get_all_versions(self, task_name: str) -> list:
        """
        Get all versions of a task.
        
        Args:
            task_name: Task name
            
        Returns:
            List of version strings
        """
        versions = []
        for key in self._versions.keys():
            name, version = key.split(":", 1)
            if name == task_name:
                versions.append(version)
        return sorted(versions)
    
    def compare_signatures(self, func1: Callable, func2: Callable) -> Dict:
        """
        Compare signatures of two functions.
        
        Args:
            func1: First function
            func2: Second function
            
        Returns:
            Dictionary describing differences
            
        Example:
            diff = manager.compare_signatures(old_func, new_func)
            if diff["changed"]:
                print("Parameters changed:", diff["parameter_changes"])
        """
        info1 = get_function_version_info(func1)
        info2 = get_function_version_info(func2)
        
        param_changes = {
            "added": [],
            "removed": [],
            "modified": []
        }
        
        params1 = set(info1["parameters"].keys())
        params2 = set(info2["parameters"].keys())
        
        param_changes["added"] = list(params2 - params1)
        param_changes["removed"] = list(params1 - params2)
        
        # Check for modified parameters
        for param in params1 & params2:
            if info1["parameters"][param] != info2["parameters"][param]:
                param_changes["modified"].append(param)
        
        return {
            "changed": info1["signature_hash"] != info2["signature_hash"],
            "parameter_count_changed": info1["parameter_count"] != info2["parameter_count"],
            "parameter_changes": param_changes,
            "old_signature": info1["signature"],
            "new_signature": info2["signature"]
        }


def version_compatible(task_func: Callable, stored_version: str, current_version: str) -> bool:
    """
    Check if a task version is compatible.
    Uses semantic versioning rules.
    
    Args:
        task_func: Task function
        stored_version: Version stored with the task
        current_version: Current task version
        
    Returns:
        True if compatible
        
    Example:
        # Task was created with v1.2.0, current is v1.3.0
        if version_compatible(func, "1.2.0", "1.3.0"):
            # Compatible, can execute
            pass
    """
    try:
        stored_parts = [int(x) for x in stored_version.split('.')]
        current_parts = [int(x) for x in current_version.split('.')]
        
        # Major version must match
        if stored_parts[0] != current_parts[0]:
            return False
        
        # Minor version of current must be >= stored
        if len(stored_parts) > 1 and len(current_parts) > 1:
            if current_parts[1] < stored_parts[1]:
                return False
        
        return True
        
    except Exception as e:
        log.error(f"Error checking version compatibility: {e}")
        return False


# Global task version manager
task_version_manager = TaskVersionManager()
