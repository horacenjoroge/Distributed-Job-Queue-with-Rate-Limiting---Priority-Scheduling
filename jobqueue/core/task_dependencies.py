"""
Task dependency management using Redis.
Supports task chaining, DAG validation, and dependency resolution.
"""
from typing import List, Set, Optional, Dict
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.utils.logger import log


class TaskDependencyGraph:
    """
    Manages task dependencies using Redis.
    Stores task relationships as a directed acyclic graph (DAG).
    """
    
    def __init__(self):
        """Initialize task dependency graph."""
        log.info("Initialized task dependency graph")
    
    def _get_dependency_key(self, task_id: str) -> str:
        """Get Redis key for task dependencies."""
        return f"task:dependencies:{task_id}"
    
    def _get_dependents_key(self, task_id: str) -> str:
        """Get Redis key for tasks that depend on this task."""
        return f"task:dependents:{task_id}"
    
    def _get_task_status_key(self, task_id: str) -> str:
        """Get Redis key for task status tracking."""
        return f"task:status:{task_id}"
    
    def add_dependencies(self, task: Task) -> bool:
        """
        Add task dependencies to the graph.
        
        Args:
            task: Task with dependencies
            
        Returns:
            True if added successfully
            
        Raises:
            ValueError: If circular dependency detected
        """
        if not task.depends_on:
            return True
        
        # Check for circular dependencies
        if self._has_circular_dependency(task.id, task.depends_on):
            raise ValueError(
                f"Circular dependency detected for task {task.id}"
            )
        
        # Store dependencies
        dep_key = self._get_dependency_key(task.id)
        redis_broker.client.delete(dep_key)
        
        for parent_id in task.depends_on:
            redis_broker.client.sadd(dep_key, parent_id)
            
            # Also store reverse mapping (who depends on this task)
            dependents_key = self._get_dependents_key(parent_id)
            redis_broker.client.sadd(dependents_key, task.id)
        
        log.info(
            f"Added dependencies for task {task.id}",
            extra={
                "task_id": task.id,
                "depends_on": task.depends_on
            }
        )
        
        return True
    
    def get_dependencies(self, task_id: str) -> List[str]:
        """
        Get list of task IDs this task depends on.
        
        Args:
            task_id: Task ID
            
        Returns:
            List of parent task IDs
        """
        key = self._get_dependency_key(task_id)
        deps = redis_broker.client.smembers(key)
        return [dep.decode() if isinstance(dep, bytes) else dep for dep in deps]
    
    def get_dependents(self, task_id: str) -> List[str]:
        """
        Get list of task IDs that depend on this task.
        
        Args:
            task_id: Task ID
            
        Returns:
            List of child task IDs
        """
        key = self._get_dependents_key(task_id)
        deps = redis_broker.client.smembers(key)
        return [dep.decode() if isinstance(dep, bytes) else dep for dep in deps]
    
    def _has_circular_dependency(
        self,
        task_id: str,
        depends_on: List[str],
        visited: Optional[Set[str]] = None
    ) -> bool:
        """
        Check for circular dependencies using DFS.
        
        Args:
            task_id: Current task ID
            depends_on: List of parent task IDs
            visited: Set of visited task IDs
            
        Returns:
            True if circular dependency detected
        """
        if visited is None:
            visited = set()
        
        # Check if task_id appears in its own dependencies
        if task_id in depends_on:
            return True
        
        visited.add(task_id)
        
        # Check each dependency recursively
        for parent_id in depends_on:
            if parent_id in visited:
                return True
            
            # Get parent's dependencies
            parent_deps = self.get_dependencies(parent_id)
            
            if parent_deps:
                if self._has_circular_dependency(parent_id, parent_deps, visited.copy()):
                    return True
        
        return False
    
    def are_dependencies_satisfied(self, task_id: str) -> tuple[bool, List[str]]:
        """
        Check if all dependencies are satisfied (completed successfully).
        
        Args:
            task_id: Task ID to check
            
        Returns:
            Tuple of (all_satisfied, pending_dependencies)
        """
        dependencies = self.get_dependencies(task_id)
        
        if not dependencies:
            return True, []
        
        pending = []
        
        for dep_id in dependencies:
            status = self.get_task_status(dep_id)
            
            if status != TaskStatus.SUCCESS:
                pending.append(dep_id)
        
        all_satisfied = len(pending) == 0
        
        return all_satisfied, pending
    
    def set_task_status(self, task_id: str, status: TaskStatus) -> None:
        """
        Update task status in dependency graph.
        
        Args:
            task_id: Task ID
            status: New status
        """
        key = self._get_task_status_key(task_id)
        redis_broker.client.set(key, status.value)
        redis_broker.client.expire(key, 3600 * 24)  # 24 hour expiry
        
        log.debug(f"Updated task {task_id} status to {status.value}")
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """
        Get task status from dependency graph.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status or None if not found
        """
        key = self._get_task_status_key(task_id)
        status_str = redis_broker.client.get(key)
        
        if status_str:
            if isinstance(status_str, bytes):
                status_str = status_str.decode()
            return TaskStatus(status_str)
        
        return None
    
    def has_failed_dependencies(self, task_id: str) -> tuple[bool, List[str]]:
        """
        Check if any dependencies have failed.
        
        Args:
            task_id: Task ID to check
            
        Returns:
            Tuple of (has_failures, failed_task_ids)
        """
        dependencies = self.get_dependencies(task_id)
        
        if not dependencies:
            return False, []
        
        failed = []
        
        for dep_id in dependencies:
            status = self.get_task_status(dep_id)
            
            if status in [TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT]:
                failed.append(dep_id)
        
        has_failures = len(failed) > 0
        
        return has_failures, failed
    
    def cancel_dependent_tasks(self, task_id: str) -> List[str]:
        """
        Cancel all tasks that depend on this task.
        Used when a parent task fails.
        
        Args:
            task_id: Failed task ID
            
        Returns:
            List of cancelled task IDs
        """
        dependents = self.get_dependents(task_id)
        cancelled = []
        
        for dep_id in dependents:
            # Mark as cancelled
            self.set_task_status(dep_id, TaskStatus.CANCELLED)
            cancelled.append(dep_id)
            
            # Recursively cancel its dependents
            sub_cancelled = self.cancel_dependent_tasks(dep_id)
            cancelled.extend(sub_cancelled)
        
        if cancelled:
            log.info(
                f"Cancelled {len(cancelled)} dependent tasks of {task_id}",
                extra={"parent_task": task_id, "cancelled": cancelled}
            )
        
        return cancelled
    
    def remove_task(self, task_id: str) -> None:
        """
        Remove task from dependency graph.
        
        Args:
            task_id: Task ID to remove
        """
        # Remove dependencies
        dep_key = self._get_dependency_key(task_id)
        redis_broker.client.delete(dep_key)
        
        # Remove from dependents lists
        dependents_key = self._get_dependents_key(task_id)
        redis_broker.client.delete(dependents_key)
        
        # Remove status
        status_key = self._get_task_status_key(task_id)
        redis_broker.client.delete(status_key)
        
        log.debug(f"Removed task {task_id} from dependency graph")
    
    def get_execution_order(self, task_ids: List[str]) -> List[List[str]]:
        """
        Calculate execution order using topological sort.
        Returns tasks grouped by level (independent tasks in same level).
        
        Args:
            task_ids: List of task IDs to order
            
        Returns:
            List of levels, each level is a list of task IDs that can run in parallel
        """
        # Build in-degree map
        in_degree = {}
        graph = {}
        
        for task_id in task_ids:
            deps = self.get_dependencies(task_id)
            in_degree[task_id] = len(deps)
            graph[task_id] = deps
        
        # Find tasks with no dependencies (level 0)
        levels = []
        remaining = set(task_ids)
        
        while remaining:
            # Find all tasks with in-degree 0
            current_level = [
                task_id for task_id in remaining
                if in_degree[task_id] == 0
            ]
            
            if not current_level:
                # Circular dependency or orphaned tasks
                break
            
            levels.append(current_level)
            
            # Remove current level and update in-degrees
            for task_id in current_level:
                remaining.remove(task_id)
                
                # Update dependents
                for other_id in remaining:
                    if task_id in graph[other_id]:
                        in_degree[other_id] -= 1
        
        return levels
    
    def get_stats(self) -> Dict:
        """
        Get dependency graph statistics.
        
        Returns:
            Dictionary with stats
        """
        # Count tasks with dependencies
        pattern = "task:dependencies:*"
        keys = list(redis_broker.client.scan_iter(match=pattern))
        
        total_with_deps = len(keys)
        
        return {
            "tasks_with_dependencies": total_with_deps
        }


# Global task dependency graph instance
task_dependency_graph = TaskDependencyGraph()
