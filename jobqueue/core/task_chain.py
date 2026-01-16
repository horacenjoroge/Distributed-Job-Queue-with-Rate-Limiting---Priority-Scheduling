"""
Task chaining utilities for creating dependent task workflows.
"""
from typing import List, Optional, Any, Dict
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.task_dependencies import task_dependency_graph
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.utils.logger import log


class TaskSignature:
    """
    Task signature - a blueprint for creating a task.
    Similar to Celery's signature pattern.
    """
    
    def __init__(
        self,
        name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
        queue_name: str = "default",
        max_retries: int = 3,
        timeout: int = 300
    ):
        """
        Initialize task signature.
        
        Args:
            name: Task function name
            args: Task arguments
            kwargs: Task keyword arguments
            priority: Task priority
            queue_name: Queue name
            max_retries: Max retry attempts
            timeout: Task timeout
        """
        self.name = name
        self.args = args or []
        self.kwargs = kwargs or {}
        self.priority = priority
        self.queue_name = queue_name
        self.max_retries = max_retries
        self.timeout = timeout
        self.depends_on = []
    
    def apply_async(self, depends_on: Optional[List[str]] = None) -> Task:
        """
        Create and enqueue task from signature.
        
        Args:
            depends_on: List of task IDs this task depends on
            
        Returns:
            Created task
        """
        task = Task(
            name=self.name,
            args=self.args,
            kwargs=self.kwargs,
            priority=self.priority,
            queue_name=self.queue_name,
            max_retries=self.max_retries,
            timeout=self.timeout,
            depends_on=depends_on or self.depends_on
        )
        
        # Add dependencies to graph
        if task.depends_on:
            task_dependency_graph.add_dependencies(task)
        
        # Enqueue task
        if task.priority:
            queue = PriorityQueue(task.queue_name)
        else:
            queue = Queue(task.queue_name)
        
        queue.enqueue(task)
        
        log.info(
            f"Applied task signature: {self.name}",
            extra={
                "task_id": task.id,
                "depends_on": task.depends_on
            }
        )
        
        return task
    
    def __or__(self, other: "TaskSignature") -> "TaskChain":
        """
        Create chain using | operator.
        
        Example:
            sig1 | sig2 | sig3
        """
        return TaskChain(self, other)
    
    def set(self, **kwargs) -> "TaskSignature":
        """
        Update signature parameters.
        
        Args:
            **kwargs: Parameters to update
            
        Returns:
            Updated signature
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        return self


class TaskChain:
    """
    Chain of tasks that execute sequentially.
    """
    
    def __init__(self, *signatures: TaskSignature):
        """
        Initialize task chain.
        
        Args:
            *signatures: Task signatures to chain
        """
        self.signatures = list(signatures)
    
    def __or__(self, other: TaskSignature) -> "TaskChain":
        """
        Add another task to chain using | operator.
        """
        self.signatures.append(other)
        return self
    
    def apply_async(self) -> List[Task]:
        """
        Apply chain by creating all tasks with dependencies.
        
        Returns:
            List of created tasks
        """
        tasks = []
        previous_task_id = None
        
        for sig in self.signatures:
            # Set dependency on previous task
            depends_on = [previous_task_id] if previous_task_id else []
            
            task = sig.apply_async(depends_on=depends_on)
            tasks.append(task)
            
            previous_task_id = task.id
        
        log.info(
            f"Applied task chain with {len(tasks)} tasks",
            extra={"task_count": len(tasks)}
        )
        
        return tasks


class TaskGroup:
    """
    Group of tasks that execute in parallel.
    """
    
    def __init__(self, *signatures: TaskSignature):
        """
        Initialize task group.
        
        Args:
            *signatures: Task signatures to group
        """
        self.signatures = list(signatures)
    
    def apply_async(self) -> List[Task]:
        """
        Apply group by creating all tasks without dependencies.
        
        Returns:
            List of created tasks
        """
        tasks = []
        
        for sig in self.signatures:
            task = sig.apply_async()
            tasks.append(task)
        
        log.info(
            f"Applied task group with {len(tasks)} tasks",
            extra={"task_count": len(tasks)}
        )
        
        return tasks


def signature(
    name: str,
    args: Optional[List[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    **options
) -> TaskSignature:
    """
    Create a task signature.
    
    Args:
        name: Task function name
        args: Task arguments
        kwargs: Task keyword arguments
        **options: Additional task options
        
    Returns:
        Task signature
        
    Example:
        sig = signature("process_data", args=[123], priority=TaskPriority.HIGH)
    """
    return TaskSignature(name, args, kwargs, **options)


def chain(*signatures: TaskSignature) -> TaskChain:
    """
    Create a chain of tasks that execute sequentially.
    
    Args:
        *signatures: Task signatures to chain
        
    Returns:
        Task chain
        
    Example:
        # Chain 3 tasks
        result = chain(
            signature("fetch_data"),
            signature("process_data"),
            signature("save_results")
        ).apply_async()
        
        # Or using | operator
        result = (
            signature("fetch_data") |
            signature("process_data") |
            signature("save_results")
        ).apply_async()
    """
    return TaskChain(*signatures)


def group(*signatures: TaskSignature) -> TaskGroup:
    """
    Create a group of tasks that execute in parallel.
    
    Args:
        *signatures: Task signatures to group
        
    Returns:
        Task group
        
    Example:
        # Execute 3 tasks in parallel
        result = group(
            signature("task1"),
            signature("task2"),
            signature("task3")
        ).apply_async()
    """
    return TaskGroup(*signatures)


def chord(group_signatures: List[TaskSignature], callback: TaskSignature) -> List[Task]:
    """
    Execute tasks in parallel, then execute callback when all complete.
    
    Args:
        group_signatures: List of signatures to execute in parallel
        callback: Signature to execute after group completes
        
    Returns:
        List of created tasks (group + callback)
        
    Example:
        # Execute 3 tasks in parallel, then run callback
        result = chord(
            [signature("task1"), signature("task2"), signature("task3")],
            signature("aggregate_results")
        )
    """
    # Create group tasks
    group_tasks = []
    for sig in group_signatures:
        task = sig.apply_async()
        group_tasks.append(task)
    
    # Create callback that depends on all group tasks
    callback_depends_on = [task.id for task in group_tasks]
    callback_task = callback.apply_async(depends_on=callback_depends_on)
    
    all_tasks = group_tasks + [callback_task]
    
    log.info(
        f"Applied chord with {len(group_tasks)} parallel tasks and 1 callback",
        extra={"group_size": len(group_tasks)}
    )
    
    return all_tasks
