"""
Task routing system for routing tasks to specific worker types.
Routes tasks based on worker_type requirement to appropriate queues.
"""
from typing import Optional, List
from jobqueue.core.task import Task, TaskPriority, WorkerType
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.utils.logger import log


class TaskRouter:
    """
    Routes tasks to appropriate queues based on worker type and priority.
    Routing key format: queue:{worker_type}:{priority}
    """
    
    @staticmethod
    def get_routing_key(worker_type: WorkerType, priority: TaskPriority) -> str:
        """
        Generate routing key for a task.
        
        Args:
            worker_type: Worker type (cpu, io, gpu, default)
            priority: Task priority (high, medium, low)
            
        Returns:
            Routing key in format: queue:{worker_type}:{priority}
            
        Example:
            >>> get_routing_key(WorkerType.CPU, TaskPriority.HIGH)
            'queue:cpu:high'
        """
        return f"queue:{worker_type.value}:{priority.value}"
    
    @staticmethod
    def get_queue_name(worker_type: WorkerType, priority: TaskPriority) -> str:
        """
        Get queue name for routing.
        
        Args:
            worker_type: Worker type
            priority: Task priority
            
        Returns:
            Queue name
        """
        return TaskRouter.get_routing_key(worker_type, priority)
    
    @staticmethod
    def route_task(task: Task, use_priority_queue: bool = False) -> str:
        """
        Route a task to the appropriate queue based on worker_type and priority.
        
        Args:
            task: Task to route
            use_priority_queue: If True, use PriorityQueue instead of FIFO Queue
            
        Returns:
            Queue name where task was routed
            
        Example:
            >>> task = Task(name="cpu_task", worker_type=WorkerType.CPU)
            >>> queue_name = route_task(task)
            >>> queue_name
            'queue:cpu:medium'
        """
        # Determine worker type (default if not specified)
        worker_type = task.worker_type or WorkerType.DEFAULT
        
        # Get routing key
        queue_name = TaskRouter.get_routing_key(worker_type, task.priority)
        
        # Route to appropriate queue
        if use_priority_queue:
            queue = PriorityQueue(queue_name)
        else:
            queue = Queue(queue_name)
        
        # Enqueue task
        queue.enqueue(task)
        
        log.debug(
            f"Routed task to queue",
            extra={
                "task_id": task.id,
                "task_name": task.name,
                "worker_type": worker_type.value,
                "priority": task.priority.value,
                "queue_name": queue_name
            }
        )
        
        return queue_name
    
    @staticmethod
    def get_worker_queues(worker_type: WorkerType, priorities: Optional[List[TaskPriority]] = None) -> List[str]:
        """
        Get list of queue names that a worker of given type should subscribe to.
        
        Args:
            worker_type: Worker type (cpu, io, gpu, default)
            priorities: List of priorities to subscribe to (default: all priorities)
            
        Returns:
            List of queue names
            
        Example:
            >>> queues = get_worker_queues(WorkerType.CPU)
            >>> queues
            ['queue:cpu:high', 'queue:cpu:medium', 'queue:cpu:low']
        """
        if priorities is None:
            priorities = [TaskPriority.HIGH, TaskPriority.MEDIUM, TaskPriority.LOW]
        
        queues = []
        for priority in priorities:
            queue_name = TaskRouter.get_routing_key(worker_type, priority)
            queues.append(queue_name)
        
        return queues
    
    @staticmethod
    def get_all_worker_type_queues(priorities: Optional[List[TaskPriority]] = None) -> dict:
        """
        Get all queues for all worker types.
        
        Args:
            priorities: List of priorities (default: all priorities)
            
        Returns:
            Dictionary mapping worker_type to list of queue names
            
        Example:
            >>> queues = get_all_worker_type_queues()
            >>> queues[WorkerType.CPU]
            ['queue:cpu:high', 'queue:cpu:medium', 'queue:cpu:low']
        """
        if priorities is None:
            priorities = [TaskPriority.HIGH, TaskPriority.MEDIUM, TaskPriority.LOW]
        
        result = {}
        for worker_type in WorkerType:
            result[worker_type] = TaskRouter.get_worker_queues(worker_type, priorities)
        
        return result
    
    @staticmethod
    def route_with_fallback(task: Task, use_priority_queue: bool = False) -> str:
        """
        Route task with fallback to default queue if worker type queue doesn't exist.
        
        Args:
            task: Task to route
            use_priority_queue: If True, use PriorityQueue
            
        Returns:
            Queue name where task was routed
        """
        worker_type = task.worker_type or WorkerType.DEFAULT
        
        # Try routing to worker type specific queue
        try:
            queue_name = TaskRouter.get_routing_key(worker_type, task.priority)
            
            if use_priority_queue:
                queue = PriorityQueue(queue_name)
            else:
                queue = Queue(queue_name)
            
            # Check if queue exists or can be created
            queue.enqueue(task)
            
            log.debug(
                f"Routed task to worker type queue",
                extra={
                    "task_id": task.id,
                    "worker_type": worker_type.value,
                    "queue_name": queue_name
                }
            )
            
            return queue_name
            
        except Exception as e:
            # Fallback to default queue
            log.warning(
                f"Failed to route to worker type queue, falling back to default",
                extra={
                    "task_id": task.id,
                    "worker_type": worker_type.value,
                    "error": str(e)
                }
            )
            
            # Route to default queue
            default_queue_name = TaskRouter.get_routing_key(WorkerType.DEFAULT, task.priority)
            
            if use_priority_queue:
                queue = PriorityQueue(default_queue_name)
            else:
                queue = Queue(default_queue_name)
            
            queue.enqueue(task)
            
            return default_queue_name


# Global task router instance
task_router = TaskRouter()
