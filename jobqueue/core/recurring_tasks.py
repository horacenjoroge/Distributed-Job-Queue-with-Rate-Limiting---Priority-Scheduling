"""
Recurring task management with cron-like scheduling.
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.core.cron_parser import CronParser, validate_cron_expression
from jobqueue.utils.logger import log


class RecurringTaskManager:
    """
    Manages recurring tasks with cron-like schedules.
    """
    
    def __init__(self):
        """Initialize recurring task manager."""
        self.recurring_tasks: Dict[str, Task] = {}
        
        log.info("Initialized recurring task manager")
    
    def register_recurring_task(
        self,
        name: str,
        cron_expression: str,
        task_name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
        queue_name: str = "default",
        max_retries: int = 3,
        timeout: int = 300,
        timezone: str = "UTC"
    ) -> Task:
        """
        Register a recurring task with cron schedule.
        
        Args:
            name: Unique name for recurring task
            cron_expression: Cron expression (e.g., "*/5 * * * *")
            task_name: Name of task function to execute
            args: Task arguments
            kwargs: Task keyword arguments
            priority: Task priority
            queue_name: Queue name
            max_retries: Max retry attempts
            timeout: Task timeout
            timezone: Timezone for cron schedule
            
        Returns:
            Created recurring task
            
        Example:
            manager.register_recurring_task(
                name="cleanup_job",
                cron_expression="0 2 * * *",  # Daily at 2 AM
                task_name="cleanup_old_data",
                priority=TaskPriority.LOW
            )
        """
        # Validate cron expression
        is_valid, error = validate_cron_expression(cron_expression)
        if not is_valid:
            raise ValueError(f"Invalid cron expression: {error}")
        
        # Parse cron and calculate next run
        cron = CronParser(cron_expression)
        next_run = cron.get_next_run()
        
        if not next_run:
            raise ValueError(f"Could not calculate next run for cron: {cron_expression}")
        
        # Create recurring task
        task = Task(
            name=task_name,
            args=args or [],
            kwargs=kwargs or {},
            priority=priority,
            queue_name=queue_name,
            max_retries=max_retries,
            timeout=timeout,
            cron_expression=cron_expression,
            is_recurring=True,
            schedule_time=next_run,
            next_run_at=next_run
        )
        
        # Store in registry
        self.recurring_tasks[name] = task
        
        # Schedule first occurrence
        scheduled_task_store.schedule_task(task)
        
        log.info(
            f"Registered recurring task: {name}",
            extra={
                "name": name,
                "cron": cron_expression,
                "task_name": task_name,
                "next_run": next_run.isoformat()
            }
        )
        
        return task
    
    def unregister_recurring_task(self, name: str) -> bool:
        """
        Unregister a recurring task.
        
        Args:
            name: Recurring task name
            
        Returns:
            True if unregistered
        """
        if name not in self.recurring_tasks:
            return False
        
        task = self.recurring_tasks[name]
        
        # Remove from scheduled store
        scheduled_task_store.remove_task(task)
        
        # Remove from registry
        del self.recurring_tasks[name]
        
        log.info(f"Unregistered recurring task: {name}")
        
        return True
    
    def get_recurring_task(self, name: str) -> Optional[Task]:
        """
        Get a recurring task by name.
        
        Args:
            name: Recurring task name
            
        Returns:
            Task or None
        """
        return self.recurring_tasks.get(name)
    
    def list_recurring_tasks(self) -> List[Dict[str, Any]]:
        """
        List all registered recurring tasks.
        
        Returns:
            List of recurring task info
        """
        tasks = []
        
        for name, task in self.recurring_tasks.items():
            tasks.append({
                "name": name,
                "task_name": task.name,
                "cron_expression": task.cron_expression,
                "queue_name": task.queue_name,
                "priority": task.priority,
                "next_run_at": task.next_run_at.isoformat() if task.next_run_at else None,
                "last_run_at": task.last_run_at.isoformat() if task.last_run_at else None,
                "is_active": True
            })
        
        return tasks
    
    def update_recurring_task(
        self,
        name: str,
        cron_expression: Optional[str] = None,
        priority: Optional[TaskPriority] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None
    ) -> bool:
        """
        Update a recurring task configuration.
        
        Args:
            name: Recurring task name
            cron_expression: New cron expression
            priority: New priority
            max_retries: New max retries
            timeout: New timeout
            
        Returns:
            True if updated
        """
        if name not in self.recurring_tasks:
            return False
        
        task = self.recurring_tasks[name]
        
        # Update cron expression
        if cron_expression:
            is_valid, error = validate_cron_expression(cron_expression)
            if not is_valid:
                raise ValueError(f"Invalid cron expression: {error}")
            
            # Remove old scheduled task
            scheduled_task_store.remove_task(task)
            
            # Update and reschedule
            task.cron_expression = cron_expression
            cron = CronParser(cron_expression)
            next_run = cron.get_next_run()
            task.schedule_time = next_run
            task.next_run_at = next_run
            
            scheduled_task_store.schedule_task(task)
        
        # Update other fields
        if priority:
            task.priority = priority
        if max_retries is not None:
            task.max_retries = max_retries
        if timeout is not None:
            task.timeout = timeout
        
        log.info(f"Updated recurring task: {name}")
        
        return True
    
    def pause_recurring_task(self, name: str) -> bool:
        """
        Pause a recurring task (remove from schedule).
        
        Args:
            name: Recurring task name
            
        Returns:
            True if paused
        """
        if name not in self.recurring_tasks:
            return False
        
        task = self.recurring_tasks[name]
        scheduled_task_store.remove_task(task)
        
        log.info(f"Paused recurring task: {name}")
        
        return True
    
    def resume_recurring_task(self, name: str) -> bool:
        """
        Resume a paused recurring task.
        
        Args:
            name: Recurring task name
            
        Returns:
            True if resumed
        """
        if name not in self.recurring_tasks:
            return False
        
        task = self.recurring_tasks[name]
        
        # Calculate next run
        if task.cron_expression:
            cron = CronParser(task.cron_expression)
            next_run = cron.get_next_run()
            task.schedule_time = next_run
            task.next_run_at = next_run
            
            # Reschedule
            scheduled_task_store.schedule_task(task)
            
            log.info(f"Resumed recurring task: {name}")
            
            return True
        
        return False
    
    def get_stats(self) -> dict:
        """
        Get recurring task statistics.
        
        Returns:
            Dictionary with stats
        """
        total = len(self.recurring_tasks)
        
        # Count by queue
        by_queue = {}
        for task in self.recurring_tasks.values():
            queue = task.queue_name
            by_queue[queue] = by_queue.get(queue, 0) + 1
        
        # Count by priority
        by_priority = {}
        for task in self.recurring_tasks.values():
            priority = task.priority
            by_priority[priority] = by_priority.get(priority, 0) + 1
        
        return {
            "total_recurring_tasks": total,
            "by_queue": by_queue,
            "by_priority": by_priority
        }


# Global recurring task manager instance
recurring_task_manager = RecurringTaskManager()


def schedule_recurring(
    name: str,
    cron: str,
    task_name: str,
    **kwargs
) -> Task:
    """
    Decorator-friendly function to schedule recurring tasks.
    
    Args:
        name: Unique recurring task name
        cron: Cron expression
        task_name: Task function name
        **kwargs: Additional task parameters
        
    Returns:
        Created task
        
    Example:
        schedule_recurring(
            name="hourly_cleanup",
            cron="0 * * * *",
            task_name="cleanup_temp_files"
        )
    """
    return recurring_task_manager.register_recurring_task(
        name=name,
        cron_expression=cron,
        task_name=task_name,
        **kwargs
    )
