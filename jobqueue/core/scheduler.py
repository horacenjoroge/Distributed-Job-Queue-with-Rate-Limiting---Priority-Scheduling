"""
Task scheduler process.
Polls Redis for scheduled tasks and moves ready tasks to execution queues.
"""
import time
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional, List
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task import Task
from jobqueue.core.cron_parser import CronParser
from jobqueue.utils.logger import log


class TaskScheduler:
    """
    Scheduler process that moves ready scheduled tasks to execution queues.
    Runs as a separate process from workers.
    """
    
    def __init__(
        self,
        poll_interval: int = 1,
        batch_size: int = 100,
        queues: Optional[List[str]] = None
    ):
        """
        Initialize task scheduler.
        
        Args:
            poll_interval: Seconds between polls (default: 1)
            batch_size: Max tasks to process per poll (default: 100)
            queues: List of queue names to monitor (default: ["default"])
        """
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.queues = queues or ["default"]
        self.running = False
        self.tasks_scheduled = 0
        self.tasks_moved = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        log.info(
            "Initialized task scheduler",
            extra={
                "poll_interval": poll_interval,
                "batch_size": batch_size,
                "queues": queues
            }
        )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        log.info(f"Scheduler received {signal_name}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the scheduler process."""
        if not redis_broker.is_connected():
            redis_broker.connect()
            log.info("Scheduler connected to Redis")
        
        self.running = True
        log.info("Scheduler started")
        
        self.run_loop()
    
    def stop(self):
        """Stop the scheduler process."""
        self.running = False
        
        log.info(
            "Scheduler stopped",
            extra={
                "tasks_scheduled": self.tasks_scheduled,
                "tasks_moved": self.tasks_moved
            }
        )
    
    def run_loop(self):
        """
        Main scheduler loop.
        Polls for ready tasks and moves them to execution queues.
        """
        log.info("Scheduler entering main loop")
        
        while self.running:
            try:
                # Process each queue
                for queue_name in self.queues:
                    self._process_queue(queue_name)
                
                # Sleep until next poll
                time.sleep(self.poll_interval)
                
            except Exception as e:
                log.error(f"Error in scheduler loop: {e}")
                time.sleep(self.poll_interval)
        
        log.info("Scheduler exited main loop")
    
    def _process_queue(self, queue_name: str):
        """
        Process scheduled tasks for a queue.
        
        Args:
            queue_name: Queue name to process
        """
        # Get ready tasks
        ready_tasks = scheduled_task_store.get_ready_tasks(
            queue_name,
            limit=self.batch_size
        )
        
        if not ready_tasks:
            return
        
        log.debug(
            f"Found {len(ready_tasks)} ready tasks in {queue_name}",
            extra={"queue": queue_name, "count": len(ready_tasks)}
        )
        
        # Move tasks to execution queue
        for task in ready_tasks:
            try:
                self._move_to_execution_queue(task)
                self.tasks_moved += 1
            except Exception as e:
                log.error(
                    f"Failed to move task {task.id} to execution queue: {e}",
                    extra={"task_id": task.id, "error": str(e)}
                )
    
    def _move_to_execution_queue(self, task: Task):
        """
        Move a ready task to its execution queue.
        
        Args:
            task: Task to move
        """
        # Remove from scheduled store
        scheduled_task_store.remove_task(task)
        
        # Handle recurring tasks
        if task.is_recurring and task.cron_expression:
            self._schedule_next_occurrence(task)
        
        # Add to execution queue based on priority
        if task.priority:
            # Use priority queue
            priority_queue = PriorityQueue(task.queue_name)
            priority_queue.enqueue(task)
        else:
            # Use regular queue
            queue = Queue(task.queue_name)
            queue.enqueue(task)
        
        log.info(
            f"Moved task {task.id} to execution queue",
            extra={
                "task_id": task.id,
                "queue": task.queue_name,
                "is_recurring": task.is_recurring
            }
        )
    
    def _schedule_next_occurrence(self, task: Task):
        """
        Schedule next occurrence of a recurring task.
        
        Args:
            task: Recurring task
        """
        if not task.cron_expression:
            return
        
        try:
            # Parse cron expression
            cron = CronParser(task.cron_expression)
            
            # Calculate next run time
            next_run = cron.get_next_run(after=datetime.utcnow())
            
            if next_run:
                # Create new task for next occurrence
                next_task = Task(
                    name=task.name,
                    args=task.args,
                    kwargs=task.kwargs,
                    priority=task.priority,
                    queue_name=task.queue_name,
                    max_retries=task.max_retries,
                    timeout=task.timeout,
                    cron_expression=task.cron_expression,
                    is_recurring=True,
                    schedule_time=next_run,
                    last_run_at=datetime.utcnow(),
                    next_run_at=next_run
                )
                
                # Schedule next occurrence
                scheduled_task_store.schedule_task(next_task)
                
                log.info(
                    f"Scheduled next occurrence of recurring task",
                    extra={
                        "task_name": task.name,
                        "next_run": next_run.isoformat(),
                        "cron": task.cron_expression
                    }
                )
            else:
                log.warning(
                    f"Could not calculate next run for recurring task {task.id}",
                    extra={"cron": task.cron_expression}
                )
                
        except Exception as e:
            log.error(
                f"Failed to schedule next occurrence: {e}",
                extra={"task_id": task.id, "cron": task.cron_expression}
            )
    
    def get_stats(self) -> dict:
        """
        Get scheduler statistics.
        
        Returns:
            Dictionary with stats
        """
        stats = {
            "running": self.running,
            "poll_interval": self.poll_interval,
            "queues": self.queues,
            "tasks_scheduled": self.tasks_scheduled,
            "tasks_moved": self.tasks_moved,
            "queue_stats": {}
        }
        
        # Add per-queue stats
        for queue_name in self.queues:
            queue_stats = scheduled_task_store.get_stats(queue_name)
            stats["queue_stats"][queue_name] = queue_stats
        
        return stats


def run_scheduler(
    poll_interval: int = 1,
    batch_size: int = 100,
    queues: Optional[List[str]] = None
):
    """
    Run the task scheduler.
    
    Args:
        poll_interval: Seconds between polls
        batch_size: Max tasks per poll
        queues: Queue names to monitor
    """
    scheduler = TaskScheduler(
        poll_interval=poll_interval,
        batch_size=batch_size,
        queues=queues
    )
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler interrupted by user")
        scheduler.stop()
    except Exception as e:
        log.error(f"Scheduler error: {e}")
        scheduler.stop()
        raise


if __name__ == "__main__":
    # Run scheduler with default settings
    run_scheduler()
