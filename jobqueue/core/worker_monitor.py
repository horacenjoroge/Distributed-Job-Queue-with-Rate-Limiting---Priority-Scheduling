"""
Worker monitor process.
Detects dead workers and re-queues their tasks.
"""
import time
import signal
import sys
from typing import List, Dict
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_recovery import task_recovery
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.utils.logger import log


class WorkerMonitor:
    """
    Monitors worker health and handles dead worker recovery.
    """
    
    def __init__(
        self,
        check_interval: int = 10,
        stale_threshold: int = 30
    ):
        """
        Initialize worker monitor.
        
        Args:
            check_interval: Seconds between checks (default: 10)
            stale_threshold: Seconds before worker considered dead (default: 30)
        """
        self.check_interval = check_interval
        self.stale_threshold = stale_threshold
        self.running = False
        self.dead_workers_detected = 0
        self.tasks_recovered = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        log.info(
            "Initialized worker monitor",
            extra={
                "check_interval": check_interval,
                "stale_threshold": stale_threshold
            }
        )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        log.info(f"Worker monitor received {signal_name}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the worker monitor."""
        if not redis_broker.is_connected():
            redis_broker.connect()
            log.info("Worker monitor connected to Redis")
        
        self.running = True
        log.info("Worker monitor started")
        
        self.run_loop()
    
    def stop(self):
        """Stop the worker monitor."""
        self.running = False
        
        log.info(
            "Worker monitor stopped",
            extra={
                "dead_workers_detected": self.dead_workers_detected,
                "tasks_recovered": self.tasks_recovered
            }
        )
    
    def run_loop(self):
        """
        Main monitor loop.
        Checks for dead workers and recovers their tasks.
        """
        log.info("Worker monitor entering main loop")
        
        while self.running:
            try:
                # Check for stale workers
                stale_workers = worker_heartbeat.get_stale_workers()
                
                if stale_workers:
                    log.warning(
                        f"Detected {len(stale_workers)} dead workers",
                        extra={"dead_workers": stale_workers}
                    )
                    
                    # Recover orphaned tasks using task recovery system
                    recovered = task_recovery.recover_orphaned_tasks()
                    self.tasks_recovered += recovered
                    
                    # Also use legacy recovery for compatibility
                    for worker_id in stale_workers:
                        legacy_recovered = self._recover_worker_tasks(worker_id)
                        self.tasks_recovered += legacy_recovered
                    
                    self.dead_workers_detected += len(stale_workers)
                
                # Sleep until next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                log.error(f"Error in worker monitor loop: {e}")
                time.sleep(self.check_interval)
        
        log.info("Worker monitor exited main loop")
    
    def _recover_worker_tasks(self, worker_id: str) -> int:
        """
        Recover tasks from a dead worker.
        
        Args:
            worker_id: Dead worker ID
            
        Returns:
            Number of tasks recovered
        """
        recovered_count = 0
        
        try:
            # Find tasks assigned to this worker
            running_tasks = self._get_running_tasks_for_worker(worker_id)
            
            if not running_tasks:
                log.debug(f"No running tasks found for dead worker {worker_id}")
                return 0
            
            log.info(
                f"Recovering {len(running_tasks)} tasks from dead worker {worker_id}",
                extra={
                    "worker_id": worker_id,
                    "task_count": len(running_tasks)
                }
            )
            
            # Re-queue each task
            for task in running_tasks:
                try:
                    # Reset task status
                    task.status = TaskStatus.PENDING
                    task.worker_id = None
                    task.started_at = None
                    
                    # Re-enqueue task
                    if task.priority:
                        queue = PriorityQueue(task.queue_name)
                    else:
                        queue = Queue(task.queue_name)
                    
                    queue.enqueue(task)
                    recovered_count += 1
                    
                    log.info(
                        f"Re-queued task {task.id} from dead worker {worker_id}",
                        extra={
                            "task_id": task.id,
                            "worker_id": worker_id,
                            "queue": task.queue_name
                        }
                    )
                    
                except Exception as e:
                    log.error(
                        f"Failed to recover task {task.id}: {e}",
                        extra={"task_id": task.id, "error": str(e)}
                    )
            
            self.tasks_recovered += recovered_count
            
        except Exception as e:
            log.error(f"Error recovering tasks from worker {worker_id}: {e}")
        
        return recovered_count
    
    def _get_running_tasks_for_worker(self, worker_id: str) -> List[Task]:
        """
        Get all tasks currently running on a worker.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            List of running tasks
        """
        running_tasks = []
        
        try:
            # Search for tasks with this worker_id in Redis
            # Tasks are stored with key pattern: task:running:{worker_id}:{task_id}
            pattern = f"task:running:{worker_id}:*"
            keys = list(redis_broker.client.scan_iter(match=pattern))
            
            for key in keys:
                try:
                    task_json = redis_broker.client.get(key)
                    if task_json:
                        if isinstance(task_json, bytes):
                            task_json = task_json.decode()
                        task = Task.from_json(task_json)
                        running_tasks.append(task)
                except Exception as e:
                    log.error(f"Error loading task from key {key}: {e}")
            
            # Also check tasks in dependency graph
            # Get all tasks with RUNNING status
            pattern = "task:status:*"
            status_keys = list(redis_broker.client.scan_iter(match=pattern))
            
            for status_key in status_keys:
                try:
                    status = redis_broker.client.get(status_key)
                    if status and status.decode() == TaskStatus.RUNNING.value:
                        # Try to find task by checking worker assignments
                        # This is a simplified approach - in production, maintain a worker->tasks mapping
                        pass
                except Exception as e:
                    log.error(f"Error checking status key {status_key}: {e}")
            
        except Exception as e:
            log.error(f"Error getting running tasks for worker {worker_id}: {e}")
        
        return running_tasks
    
    def get_stats(self) -> Dict:
        """
        Get monitor statistics.
        
        Returns:
            Dictionary with stats
        """
        all_workers = worker_heartbeat.get_all_workers()
        alive_workers = [w for w in all_workers if worker_heartbeat.is_worker_alive(w)]
        dead_workers = [w for w in all_workers if not worker_heartbeat.is_worker_alive(w)]
        
        return {
            "running": self.running,
            "check_interval": self.check_interval,
            "stale_threshold": self.stale_threshold,
            "total_workers": len(all_workers),
            "alive_workers": len(alive_workers),
            "dead_workers": len(dead_workers),
            "dead_workers_detected": self.dead_workers_detected,
            "tasks_recovered": self.tasks_recovered
        }


def run_worker_monitor(
    check_interval: int = 10,
    stale_threshold: int = 30
):
    """
    Run the worker monitor process.
    
    Args:
        check_interval: Seconds between checks
        stale_threshold: Seconds before worker considered dead
    """
    monitor = WorkerMonitor(
        check_interval=check_interval,
        stale_threshold=stale_threshold
    )
    
    try:
        monitor.start()
    except KeyboardInterrupt:
        log.info("Worker monitor interrupted by user")
        monitor.stop()
    except Exception as e:
        log.error(f"Worker monitor error: {e}")
        monitor.stop()
        raise


if __name__ == "__main__":
    # Run monitor with default settings
    run_worker_monitor()
