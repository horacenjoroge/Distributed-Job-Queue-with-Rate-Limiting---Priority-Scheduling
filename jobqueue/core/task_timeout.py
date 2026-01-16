"""
Task timeout enforcement utilities.
Supports soft timeout (warning) and hard timeout (kill).
"""
import signal
import threading
import time
import os
import psutil
from typing import Optional, Callable, Any
from jobqueue.utils.logger import log


class TimeoutHandler:
    """
    Handles task timeouts using signals or threading timers.
    """
    
    def __init__(self, timeout_seconds: int):
        """
        Initialize timeout handler.
        
        Args:
            timeout_seconds: Timeout in seconds
        """
        self.timeout_seconds = timeout_seconds
        self.start_time: Optional[float] = None
        self.timer: Optional[threading.Timer] = None
        self.timed_out = False
        self.soft_timeout_triggered = False
        
        log.debug(f"Initialized timeout handler with {timeout_seconds}s timeout")
    
    def start(self) -> None:
        """Start timeout timer."""
        self.start_time = time.time()
        self.timed_out = False
        self.soft_timeout_triggered = False
        
        log.debug(f"Started timeout timer: {self.timeout_seconds}s")
    
    def elapsed_time(self) -> float:
        """
        Get elapsed time since start.
        
        Returns:
            Elapsed time in seconds
        """
        if self.start_time is None:
            return 0.0
        
        return time.time() - self.start_time
    
    def remaining_time(self) -> float:
        """
        Get remaining time before timeout.
        
        Returns:
            Remaining time in seconds (negative if exceeded)
        """
        if self.start_time is None:
            return self.timeout_seconds
        
        elapsed = self.elapsed_time()
        return self.timeout_seconds - elapsed
    
    def check_soft_timeout(self, soft_timeout_ratio: float = 0.8) -> bool:
        """
        Check if soft timeout threshold reached.
        
        Args:
            soft_timeout_ratio: Ratio of timeout for soft warning (default: 0.8 = 80%)
            
        Returns:
            True if soft timeout reached
        """
        if self.soft_timeout_triggered:
            return False
        
        remaining = self.remaining_time()
        soft_threshold = self.timeout_seconds * soft_timeout_ratio
        
        if remaining <= (self.timeout_seconds - soft_threshold):
            self.soft_timeout_triggered = True
            return True
        
        return False
    
    def is_timed_out(self) -> bool:
        """
        Check if timeout exceeded.
        
        Returns:
            True if timed out
        """
        if self.start_time is None:
            return False
        
        return self.elapsed_time() >= self.timeout_seconds
    
    def stop(self) -> None:
        """Stop timeout timer."""
        if self.timer:
            self.timer.cancel()
            self.timer = None
        
        log.debug(f"Stopped timeout timer")


class ProcessTimeoutKiller:
    """
    Kills processes that exceed timeout, including child processes.
    """
    
    @staticmethod
    def kill_process_tree(pid: int, timeout: int = 5) -> bool:
        """
        Kill a process and all its children.
        
        Args:
            pid: Process ID
            timeout: Timeout for graceful shutdown (seconds)
            
        Returns:
            True if killed successfully
        """
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            
            # Try graceful shutdown first
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            
            try:
                parent.terminate()
            except psutil.NoSuchProcess:
                pass
            
            # Wait for processes to terminate
            gone, alive = psutil.wait_procs(children + [parent], timeout=timeout)
            
            # Force kill if still alive
            for proc in alive:
                try:
                    proc.kill()
                except psutil.NoSuchProcess:
                    pass
            
            log.warning(
                f"Killed process tree for PID {pid}",
                extra={"pid": pid, "killed": len(gone), "force_killed": len(alive)}
            )
            
            return True
            
        except psutil.NoSuchProcess:
            log.debug(f"Process {pid} already terminated")
            return True
        except Exception as e:
            log.error(f"Error killing process tree {pid}: {e}")
            return False
    
    @staticmethod
    def kill_current_process() -> None:
        """
        Kill the current process (for timeout enforcement).
        """
        try:
            pid = os.getpid()
            log.error(f"Killing current process {pid} due to timeout")
            os.kill(pid, signal.SIGTERM)
        except Exception as e:
            log.error(f"Error killing current process: {e}")


def execute_with_timeout(
    func: Callable,
    timeout_seconds: int,
    *args,
    soft_timeout_ratio: float = 0.8,
    **kwargs
) -> tuple[Any, Optional[str]]:
    """
    Execute a function with timeout enforcement.
    
    Args:
        func: Function to execute
        timeout_seconds: Timeout in seconds
        *args: Function arguments
        soft_timeout_ratio: Soft timeout ratio (default: 0.8)
        **kwargs: Function keyword arguments
        
    Returns:
        Tuple of (result, error_message)
        
    Example:
        result, error = execute_with_timeout(long_running_task, timeout_seconds=10)
        if error:
            print(f"Task timed out: {error}")
    """
    result = None
    error = None
    timeout_handler = TimeoutHandler(timeout_seconds)
    
    def timeout_callback():
        """Callback when timeout is reached."""
        timeout_handler.timed_out = True
        log.error(f"Task execution exceeded timeout of {timeout_seconds}s")
    
    # Start timer
    timeout_handler.start()
    timer = threading.Timer(timeout_seconds, timeout_callback)
    timer.start()
    
    try:
        # Execute function
        result = func(*args, **kwargs)
        
        # Check if timed out during execution
        if timeout_handler.timed_out:
            error = f"Task exceeded timeout of {timeout_seconds} seconds"
        
    except Exception as e:
        error = str(e)
    finally:
        # Stop timer
        timer.cancel()
        timeout_handler.stop()
    
    return result, error


def check_timeout_remaining(
    timeout_handler: TimeoutHandler,
    soft_timeout_ratio: float = 0.8
) -> tuple[bool, bool, float]:
    """
    Check timeout status and return warnings.
    
    Args:
        timeout_handler: Timeout handler instance
        soft_timeout_ratio: Soft timeout ratio
        
    Returns:
        Tuple of (is_timed_out, soft_timeout_warning, remaining_seconds)
    """
    is_timed_out = timeout_handler.is_timed_out()
    soft_warning = timeout_handler.check_soft_timeout(soft_timeout_ratio)
    remaining = timeout_handler.remaining_time()
    
    return is_timed_out, soft_warning, remaining


class TimeoutManager:
    """
    Manages timeouts for task execution with soft and hard timeouts.
    """
    
    def __init__(
        self,
        timeout_seconds: int,
        soft_timeout_ratio: float = 0.8,
        enable_soft_timeout: bool = True
    ):
        """
        Initialize timeout manager.
        
        Args:
            timeout_seconds: Hard timeout in seconds
            soft_timeout_ratio: Soft timeout ratio (default: 0.8 = 80%)
            enable_soft_timeout: Enable soft timeout warnings (default: True)
        """
        self.timeout_seconds = timeout_seconds
        self.soft_timeout_ratio = soft_timeout_ratio
        self.enable_soft_timeout = enable_soft_timeout
        self.handler: Optional[TimeoutHandler] = None
        self.soft_timeout_logged = False
        
        log.debug(
            f"Initialized timeout manager",
            extra={
                "timeout_seconds": timeout_seconds,
                "soft_timeout_ratio": soft_timeout_ratio,
                "enable_soft_timeout": enable_soft_timeout
            }
        )
    
    def start(self) -> None:
        """Start timeout monitoring."""
        self.handler = TimeoutHandler(self.timeout_seconds)
        self.handler.start()
        self.soft_timeout_logged = False
    
    def check(self) -> tuple[bool, bool]:
        """
        Check timeout status.
        
        Returns:
            Tuple of (is_timed_out, soft_timeout_warning)
        """
        if not self.handler:
            return False, False
        
        is_timed_out = self.handler.is_timed_out()
        
        # Check soft timeout
        soft_warning = False
        if self.enable_soft_timeout and not self.soft_timeout_logged:
            soft_warning = self.handler.check_soft_timeout(self.soft_timeout_ratio)
            
            if soft_warning:
                remaining = self.handler.remaining_time()
                log.warning(
                    f"Task approaching timeout (soft timeout)",
                    extra={
                        "timeout_seconds": self.timeout_seconds,
                        "remaining_seconds": remaining,
                        "elapsed_seconds": self.handler.elapsed_time()
                    }
                )
                self.soft_timeout_logged = True
        
        return is_timed_out, soft_warning
    
    def stop(self) -> None:
        """Stop timeout monitoring."""
        if self.handler:
            self.handler.stop()
            self.handler = None
    
    def get_remaining_time(self) -> float:
        """Get remaining time before timeout."""
        if not self.handler:
            return self.timeout_seconds
        
        return self.handler.remaining_time()
    
    def get_elapsed_time(self) -> float:
        """Get elapsed time since start."""
        if not self.handler:
            return 0.0
        
        return self.handler.elapsed_time()
