# Distributed Job Queue with Rate Limiting & Priority Scheduling

A robust, production-ready distributed job queue system built from scratch in Python. This system provides advanced features like priority scheduling, rate limiting, task dependencies, dead letter queues, and comprehensive monitoring capabilities.

## Features

### Core Features
- [x] **Custom Task Queue Implementation** - Built from scratch without using Celery
- [x] **Priority Queues** - High/Medium/Low priority task scheduling
- [x] **Rate Limiting** - Configurable rate limits per queue (max X tasks per minute)
- [x] **Dead Letter Queue** - Failed tasks automatically moved after N retries
- [x] **Task Chaining & Dependencies** - Task B runs only after Task A succeeds
- [x] **Distributed Workers** - Multiple workers across different machines
- [x] **Worker Health Monitoring** - Heartbeats and auto-restart capabilities
- [x] **Result Backend** - Store results in Redis with configurable TTL
- [x] **Exponential Backoff** - Intelligent retry mechanism with backoff
- [x] **Task Cancellation** - Cancel running or pending tasks

### Advanced Features
- [ ] **Task Recovery** - Handle worker crashes mid-task
- [ ] **Task Deduplication** - Prevent duplicate task execution
- [ ] **Web UI** - Monitor queues and tasks via web interface
- [ ] **Connection Failure Handling** - Graceful handling of Redis/DB failures
- [ ] **Task Timeouts** - Automatically kill long-running tasks
- [ ] **Metrics & Observability** - Track duration, success rate, etc.

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│   FastAPI API   │
└────────┬────────┘
         │
         ▼
    ┌────────┐         ┌──────────────┐
    │ Redis  │◄────────┤   Workers    │
    │ Broker │         │ (Distributed)│
    └────────┘         └──────┬───────┘
         │                    │
         ▼                    ▼
    ┌──────────────────────────┐
    │   PostgreSQL Backend     │
    │ (Tasks, Results, Metrics)│
    └──────────────────────────┘
```

## Project Structure

```
jobqueue/
├── core/                   # Core queue logic
│   ├── __init__.py
│   ├── task.py            # Task models and status
│   └── queue.py           # Queue implementation
├── worker/                # Worker implementation
│   ├── __init__.py
│   └── main.py            # Worker entry point
├── broker/                # Message broker interface
│   ├── __init__.py
│   └── redis_broker.py    # Redis connection manager
├── backend/               # Result backend
│   ├── __init__.py
│   └── postgres_backend.py # PostgreSQL manager
├── api/                   # REST API (FastAPI)
│   ├── __init__.py
│   └── main.py            # API entry point
├── ui/                    # Web UI
│   └── __init__.py
├── utils/                 # Utilities
│   ├── __init__.py
│   └── logger.py          # Structured logging
└── tests/                 # Test suite
    └── __init__.py
```

## Tech Stack

- **Language**: Python 3.11+
- **Message Broker**: Redis 7
- **Database**: PostgreSQL 16
- **API Framework**: FastAPI
- **Concurrency**: multiprocessing/threading
- **Logging**: loguru (structured logging)
- **Containerization**: Docker & Docker Compose

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Docker and Docker Compose
- Redis (if running locally)
- PostgreSQL (if running locally)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "Distributed Job Queue with Rate Limiting & Priority Scheduling"
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

### Running with Docker (Recommended)

1. **Start all services**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - Redis (port 6379)
   - PostgreSQL (port 5432)
   - API Server (port 8000)
   - 3 Worker instances

2. **View logs**
   ```bash
   docker-compose logs -f
   ```

3. **Stop services**
   ```bash
   docker-compose down
   ```

### Running Locally

1. **Start Redis and PostgreSQL**
   ```bash
   # Using Docker
   docker run -d -p 6379:6379 redis:7-alpine
   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=jobqueue123 postgres:16-alpine
   ```

2. **Initialize database schema**
   ```python
   from jobqueue.backend.postgres_backend import postgres_backend
   
   postgres_backend.connect()
   postgres_backend.initialize_schema()
   ```

3. **Start API server**
   ```bash
   python -m jobqueue.api.main
   ```

4. **Start workers** (in separate terminals)
   ```bash
   python -m jobqueue.worker.main
   ```

## Usage Examples

### Submitting Tasks

```python
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority

# Initialize queue
queue = JobQueue(name="default")

# Submit a simple task
task = queue.submit_task(
    task_name="process_data",
    args=[1, 2, 3],
    kwargs={"multiplier": 2},
    priority=TaskPriority.HIGH
)

print(f"Task submitted: {task.id}")
```

### Task with Dependencies

```python
# Submit parent task
parent_task = queue.submit_task(
    task_name="fetch_data",
    priority=TaskPriority.HIGH
)

# Submit child task that depends on parent
child_task = queue.submit_task(
    task_name="process_data",
    depends_on=[parent_task.id],
    priority=TaskPriority.MEDIUM
)
```

### Checking Task Status

```python
# Get task by ID
task = queue.get_task(task_id)
print(f"Status: {task.status}")
print(f"Result: {task.result}")
```

### Queue Statistics

```python
stats = queue.get_queue_stats()
print(f"Total queued: {stats['total_queued']}")
print(f"By priority: {stats['queued_by_priority']}")
print(f"By status: {stats['status_counts']}")
```

### Rate Limiting

Configure rate limits per queue to control task execution rates:

```python
from jobqueue.core.queue_config import queue_config_manager
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter

# Set rate limit: 100 tasks per minute with 20 burst capacity
queue_config_manager.set_rate_limit(
    queue_name="default",
    max_tasks_per_minute=100,
    burst_allowance=20,
    enabled=True
)

# Get rate limit statistics
stats = distributed_rate_limiter.get_stats("default")
print(f"Current count: {stats['current_count']}")
print(f"Remaining capacity: {stats['remaining_capacity']}")
print(f"Burst used: {stats['burst_used']}")
print(f"Wait time: {stats['wait_time_seconds']}s")
```

#### Rate Limiting Features

- **Sliding Window**: Uses Redis sorted sets for accurate sliding window rate limiting
- **Burst Allowance**: Allow temporary traffic spikes beyond the regular limit
- **Distributed**: Atomic operations ensure correct behavior across multiple workers
- **Per-Queue Configuration**: Each queue can have independent rate limits
- **Dynamic Updates**: Change rate limits without restarting workers
- **Admin API**: RESTful endpoints for monitoring and configuration

Example with API:

```bash
# Configure rate limit via API
curl -X PUT http://localhost:8000/queues/default/rate-limit \
  -H "Content-Type: application/json" \
  -d '{"max_tasks_per_minute": 100, "burst_allowance": 20, "enabled": true}'

# Get rate limit stats
curl http://localhost:8000/queues/default/rate-limit

# Reset rate limit counters
curl -X DELETE http://localhost:8000/queues/default/rate-limit
```

### Task Scheduling

Schedule tasks to run at specific times or with delays:

```python
from datetime import datetime, timedelta
from jobqueue.core.task import Task
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.utils.timezone import schedule_task_at_time

# Schedule with absolute time (eta)
eta = datetime.utcnow() + timedelta(hours=2)
task = Task(
    name="send_reminder",
    args=["user@example.com"],
    eta=eta
)

# Schedule with countdown (relative time)
task = Task(
    name="process_data",
    args=[123],
    countdown=300  # Execute in 5 minutes
)

# Schedule for specific time in timezone
eta = schedule_task_at_time(
    hour=9,
    minute=0,
    tz_name="America/New_York",
    days_ahead=1  # Tomorrow at 9 AM EST
)
task = Task(name="morning_report", eta=eta)
```

#### Recurring Tasks with Cron

Create recurring tasks with cron-like expressions:

```python
from jobqueue.core.recurring_tasks import recurring_task_manager
from jobqueue.core.task import TaskPriority

# Daily at 2 AM
recurring_task_manager.register_recurring_task(
    name="daily_cleanup",
    cron_expression="0 2 * * *",
    task_name="cleanup_old_data",
    priority=TaskPriority.LOW
)

# Every 5 minutes
recurring_task_manager.register_recurring_task(
    name="health_check",
    cron_expression="*/5 * * * *",
    task_name="check_system_health"
)

# Using shortcuts
recurring_task_manager.register_recurring_task(
    name="hourly_sync",
    cron_expression="@hourly",
    task_name="sync_data"
)
```

#### Scheduler Process

Run the scheduler to move ready tasks to execution queues:

```python
from jobqueue.core.scheduler import run_scheduler

# Start scheduler (separate process from workers)
run_scheduler(
    poll_interval=1,  # Check every second
    queues=["default", "scheduled"]
)
```

Or via command line:

```bash
python -m jobqueue.core.scheduler
```

#### Scheduling Features

- **ETA**: Schedule tasks for absolute execution time
- **Countdown**: Schedule tasks with relative delay (seconds)
- **Cron Expressions**: Standard cron syntax for recurring tasks
- **Timezone Support**: Schedule in any timezone, stored as UTC
- **Scheduler Process**: Separate process polls and moves ready tasks
- **Recurring Tasks**: Automatic rescheduling with cron patterns
- **Task Management**: Cancel, pause, resume scheduled tasks

### Task Dependencies & Chaining

Create dependent task workflows where Task B runs only after Task A succeeds:

```python
from jobqueue.core.task import Task
from jobqueue.core.task_dependencies import task_dependency_graph

# Create parent task
parent_task = Task(
    name="fetch_data",
    args=["database"]
)

# Create child task that depends on parent
child_task = Task(
    name="process_data",
    args=[],
    depends_on=[parent_task.id]  # Will wait for parent to complete
)

# Add dependencies to graph
task_dependency_graph.add_dependencies(child_task)
```

#### Using chain() Helper

Create sequential task chains easily:

```python
from jobqueue.core.task_chain import signature, chain

# Create a chain of 3 tasks
result = chain(
    signature("fetch_data", args=["api"]),
    signature("process_data", args=[]),
    signature("save_results", args=[])
).apply_async()

# Or using | operator
result = (
    signature("fetch_data", args=["api"]) |
    signature("process_data", args=[]) |
    signature("save_results", args=[])
).apply_async()
```

#### Parallel Execution with group()

Execute multiple tasks in parallel:

```python
from jobqueue.core.task_chain import group

# Execute 3 tasks simultaneously
tasks = group(
    signature("fetch_from_source1"),
    signature("fetch_from_source2"),
    signature("fetch_from_source3")
).apply_async()
```

#### Chord Pattern (Parallel + Callback)

Run tasks in parallel, then execute callback when all complete:

```python
from jobqueue.core.task_chain import chord

# Parallel tasks + callback
tasks = chord(
    [
        signature("process_chunk1"),
        signature("process_chunk2"),
        signature("process_chunk3")
    ],
    signature("aggregate_results")  # Runs after all chunks processed
)
```

#### Dependency Features

- **Circular Detection**: Automatically detects and prevents circular dependencies
- **Failure Propagation**: Child tasks cancelled if parent fails
- **DAG Support**: Complex directed acyclic graphs with multiple levels
- **Execution Order**: Automatic topological sort for optimal execution
- **Parallel Detection**: Identifies independent tasks that can run concurrently
- **Re-queuing**: Tasks re-queued if dependencies not yet satisfied

### Retry Logic with Exponential Backoff

Failed tasks are automatically retried with exponential backoff:

```python
from jobqueue.core.task import Task

# Task with retry configuration
task = Task(
    name="unstable_api_call",
    args=["https://api.example.com"],
    max_retries=3  # Will retry up to 3 times
)

# Automatic retry with exponential backoff
# Retry 0: 1 second   (2^0)
# Retry 1: 2 seconds  (2^1)
# Retry 2: 4 seconds  (2^2)
# Retry 3: 8 seconds  (2^3)
```

#### Backoff Configuration

Customize backoff behavior:

```python
from jobqueue.core.retry_backoff import ExponentialBackoff

# Custom backoff settings
backoff = ExponentialBackoff(
    base=2.0,        # Base for exponential calculation
    max_delay=300.0, # Maximum delay cap (5 minutes)
    jitter=True      # Add randomness to prevent thundering herd
)

# Calculate delay for retry attempt
delay = backoff.calculate_delay(retry_count=2)  # Returns ~4 seconds
```

#### Retry History

Track all retry attempts:

```python
# After task execution, view retry history
for attempt in task.retry_history:
    print(f"Attempt {attempt['attempt']}: {attempt['error']}")
    print(f"Backoff: {attempt['backoff_seconds']}s")
    print(f"Timestamp: {attempt['timestamp']}")
```

#### Backoff Strategies

Multiple backoff strategies available:

```python
from jobqueue.core.retry_backoff import get_retry_delays

# Exponential (default): 1s, 2s, 4s, 8s, 16s
exp_delays = get_retry_delays(5, strategy="exponential", base=2.0)

# Linear: 5s, 5s, 5s, 5s, 5s
linear_delays = get_retry_delays(5, strategy="linear", delay=5.0)

# Fibonacci: 1s, 1s, 2s, 3s, 5s
fib_delays = get_retry_delays(5, strategy="fibonacci", base=1.0)
```

#### Retry Features

- **Exponential Backoff**: Delays increase exponentially (1s, 2s, 4s, 8s, 16s)
- **Maximum Cap**: Delays capped at 300 seconds by default
- **Jitter**: Random variance prevents thundering herd
- **Retry History**: Complete log of all retry attempts
- **Scheduled Retries**: Uses scheduler for delayed execution
- **Dead Letter Queue**: Failed tasks after max retries moved to DLQ

### Dead Letter Queue

Tasks that exceed maximum retry attempts are automatically moved to the Dead Letter Queue (DLQ):

```python
from jobqueue.core.dead_letter_queue import dead_letter_queue

# View tasks in DLQ
tasks = dead_letter_queue.get_tasks(limit=100, offset=0)

# Get DLQ statistics
stats = dead_letter_queue.get_stats()
print(f"DLQ Size: {stats['size']}")
print(f"By Task Name: {stats['by_task_name']}")
print(f"By Queue: {stats['by_queue']}")

# Retry a task from DLQ
retried_task = dead_letter_queue.retry_task(task_id, reset_retry_count=True)

# Check alert threshold
exceeds, size = dead_letter_queue.check_alert_threshold(threshold=100)
if exceeds:
    print(f"Alert: DLQ size ({size}) exceeds threshold!")
```

#### DLQ Features

- **Automatic Movement**: Tasks exceeding max retries automatically moved to DLQ
- **Failure Details**: Stores failure reason and full stack trace
- **Retry History**: Complete retry history preserved in DLQ entries
- **Manual Retry**: Retry tasks from DLQ with optional retry count reset
- **Statistics**: Breakdown by task name, queue, and timestamps
- **Alerting**: Threshold-based alerts when DLQ size exceeds limit
- **Pagination**: Efficient retrieval with limit and offset
- **Purge**: Clear all tasks from DLQ

#### Admin API

```bash
# Get DLQ tasks
curl http://localhost:8000/dlq?limit=100&offset=0

# Get DLQ statistics
curl http://localhost:8000/dlq/stats

# Get specific task
curl http://localhost:8000/dlq/{task_id}

# Retry task from DLQ
curl -X POST http://localhost:8000/dlq/{task_id}/retry?reset_retry_count=true

# Purge DLQ
curl -X DELETE http://localhost:8000/dlq

# Check alerts
curl http://localhost:8000/dlq/alerts?threshold=100
```

### Task Timeouts

Tasks that exceed their timeout limit are automatically killed and marked as TIMEOUT:

```python
from jobqueue.core.task import Task

# Create task with timeout
task = Task(
    name="long_running_task",
    args=[data],
    timeout=300  # 5 minutes
)

# Task will be killed if it runs longer than 300 seconds
```

#### Soft vs Hard Timeouts

- **Soft Timeout**: Warning logged at 80% of timeout (configurable)
- **Hard Timeout**: Task killed when timeout exceeded

```python
from jobqueue.core.task_timeout import TimeoutManager

# Create timeout manager
manager = TimeoutManager(
    timeout_seconds=300,
    soft_timeout_ratio=0.8,  # 80% = 240 seconds
    enable_soft_timeout=True
)

manager.start()

# Check timeout status
is_timed_out, soft_warning = manager.check()

if soft_warning:
    print("Task approaching timeout (soft warning)")

if is_timed_out:
    print("Task exceeded timeout (hard timeout)")
```

#### Timeout Features

- **Automatic Enforcement**: Tasks killed when timeout exceeded
- **Soft Timeout Warnings**: Alert at configurable threshold (default: 80%)
- **Thread-Based**: Uses threading.Timer for timeout enforcement
- **Process Tree Killing**: Kills child processes (requires psutil)
- **Timeout Logging**: Detailed logs with elapsed time and timeout limits
- **Status Tracking**: Tasks marked as TIMEOUT status
- **Resource Cleanup**: Automatic cleanup after timeout

#### Timeout Configuration

```python
# Disable timeout (0 = no timeout)
task = Task(name="unlimited_task", timeout=0)

# Short timeout (5 seconds)
task = Task(name="quick_task", timeout=5)

# Long timeout (1 hour)
task = Task(name="batch_job", timeout=3600)
```

### Worker Heartbeat & Health Monitoring

Workers send heartbeats every 10 seconds to indicate they're alive. Dead workers are automatically detected and their tasks are recovered:

```python
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus

# Get all workers
workers = worker_heartbeat.get_all_workers_info()

# Check worker status
for worker in workers:
    print(f"{worker['worker_id']}: {worker['status']} (alive: {worker['is_alive']})")
```

#### Worker Status

- **ACTIVE**: Worker is currently processing a task
- **IDLE**: Worker is running but not processing tasks
- **DEAD**: Worker has not sent heartbeat in 30 seconds (stale)

#### Dead Worker Detection

The worker monitor process automatically detects dead workers:

```python
from jobqueue.core.worker_monitor import run_worker_monitor

# Start monitor process (separate from workers)
run_worker_monitor(
    check_interval=10,  # Check every 10 seconds
    stale_threshold=30  # Worker considered dead after 30s
)
```

Or via command line:

```bash
python -m jobqueue.core.worker_monitor
```

#### Task Recovery

The system automatically recovers tasks from crashed workers using active task tracking and orphaned task detection:

```python
from jobqueue.core.task_recovery import task_recovery

# Get orphaned tasks (tasks from dead workers)
orphaned = task_recovery.get_orphaned_tasks()

# Recover all orphaned tasks
recovered_count = task_recovery.recover_orphaned_tasks()
print(f"Recovered {recovered_count} tasks")
```

**How It Works:**

1. **Active Task Tracking**: When a worker starts a task, it's added to `worker:{id}:active` Redis set
2. **Task Locking**: Each task gets a lock (`task:lock:{task_id}`) to prevent duplicate execution
3. **Orphaned Detection**: Monitor scans for tasks in dead workers' active sets
4. **Recovery**: Orphaned tasks are re-queued with PENDING status
5. **Lock Mechanism**: Recovery locks prevent duplicate recovery by multiple monitors

**Recovery Process:**

```python
# Worker starts task
task_recovery.add_active_task(worker_id, task)

# Worker crashes (stops sending heartbeats)
# Monitor detects dead worker after stale threshold

# Monitor recovers orphaned tasks
recovered = task_recovery.recover_orphaned_tasks()
# Tasks are re-queued and can be picked up by other workers
```

**Recovery Statistics:**

```python
# Get recovery statistics
stats = task_recovery.get_recovery_stats()

print(f"Orphaned tasks: {stats['orphaned_tasks']}")
print(f"By worker: {stats['by_worker']}")
print(f"By queue: {stats['by_queue']}")
```

**Prevent Duplicate Execution:**

The lock mechanism ensures tasks aren't executed twice:

```python
# Check if task is locked
is_locked = task_recovery.is_task_locked(task_id)

# Get lock owner
owner = task_recovery.get_task_lock_owner(task_id)
```

**Test Case: Crash Worker, Verify Task Recovery**

```python
# 1. Worker starts processing task
task_recovery.add_active_task(worker_id, task)

# 2. Worker crashes (stops sending heartbeats)
# Wait for stale threshold...

# 3. Monitor detects orphaned task
orphaned = task_recovery.get_orphaned_tasks()

# 4. Task is recovered and re-queued
recovered = task_recovery.recover_orphaned_tasks()
assert recovered == 1
assert queue.size() == 1  # Task re-queued
```

**Recovery Features:**

- **Active Task Storage**: Tasks stored in Redis sets (`worker:{id}:active`)
- **Orphaned Detection**: Automatic detection of tasks from dead workers
- **Lock Mechanism**: Prevents duplicate execution and recovery
- **Automatic Recovery**: Monitor automatically recovers orphaned tasks
- **Statistics**: Track recovery metrics by worker and queue
- **Safe Re-queuing**: Tasks reset to PENDING status before re-queuing

#### Worker Dashboard API

```bash
# List all workers
curl http://localhost:8000/workers

# Get specific worker
curl http://localhost:8000/workers/{worker_id}

# Get worker statistics
curl http://localhost:8000/workers/stats

# Get monitor statistics
curl http://localhost:8000/workers/monitor/stats
```

#### Heartbeat Features

- **Automatic Heartbeats**: Workers send heartbeat every 10 seconds
- **Status Tracking**: ACTIVE when processing, IDLE when waiting
- **Dead Detection**: Workers marked DEAD after 30s without heartbeat
- **Task Recovery**: Automatic re-queuing of tasks from dead workers
- **Worker Dashboard**: Real-time view of all workers and their status
- **Metadata Storage**: Hostname, PID, queue, and start time tracked

### Task Result Backend

The system stores task results in Redis with TTL support. Results include status, return value, error messages, and execution duration:

```python
from jobqueue.backend.result_backend import result_backend, TaskResult

# Store result (automatically done by workers)
result_backend.store_result(task)

# Fetch result by task_id
result = result_backend.get_result(task_id)

if result:
    print(f"Status: {result.status.value}")
    print(f"Result: {result.result}")
    print(f"Duration: {result.duration}s")
```

**TaskResult Class:**

```python
class TaskResult:
    task_id: str
    status: TaskStatus
    result: Any  # Return value (if successful)
    error: str  # Error message (if failed)
    started_at: datetime
    completed_at: datetime
    duration: float  # Execution time in seconds
```

**Result Storage:**

- **Key Format**: `result:{task_id}`
- **Default TTL**: 24 hours (86400 seconds)
- **Custom TTL**: Can be specified per result
- **Automatic Expiration**: Results expire after TTL

**Result Backend API:**

```python
# Store result
result_backend.store_result(task, ttl=3600)  # 1 hour TTL

# Get result
result = result_backend.get_result(task_id)

# Check if result exists
exists = result_backend.result_exists(task_id)

# Get remaining TTL
ttl = result_backend.get_result_ttl(task_id)

# Extend TTL
result_backend.extend_result_ttl(task_id, 7200)  # Extend to 2 hours

# Delete result
result_backend.delete_result(task_id)
```

**Create Result from Task:**

```python
# Automatically create TaskResult from completed Task
result = TaskResult.from_task(task)

# Result includes all execution details
print(f"Task ID: {result.task_id}")
print(f"Status: {result.status.value}")
print(f"Result: {result.result}")
print(f"Duration: {result.duration}s")
```

**REST API Endpoints:**

```bash
# Get task result
GET /tasks/{task_id}/result

# Delete task result
DELETE /tasks/{task_id}/result

# Get result TTL
GET /tasks/{task_id}/result/ttl

# Extend result TTL
PUT /tasks/{task_id}/result/ttl?ttl_seconds=7200
```

**Test Case: Execute Task, Fetch Result**

```python
# 1. Execute task
task = Task(name="my_task", args=[1, 2, 3])
task.mark_success("Task completed")

# 2. Store result
result_backend.store_result(task)

# 3. Fetch result
result = result_backend.get_result(task.id)

assert result is not None
assert result.status == TaskStatus.SUCCESS
assert result.result == "Task completed"
```

**Result Features:**

- **Automatic Storage**: Workers automatically store results
- **TTL Support**: Results expire after configurable time (default 24h)
- **Complex Data**: Supports any serializable Python objects
- **Error Tracking**: Failed tasks store error messages
- **Duration Tracking**: Execution time automatically calculated
- **API Access**: REST endpoints for result retrieval
- **TTL Management**: Extend or check remaining TTL

### Task Deduplication

The system prevents duplicate task execution using task signatures (hash of name + args + kwargs):

```python
from jobqueue.core.task import Task
from jobqueue.core.redis_queue import Queue

# Create task with unique=True
task = Task(
    name="process_data",
    args=[1, 2, 3],
    kwargs={"key": "value"},
    unique=True  # Enable deduplication
)

queue = Queue("default")
task_id = queue.enqueue(task)
```

**How It Works:**

1. **Task Signature**: Hash of task name + args + kwargs (SHA256)
2. **Storage**: Signatures stored in Redis: `dedup:{signature}` with task_id
3. **Duplicate Check**: On enqueue, check if signature exists
4. **Pending Tasks**: If duplicate pending/running → return existing task_id
5. **Completed Tasks**: If duplicate completed → optional re-execution

**Task Signature:**

```python
# Generate signature
task.compute_and_set_signature()
signature = task.task_signature

# Signature is hash of: name + args + kwargs
print(f"Signature: {signature}")
```

**Deduplication API:**

```python
from jobqueue.core.task_deduplication import task_deduplication

# Check for duplicate
existing_task_id = task_deduplication.check_duplicate(
    task,
    allow_re_execution=False  # Prevent re-execution of completed tasks
)

# Register task for deduplication
task_deduplication.register_task(task, ttl=86400)  # 24 hours

# Update task status
task_deduplication.update_task_status(task)

# Get duplicate information
info = task_deduplication.get_duplicate_info(signature)
```

**Enqueue Behavior:**

```python
# First enqueue
task1 = Task(name="my_task", args=[1], unique=True)
task_id1 = queue.enqueue(task1)  # Returns task1.id

# Duplicate enqueue
task2 = Task(name="my_task", args=[1], unique=True)
task_id2 = queue.enqueue(task2)  # Returns task1.id (duplicate detected)

assert task_id1 == task_id2  # Same task_id returned
assert queue.size() == 1     # Only one task in queue
```

**REST API Endpoints:**

```bash
# Check for duplicate task
GET /tasks/{task_id}/duplicate

# Submit task with deduplication
POST /tasks
{
  "task_name": "my_task",
  "args": [1, 2, 3],
  "unique": true
}
```

**Test Case: Enqueue Same Task Twice, Verify Only Runs Once**

```python
# 1. Enqueue task with unique=True
task1 = Task(name="process", args=[1], unique=True)
task_id1 = queue.enqueue(task1)

# 2. Enqueue same task again
task2 = Task(name="process", args=[1], unique=True)
task_id2 = queue.enqueue(task2)

# 3. Verify only one task
assert task_id1 == task_id2  # Same task_id returned
assert queue.size() == 1     # Only one task in queue
```

**Deduplication Features:**

- **Signature-Based**: Uses SHA256 hash of name + args + kwargs
- **Configurable**: Enable per task with `unique=True`
- **Status-Aware**: Handles pending, running, and completed tasks
- **TTL Support**: Dedup keys expire after configurable time (default 24h)
- **Re-Execution Control**: Optional re-execution of completed tasks
- **Hash Collision Handling**: Detects potential collisions (extremely rare with SHA256)
- **Queue Support**: Works with both FIFO and priority queues

**TTL for Dedup Keys:**

- **Default TTL**: 24 hours (86400 seconds)
- **Configurable**: Can be set per task registration
- **Automatic Expiration**: Stale entries removed after TTL
- **Status Tracking**: Task status tracked separately with same TTL

**Hash Collisions:**

SHA256 collisions are extremely rare (practically impossible). The system includes collision detection that compares task details if the same signature maps to different tasks.

### Task Cancellation

The system supports cancelling pending or running tasks with graceful shutdown and force kill options:

```python
from jobqueue.core.task_cancellation import task_cancellation, CancellationReason
from jobqueue.core.task import Task

# Cancel pending task (removes from queue)
task = Task(name="my_task", args=[1], status=TaskStatus.PENDING)
task_cancellation.cancel_pending_task(task, reason=CancellationReason.USER_REQUESTED)

# Cancel running task (requests cancellation)
task = Task(name="my_task", args=[1], status=TaskStatus.RUNNING)
task_cancellation.cancel_running_task(task, force=False)  # Graceful
```

**Cancellation Reasons:**

- `USER_REQUESTED`: User requested cancellation
- `TIMEOUT`: Task exceeded timeout
- `DEPENDENCY_FAILED`: Dependency task failed
- `WORKER_SHUTDOWN`: Worker shutting down
- `SYSTEM_ERROR`: System error occurred
- `RATE_LIMIT_EXCEEDED`: Rate limit exceeded
- `OTHER`: Other reasons

**How It Works:**

1. **Pending Tasks**: Removed from queue immediately
2. **Running Tasks**: Cancellation requested, worker checks `should_cancel()` periodically
3. **Graceful Shutdown**: Worker stops execution cleanly
4. **Force Kill**: Worker forcefully terminates task (if supported)

**Cancellation API:**

```python
# Request cancellation
task_cancellation.request_cancellation(
    task_id,
    reason=CancellationReason.USER_REQUESTED,
    force=False  # True for force kill
)

# Check if task should be cancelled
should_cancel, force, reason = task_cancellation.should_cancel(task_id)

# Cancel task (handles pending/running automatically)
task_cancellation.cancel_task(task, reason=CancellationReason.USER_REQUESTED)

# Get cancellation information
info = task_cancellation.get_cancellation_info(task_id)
```

**Worker Cancellation Check:**

Workers check for cancellation:
- Before starting task execution
- After task execution completes
- Periodically during long-running tasks (if task supports it)

```python
# In worker code
should_cancel, force, reason = task_cancellation.should_cancel(task.id)
if should_cancel:
    task.mark_cancelled(reason)
    return  # Stop execution
```

**REST API Endpoint:**

```bash
# Cancel task
POST /tasks/{task_id}/cancel?reason=user_requested&force=false

# Response
{
  "message": "Task cancelled",
  "task_id": "task-123",
  "status": "cancelled",
  "reason": "user_requested",
  "force": false
}
```

**Test Case: Cancel Pending and Running Tasks**

```python
# 1. Cancel pending task
task1 = Task(name="task", args=[1], status=TaskStatus.PENDING)
queue.enqueue(task1)
task_cancellation.cancel_pending_task(task1)
assert task1.status == TaskStatus.CANCELLED
assert queue.size() == 0  # Removed from queue

# 2. Cancel running task
task2 = Task(name="task", args=[1], status=TaskStatus.RUNNING)
task_cancellation.cancel_running_task(task2)
should_cancel, _, _ = task_cancellation.should_cancel(task2.id)
assert should_cancel is True  # Worker will check and stop
```

**Cancellation Features:**

- **Pending Task Removal**: Tasks removed from queue immediately
- **Running Task Cancellation**: Workers check `should_cancel()` and stop gracefully
- **Cancellation Reasons**: Track why task was cancelled
- **Graceful vs Force**: Support both graceful shutdown and force kill
- **Status Tracking**: Tasks marked as CANCELLED with reason
- **Redis Storage**: Cancellation flags stored in `cancel:{task_id}`
- **Worker Integration**: Automatic cancellation checks in workers

**Deep End: Long-Running Tasks**

For tasks that don't check `should_cancel()` (e.g., long-running loops), consider:
- Breaking loops into smaller chunks
- Adding periodic cancellation checks in task code
- Using timeouts as fallback
- Implementing cleanup handlers for partially completed work

### Metrics Collection

The system tracks performance metrics using Redis sorted sets for time-series data:

```python
from jobqueue.core.metrics import metrics_collector

# Get all metrics
metrics = metrics_collector.get_all_metrics(window_seconds=3600)

print(f"Tasks enqueued/sec: {metrics['tasks_enqueued_per_second']}")
print(f"Tasks completed/sec: {metrics['tasks_completed_per_second']}")
print(f"Duration p50: {metrics['duration_percentiles'][0.5]}ms")
print(f"Success rate: {metrics['success_rate']['success_rate']:.2%}")
```

**Metrics Tracked:**

- **Tasks Enqueued Per Second**: Rate of task enqueuing
- **Tasks Completed Per Second**: Rate of task completion
- **Task Duration Percentiles**: p50 (median), p95, p99 in milliseconds
- **Success vs Failure Rate**: Percentage of successful vs failed tasks
- **Queue Size Per Priority**: Current queue size for each priority level
- **Worker Utilization**: Active vs idle workers, utilization percentage

**Metrics API:**

```python
# Record task enqueued
metrics_collector.record_task_enqueued(queue_name, priority)

# Record task completed
metrics_collector.record_task_completed(task, duration, success=True)

# Get tasks per second
enqueued_rate = metrics_collector.get_tasks_enqueued_per_second(window_seconds=60)
completed_rate = metrics_collector.get_tasks_completed_per_second(window_seconds=60)

# Get duration percentiles
percentiles = metrics_collector.get_task_duration_percentiles(
    window_seconds=3600,
    percentiles=[0.5, 0.95, 0.99]
)

# Get success rate
rates = metrics_collector.get_success_rate(window_seconds=3600)
print(f"Success: {rates['success_rate']:.2%}")
print(f"Failure: {rates['failure_rate']:.2%}")

# Get worker utilization
utilization = metrics_collector.get_worker_utilization()
print(f"Utilization: {utilization['utilization_percent']:.2f}%")

# Aggregate metrics
hourly = metrics_collector.aggregate_metrics("hourly")
daily = metrics_collector.aggregate_metrics("daily")
```

**Time-Series Storage:**

Metrics are stored in Redis using sorted sets (ZADD) for efficient time-range queries:

- **Key Format**: `metrics:{metric_name}`
- **Score**: Timestamp (for time-series queries)
- **Member**: Metric value or identifier
- **TTL**: 7 days for raw metrics, 30 days for aggregated

**REST API Endpoints:**

```bash
# Get all metrics
GET /metrics?window_seconds=3600

# Response
{
  "timestamp": 1234567890.0,
  "window_seconds": 3600,
  "tasks": {
    "enqueued_per_second": 10.5,
    "completed_per_second": 9.8
  },
  "duration_percentiles": {
    "p50_ms": 150.0,
    "p95_ms": 500.0,
    "p99_ms": 1000.0
  },
  "success_rate": {
    "success_rate": 0.95,
    "failure_rate": 0.05,
    "total": 1000,
    "success_count": 950,
    "failure_count": 50
  },
  "queue_size_per_priority": {
    "high": 10,
    "medium": 50,
    "low": 20
  },
  "worker_utilization": {
    "total_workers": 5,
    "active_workers": 3,
    "idle_workers": 2,
    "dead_workers": 0,
    "utilization_percent": 60.0
  }
}

# Get aggregated metrics
GET /metrics/aggregate?aggregation=hourly
GET /metrics/aggregate?aggregation=daily
```

**Test Case: Generate Metrics, Verify Accuracy**

```python
# 1. Enqueue tasks
for i in range(20):
    queue.enqueue(Task(name="task", args=[i]))

# 2. Complete tasks
for i in range(15):
    task = Task(name="task", args=[i])
    metrics_collector.record_task_completed(task, 0.1, success=True)

# 3. Generate metrics
metrics = metrics_collector.get_all_metrics(window_seconds=60)

# 4. Verify accuracy
assert metrics['tasks_enqueued_per_second'] > 0
assert metrics['tasks_completed_per_second'] > 0
assert 0.5 in metrics['duration_percentiles']  # p50 exists
assert metrics['success_rate']['total'] == 15
```

**Metrics Features:**

- **Automatic Collection**: Metrics recorded automatically on enqueue/completion
- **Time-Series Storage**: Redis sorted sets for efficient queries
- **Percentile Calculation**: p50, p95, p99 for task durations
- **Aggregation**: Hourly and daily aggregations
- **Per-Queue Metrics**: Track metrics per queue and priority
- **Per-Task Metrics**: Track duration per task name
- **Worker Metrics**: Real-time worker utilization
- **TTL Management**: Automatic cleanup of old metrics

**Percentile Calculation:**

Percentiles are calculated from sorted duration values:
- **p50 (median)**: 50% of tasks complete within this duration
- **p95**: 95% of tasks complete within this duration
- **p99**: 99% of tasks complete within this duration

### Multiple Workers

The system supports running multiple workers safely with no duplicate task processing:

```python
from jobqueue.core.worker_pool import WorkerPool, distributed_worker_manager
from jobqueue.worker.simple_worker import SimpleWorker

# Create worker pool
pool = WorkerPool(
    pool_name="my_pool",
    worker_class=SimpleWorker,
    queue_name="default",
    initial_workers=5
)

# Start pool
pool.start()

# Scale up/down
pool.scale_up(3)  # Add 3 workers
pool.scale_down(2)  # Remove 2 workers
```

**Unique Worker IDs:**

Each worker has a unique ID based on hostname + PID:
- **Format**: `worker-{hostname}-{pid}`
- **Uniqueness**: Guaranteed across different machines and processes
- **Tracking**: Workers tracked in Redis for health monitoring

**Atomic Task Dequeue:**

Workers compete for tasks using Redis BRPOP, which is atomic:
- **BRPOP**: Blocking right pop - only one worker gets each task
- **Atomic Operation**: Redis ensures no task is processed twice
- **Race Condition Safe**: Multiple workers can safely call BRPOP simultaneously

**Worker Pool Management:**

```python
# Create pool
pool = distributed_worker_manager.create_pool(
    pool_name="production_pool",
    worker_class=SimpleWorker,
    queue_name="default",
    initial_workers=10
)

pool.start()

# Get pool status
status = pool.get_pool_status()
print(f"Total workers: {status['total_workers']}")
print(f"Alive workers: {status['alive_workers']}")

# Scale workers
pool.scale_up(5)  # Add 5 workers
pool.scale_down(3)  # Remove 3 workers

# Restart worker
pool.restart_worker(worker_id)

# Stop pool
pool.stop(graceful=True)
```

**Worker Scaling:**

```python
# Scale up (add workers)
new_workers = pool.scale_up(count=5)
print(f"Added {len(new_workers)} workers")

# Scale down (remove workers)
removed_workers = pool.scale_down(count=3)
print(f"Removed {len(removed_workers)} workers")
```

**REST API Endpoints:**

```bash
# Create worker pool
POST /worker-pools?pool_name=my_pool&queue_name=default&initial_workers=5

# List all pools
GET /worker-pools

# Get pool status
GET /worker-pools/{pool_name}

# Scale up
POST /worker-pools/{pool_name}/scale-up?count=3

# Scale down
POST /worker-pools/{pool_name}/scale-down?count=2

# Delete pool
DELETE /worker-pools/{pool_name}?graceful=true
```

**Test Case: 10 Workers, 1000 Tasks, Verify No Duplicates**

```python
# 1. Enqueue 1000 tasks
queue = Queue("test_queue")
for i in range(1000):
    task = Task(name="task", args=[i])
    queue.enqueue(task)

# 2. Start 10 workers
pool = WorkerPool(initial_workers=10)
pool.start()

# 3. Workers process tasks using BRPOP (atomic)
# Each worker calls queue.dequeue() which uses BRPOP
# Redis ensures only one worker gets each task

# 4. Verify no duplicates
processed = []  # Track processed task IDs
# ... workers process tasks ...

assert len(processed) == 1000  # All tasks processed
assert len(set(processed)) == 1000  # No duplicates
```

**Multiple Workers Features:**

- **Unique Worker IDs**: Hostname + PID ensures uniqueness
- **Atomic Dequeue**: BRPOP prevents duplicate processing
- **Worker Pool Management**: Create, scale, and manage worker pools
- **Scaling**: Scale workers up/down dynamically
- **Graceful Shutdown**: Workers shutdown gracefully with SIGTERM
- **Process Management**: Multiprocessing for true parallelism
- **Status Tracking**: Real-time worker pool status
- **Distributed Support**: Run workers across multiple machines

**Deep End: Race Conditions and Network Partitions**

- **Two Workers Pop Same Task**: Cannot happen - BRPOP is atomic at Redis level
- **Network Partition**: If worker loses connection to Redis:
  - Task remains in queue (not removed)
  - Worker cannot complete task
  - Task recovery system will detect orphaned task
  - Task will be re-queued for another worker

**How BRPOP Ensures Atomicity:**

1. Multiple workers call `BRPOP queue:default 5`
2. Redis queues the requests
3. When a task is available, Redis atomically:
   - Removes task from queue
   - Returns it to ONE worker
4. Other workers continue waiting
5. No task is ever returned to multiple workers

### Worker Autoscaling

The system supports automatic scaling of workers based on queue size:

```python
from jobqueue.core.worker_pool import WorkerPool
from jobqueue.core.worker_autoscaling import create_autoscaler
from jobqueue.worker.simple_worker import SimpleWorker

# Create worker pool
pool = WorkerPool(
    pool_name="my_pool",
    worker_class=SimpleWorker,
    queue_name="default",
    initial_workers=1
)
pool.start()

# Enable autoscaling
autoscaler = create_autoscaler(
    pool=pool,
    scale_up_threshold=100,    # Scale up when queue > 100
    scale_down_threshold=10,   # Scale down when queue < 10
    min_workers=1,             # Minimum workers
    max_workers=50,            # Maximum workers
    check_interval=30,         # Check every 30 seconds
    cooldown_seconds=60        # Cooldown between scaling actions
)

autoscaler.start()
```

**Autoscaling Features:**

- **Queue Size Monitoring**: Monitors queue size periodically
- **Scale Up**: When queue size > threshold, add workers
- **Scale Down**: When queue size < threshold, remove idle workers
- **Min/Max Limits**: Enforce minimum and maximum worker counts
- **Cooldown Period**: Prevent rapid scaling (configurable)
- **Health Checks**: Verify system health before scaling
- **Scaling History**: Track all scaling actions

**Autoscaling Logic:**

```python
# Scale up when queue size > threshold
if queue_size > scale_up_threshold and workers < max_workers:
    add_workers()

# Scale down when queue size < threshold
if queue_size < scale_down_threshold and workers > min_workers:
    remove_idle_workers()
```

**Configuration:**

- **scale_up_threshold**: Queue size to trigger scale up (default: 100)
- **scale_down_threshold**: Queue size to trigger scale down (default: 10)
- **min_workers**: Minimum number of workers (default: 1)
- **max_workers**: Maximum number of workers (default: 50)
- **check_interval**: How often to check queue size in seconds (default: 30)
- **cooldown_seconds**: Cooldown period between scaling actions (default: 60)

**REST API Endpoints:**

```bash
# Enable autoscaling
POST /worker-pools/{pool_name}/autoscale?scale_up_threshold=100&scale_down_threshold=10&min_workers=1&max_workers=50

# Get autoscaler status
GET /worker-pools/{pool_name}/autoscale

# Disable autoscaling
DELETE /worker-pools/{pool_name}/autoscale
```

**Test Case: Queue Grows, Verify Workers Scale Up**

```python
# 1. Start with 1 worker
pool = WorkerPool(initial_workers=1)
pool.start()

# 2. Enqueue tasks to grow queue > 100
queue = Queue("default")
for i in range(150):
    task = Task(name="task", args=[i])
    queue.enqueue(task)

# 3. Enable autoscaling
autoscaler = create_autoscaler(
    pool=pool,
    scale_up_threshold=100,
    check_interval=2
)
autoscaler.start()

# 4. Wait for scaling
time.sleep(5)

# 5. Verify workers scaled up
status = autoscaler.get_status()
assert status['current_workers'] > 1  # Workers increased
```

**Autoscaling Behavior:**

- **Scale Up**: Adds workers when queue size exceeds threshold
- **Scale Down**: Removes idle workers when queue size is below threshold
- **Cooldown**: Prevents rapid scaling (waits for cooldown period)
- **Health Check**: Verifies Redis connection and pool status before scaling
- **Min/Max Limits**: Respects minimum and maximum worker limits
- **History Tracking**: Records all scaling actions with timestamps

**Deep End: Autoscaling Challenges**

- **Rapid Queue Growth**: Cooldown period prevents excessive scaling
- **Queue Size Fluctuations**: Cooldown prevents thrashing
- **Worker Startup Time**: Consider worker startup time in scaling decisions
- **Resource Constraints**: Max workers limit prevents resource exhaustion

## Configuration

All configuration is managed through environment variables. See `.env.example` for available options:

### Redis Configuration
- `REDIS_HOST`: Redis server host (default: localhost)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `REDIS_MAX_CONNECTIONS`: Connection pool size (default: 50)

### PostgreSQL Configuration
- `POSTGRES_HOST`: PostgreSQL server host (default: localhost)
- `POSTGRES_PORT`: PostgreSQL server port (default: 5432)
- `POSTGRES_DB`: Database name (default: jobqueue)
- `POSTGRES_USER`: Database user (default: jobqueue)
- `POSTGRES_PASSWORD`: Database password (default: jobqueue123)

### Worker Configuration
- `WORKER_CONCURRENCY`: Number of concurrent tasks per worker (default: 4)
- `WORKER_HEARTBEAT_INTERVAL`: Heartbeat interval in seconds (default: 30)
- `WORKER_MAX_TASKS_PER_CHILD`: Max tasks before worker restart (default: 1000)

### Queue Configuration
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `RETRY_BACKOFF_BASE`: Exponential backoff base (default: 2)
- `TASK_TIMEOUT`: Task timeout in seconds (default: 300)
- `RESULT_TTL`: Result TTL in seconds (default: 3600)

### Rate Limiting
- `RATE_LIMIT_HIGH`: High priority rate limit (tasks/min, default: 1000)
- `RATE_LIMIT_MEDIUM`: Medium priority rate limit (tasks/min, default: 500)
- `RATE_LIMIT_LOW`: Low priority rate limit (tasks/min, default: 100)

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=jobqueue --cov-report=html

# Run specific test file
pytest tests/test_queue.py
```

## Monitoring

### Database Tables

1. **tasks** - All task metadata and results
2. **workers** - Worker registration and health status
3. **dead_letter_queue** - Failed tasks for manual inspection
4. **task_metrics** - Performance metrics and statistics

### Logging

Logs are structured JSON by default and written to:
- `stdout` (for container logs)
- `logs/jobqueue_{date}.log` (rotating daily, 30-day retention)

### Health Checks

```bash
# Check API health
curl http://localhost:8000/health

# Check Redis connection
redis-cli ping

# Check PostgreSQL connection
psql -h localhost -U jobqueue -d jobqueue -c "SELECT 1"
```

## Development

### Code Style

```bash
# Format code
black jobqueue/

# Sort imports
isort jobqueue/

# Lint
flake8 jobqueue/

# Type checking
mypy jobqueue/
```

### Adding New Task Types

1. Define your task function
2. Register it with the worker
3. Submit tasks using the task name

Example:
```python
# In your task definitions
def my_custom_task(arg1, arg2, multiplier=1):
    result = (arg1 + arg2) * multiplier
    return result

# Submit the task
queue.submit_task(
    task_name="my_custom_task",
    args=[10, 20],
    kwargs={"multiplier": 2}
)
```

## Troubleshooting

### Common Issues

1. **Connection refused errors**
   - Ensure Redis and PostgreSQL are running
   - Check firewall settings
   - Verify connection parameters in `.env`

2. **Tasks not being processed**
   - Check if workers are running: `docker-compose ps`
   - View worker logs: `docker-compose logs worker1`
   - Verify queue has tasks: Check Redis or API

3. **Database schema errors**
   - Re-initialize schema: `postgres_backend.initialize_schema()`
   - Check PostgreSQL logs for errors

## Roadmap

### Phase 1: Core Queue Implementation (Completed)
- [x] Project setup and structure
- [x] Redis connection manager
- [x] PostgreSQL backend
- [x] Basic task queue
- [x] Priority scheduling
- [x] Docker configuration

### Phase 2: Worker Implementation (Next)
- [ ] Worker process management
- [ ] Task execution engine
- [ ] Retry logic with exponential backoff
- [ ] Worker heartbeat system
- [ ] Task timeout handling

### Phase 3: Advanced Features
- [ ] Task dependencies and chaining
- [ ] Rate limiting implementation
- [ ] Dead letter queue
- [ ] Task deduplication
- [ ] Task cancellation

### Phase 4: Monitoring & UI
- [ ] REST API endpoints
- [ ] Web-based monitoring UI
- [ ] Metrics collection
- [ ] Prometheus integration
- [ ] Grafana dashboards

### Phase 5: Production Hardening
- [ ] Connection failure recovery
- [ ] Worker crash recovery
- [ ] Comprehensive test suite
- [ ] Performance optimization
- [ ] Documentation completion

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues, questions, or contributions, please open an issue on GitHub.

---

**Built as a learning project to understand distributed systems, job queues, and production-grade Python applications.**
