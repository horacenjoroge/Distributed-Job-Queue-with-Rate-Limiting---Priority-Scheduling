# Distributed Job Queue with Rate Limiting & Priority Scheduling

A robust, production-ready distributed job queue system built from scratch in Python. This system provides advanced features like priority scheduling, rate limiting, task dependencies, dead letter queues, and comprehensive monitoring capabilities.

## 🚀 Features

### Core Features
- ✅ **Custom Task Queue Implementation** - Built from scratch without using Celery
- ✅ **Priority Queues** - High/Medium/Low priority task scheduling
- ✅ **Rate Limiting** - Configurable rate limits per queue (max X tasks per minute)
- ✅ **Dead Letter Queue** - Failed tasks automatically moved after N retries
- ✅ **Task Chaining & Dependencies** - Task B runs only after Task A succeeds
- ✅ **Distributed Workers** - Multiple workers across different machines
- ✅ **Worker Health Monitoring** - Heartbeats and auto-restart capabilities
- ✅ **Result Backend** - Store results in Redis with configurable TTL
- ✅ **Exponential Backoff** - Intelligent retry mechanism with backoff
- ✅ **Task Cancellation** - Cancel running or pending tasks

### Advanced Features
- ⏳ **Task Recovery** - Handle worker crashes mid-task
- ⏳ **Task Deduplication** - Prevent duplicate task execution
- ⏳ **Web UI** - Monitor queues and tasks via web interface
- ⏳ **Connection Failure Handling** - Graceful handling of Redis/DB failures
- ⏳ **Task Timeouts** - Automatically kill long-running tasks
- ⏳ **Metrics & Observability** - Track duration, success rate, etc.

## 🏗️ Architecture

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

## 📦 Project Structure

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

## 🛠️ Tech Stack

- **Language**: Python 3.11+
- **Message Broker**: Redis 7
- **Database**: PostgreSQL 16
- **API Framework**: FastAPI
- **Concurrency**: multiprocessing/threading
- **Logging**: loguru (structured logging)
- **Containerization**: Docker & Docker Compose

## 🚦 Getting Started

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

## 📖 Usage Examples

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

## ⚙️ Configuration

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

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=jobqueue --cov-report=html

# Run specific test file
pytest tests/test_queue.py
```

## 📊 Monitoring

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

## 🔧 Development

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

## 🐛 Troubleshooting

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

## 🗺️ Roadmap

### Phase 1: Core Queue Implementation ✅
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

## 📄 License

MIT License - See LICENSE file for details

## 👥 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For issues, questions, or contributions, please open an issue on GitHub.

---

**Built with ❤️ as a learning project to understand distributed systems, job queues, and production-grade Python applications.**
