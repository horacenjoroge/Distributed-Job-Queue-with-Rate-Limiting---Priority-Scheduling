# Test Suite

Comprehensive test suite for the distributed job queue system.

## Test Structure

```
tests/
├── unit/              # Unit tests for individual components
├── integration/       # Integration tests for component interactions
├── load/              # Load tests (1000+ tasks)
├── race/              # Race condition tests
├── failure/           # Failure recovery tests
└── conftest.py        # Pytest fixtures and configuration
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=jobqueue --cov-report=html
```

### Run specific test categories
```bash
# Unit tests only
pytest -m unit

# Integration tests
pytest -m integration

# Load tests
pytest -m load

# Race condition tests
pytest -m race

# Failure recovery tests
pytest -m failure

# Slow tests
pytest -m slow
```

### Run specific test file
```bash
pytest tests/unit/test_task.py
```

## Load Testing with Locust

### Start Locust
```bash
locust -f locustfile.py --host=http://localhost:8000
```

Then open http://localhost:8089 in your browser to start the load test.

### Run headless
```bash
locust -f locustfile.py --host=http://localhost:8000 --headless -u 100 -r 10 -t 60s
```

This runs:
- 100 users
- 10 users spawned per second
- For 60 seconds

## Test Coverage

Target: 90%+ coverage

View coverage report:
```bash
pytest --cov=jobqueue --cov-report=html
open htmlcov/index.html
```

## Test Categories

### Unit Tests
- Test individual functions and classes in isolation
- Fast execution
- No external dependencies (use mocks/fakes)

### Integration Tests
- Test component interactions
- Use fake Redis and mock PostgreSQL
- Test real workflows

### Load Tests
- Test system under high load
- 1000+ tasks
- Multiple workers
- Concurrent operations

### Race Condition Tests
- Multiple workers competing for tasks
- Concurrent submissions
- Distributed lock behavior

### Failure Recovery Tests
- Worker crashes
- Task timeouts
- Network failures
- Orphaned task recovery

## Writing New Tests

1. Place tests in appropriate directory (unit/integration/load/race/failure)
2. Use appropriate pytest markers
3. Use fixtures from `conftest.py`
4. Follow naming convention: `test_*.py` files, `test_*` functions
5. Aim for clear, descriptive test names

## Example Test

```python
import pytest
from jobqueue.core.task import Task

@pytest.mark.unit
class TestTask:
    def test_task_creation(self):
        task = Task(name="test_task")
        assert task.name == "test_task"
        assert task.status.value == "pending"
```
