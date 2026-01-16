.PHONY: help install dev-install test lint format clean docker-up docker-down docker-logs

help:
	@echo "Available commands:"
	@echo "  make install       - Install production dependencies"
	@echo "  make dev-install   - Install development dependencies"
	@echo "  make test          - Run tests with coverage"
	@echo "  make lint          - Run linters (flake8, mypy)"
	@echo "  make format        - Format code (black, isort)"
	@echo "  make clean         - Clean up generated files"
	@echo "  make docker-up     - Start all Docker services"
	@echo "  make docker-down   - Stop all Docker services"
	@echo "  make docker-logs   - View Docker logs"
	@echo "  make init-db       - Initialize database schema"

install:
	pip install -r requirements.txt

dev-install:
	pip install -r requirements.txt
	pip install pytest pytest-asyncio pytest-cov pytest-mock black flake8 isort mypy

test:
	pytest --cov=jobqueue --cov-report=html --cov-report=term

lint:
	flake8 jobqueue/ --max-line-length=100
	mypy jobqueue/

format:
	black jobqueue/ tests/
	isort jobqueue/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .pytest_cache/ .coverage htmlcov/

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-build:
	docker-compose build

init-db:
	python -c "from jobqueue.backend.postgres_backend import postgres_backend; \
	           postgres_backend.connect(); \
	           postgres_backend.initialize_schema(); \
	           print('Database schema initialized successfully')"

run-api:
	python -m jobqueue.api.main

run-worker:
	python -m jobqueue.worker.main
