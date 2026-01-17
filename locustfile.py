"""
Locust load testing script for the job queue API.
"""
from locust import HttpUser, task, between
import random
import json


class JobQueueUser(HttpUser):
    """Locust user for load testing job queue API."""
    
    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks
    
    def on_start(self):
        """Set up test user."""
        # Set API key if needed
        self.headers = {
            "Content-Type": "application/json",
            # "X-API-Key": "test-key"  # Uncomment if API keys are required
        }
    
    @task(3)
    def submit_task(self):
        """Submit a task (most common operation)."""
        task_data = {
            "task_name": "test_task",
            "args": [random.randint(1, 100), random.randint(1, 100)],
            "kwargs": {"key": f"value_{random.randint(1, 1000)}"},
            "priority": random.choice(["high", "medium", "low"]),
            "queue_name": "default",
            "max_retries": 3,
            "timeout": 300
        }
        
        response = self.client.post(
            "/tasks",
            json=task_data,
            headers=self.headers,
            name="Submit Task"
        )
        
        if response.status_code == 201:
            self.task_id = response.json().get("id")
    
    @task(2)
    def get_task_status(self):
        """Get task status."""
        if hasattr(self, 'task_id') and self.task_id:
            self.client.get(
                f"/tasks/{self.task_id}",
                headers=self.headers,
                name="Get Task Status"
            )
    
    @task(1)
    def list_tasks(self):
        """List tasks."""
        self.client.get(
            "/tasks?skip=0&limit=50",
            headers=self.headers,
            name="List Tasks"
        )
    
    @task(1)
    def get_queues(self):
        """Get queue information."""
        self.client.get(
            "/queues",
            headers=self.headers,
            name="Get Queues"
        )
    
    @task(1)
    def get_workers(self):
        """Get worker information."""
        self.client.get(
            "/workers",
            headers=self.headers,
            name="Get Workers"
        )
    
    @task(1)
    def get_metrics(self):
        """Get metrics."""
        self.client.get(
            "/metrics",
            headers=self.headers,
            name="Get Metrics"
        )
