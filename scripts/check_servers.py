#!/usr/bin/env python3
"""
Check which servers are currently running.
"""
import subprocess
import sys
import requests
import json
from typing import Dict, List, Tuple

def check_process(pattern: str) -> List[Dict]:
    """Check if a process matching pattern is running."""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            check=True
        )
        lines = result.stdout.split('\n')
        matches = []
        for line in lines:
            if pattern in line and 'grep' not in line:
                parts = line.split()
                if len(parts) >= 11:
                    matches.append({
                        "pid": parts[1],
                        "cpu": parts[2],
                        "mem": parts[3],
                        "command": ' '.join(parts[10:])
                    })
        return matches
    except Exception as e:
        return []

def check_port(port: int) -> bool:
    """Check if a port is listening."""
    try:
        result = subprocess.run(
            ["lsof", "-i", f":{port}"],
            capture_output=True,
            text=True
        )
        return "LISTEN" in result.stdout
    except Exception:
        try:
            result = subprocess.run(
                ["netstat", "-an"],
                capture_output=True,
                text=True
            )
            return f":{port}" in result.stdout and "LISTEN" in result.stdout
        except Exception:
            return False

def check_api_endpoint(endpoint: str) -> Tuple[bool, dict]:
    """Check if an API endpoint is responding."""
    try:
        response = requests.get(f"http://localhost:8000{endpoint}", timeout=2)
        if response.status_code == 200:
            return True, response.json()
        return False, {}
    except Exception:
        return False, {}

def check_docker_services() -> List[str]:
    """Check which Docker services are running."""
    try:
        result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            check=True
        )
        services = []
        for line in result.stdout.split('\n'):
            if 'jobqueue' in line and 'Up' in line:
                service_name = line.split()[0]
                services.append(service_name)
        return services
    except Exception:
        return []

def main():
    print("=" * 60)
    print("SERVER STATUS CHECK")
    print("=" * 60)
    print()
    
    # Check infrastructure services
    print("📦 Infrastructure Services:")
    print("-" * 60)
    docker_services = check_docker_services()
    if docker_services:
        for service in docker_services:
            print(f"  ✅ {service} (Docker)")
    else:
        print("  ❌ No Docker services found")
    
    redis_listening = check_port(6379)
    postgres_listening = check_port(5432)
    print(f"  {'✅' if redis_listening else '❌'} Redis (port 6379) - {'Listening' if redis_listening else 'Not listening'}")
    print(f"  {'✅' if postgres_listening else '❌'} PostgreSQL (port 5432) - {'Listening' if postgres_listening else 'Not listening'}")
    print()
    
    # Check API Server
    print("🌐 API Server:")
    print("-" * 60)
    api_processes = check_process("jobqueue.api.main")
    api_port = check_port(8000)
    api_health, health_data = check_api_endpoint("/health")
    
    if api_processes:
        for proc in api_processes:
            print(f"  ✅ Running (PID: {proc['pid']})")
            print(f"     Command: {proc['command'][:80]}...")
    else:
        print("  ❌ Not running")
    
    print(f"  {'✅' if api_port else '❌'} Port 8000 - {'Listening' if api_port else 'Not listening'}")
    print(f"  {'✅' if api_health else '❌'} Health endpoint - {'Responding' if api_health else 'Not responding'}")
    if api_health:
        print(f"     Status: {health_data.get('status', 'unknown')}")
        print(f"     Redis: {'✅' if health_data.get('redis_connected') else '❌'}")
        print(f"     PostgreSQL: {'✅' if health_data.get('postgres_connected') else '❌'}")
    print()
    
    # Check Workers
    print("👷 Workers:")
    print("-" * 60)
    worker_processes = check_process("jobqueue.worker.main")
    if worker_processes:
        for proc in worker_processes:
            print(f"  ✅ Running (PID: {proc['pid']})")
    else:
        print("  ❌ No workers running")
    
    # Check API for worker count
    workers_ok, workers_data = check_api_endpoint("/workers")
    if workers_ok:
        workers_list = workers_data.get("workers", [])
        active_count = len([w for w in workers_list if w.get("status") == "ACTIVE"])
        print(f"  📊 API reports {len(workers_list)} worker(s), {active_count} active")
    print()
    
    # Check Scheduler
    print("⏰ Scheduler:")
    print("-" * 60)
    scheduler_processes = check_process("jobqueue.core.scheduler")
    if scheduler_processes:
        for proc in scheduler_processes:
            print(f"  ✅ Running (PID: {proc['pid']})")
    else:
        print("  ❌ Not running (optional - only needed for scheduled tasks)")
    print()
    
    # Check Worker Monitor
    print("🔍 Worker Monitor:")
    print("-" * 60)
    monitor_processes = check_process("worker_monitor")
    if monitor_processes:
        for proc in monitor_processes:
            print(f"  ✅ Running (PID: {proc['pid']})")
    else:
        print("  ❌ Not running (optional - monitors dead workers)")
    print()
    
    # Check Queues
    print("📋 Queue Status:")
    print("-" * 60)
    queues_ok, queues_data = check_api_endpoint("/queues")
    if queues_ok:
        queues_list = queues_data.get("queues", [])
        total_size = sum(q.get("size", 0) for q in queues_list)
        print(f"  📊 {len(queues_list)} queue(s) found")
        print(f"  📊 Total tasks in queues: {total_size}")
        for queue in queues_list[:5]:  # Show first 5
            print(f"     - {queue.get('name', 'unknown')}: {queue.get('size', 0)} tasks")
    else:
        print("  ❌ Could not fetch queue status")
    print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY:")
    print("-" * 60)
    required_running = [
        ("API Server", api_processes or api_health),
        ("Worker", worker_processes),
        ("Redis", redis_listening),
        ("PostgreSQL", postgres_listening),
    ]
    
    optional_running = [
        ("Scheduler", scheduler_processes),
        ("Worker Monitor", monitor_processes),
    ]
    
    all_required = all(status for _, status in required_running)
    
    for name, status in required_running:
        print(f"  {'✅' if status else '❌'} {name}")
    
    print()
    print("Optional Services:")
    for name, status in optional_running:
        print(f"  {'✅' if status else '⚪'} {name}")
    
    print()
    if all_required:
        print("✅ All required services are running!")
    else:
        print("❌ Some required services are not running.")
        print()
        print("To start services:")
        print("  API:    python -m jobqueue.api.main")
        print("  Worker: python -m jobqueue.worker.main")
        print("  Docker: docker-compose up -d redis postgres")
    print("=" * 60)

if __name__ == "__main__":
    main()
