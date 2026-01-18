#!/usr/bin/env python3
"""
Quick script to check if API is working and returning data for frontend.
"""
import requests
import json

API_URL = "http://localhost:8000"

def check_endpoint(name, url):
    """Check an API endpoint."""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ {name}: OK")
            return data
        else:
            print(f"✗ {name}: HTTP {response.status_code}")
            return None
    except requests.exceptions.ConnectionError:
        print(f"✗ {name}: Connection refused (API not running?)")
        return None
    except Exception as e:
        print(f"✗ {name}: Error - {e}")
        return None

def main():
    print("Checking API endpoints for frontend...")
    print("=" * 50)
    
    # Health check
    health = check_endpoint("Health", f"{API_URL}/health")
    
    # Workers
    workers = check_endpoint("Workers", f"{API_URL}/workers")
    if workers:
        print(f"  Workers found: {workers.get('total', 0)}")
        print(f"  Alive: {workers.get('alive', 0)}")
    
    # Metrics
    metrics = check_endpoint("Metrics", f"{API_URL}/metrics")
    if metrics:
        sr = metrics.get('success_rate', {})
        print(f"  Success rate: {sr.get('success_rate', 0) * 100:.1f}%")
        print(f"  Total tasks: {sr.get('total', 0)}")
        
        qi = metrics.get('queue_info', {})
        sc = qi.get('status_counts', {})
        print(f"  Status counts - Success: {sc.get('success', 0)}, Failed: {sc.get('failed', 0)}")
    
    # Queues
    queues = check_endpoint("Queues", f"{API_URL}/queues")
    if queues:
        print(f"  Queues found: {len(queues.get('queues', []))}")
    
    # Tasks
    tasks = check_endpoint("Tasks", f"{API_URL}/tasks?limit=5")
    if tasks:
        print(f"  Tasks found: {tasks.get('total', 0)}")
    
    print("=" * 50)
    print("\nIf all endpoints show ✓, the API is working.")
    print("If frontend still shows nothing, check browser console for errors.")

if __name__ == "__main__":
    main()
