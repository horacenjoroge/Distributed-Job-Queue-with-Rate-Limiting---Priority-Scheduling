#!/bin/bash
# Start a worker to process tasks

echo "Starting worker..."
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Error: Virtual environment not found. Please run: python3 -m venv venv"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Start worker
echo "Worker starting..."
python -m jobqueue.worker.main
