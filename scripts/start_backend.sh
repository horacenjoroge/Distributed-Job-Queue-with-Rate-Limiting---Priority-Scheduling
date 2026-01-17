#!/bin/bash
# Start the backend API server

echo "Starting Job Queue Backend API..."
echo "Make sure Redis and PostgreSQL are running!"
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start the API server
python -m jobqueue.api.main
