# Quick Start Guide

Get the backend and frontend running locally in minutes.

## Prerequisites

- Python 3.11+ (you have 3.13.7 ✓)
- Node.js and npm
- Docker and Docker Compose (for Redis/PostgreSQL)

## Step 1: Set Up Backend

```bash
# Activate virtual environment
source venv/bin/activate

# Install Python dependencies (if not already done)
pip install -r requirements.txt
```

## Step 2: Start Services (Redis & PostgreSQL)

```bash
# Start Redis and PostgreSQL
docker-compose up -d redis postgres

# Or use the helper script
./scripts/start_services.sh
```

Wait a few seconds for services to start, then verify:
```bash
# Check Redis
redis-cli ping  # Should return "PONG"

# Check PostgreSQL (if psql is installed)
psql -h localhost -U postgres -d jobqueue -c "SELECT 1"
```

## Step 3: Start Backend API

In terminal 1:
```bash
# Activate venv
source venv/bin/activate

# Start API server
python -m jobqueue.api.main

# Or use helper script
./scripts/start_backend.sh
```

Backend will be available at:
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Step 4: Start Frontend

In terminal 2:
```bash
# Navigate to frontend
cd frontend

# Install dependencies (first time only)
npm install

# Start development server
npm run dev

# Or from project root
./scripts/start_frontend.sh
```

Frontend will be available at:
- Dashboard: http://localhost:3000

## Step 5: Access the Dashboard

Open your browser and go to:
```
http://localhost:3000
```

You should see the Job Queue Dashboard!

## Troubleshooting

### Backend won't start
- Check if Redis is running: `redis-cli ping`
- Check if PostgreSQL is running: `docker-compose ps`
- Check logs: Look at terminal output for errors

### Frontend won't start
- Make sure you're in the `frontend/` directory
- Run `npm install` if you see module errors
- Check if port 3000 is already in use

### Services won't start
- Make sure Docker is running
- Check `docker-compose ps` to see service status
- View logs: `docker-compose logs redis postgres`

## Stopping Services

```bash
# Stop backend: Ctrl+C in terminal 1
# Stop frontend: Ctrl+C in terminal 2

# Stop Redis and PostgreSQL
docker-compose down
```

## Next Steps

- Submit tasks via API: http://localhost:8000/docs
- Monitor tasks in dashboard: http://localhost:3000
- Start a worker to process tasks: `python -m jobqueue.worker.main`
