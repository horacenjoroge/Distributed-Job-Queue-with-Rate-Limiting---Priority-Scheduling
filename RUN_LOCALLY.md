# Running Backend and Frontend Locally

Quick guide to get everything running.

## Prerequisites Check

✅ Redis and PostgreSQL are running (via Docker Compose)
✅ Python virtual environment is set up
✅ Node.js and npm are installed

## Step 1: Start Services (Redis & PostgreSQL)

```bash
docker-compose up -d redis postgres
```

Wait a few seconds, then verify:
```bash
docker-compose ps
```

Both `jobqueue-redis` and `jobqueue-postgres` should show as "Up".

## Step 2: Start Backend API

**Terminal 1:**
```bash
# Activate virtual environment
source venv/bin/activate

# Start API server
python -m jobqueue.api.main
```

You should see:
```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

✅ Backend is running at: http://localhost:8000
✅ API Docs: http://localhost:8000/docs

## Step 3: Start Frontend

**Terminal 2:**
```bash
cd frontend
npm run dev
```

You should see:
```
VITE v5.4.21  ready in XXX ms
➜  Local:   http://localhost:3000/
```

✅ Frontend is running at: http://localhost:3000

## Step 4: Open Dashboard

Open your browser:
```
http://localhost:3000
```

You should see the Job Queue Dashboard!

## Troubleshooting

### Backend won't start
- **Error: "No module named 'pydantic'"**
  ```bash
  source venv/bin/activate
  pip install -r requirements.txt
  ```

- **Error: "Connection refused" (Redis/PostgreSQL)**
  ```bash
  # Check if services are running
  docker-compose ps
  
  # Restart services
  docker-compose restart redis postgres
  ```

### Frontend won't start
- **Error: "Cannot find module"**
  ```bash
  cd frontend
  rm -rf node_modules package-lock.json
  npm install
  ```

- **Error: PostCSS config**
  - Already fixed in the repo
  - If you see it, delete `node_modules` and reinstall

### Port already in use
- **Port 8000 in use (Backend)**
  - Change port in `config.py` or kill the process using port 8000
  - `lsof -ti:8000 | xargs kill -9`

- **Port 3000 in use (Frontend)**
  - Vite will automatically try the next port (3001, 3002, etc.)
  - Or change port in `frontend/vite.config.js`

## Quick Commands

```bash
# Start everything
docker-compose up -d redis postgres
source venv/bin/activate && python -m jobqueue.api.main  # Terminal 1
cd frontend && npm run dev  # Terminal 2

# Stop everything
# Ctrl+C in both terminals
docker-compose down
```

## Next Steps

1. **Start a worker** to process tasks:
   ```bash
   source venv/bin/activate
   python -m jobqueue.worker.main
   ```

2. **Submit a task** via API:
   - Go to http://localhost:8000/docs
   - Use the `/tasks` POST endpoint
   - Or use the dashboard at http://localhost:3000

3. **Monitor** in the dashboard:
   - View tasks, queues, workers
   - See real-time updates via WebSocket
