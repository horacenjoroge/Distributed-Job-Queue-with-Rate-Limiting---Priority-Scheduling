# Job Queue Dashboard

React-based web dashboard for monitoring the distributed job queue system.

## Features

- Real-time updates via WebSocket
- Task management and monitoring
- Queue statistics and management
- Worker health monitoring
- Dead Letter Queue (DLQ) management
- Metrics visualization with Chart.js
- Responsive design with Tailwind CSS

## Setup

1. Install dependencies:
```bash
npm install
```

2. Configure environment variables (optional):
```bash
# Create .env file
VITE_API_URL=http://localhost:8000
VITE_WS_URL=ws://localhost:8000
VITE_API_KEY=your-api-key
```

3. Start development server:
```bash
npm run dev
```

The dashboard will be available at `http://localhost:3000`

## Build

Build for production:
```bash
npm run build
```

The built files will be in the `dist/` directory.

## Pages

- **Home**: Overview dashboard with queue sizes, active workers, and success rate
- **Tasks**: List all tasks with filtering, search, and pagination
- **Task Detail**: Full task information, logs, and retry button
- **Queues**: List queues with sizes and purge functionality
- **Workers**: Monitor worker status and heartbeat
- **DLQ**: View failed tasks with retry and delete actions
- **Metrics**: Charts for task throughput, latency, and success rates
