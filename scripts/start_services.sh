#!/bin/bash
# Start Redis and PostgreSQL using Docker Compose

echo "Starting Redis and PostgreSQL services..."
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose not found. Please install Docker and Docker Compose."
    echo "Alternatively, you can run Redis and PostgreSQL manually."
    exit 1
fi

# Start services
docker-compose up -d redis postgres

echo ""
echo "Services started!"
echo "Redis: localhost:6379"
echo "PostgreSQL: localhost:5432"
echo ""
echo "To stop services: docker-compose down"
