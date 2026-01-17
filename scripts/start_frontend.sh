#!/bin/bash
# Start the frontend development server

echo "Starting Job Queue Frontend..."
echo ""

cd frontend

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "Installing frontend dependencies..."
    npm install
fi

# Start the development server
npm run dev
