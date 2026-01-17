#!/bin/bash
# Setup PostgreSQL database and user for local development

echo "Setting up PostgreSQL database and user..."
echo ""

# Check if Docker container is running
if docker-compose ps postgres 2>/dev/null | grep -q "Up"; then
    echo "PostgreSQL Docker container is running."
    echo "Creating user and database in container..."
    
    # In Docker, user and database are already created by environment variables
    # Just verify connection works
    echo "Verifying PostgreSQL connection..."
    docker-compose exec -T postgres psql -U jobqueue -d jobqueue -c "SELECT version();" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✓ PostgreSQL connection verified"
        echo "✓ User 'jobqueue' exists"
        echo "✓ Database 'jobqueue' exists"
    else
        echo "⚠ Warning: Could not verify connection, but user/database should exist"
        echo "  (Created automatically by docker-compose environment variables)"
    fi
    
    echo ""
    echo "PostgreSQL setup complete in Docker container!"
    
elif command -v psql &> /dev/null && pg_isready -h localhost -p 5432 &> /dev/null; then
    echo "PostgreSQL is running locally (not in Docker)."
    echo "Creating user and database..."
    
    # Create user and database locally
    psql -h localhost -U postgres -c "CREATE USER jobqueue WITH PASSWORD 'jobqueue123';" 2>/dev/null || echo "User 'jobqueue' may already exist"
    psql -h localhost -U postgres -c "CREATE DATABASE jobqueue OWNER jobqueue;" 2>/dev/null || echo "Database 'jobqueue' may already exist"
    psql -h localhost -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE jobqueue TO jobqueue;" 2>/dev/null
    
    echo ""
    echo "PostgreSQL setup complete!"
else
    echo "PostgreSQL is not running. Please start it first:"
    echo "  docker-compose up -d postgres"
    echo ""
    echo "Or if running PostgreSQL locally, make sure it's running and accessible."
    exit 1
fi

echo ""
echo "Next steps:"
echo "1. Run the database initialization script:"
echo "   source venv/bin/activate"
echo "   python scripts/init_db.py"
echo ""
echo "2. Start the backend API:"
echo "   python -m jobqueue.api.main"
echo ""
