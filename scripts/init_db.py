#!/usr/bin/env python
"""
Initialize the database schema.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.utils.logger import log


def main():
    """Initialize database schema."""
    try:
        log.info("Connecting to PostgreSQL...")
        postgres_backend.connect()
        
        log.info("Initializing database schema...")
        postgres_backend.initialize_schema()
        
        log.info("Database schema initialized successfully!")
        
        # Verify tables were created
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
        
        results = postgres_backend.execute_query(query, fetch_all=True)
        
        if results:
            log.info(f"Created {len(results)} tables:")
            for row in results:
                log.info(f"  - {row['table_name']}")
        
        postgres_backend.disconnect()
        
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
