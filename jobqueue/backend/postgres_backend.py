"""
PostgreSQL connection manager and result backend implementation.
"""
import psycopg2
from psycopg2 import pool, sql, extras
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from config import settings
from jobqueue.utils.logger import log


class PostgresBackend:
    """
    PostgreSQL connection manager with connection pooling.
    Handles task metadata and result storage.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        max_connections: Optional[int] = None,
    ):
        """
        Initialize PostgreSQL backend with connection pooling.
        
        Args:
            host: PostgreSQL host address
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
            max_connections: Maximum number of connections in pool
        """
        self.host = host or settings.postgres_host
        self.port = port or settings.postgres_port
        self.database = database or settings.postgres_db
        self.user = user or settings.postgres_user
        self.password = password or settings.postgres_password
        self.max_connections = max_connections or settings.postgres_max_connections
        
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        
        log.info(
            "Initializing PostgreSQL backend",
            extra={
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user,
                "max_connections": self.max_connections,
            }
        )
    
    def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        try:
            self._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.max_connections,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
            )
            
            # Test connection
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            
            log.info("Successfully connected to PostgreSQL")
            
        except psycopg2.OperationalError as e:
            log.error(f"Failed to connect to PostgreSQL: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error connecting to PostgreSQL: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            log.info("Closed PostgreSQL connection pool")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager to get a connection from the pool.
        
        Yields:
            psycopg2 connection object
        """
        if self._pool is None:
            raise RuntimeError("PostgreSQL backend not connected. Call connect() first.")
        
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(f"Database error, rolling back transaction: {e}")
            raise
        finally:
            self._pool.putconn(conn)
    
    def is_connected(self) -> bool:
        """
        Check if PostgreSQL connection is alive.
        
        Returns:
            True if connected and responsive, False otherwise
        """
        try:
            if self._pool:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                return True
        except Exception:
            pass
        return False
    
    def initialize_schema(self) -> None:
        """Create necessary database tables and indexes."""
        schema_sql = """
        -- Tasks table
        CREATE TABLE IF NOT EXISTS tasks (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            priority VARCHAR(20) NOT NULL,
            status VARCHAR(20) NOT NULL,
            queue_name VARCHAR(255) NOT NULL,
            args TEXT,
            kwargs TEXT,
            result TEXT,
            error TEXT,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            timeout INTEGER DEFAULT 300,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            worker_id VARCHAR(255),
            parent_task_id VARCHAR(36),
            depends_on TEXT
        );
        
        -- Indexes for efficient queries
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(queue_name);
        CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority);
        CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);
        
        -- Workers table for health monitoring
        CREATE TABLE IF NOT EXISTS workers (
            id VARCHAR(255) PRIMARY KEY,
            hostname VARCHAR(255) NOT NULL,
            pid INTEGER NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            current_task_id VARCHAR(36),
            processed_tasks INTEGER DEFAULT 0,
            failed_tasks INTEGER DEFAULT 0
        );
        
        CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
        CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(last_heartbeat);
        
        -- Dead letter queue
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id SERIAL PRIMARY KEY,
            task_id VARCHAR(36) NOT NULL,
            task_data TEXT NOT NULL,
            error TEXT,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER,
            original_queue VARCHAR(255)
        );
        
        CREATE INDEX IF NOT EXISTS idx_dlq_task_id ON dead_letter_queue(task_id);
        CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_queue(failed_at);
        
        -- Task metrics
        CREATE TABLE IF NOT EXISTS task_metrics (
            id SERIAL PRIMARY KEY,
            task_id VARCHAR(36) NOT NULL,
            task_name VARCHAR(255) NOT NULL,
            queue_name VARCHAR(255) NOT NULL,
            duration_ms INTEGER,
            success BOOLEAN NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_metrics_task_name ON task_metrics(task_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON task_metrics(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_metrics_success ON task_metrics(success);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)
            log.info("Successfully initialized database schema")
        except Exception as e:
            log.error(f"Error initializing database schema: {e}")
            raise
    
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch_one: bool = False,
        fetch_all: bool = False,
    ) -> Optional[Any]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch_one: Whether to fetch one result
            fetch_all: Whether to fetch all results
            
        Returns:
            Query results if fetch_one or fetch_all is True
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    cur.execute(query, params)
                    
                    if fetch_one:
                        return cur.fetchone()
                    elif fetch_all:
                        return cur.fetchall()
                    
                    return None
        except Exception as e:
            log.error(f"Error executing query: {e}")
            raise
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, params_list)
        except Exception as e:
            log.error(f"Error executing batch query: {e}")
            raise


# Global PostgreSQL backend instance
postgres_backend = PostgresBackend()
