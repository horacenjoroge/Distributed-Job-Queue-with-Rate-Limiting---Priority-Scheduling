"""
Structured logging configuration using loguru.
"""
import sys
import json
from pathlib import Path
from loguru import logger
from config import settings


def serialize(record):
    """Serialize log record to JSON format."""
    subset = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "module": record["module"],
        "function": record["function"],
        "line": record["line"],
    }
    
    # Add extra fields if present
    if record["extra"]:
        subset["extra"] = record["extra"]
    
    # Add exception info if present
    if record["exception"]:
        subset["exception"] = {
            "type": record["exception"].type.__name__,
            "value": str(record["exception"].value),
        }
    
    return json.dumps(subset)


def patched_writer(message):
    """Write JSON messages to stdout."""
    sys.stdout.write(message + "\n")


def setup_logging():
    """Configure loguru logger based on settings."""
    # Remove default handler
    logger.remove()
    
    # Determine format based on configuration
    if settings.log_format == "json":
        logger.add(
            patched_writer,
            format=serialize,
            level=settings.log_level,
            serialize=True,
        )
    else:
        # Human-readable format
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=settings.log_level,
            colorize=True,
        )
    
    # Also log to file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger.add(
        "logs/jobqueue_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        level=settings.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )
    
    return logger


# Initialize logger
log = setup_logging()
