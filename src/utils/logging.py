"""
Logging setup for the data engineering pipeline.

Uses Loguru for structured, colorized logging with file rotation.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from loguru import logger


# Remove default handler
logger.remove()

# Track if logging has been configured
_logging_configured = False


def setup_logging(
    log_dir: Path,
    level: str = "INFO",
    log_format: Optional[str] = None,
    console: bool = True,
    file: bool = True,
) -> Path:
    """
    Configure logging for the pipeline.
    
    Args:
        log_dir: Directory to store log files.
        level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_format: Custom log format string.
        console: Whether to log to console.
        file: Whether to log to file.
        
    Returns:
        Path to the log file created.
    """
    global _logging_configured
    
    if log_format is None:
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )
    
    # File format (no colors)
    file_format = (
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level:<8} | "
        "{name}:{function}:{line} | "
        "{message}"
    )
    
    # Ensure log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamped log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    # Clear existing handlers if reconfiguring
    if _logging_configured:
        logger.remove()
    
    # Add console handler
    if console:
        logger.add(
            sys.stderr,
            format=log_format,
            level=level,
            colorize=True,
        )
    
    # Add file handler
    if file:
        logger.add(
            log_file,
            format=file_format,
            level=level,
            rotation="10 MB",
            retention="30 days",
            compression="gz",
        )
    
    _logging_configured = True
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return log_file


def get_logger(name: str = "pipeline"):
    """
    Get a logger instance with the given name.
    
    Args:
        name: Logger name (typically module name).
        
    Returns:
        Loguru logger instance with context.
    """
    return logger.bind(name=name)
