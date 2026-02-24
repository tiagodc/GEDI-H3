"""
Logging configuration for gedih3 package.

Provides a consistent logging interface across all modules and CLI tools.
Uses Python's standard logging module with configurable levels and handlers.

Usage:
    from gedih3.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.debug("Detailed information")
    logger.warning("Something unexpected")
    logger.error("An error occurred")
"""

import logging
import sys
from typing import Optional

# Default format strings
CONSOLE_FORMAT = "%(message)s"
CONSOLE_FORMAT_VERBOSE = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
FILE_FORMAT = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Package root logger name
ROOT_LOGGER_NAME = "gedih3"

# Track if logging has been configured
_logging_configured = False


def configure_logging(
    level: int = logging.INFO, verbose: bool = False, log_file: Optional[str] = None, quiet: bool = False
) -> logging.Logger:
    """
    Configure the gedih3 logging system.

    Parameters
    ----------
    level : int
        Logging level (logging.DEBUG, logging.INFO, logging.WARNING, etc.)
    verbose : bool
        If True, use verbose format with timestamps and module names
    log_file : str, optional
        Path to log file. If provided, logs will also be written to this file.
    quiet : bool
        If True, suppress console output (file logging still works if configured)

    Returns
    -------
    logging.Logger
        The configured root logger for gedih3

    Examples
    --------
    >>> from gedih3.logging_config import configure_logging
    >>> configure_logging(level=logging.DEBUG, verbose=True)
    >>> configure_logging(log_file="/tmp/gedih3.log")
    >>> configure_logging(quiet=True)  # Suppress console output
    """
    global _logging_configured

    # Get or create the root logger for gedih3
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates on reconfiguration
    logger.handlers.clear()

    # Console handler
    if not quiet:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_format = CONSOLE_FORMAT_VERBOSE if verbose else CONSOLE_FORMAT
        console_formatter = logging.Formatter(console_format, datefmt=DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(FILE_FORMAT, datefmt=DATE_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    _logging_configured = True
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger for the specified module.

    Parameters
    ----------
    name : str, optional
        Module name (typically __name__). If None, returns the root gedih3 logger.

    Returns
    -------
    logging.Logger
        Logger instance for the module

    Examples
    --------
    >>> from gedih3.logging_config import get_logger
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing started")
    """
    global _logging_configured

    # Auto-configure with defaults if not yet configured
    if not _logging_configured:
        configure_logging()

    if name is None:
        return logging.getLogger(ROOT_LOGGER_NAME)

    # Create child logger under gedih3 namespace
    if name.startswith(ROOT_LOGGER_NAME):
        return logging.getLogger(name)
    else:
        return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")


def set_level(level: int) -> None:
    """
    Change the logging level for all gedih3 loggers.

    Parameters
    ----------
    level : int
        New logging level (logging.DEBUG, logging.INFO, etc.)
    """
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


def silence() -> None:
    """Silence all gedih3 logging output."""
    set_level(logging.CRITICAL + 1)


def enable_debug() -> None:
    """Enable debug-level logging."""
    set_level(logging.DEBUG)


class LoggingContext:
    """
    Context manager for temporarily changing log level.

    Examples
    --------
    >>> with LoggingContext(logging.DEBUG):
    ...     # Debug logging enabled here
    ...     pass
    >>> # Original log level restored
    """

    def __init__(self, level: int):
        self.level = level
        self.logger = logging.getLogger(ROOT_LOGGER_NAME)
        self.old_level = None

    def __enter__(self):
        self.old_level = self.logger.level
        set_level(self.level)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_level is not None:
            set_level(self.old_level)
        return False
