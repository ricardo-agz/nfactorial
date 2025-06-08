import logging
import sys
import time
import hashlib
from threading import Lock
from typing import Any


class ColoredFormatter(logging.Formatter):
    """Simple colored formatter similar to FastAPI/Uvicorn"""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",  # Reset
        "DIM": "\033[2m",  # Dim
        "GRAY": "\033[90m",  # Gray
    }

    def format(self, record: logging.LogRecord) -> str:
        level_color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]

        # "LEVEL:     message"
        level_name = f"{level_color}{record.levelname}{reset}:"
        message = record.getMessage()
        log_line = f"{level_name}{' ' * (9 - len(record.levelname))}{message}"

        if record.exc_info:
            log_line += f"\n{self.formatException(record.exc_info)}"

        return log_line


class Logger(logging.Logger):
    """Custom logger with optional exception deduplication"""

    def __init__(
        self, name: str, dedupe_exceptions: bool = True, dedupe_window: int = 300
    ):
        super().__init__(name)
        self.dedupe_exceptions = dedupe_exceptions
        self.dedupe_window = dedupe_window
        self._exception_cache: dict[str, tuple[float, int]] = {}
        self._lock = Lock()

    def _get_exception_signature(self, exception: Exception) -> str:
        """Create a unique signature for an exception type and message"""
        exc_type = type(exception).__name__
        exc_message = str(exception)
        signature = f"{exc_type}:{exc_message}"
        return hashlib.md5(signature.encode()).hexdigest()

    def _cleanup_old_entries(self, current_time: float) -> None:
        """Remove entries older than the dedupe window"""
        cutoff_time = current_time - self.dedupe_window
        expired_keys = [
            key
            for key, (first_seen, _) in self._exception_cache.items()
            if first_seen < cutoff_time
        ]
        for key in expired_keys:
            del self._exception_cache[key]

    def error(
        self, msg: object, *args: object, exc_info: Any = None, **kwargs: Any
    ) -> None:
        """Override error to handle exception deduplication"""
        if not self.dedupe_exceptions or exc_info is None:
            return super().error(msg, *args, exc_info=exc_info, **kwargs)

        # Handle exception deduplication
        if isinstance(exc_info, Exception):
            exception = exc_info
        elif exc_info is True:
            # Can't dedupe when exc_info=True since we don't have the exception object
            return super().error(msg, *args, exc_info=exc_info, **kwargs)
        else:
            return super().error(msg, *args, exc_info=exc_info, **kwargs)

        current_time = time.time()
        exc_signature = self._get_exception_signature(exception)

        with self._lock:
            self._cleanup_old_entries(current_time)

            if exc_signature in self._exception_cache:
                first_seen, count = self._exception_cache[exc_signature]
                self._exception_cache[exc_signature] = (first_seen, count + 1)

                # Log brief message for repeated exceptions
                exc_type = type(exception).__name__
                exc_message = str(exception)
                brief_msg = f"{msg} - {exc_type}: {exc_message}"
                return super().error(brief_msg, *args, **kwargs)
            else:
                # First time seeing this exception, log with full stack trace
                self._exception_cache[exc_signature] = (current_time, 1)
                return super().error(msg, *args, exc_info=exception, **kwargs)


def colored(text: str, color: str) -> str:
    color = color.upper()
    if color not in ColoredFormatter.COLORS:
        raise ValueError(f"Invalid color: {color}")
    return f"{ColoredFormatter.COLORS[color]}{text}{ColoredFormatter.COLORS['RESET']}"


def get_logger(name: str, dedupe_exceptions: bool = True) -> Logger:
    """Get a logger with colored output and optional exception deduplication"""
    # Set our custom logger class
    logging.setLoggerClass(Logger)
    logger = logging.getLogger(name)

    if not isinstance(logger, Logger):
        # If it's not our custom logger, create a new one
        logger = Logger(name, dedupe_exceptions=dedupe_exceptions)
        # Clear any existing handlers and set up fresh
        logger.handlers.clear()
    else:
        # Update the dedupe setting if it's different
        logger.dedupe_exceptions = dedupe_exceptions
        if logger.handlers:
            return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    if sys.stdout.isatty():
        formatter = ColoredFormatter()
    else:
        formatter = ColoredFormatter(
            "%(asctime)s | %(levelname)-8s | %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
