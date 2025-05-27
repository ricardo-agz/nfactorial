import logging
import sys


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


def get_logger(name: str) -> logging.Logger:
    """Get a logger with colored output"""
    logger = logging.getLogger(name)

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
