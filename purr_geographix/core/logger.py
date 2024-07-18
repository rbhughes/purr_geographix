from loguru import logger
import sys

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

LOG_LEVEL = "INFO"


def setup_logger():
    # Remove the default logger configuration
    logger.remove()

    # Add a logger that outputs to console
    logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    )

    # Add a logger that outputs to a file
    logger.add(
        "purr.log",
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
        rotation="500 MB",
        compression="zip",
    )

    return logger


# Setup the logger
logger = setup_logger()
