from loguru import logger
import sys

#
# # CRITICAL
# # ERROR
# # WARNING
# # INFO
# # DEBUG
#
# LOG_LEVEL = "INFO"
#
#
# def setup_logger():
#     logging.basicConfig(
#         level=LOG_LEVEL,
#         format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#         handlers=[
#             logging.StreamHandler(),  # logs to console
#             logging.FileHandler("purr.log"),  # logs to file
#         ],
#     )
#     return logging.getLogger(__name__)
#
#
# logger = setup_logger()
#
# from loguru import logger

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
