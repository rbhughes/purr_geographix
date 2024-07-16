import logging

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

LOG_LEVEL = logging.INFO


def setup_logger():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # logs to console
            logging.FileHandler("purr.log"),  # logs to file
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logger()
