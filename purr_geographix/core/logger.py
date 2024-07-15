import logging


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # logs to console
            logging.FileHandler("purr_error.log"),  # logs to file
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logger()
