import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(log_dir: str = 'logs', level=logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Remove all handlers associated with the logger
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create the logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Create a file handler to write logs to a file
    # A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
    file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
    file_handler.setFormatter(formatter)

    # Create a stream handler to print logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Get the root logger and add the handlers to it
    logger.addHandler(file_handler)
    logger.addHandler(console_handler) 

    return logger
