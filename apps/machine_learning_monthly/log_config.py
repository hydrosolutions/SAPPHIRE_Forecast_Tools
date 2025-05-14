import os
import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logging():
    """Configure logging for the entire application."""
    # Configure the root logger
    logging.basicConfig(level=logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create the logs directory if it doesn't exist
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create a file handler to write logs to a file
    file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
    file_handler.setFormatter(formatter)
    
    # Create a stream handler to print logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Get the root logger and add the handlers to it
    root_logger = logging.getLogger()
    # Clear any existing handlers
    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger