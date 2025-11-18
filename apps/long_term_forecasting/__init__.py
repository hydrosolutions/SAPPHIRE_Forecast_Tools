# long_term_forecasting/__init__.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os

# Logger setup
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)

logger = logging.getLogger('long_term_forecasting')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)

# Suppress graphviz debug warnings
logging.getLogger('graphviz').setLevel(logging.WARNING)