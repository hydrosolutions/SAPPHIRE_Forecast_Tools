# long_term_forecasting/__init__.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pandas as pd

# Logger setup
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)

logger = logging.getLogger('long_term_forecasting')
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(file_handler)

# Suppress graphviz debug warnings
logging.getLogger('graphviz').setLevel(logging.WARNING)


# Shared today variable, this is useful on different levels:
# 1. Allows to "mock" today to generate operational forecasts for a specific date in the past (e.g. for testing or backtesting)
# 2. Avoids multiple calls to pd.Timestamp.now() which can lead to inconsistencies
# 3. Enables to regenerate forecasts for a specific "today" date without changing the system date or environment variables                                                                                                                                     
today = None                                                                                                                                                       
                                                                                                                                                                    
def initialize_today(today_override=None):                                                                                                                         
    global today                                                                                                                                                   
    today = pd.to_datetime(today_override) if today_override else pd.Timestamp.now()                                                                               
    return today                                                                                                                                                   
                                                                                                                                                                    
def get_today():                                                                                                                                                   
    return today 


# Columns retained in long-term forecasting dataframes + Prediction Columns
LT_FORECAST_BASE_COLUMNS = [
    'date',
    'code',
    'valid_from',
    'valid_to',
    'flag',
]