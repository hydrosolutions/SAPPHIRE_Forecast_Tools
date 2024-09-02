import os
import time

# Multi-threading
from concurrent.futures import ThreadPoolExecutor

# Logging
import logging
logger = logging.getLogger(__name__)


# Function to write data to Excel
def write_to_excel(data, report):
    logger.info('Writing bulletin ...')
    report.generate_report()


def run_in_background(data, report, status):
    status.object = 'Status: Writing...'
    with ThreadPoolExecutor() as executor:
        future = executor.submit(write_to_excel, data, report)
        future.add_done_callback(lambda f: status_update(status, f))

# Function to update status after background task is done
def status_update(status, future):
    try:
        result = future.result()
        status.object = 'Status: Done'
    except Exception as e:
        status.object = f'Status: Failed with error {e}'
