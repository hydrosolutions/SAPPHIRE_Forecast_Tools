import time
from concurrent.futures import ThreadPoolExecutor

# Function to write data to Excel
def write_to_excel(data):
    print('Writing to Excel...')
    time.sleep(20)  # Simulate a time-consuming process

def run_in_background(data, status):
    status.object = 'Status: Writing...'
    with ThreadPoolExecutor() as executor:
        future = executor.submit(write_to_excel, data)
        future.add_done_callback(lambda f: status_update(status, f))

# Function to update status after background task is done
def status_update(status, future):
    status.object = 'Status: Done'