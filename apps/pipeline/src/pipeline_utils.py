import docker
import os
import luigi
import datetime
import requests
from dateutil.parser import parse
import pytz
from contextlib import contextmanager
import threading
import signal



def get_docker_hub_image_creation_date(namespace, image_name):
    # Docker Hub API URL for fetching manifest data
    # curl --location 'https://hub.docker.com//v2/namespaces/{namespace}/repositories/{image_name}/'
    url = f"https://hub.docker.com//v2/namespaces/{namespace}/repositories/{image_name}/"

    # Get the docker image manifest data
    response = requests.get(url)
    if response.status_code == 200:
        manifest_data = response.json()
        return manifest_data['last_updated']  
    else:
        print(f"Failed to fetch image data from Docker Hub: {response.status_code}")
        return None

def there_is_a_newer_image_on_docker_hub(client, repository, image_name, tag):
    try:
        # Read the local images creation time
        local_image = client.images.get(f"{repository}/{image_name}:{tag}")
        local_image_creation_date = parse(local_image.attrs['Created'])
        # Ensure the local image creation date is offset-aware
        if local_image_creation_date.tzinfo is None or local_image_creation_date.tzinfo.utcoffset(local_image_creation_date) is None:
            local_image_creation_date = local_image_creation_date.replace(tzinfo=pytz.UTC)

        # Get the Docker Hub image creation time
        hub_image_creation_date = parse(get_docker_hub_image_creation_date(repository, image_name))
        # Ensure the Docker Hub image creation date is offset-aware
        if hub_image_creation_date.tzinfo is None or hub_image_creation_date.tzinfo.utcoffset(hub_image_creation_date) is None:
            hub_image_creation_date = hub_image_creation_date.replace(tzinfo=pytz.UTC)

        if not hub_image_creation_date:
            print("Failed to fetch the Docker Hub image creation date.")
            return False

        if hub_image_creation_date > local_image_creation_date:
            print("The Docker Hub image is newer than the local image.")
            return True
        else:
            print("The local image is up-to-date or newer than the Docker Hub image.")
            return False
    except docker.errors.ImageNotFound:
        print("The local image does not exist.")
        return True

@contextmanager
def timeout(seconds):
    """Context manager for timeout functionality"""
    timer = threading.Timer(seconds, lambda: signal.pthread_kill(threading.main_thread().ident, signal.SIGINT))
    timer.start()
    try:
        yield
    finally:
        timer.cancel()

class TimeoutError(Exception):
    pass

class TaskLogger:
    """Utility class to handle task logging"""
    def __init__(self, log_file=None):
        if log_file is None:
            root_dir = os.getenv('ieasyforecast_intermediate_data_path', '/app')
            self.log_file = os.path.join(root_dir, 'task_timings.log')
        else:
            self.log_file = log_file

    def log_task_timing(self, task_name: str, start_time: datetime.datetime, end_time: datetime.datetime, status: str, details: str = ""):
        with open(self.log_file, 'a') as f:
            duration = (end_time - start_time).total_seconds()
            log_entry = (
                f"Task: {task_name}\n"
                f"Start Time: {start_time.isoformat()}\n"
                f"End Time: {end_time.isoformat()}\n"
                f"Duration: {duration:.2f} seconds\n"
                f"Status: {status}\n"
                f"Details: {details}\n"
                f"{'-'*50}\n"
            )
            f.write(log_entry)

class TimeoutMixin:
    """Mixin to add timeout functionality to Luigi tasks"""
    timeout_seconds = luigi.IntParameter(default=360)  # 6 minutes default

    def run_with_timeout(self, func, *args, **kwargs):
        try:
            with timeout(self.timeout_seconds):
                return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise TimeoutError(f"Task timed out after {self.timeout_seconds} seconds")

