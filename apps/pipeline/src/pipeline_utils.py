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



def get_docker_hub_image_creation_date(namespace, image_name, tag):
    """Fetch the creation/push date of a Docker Hub image tag.

    Args:
        namespace: Docker Hub namespace (e.g., "mabesa")
        image_name: Image name (e.g., "sapphire-ml")
        tag: Image tag (e.g., "latest", "py312")

    Returns:
        ISO 8601 date string or None if unavailable
    """
    # Docker Hub API URL for fetching tag-specific data
    # curl --location 'https://hub.docker.com/v2/repositories/{namespace}/{image_name}/tags/{tag}'
    url = f"https://hub.docker.com/v2/repositories/{namespace}/{image_name}/tags/{tag}"

    # Get the docker image tag data
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            tag_data = response.json()
            return tag_data.get('tag_last_pushed')  # Tag-specific push date
        else:
            print(f"Failed to fetch image data from Docker Hub: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Failed to reach Docker Hub: {e}")
        return None


def get_docker_hub_image_digest(namespace, image_name, tag):
    """Fetch the digest (sha256 hash) of a Docker Hub image tag.

    Args:
        namespace: Docker Hub namespace (e.g., "mabesa")
        image_name: Image name (e.g., "sapphire-ml")
        tag: Image tag (e.g., "latest", "py312")

    Returns:
        Digest string (e.g., "sha256:78ce10fffe...") or None if unavailable
    """
    url = f"https://hub.docker.com/v2/repositories/{namespace}/{image_name}/tags/{tag}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            tag_data = response.json()
            return tag_data.get('digest')
        else:
            print(f"Failed to fetch image digest from Docker Hub: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Failed to reach Docker Hub: {e}")
        return None


def get_local_image_digest(client, repository, image_name, tag):
    """Extract the digest of a local Docker image from its RepoDigests.

    Args:
        client: Docker client instance
        repository: Docker Hub namespace (e.g., "mabesa")
        image_name: Image name (e.g., "sapphire-ml")
        tag: Image tag (e.g., "latest")

    Returns:
        Digest string (e.g., "sha256:78ce10fffe...") or None if unavailable

    Raises:
        docker.errors.ImageNotFound: If the local image doesn't exist
    """
    full_name = f"{repository}/{image_name}:{tag}"
    image = client.images.get(full_name)

    repo_digests = image.attrs.get('RepoDigests', [])
    if not repo_digests:
        return None

    # RepoDigests format: ['namespace/repo@sha256:abc123...']
    # Extract digest from the first matching entry
    expected_prefix = f"{repository}/{image_name}@"
    for repo_digest in repo_digests:
        if repo_digest.startswith(expected_prefix):
            return repo_digest.split('@')[1]

    # Fallback: return first available digest if prefix doesn't match
    if '@' in repo_digests[0]:
        return repo_digests[0].split('@')[1]

    return None

def _compare_by_timestamp(client, repository, image_name, tag):
    """Fallback timestamp-based comparison (legacy behavior).

    This is less reliable than digest comparison but provides backward
    compatibility for images without RepoDigests (e.g., locally built images).

    Returns:
        True if Docker Hub image appears newer, False otherwise
    """
    try:
        local_image = client.images.get(f"{repository}/{image_name}:{tag}")
        local_image_creation_date = parse(local_image.attrs['Created'])

        if local_image_creation_date.tzinfo is None or local_image_creation_date.tzinfo.utcoffset(local_image_creation_date) is None:
            local_image_creation_date = local_image_creation_date.replace(tzinfo=pytz.UTC)

        hub_date_str = get_docker_hub_image_creation_date(repository, image_name, tag)
        if not hub_date_str:
            print("Failed to fetch Docker Hub timestamp. Assuming local is current.")
            return False

        hub_image_creation_date = parse(hub_date_str)
        if hub_image_creation_date.tzinfo is None or hub_image_creation_date.tzinfo.utcoffset(hub_image_creation_date) is None:
            hub_image_creation_date = hub_image_creation_date.replace(tzinfo=pytz.UTC)

        if hub_image_creation_date > local_image_creation_date:
            print("The Docker Hub image appears newer (timestamp fallback).")
            return True
        else:
            print("The local image appears current (timestamp fallback).")
            return False

    except docker.errors.ImageNotFound:
        return True


def there_is_a_newer_image_on_docker_hub(client, repository, image_name, tag):
    """Check if Docker Hub has a different (newer) image than local.

    Uses content-based digest comparison for reliable detection.
    Falls back to timestamp comparison if digests are unavailable.

    Args:
        client: Docker client instance
        repository: Docker Hub namespace (e.g., "mabesa")
        image_name: Image name (e.g., "sapphire-ml")
        tag: Image tag (e.g., "latest")

    Returns:
        True if Docker Hub image differs from local (or local doesn't exist)
        False if images are identical or comparison fails
    """
    # Try to get local image digest
    try:
        local_digest = get_local_image_digest(client, repository, image_name, tag)
    except docker.errors.ImageNotFound:
        print("The local image does not exist.")
        return True

    # Get Docker Hub digest
    hub_digest = get_docker_hub_image_digest(repository, image_name, tag)

    # Primary comparison: use digests if both available
    if hub_digest and local_digest:
        if hub_digest == local_digest:
            print("The local image is up-to-date (digests match).")
            return False
        else:
            print(f"The Docker Hub image differs from local (digest mismatch).")
            print(f"  Local:  {local_digest[:27]}...")
            print(f"  Remote: {hub_digest[:27]}...")
            return True

    # Fallback to timestamp comparison if digests unavailable
    if not local_digest:
        print("Warning: Local image has no RepoDigests, falling back to timestamp comparison.")
    if not hub_digest:
        print("Warning: Could not fetch Docker Hub digest, falling back to timestamp comparison.")

    return _compare_by_timestamp(client, repository, image_name, tag)

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

