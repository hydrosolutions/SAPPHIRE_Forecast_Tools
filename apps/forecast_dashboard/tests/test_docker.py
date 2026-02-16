'''
Test the docker image for the forecast dashboard

This test script gets a docker client and gets a list of containers.
Requires Docker daemon to be running — skips gracefully if not.
'''
import os
import docker
import pytest

# Check if Docker daemon is reachable
try:
    _client = docker.from_env()
    _client.ping()
    DOCKER_AVAILABLE = True
except docker.errors.DockerException:
    DOCKER_AVAILABLE = False


@pytest.mark.skipif(
    not DOCKER_AVAILABLE,
    reason="Docker daemon not running",
)
def test_docker_image():
    '''
    Test the docker image for the forecast dashboard
    '''

    # Echo DOCKER_HOST
    print("DOCKER_HOST:", os.environ.get("DOCKER_HOST"))

    # Get a docker client
    client = docker.from_env()
    print(client.ping())

    # Get a list of containers
    containers = client.containers.list()

    # Print the list of containers
    print("List of containers:")
    for container in containers:
        print(container.name)
        print(container.status)


if __name__ == "__main__":
    test_docker_image()