'''
Test the docker image for the forecast dashboard

This test script gets a docker client and gets a list of containers.
'''
import os
import docker

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