import docker
import requests
from dateutil.parser import parse
import pytz



def get_docker_hub_image_creation_date(namespace, image_name):
    # Docker Hub API URL for fetching manifest data
    # curl --location 'https://hub.docker.com//v2/namespaces/{namespace}/repositories/{image_name}/'
    url = f"https://hub.docker.com//v2/namespaces/{namespace}/repositories/{image_name}/"

    # Get the docker image manifest data
    response = requests.get(url)
    if response.status_code == 200:
        manifest_data = response.json()
        return manifest_data['last_updated'][0]
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


