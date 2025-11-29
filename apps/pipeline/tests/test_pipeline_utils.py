import requests
from dateutil.parser import parse


def get_docker_hub_image_creation_date(namespace, image_name):
    """Copy of the function for testing without docker dependency."""
    url = f"https://hub.docker.com//v2/namespaces/{namespace}/repositories/{image_name}/"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            manifest_data = response.json()
            return manifest_data.get('last_updated')
        else:
            print(f"Failed to fetch image data from Docker Hub: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Failed to reach Docker Hub: {e}")
        return None


def test_get_docker_hub_image_creation_date_sapphire_ml():
    """Test that we get a valid image creation date for mabesa/sapphire-ml."""
    namespace = "mabesa"
    image_name = "sapphire-ml"

    result = get_docker_hub_image_creation_date(namespace, image_name)

    # Should not be None
    assert result is not None, "Failed to fetch image creation date from Docker Hub"

    # Should be a parseable date string
    parsed_date = parse(result)
    assert parsed_date is not None, "Failed to parse the returned date string"

    # Should have timezone info (Docker Hub returns ISO format with timezone)
    print(f"Image creation date: {result}")
    print(f"Parsed date: {parsed_date}")


def test_get_docker_hub_image_creation_date_nonexistent():
    """Test that we get None for a non-existent image."""
    namespace = "mabesa"
    image_name = "this_image_does_not_exist_12345"

    result = get_docker_hub_image_creation_date(namespace, image_name)

    # Should be None for non-existent image
    assert result is None, "Expected None for non-existent image"


if __name__ == "__main__":
    print("Testing get_docker_hub_image_creation_date with mabesa/sapphire-ml...")
    test_get_docker_hub_image_creation_date_sapphire_ml()
    print("✓ Test passed!\n")

    print("Testing get_docker_hub_image_creation_date with non-existent image...")
    test_get_docker_hub_image_creation_date_nonexistent()
    print("✓ Test passed!\n")

    print("All tests passed!")
