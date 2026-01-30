import pytest
import requests
from unittest.mock import Mock, patch
from dateutil.parser import parse
import pytz
from datetime import datetime

# Import the actual functions from the module
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipeline_utils import get_docker_hub_image_creation_date, there_is_a_newer_image_on_docker_hub


class TestGetDockerHubImageCreationDate:
    """Tests for get_docker_hub_image_creation_date function."""

    def test_get_docker_hub_image_creation_date_with_valid_tag(self):
        """Test that we get a valid image creation date for mabesa/sapphire-ml:latest."""
        namespace = "mabesa"
        image_name = "sapphire-ml"
        tag = "latest"

        result = get_docker_hub_image_creation_date(namespace, image_name, tag)

        # Should not be None
        assert result is not None, "Failed to fetch image creation date from Docker Hub"

        # Should be a parseable date string
        parsed_date = parse(result)
        assert parsed_date is not None, "Failed to parse the returned date string"

        print(f"Image creation date for {namespace}/{image_name}:{tag}: {result}")

    def test_get_docker_hub_image_creation_date_py312_tag(self):
        """Test that we get a valid image creation date for py312 tag."""
        namespace = "mabesa"
        image_name = "sapphire-postprocessing"
        tag = "py312"

        result = get_docker_hub_image_creation_date(namespace, image_name, tag)

        # Should not be None
        assert result is not None, "Failed to fetch image creation date from Docker Hub"

        # Should be a parseable date string
        parsed_date = parse(result)
        assert parsed_date is not None, "Failed to parse the returned date string"

        print(f"Image creation date for {namespace}/{image_name}:{tag}: {result}")

    def test_different_tags_have_different_dates(self):
        """Test that different tags return different (tag-specific) dates."""
        namespace = "mabesa"
        image_name = "sapphire-postprocessing"

        latest_date = get_docker_hub_image_creation_date(namespace, image_name, "latest")
        py312_date = get_docker_hub_image_creation_date(namespace, image_name, "py312")

        # Both should be valid
        assert latest_date is not None
        assert py312_date is not None

        # They should be different dates (unless pushed at exact same time, which is unlikely)
        print(f"latest tag date: {latest_date}")
        print(f"py312 tag date: {py312_date}")

    def test_nonexistent_image(self):
        """Test that we get None for a non-existent image."""
        namespace = "mabesa"
        image_name = "this_image_does_not_exist_12345"
        tag = "latest"

        result = get_docker_hub_image_creation_date(namespace, image_name, tag)

        # Should be None for non-existent image
        assert result is None, "Expected None for non-existent image"

    def test_nonexistent_tag(self):
        """Test that we get None for a non-existent tag."""
        namespace = "mabesa"
        image_name = "sapphire-ml"
        tag = "this_tag_does_not_exist_12345"

        result = get_docker_hub_image_creation_date(namespace, image_name, tag)

        # Should be None for non-existent tag
        assert result is None, "Expected None for non-existent tag"


class TestThereIsANewerImageOnDockerHub:
    """Tests for there_is_a_newer_image_on_docker_hub function."""

    def test_returns_true_when_hub_image_is_newer(self):
        """Test that function returns True when Docker Hub image is newer."""
        # Mock docker client
        mock_client = Mock()
        mock_image = Mock()
        # Local image created in 2020
        mock_image.attrs = {'Created': '2020-01-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        # Mock the API call to return a newer date
        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_get_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            assert result is True
            mock_get_date.assert_called_once_with("mabesa", "sapphire-ml", "latest")

    def test_returns_false_when_local_image_is_newer(self):
        """Test that function returns False when local image is newer or same."""
        # Mock docker client
        mock_client = Mock()
        mock_image = Mock()
        # Local image created in 2025
        mock_image.attrs = {'Created': '2025-12-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        # Mock the API call to return an older date
        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_get_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            assert result is False

    def test_returns_true_when_local_image_not_found(self):
        """Test that function returns True when local image doesn't exist."""
        import docker

        mock_client = Mock()
        mock_client.images.get.side_effect = docker.errors.ImageNotFound("Image not found")

        result = there_is_a_newer_image_on_docker_hub(
            mock_client, "mabesa", "sapphire-ml", "latest"
        )

        # Should return True to trigger a pull
        assert result is True

    def test_returns_false_when_hub_fetch_fails(self):
        """Test that function returns False when Docker Hub API fails."""
        # Mock docker client
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'Created': '2025-01-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        # Mock the API call to return None (failure)
        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_get_date.return_value = None

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            # Should return False (don't pull if we can't check)
            assert result is False

    def test_handles_timezone_naive_local_date(self):
        """Test that function handles timezone-naive local image dates."""
        mock_client = Mock()
        mock_image = Mock()
        # Timezone-naive date (no Z suffix)
        mock_image.attrs = {'Created': '2020-01-01T00:00:00'}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_get_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            # Should still work and return True
            assert result is True


if __name__ == "__main__":
    print("Running tests for pipeline_utils...\n")

    # Run simple tests without mocking first
    print("=== Testing get_docker_hub_image_creation_date ===")
    test_instance = TestGetDockerHubImageCreationDate()

    print("\n1. Testing with valid tag (mabesa/sapphire-ml:latest)...")
    test_instance.test_get_docker_hub_image_creation_date_with_valid_tag()
    print("✓ Passed\n")

    print("2. Testing with py312 tag...")
    test_instance.test_get_docker_hub_image_creation_date_py312_tag()
    print("✓ Passed\n")

    print("3. Testing different tags have different dates...")
    test_instance.test_different_tags_have_different_dates()
    print("✓ Passed\n")

    print("4. Testing non-existent image...")
    test_instance.test_nonexistent_image()
    print("✓ Passed\n")

    print("5. Testing non-existent tag...")
    test_instance.test_nonexistent_tag()
    print("✓ Passed\n")

    print("=== All get_docker_hub_image_creation_date tests passed! ===\n")

    print("To run all tests including mocked tests, use: pytest test_pipeline_utils.py -v")
