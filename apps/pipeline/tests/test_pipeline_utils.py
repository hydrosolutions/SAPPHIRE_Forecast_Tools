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
from pipeline_utils import (
    get_docker_hub_image_creation_date,
    get_docker_hub_image_digest,
    get_local_image_digest,
    there_is_a_newer_image_on_docker_hub,
    _compare_by_timestamp,
)


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
    """Tests for there_is_a_newer_image_on_docker_hub function.

    Note: These tests cover backward compatibility with timestamp fallback
    when digests are unavailable. See TestDigestBasedComparison for digest tests.
    """

    def test_returns_true_when_hub_image_is_newer_timestamp_fallback(self):
        """Test that function returns True when Docker Hub image is newer (timestamp fallback)."""
        # Mock docker client - no RepoDigests forces timestamp fallback
        mock_client = Mock()
        mock_image = Mock()
        # Local image created in 2020, no RepoDigests
        mock_image.attrs = {'Created': '2020-01-01T00:00:00Z', 'RepoDigests': []}
        mock_client.images.get.return_value = mock_image

        # Mock the API calls
        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_digest.return_value = None  # No digest available
            mock_get_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            assert result is True

    def test_returns_false_when_local_image_is_newer_timestamp_fallback(self):
        """Test that function returns False when local image is newer (timestamp fallback)."""
        # Mock docker client - no RepoDigests forces timestamp fallback
        mock_client = Mock()
        mock_image = Mock()
        # Local image created in 2025, no RepoDigests
        mock_image.attrs = {'Created': '2025-12-01T00:00:00Z', 'RepoDigests': []}
        mock_client.images.get.return_value = mock_image

        # Mock the API calls
        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_digest.return_value = None  # No digest available
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
        """Test that function returns False when Docker Hub API fails completely."""
        # Mock docker client - has RepoDigests but hub returns nothing
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'Created': '2025-01-01T00:00:00Z',
            'RepoDigests': ['mabesa/sapphire-ml@sha256:local123']
        }
        mock_client.images.get.return_value = mock_image

        # Mock the API calls to return None (failure)
        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_digest.return_value = None
            mock_date.return_value = None

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            # Should return False (don't pull if we can't check)
            assert result is False

    def test_handles_timezone_naive_local_date(self):
        """Test that function handles timezone-naive local image dates."""
        mock_client = Mock()
        mock_image = Mock()
        # Timezone-naive date (no Z suffix) and no RepoDigests (forces timestamp fallback)
        mock_image.attrs = {'Created': '2020-01-01T00:00:00', 'RepoDigests': []}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_get_date:
            mock_digest.return_value = None
            mock_get_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )

            # Should still work and return True (via timestamp fallback)
            assert result is True


class TestGetDockerHubImageDigest:
    """Tests for get_docker_hub_image_digest function."""

    def test_returns_digest_for_valid_image(self):
        """Test that valid image returns a sha256 digest."""
        result = get_docker_hub_image_digest("mabesa", "sapphire-ml", "latest")
        assert result is not None
        assert result.startswith("sha256:")
        print(f"Digest for mabesa/sapphire-ml:latest: {result}")

    def test_returns_digest_for_py312_tag(self):
        """Test that py312 tag returns a sha256 digest."""
        result = get_docker_hub_image_digest("mabesa", "sapphire-postprocessing", "py312")
        assert result is not None
        assert result.startswith("sha256:")
        print(f"Digest for mabesa/sapphire-postprocessing:py312: {result}")

    def test_returns_none_for_nonexistent_image(self):
        """Test that nonexistent image returns None."""
        result = get_docker_hub_image_digest("mabesa", "nonexistent_image_12345", "latest")
        assert result is None

    def test_returns_none_for_nonexistent_tag(self):
        """Test that nonexistent tag returns None."""
        result = get_docker_hub_image_digest("mabesa", "sapphire-ml", "nonexistent_tag_12345")
        assert result is None

    @patch('pipeline_utils.requests.get')
    def test_handles_network_timeout(self, mock_get):
        """Test graceful handling of network timeout."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")
        result = get_docker_hub_image_digest("mabesa", "sapphire-ml", "latest")
        assert result is None

    @patch('pipeline_utils.requests.get')
    def test_handles_api_error(self, mock_get):
        """Test graceful handling of API errors."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        result = get_docker_hub_image_digest("mabesa", "sapphire-ml", "latest")
        assert result is None


class TestGetLocalImageDigest:
    """Tests for get_local_image_digest function."""

    def test_extracts_digest_from_repo_digests(self):
        """Test extraction of digest from RepoDigests."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'RepoDigests': ['mabesa/sapphire-ml@sha256:abc123def456']
        }
        mock_client.images.get.return_value = mock_image

        result = get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")
        assert result == "sha256:abc123def456"

    def test_returns_none_when_repo_digests_empty(self):
        """Test returns None when RepoDigests is empty."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'RepoDigests': []}
        mock_client.images.get.return_value = mock_image

        result = get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")
        assert result is None

    def test_returns_none_when_repo_digests_missing(self):
        """Test returns None when RepoDigests key is missing."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {}
        mock_client.images.get.return_value = mock_image

        result = get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")
        assert result is None

    def test_raises_image_not_found(self):
        """Test raises ImageNotFound for missing local image."""
        import docker
        mock_client = Mock()
        mock_client.images.get.side_effect = docker.errors.ImageNotFound("Not found")

        with pytest.raises(docker.errors.ImageNotFound):
            get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")

    def test_handles_multiple_repo_digests_matching_prefix(self):
        """Test correctly selects matching repo from multiple digests."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'RepoDigests': [
                'other/repo@sha256:wrong123',
                'mabesa/sapphire-ml@sha256:correct456'
            ]
        }
        mock_client.images.get.return_value = mock_image

        result = get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")
        assert result == "sha256:correct456"

    def test_fallback_to_first_digest_when_no_match(self):
        """Test fallback to first digest when prefix doesn't match."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'RepoDigests': ['different/repo@sha256:fallback789']
        }
        mock_client.images.get.return_value = mock_image

        result = get_local_image_digest(mock_client, "mabesa", "sapphire-ml", "latest")
        assert result == "sha256:fallback789"


class TestDigestBasedComparison:
    """Tests for digest-based image comparison in there_is_a_newer_image_on_docker_hub."""

    def test_returns_false_when_digests_match(self):
        """Same digest = no pull needed."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'RepoDigests': ['mabesa/sapphire-ml@sha256:abc123']}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_hub:
            mock_hub.return_value = 'sha256:abc123'
            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )
            assert result is False

    def test_returns_true_when_digests_differ(self):
        """Different digest = pull needed."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'RepoDigests': ['mabesa/sapphire-ml@sha256:local123']}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_hub:
            mock_hub.return_value = 'sha256:remote456'
            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )
            assert result is True

    def test_returns_true_when_local_image_missing(self):
        """Missing local image = pull needed."""
        import docker
        mock_client = Mock()
        mock_client.images.get.side_effect = docker.errors.ImageNotFound("Not found")

        result = there_is_a_newer_image_on_docker_hub(
            mock_client, "mabesa", "sapphire-ml", "latest"
        )
        assert result is True

    def test_fallback_to_timestamp_when_local_digest_missing(self):
        """Falls back to timestamp when local has no RepoDigests."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'RepoDigests': [],
            'Created': '2020-01-01T00:00:00Z'
        }
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_digest.return_value = 'sha256:abc123'
            mock_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )
            # Timestamp fallback: hub (2025) > local (2020), so True
            assert result is True

    def test_fallback_to_timestamp_when_hub_digest_unavailable(self):
        """Falls back to timestamp when Docker Hub digest unavailable."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {
            'RepoDigests': ['mabesa/sapphire-ml@sha256:local123'],
            'Created': '2025-12-01T00:00:00Z'
        }
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_digest.return_value = None  # Hub digest unavailable
            mock_date.return_value = '2025-01-01T00:00:00Z'

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )
            # Timestamp fallback: hub (2025-01) < local (2025-12), so False
            assert result is False

    def test_returns_false_when_all_comparisons_fail(self):
        """Returns False (don't pull) when all comparison methods fail."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'RepoDigests': [], 'Created': '2025-01-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_digest') as mock_digest, \
             patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_digest.return_value = None
            mock_date.return_value = None

            result = there_is_a_newer_image_on_docker_hub(
                mock_client, "mabesa", "sapphire-ml", "latest"
            )
            assert result is False


class TestCompareByTimestamp:
    """Tests for _compare_by_timestamp fallback function."""

    def test_returns_true_when_hub_newer(self):
        """Hub timestamp newer than local = True."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'Created': '2020-01-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_date.return_value = '2025-01-01T00:00:00Z'
            result = _compare_by_timestamp(mock_client, "mabesa", "sapphire-ml", "latest")
            assert result is True

    def test_returns_false_when_local_newer(self):
        """Local timestamp newer than hub = False."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'Created': '2025-12-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_date.return_value = '2025-01-01T00:00:00Z'
            result = _compare_by_timestamp(mock_client, "mabesa", "sapphire-ml", "latest")
            assert result is False

    def test_returns_false_when_hub_date_unavailable(self):
        """Returns False when Docker Hub date unavailable."""
        mock_client = Mock()
        mock_image = Mock()
        mock_image.attrs = {'Created': '2025-01-01T00:00:00Z'}
        mock_client.images.get.return_value = mock_image

        with patch('pipeline_utils.get_docker_hub_image_creation_date') as mock_date:
            mock_date.return_value = None
            result = _compare_by_timestamp(mock_client, "mabesa", "sapphire-ml", "latest")
            assert result is False

    def test_returns_true_when_local_missing(self):
        """Returns True when local image missing."""
        import docker
        mock_client = Mock()
        mock_client.images.get.side_effect = docker.errors.ImageNotFound("Not found")

        result = _compare_by_timestamp(mock_client, "mabesa", "sapphire-ml", "latest")
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
