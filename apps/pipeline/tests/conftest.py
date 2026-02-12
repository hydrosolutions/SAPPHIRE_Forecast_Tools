"""Pytest fixtures for pipeline tests.

This module provides common fixtures for testing pipeline_docker.py functionality,
particularly marker file operations and gateway dependency resolution.
"""
import pytest
import os
import sys
from pathlib import Path

# Add the repository root to the path for pipeline_docker import.
# pipeline_docker.py uses imports like "from apps.pipeline.src import ..."
# which requires the repository root (parent of apps/) to be in sys.path.
# Also add the pipeline directory itself for direct module imports.
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
_pipeline_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, _repo_root)
sys.path.insert(0, _pipeline_dir)

# Set up minimal environment variables required by pipeline_docker before importing.
# pipeline_docker.py evaluates environment variables at module import time,
# so these must be set before the import happens.
# These are test-specific values that won't affect production.
_test_tmp = '/tmp/pipeline_tests'
os.makedirs(_test_tmp, exist_ok=True)
os.makedirs(f'{_test_tmp}/marker_files', exist_ok=True)

os.environ.setdefault('ieasyhydroforecast_env_file_path', '')
os.environ.setdefault('ieasyhydroforecast_backend_docker_image_tag', 'py312')
os.environ.setdefault('ieasyhydroforecast_organization', 'demo')
os.environ.setdefault('SAPPHIRE_DG_HOST', 'http://localhost:8000')
os.environ.setdefault('ieasyhydroforecast_run_ML_models', 'False')
os.environ.setdefault('ieasyhydroforecast_run_CM_models', 'False')
os.environ.setdefault('ieasyforecast_intermediate_data_path', _test_tmp)
os.environ.setdefault('ieasyforecast_configuration_path', _test_tmp)
os.environ.setdefault('ieasyhydroforecast_OUTPUT_PATH_DG', 'gateway_output')
os.environ.setdefault('ieasyhydroforecast_available_ML_models', 'TFT,TIDE')


@pytest.fixture
def temp_marker_dir(tmp_path):
    """Create a temporary directory for marker files.

    Args:
        tmp_path: pytest's built-in tmp_path fixture

    Returns:
        Path to the temporary marker directory
    """
    marker_dir = tmp_path / "marker_files"
    marker_dir.mkdir()
    return marker_dir


@pytest.fixture
def mock_env(temp_marker_dir, monkeypatch):
    """Mock environment with temp marker directory.

    This fixture patches the MARKER_DIR constant in pipeline_docker
    to use a temporary directory, allowing tests to run in isolation
    without affecting real marker files.

    Args:
        temp_marker_dir: Temporary directory for marker files
        monkeypatch: pytest's monkeypatch fixture

    Returns:
        Dictionary with marker_dir path for test reference
    """
    # Import the module to patch (using relative import from sys.path)
    import pipeline_docker

    # Store the original value
    original_marker_dir = pipeline_docker.MARKER_DIR

    # Patch the MARKER_DIR constant
    monkeypatch.setattr(pipeline_docker, 'MARKER_DIR', str(temp_marker_dir))

    yield {'marker_dir': temp_marker_dir, 'original_marker_dir': original_marker_dir}

    # monkeypatch automatically restores the original value


@pytest.fixture
def clean_marker_dir(mock_env):
    """Ensure marker directory is clean before each test.

    Args:
        mock_env: The mock environment fixture

    Returns:
        Path to the clean marker directory
    """
    marker_dir = mock_env['marker_dir']
    # Clean any existing marker files
    for f in marker_dir.glob('*.marker'):
        f.unlink()
    return marker_dir
