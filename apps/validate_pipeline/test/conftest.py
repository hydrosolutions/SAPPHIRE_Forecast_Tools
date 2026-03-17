"""Shared fixtures for validate_pipeline tests."""

import os
import sys

import pytest

# Add validate_pipeline/ to path so validate_pipeline.py can be imported.
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)
