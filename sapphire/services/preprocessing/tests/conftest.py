"""
Pytest configuration for preprocessing service tests.

IMPORTANT: All env vars must be set before any app imports because
app/config.py instantiates Settings() at module level.
"""

import os
import sys
from pathlib import Path

# --- Environment setup (before ANY app imports) ---
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["LOG_LEVEL"] = "INFO"
os.environ["API_BASE_URL"] = "http://localhost:8000"
os.environ["BATCH_SIZE"] = "1000"
os.environ["CSV_FOLDER"] = "/tmp"

# Add app directory to Python path for imports
app_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(app_dir))
