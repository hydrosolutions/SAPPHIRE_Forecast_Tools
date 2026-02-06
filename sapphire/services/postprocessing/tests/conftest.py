"""
Pytest configuration for postprocessing service tests.
"""

import sys
from pathlib import Path

# Add app directory to Python path for imports
app_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(app_dir))
