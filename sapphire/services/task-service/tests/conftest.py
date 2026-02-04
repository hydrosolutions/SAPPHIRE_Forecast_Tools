import pytest
import sys
from pathlib import Path

# Add the parent directory to the path so we can import app
sys.path.insert(0, str(Path(__file__).parent.parent))
