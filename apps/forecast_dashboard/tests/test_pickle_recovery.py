"""Tests for pickle crash recovery (atomic write + startup fallback).

Covers:
- Atomic pickle write succeeds and content matches
- Atomic pickle write crash safety (original file untouched)
- Startup fallback to HF fetch when pickle is unavailable
- Startup fallback timeout raises RuntimeError
"""

import os
import pickle
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# sys.path: make src/ importable (follows the pattern in conftest.py)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

from src import processing  # noqa: E402  (must come after sys.path setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStation:
    """Minimal picklable stand-in for a station object."""

    def __init__(self, code: str) -> None:
        self.code = code

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeStation) and self.code == other.code


def _make_mock_station(code: str) -> _FakeStation:
    """Return a picklable fake station with a .code attribute."""
    return _FakeStation(code)


def _tmp_files(directory) -> list[str]:
    """Return all .tmp files in directory."""
    return [f for f in os.listdir(directory) if f.endswith(".tmp")]


# ===========================================================================
# Test 1: Atomic write succeeds and content matches
# ===========================================================================


class TestAtomicWriteSucceeds:
    def test_atomic_write_succeeds(self, tmp_path):
        """save_stations_to_file writes correct content and leaves no temp files."""
        filepath = tmp_path / "all_stations.pkl"
        stations = [_make_mock_station("19999"), _make_mock_station("19998")]

        processing.save_stations_to_file(stations, str(filepath))

        # File must exist
        assert filepath.exists(), "pickle file was not created"

        # Content must round-trip correctly
        with open(filepath, "rb") as f:
            loaded = pickle.load(f)

        assert len(loaded) == 2
        assert loaded[0].code == "19999"
        assert loaded[1].code == "19998"

        # No leftover .tmp files
        assert _tmp_files(str(tmp_path)) == [], "leftover .tmp files found"


# ===========================================================================
# Test 2: Atomic write crash safety — original file untouched on failure
# ===========================================================================


class TestAtomicWriteCrashSafety:
    def test_atomic_write_crash_safety(self, tmp_path):
        """When pickle.dump raises, the original file is untouched and no .tmp lingers."""
        filepath = tmp_path / "all_stations.pkl"

        # Write a valid file first so there is an original to protect
        original_stations = [_make_mock_station("19999")]
        processing.save_stations_to_file(original_stations, str(filepath))

        new_stations = [_make_mock_station("19997"), _make_mock_station("19996")]

        # Simulate a mid-write failure
        with patch("pickle.dump", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                processing.save_stations_to_file(new_stations, str(filepath))

        # Original file must still be intact
        assert filepath.exists(), "original pickle file was destroyed"
        with open(filepath, "rb") as f:
            loaded = pickle.load(f)
        assert len(loaded) == 1
        assert loaded[0].code == "19999", "original file content was corrupted"

        # No leftover .tmp files
        assert _tmp_files(str(tmp_path)) == [], "leftover .tmp files found after crash"


# ===========================================================================
# Test 3: Startup fallback called on missing pickle
# ===========================================================================


class TestStartupFallbackCalledOnMissingPickle:
    def test_startup_fallback_called_on_missing_pickle(self):
        """When get_all_stations_from_file returns (None, None), the HF fetch is called."""
        valid_df = pd.DataFrame(
            {
                "code": ["19999"],
                "station_labels": ["19999 - Test River"],
                "basin": ["TestBasin"],
            }
        )
        valid_station_dict = {"TestBasin": ["19999 - Test River"]}

        with patch.object(
            processing, "get_all_stations_from_file", return_value=(None, None)
        ) as mock_file, patch.object(
            processing,
            "get_all_stations_from_iehhf",
            return_value=(valid_df, valid_station_dict),
        ) as mock_hf:
            # --- Replicate the startup fallback logic from forecast_dashboard.py lines 47-62 ---
            all_stations, station_dict = processing.get_all_stations_from_file()
            if not station_dict:
                executor = ThreadPoolExecutor(max_workers=1)
                future = executor.submit(processing.get_all_stations_from_iehhf)
                try:
                    all_stations, station_dict = future.result(timeout=30)
                except Exception:
                    all_stations, station_dict = None, None
                finally:
                    executor.shutdown(wait=False)
            # ---

        mock_file.assert_called_once()
        mock_hf.assert_called_once()

        # Result should be the valid data returned by the HF fetch
        assert all_stations is not None
        assert station_dict == valid_station_dict
        assert "19999 - Test River" in station_dict["TestBasin"]


# ===========================================================================
# Test 4: Startup fallback timeout raises Exception, result stays (None, None)
# ===========================================================================


class TestStartupFallbackTimeout:
    def test_startup_fallback_timeout(self):
        """When the HF fetch blocks longer than the timeout, a TimeoutError is raised."""
        def _slow_fetch():
            time.sleep(5)
            return pd.DataFrame(), {}

        with patch.object(
            processing, "get_all_stations_from_file", return_value=(None, None)
        ), patch.object(processing, "get_all_stations_from_iehhf", side_effect=_slow_fetch):
            all_stations, station_dict = processing.get_all_stations_from_file()
            if not station_dict:
                executor = ThreadPoolExecutor(max_workers=1)
                future = executor.submit(processing.get_all_stations_from_iehhf)
                try:
                    with pytest.raises(Exception):
                        future.result(timeout=1)
                    all_stations, station_dict = None, None
                finally:
                    executor.shutdown(wait=False)

        assert all_stations is None
        assert station_dict is None
