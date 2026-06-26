"""Tests for quarterly/seasonal gap detection in gap_detector.py.

Phase 4b Step 7.
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.gap_detector import (
    detect_missing_quarterly_ensembles,
    detect_missing_seasonal_ensembles,
)

# ===================================================================
# Quarterly gap detection
# ===================================================================


class TestQuarterlyGapDetection:
    def test_detects_missing_em(self):
        """EM missing for one (year, quarter, code) pair."""
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025],
                "quarter_in_year": [1, 1, 1],
                "code": ["S1", "S1", "S2"],
                "model_short": ["LR", "EM", "LR"],
                "forecasted_discharge": [100, 100, 80],
            }
        )
        gaps = detect_missing_quarterly_ensembles(combined)
        assert len(gaps) == 1
        assert gaps.iloc[0]["code"] == "S2"
        assert gaps.iloc[0]["model_short"] == "EM"

    def test_no_gaps_returns_empty(self):
        combined = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["S1", "S1"],
                "model_short": ["LR", "EM"],
                "forecasted_discharge": [100, 100],
            }
        )
        gaps = detect_missing_quarterly_ensembles(combined)
        assert gaps.empty

    def test_lookback_window(self):
        """Only detects gaps within lookback window."""
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2024, 2024],
                "quarter_in_year": [4, 4, 1, 1],
                "code": ["S1", "S1", "S1", "S1"],
                "model_short": ["LR", "EM", "LR", "LR"],
                "forecasted_discharge": [100, 100, 80, 80],
            }
        )
        # lookback=2 → Q4-2025 and Q3-2025 → Q1-2024 not in window
        gaps = detect_missing_quarterly_ensembles(combined, lookback_quarters=2)
        assert gaps.empty

    def test_empty_input(self):
        gaps = detect_missing_quarterly_ensembles(pd.DataFrame())
        assert gaps.empty
        assert "year" in gaps.columns
        assert "quarter_in_year" in gaps.columns


# ===================================================================
# Seasonal gap detection
# ===================================================================


class TestSeasonalGapDetection:
    def test_detects_missing_em(self):
        combined = pd.DataFrame(
            {
                "season_year": [2025, 2025, 2025],
                "season_in_year": [1, 1, 1],
                "code": ["S1", "S1", "S2"],
                "model_short": ["LR", "EM", "LR"],
                "forecasted_discharge": [100, 100, 80],
            }
        )
        gaps = detect_missing_seasonal_ensembles(combined)
        assert len(gaps) == 1
        assert gaps.iloc[0]["code"] == "S2"

    def test_no_gaps_returns_empty(self):
        combined = pd.DataFrame(
            {
                "season_year": [2025, 2025],
                "season_in_year": [1, 1],
                "code": ["S1", "S1"],
                "model_short": ["LR", "EM"],
                "forecasted_discharge": [100, 100],
            }
        )
        gaps = detect_missing_seasonal_ensembles(combined)
        assert gaps.empty

    def test_lookback_window(self):
        """Only detects gaps for recent season years."""
        combined = pd.DataFrame(
            {
                "season_year": [2025, 2025, 2023, 2023],
                "season_in_year": [1, 1, 1, 1],
                "code": ["S1", "S1", "S1", "S1"],
                "model_short": ["LR", "EM", "LR", "LR"],
                "forecasted_discharge": [100, 100, 80, 80],
            }
        )
        # lookback=1 → only 2025
        gaps = detect_missing_seasonal_ensembles(combined, lookback_seasons=1)
        assert gaps.empty

    def test_empty_input(self):
        gaps = detect_missing_seasonal_ensembles(pd.DataFrame())
        assert gaps.empty
        assert "season_year" in gaps.columns
        assert "season_in_year" in gaps.columns

    def test_detects_one_missing_issue_lead_out_of_four(self):
        combined = pd.DataFrame(
            {
                "season_year": [2025] * 7,
                "season_in_year": [3, 3, 2, 2, 1, 1, 0],
                "code": ["PP4_S_SENTINEL"] * 7,
                "model_short": ["LR", "EM", "LR", "EM", "LR", "EM", "LR"],
                "forecasted_discharge": [100, 100, 110, 110, 120, 120, 130],
            }
        )

        gaps = detect_missing_seasonal_ensembles(combined)

        assert len(gaps) == 1
        gap = gaps.iloc[0]
        assert gap["season_year"] == 2025
        assert gap["season_in_year"] == 0
        assert gap["code"] == "PP4_S_SENTINEL"
        assert gap["model_short"] == "EM"
