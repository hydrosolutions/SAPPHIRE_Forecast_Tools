"""Tests for quarterly/seasonal gap detection in gap_detector.py.

Phase 4b Step 7.
"""

import logging
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


class TestQuarterlyGapDetectionLeadAware:
    """M1 P1b: under SAPPHIRE_SKILL_LEAD_AWARE, gaps are detected per-lead

    (mirrors detect_missing_seasonal_ensembles' unconditional
    season_in_year key), instead of conflating leads.
    """

    def test_flag_on_detects_per_lead_gap(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025],
                "quarter_in_year": [1, 1, 1],
                "code": ["19999", "19999", "19999"],
                "model_short": ["LR", "EM", "LR"],
                "horizon_value": [0, 0, 1],
                "forecasted_discharge": [100, 100, 80],
            }
        )
        gaps = detect_missing_quarterly_ensembles(combined)
        assert len(gaps) == 1
        assert gaps.iloc[0]["horizon_value"] == 1
        assert gaps.iloc[0]["model_short"] == "EM"

    def test_flag_off_conflates_leads(self, monkeypatch):
        """Companion assertion: flag OFF keeps today's behavior -- the EM

        row at lead 0 satisfies the (year, quarter, code) grain for BOTH
        leads, so no gap is detected (leads conflated).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025],
                "quarter_in_year": [1, 1, 1],
                "code": ["19999", "19999", "19999"],
                "model_short": ["LR", "EM", "LR"],
                "horizon_value": [0, 0, 1],
                "forecasted_discharge": [100, 100, 80],
            }
        )
        gaps = detect_missing_quarterly_ensembles(combined)
        assert gaps.empty

    def test_flag_on_null_lead_row_skipped_with_warning_not_phantom_gap(self, monkeypatch, caplog):
        """FIX 5 (revised): a legacy NULL-horizon_value quarter row cannot

        be keyed to a real lead. Manufacturing an "unknown-lead" sentinel
        gap only creates a phantom gap that downstream maintenance (which
        regenerates ensembles at REAL leads) can never fill. So such rows
        are EXCLUDED from the lead-aware gap universe -- but surfaced with
        a WARNING naming the count (not silently dropped).
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR"],
                "horizon_value": [None],  # NaN / legacy null lead
                "forecasted_discharge": [100],
            }
        )
        with caplog.at_level(logging.WARNING):
            gaps = detect_missing_quarterly_ensembles(combined, ensemble_models={"EM"})
        # The NULL-lead row is the only row: no fillable/phantom gap.
        assert gaps.empty
        # ...and it is NOT keyed at a negative/-1 sentinel lead.
        if "horizon_value" in gaps.columns:
            assert (gaps["horizon_value"] >= 0).all()
        # A WARNING naming the count of skipped rows was emitted.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "expected a WARNING for the skipped NULL-lead row"
        assert any("1" in r.getMessage() for r in warnings)

    def test_flag_on_null_lead_row_skipped_but_real_lead_gaps_still_detected(
        self, monkeypatch, caplog
    ):
        """FIX 5 (revised): the NULL-lead row is excluded, but real-lead

        gap detection is unchanged: hv=0 (LR, no EM) still reports its EM
        gap, hv=1 (LR+EM) does not, and NO gap row is keyed at -1.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025],
                "quarter_in_year": [1, 1, 1, 1],
                "code": ["19999", "19999", "19999", "19999"],
                "model_short": ["LR", "LR", "LR", "EM"],
                "horizon_value": [None, 0, 1, 1],  # NULL LR; hv=0 LR; hv=1 LR+EM
                "forecasted_discharge": [80, 100, 90, 90],
            }
        )
        with caplog.at_level(logging.WARNING):
            gaps = detect_missing_quarterly_ensembles(combined, ensemble_models={"EM"})
        # Only the real hv=0 lead reports a missing-EM gap.
        assert len(gaps) == 1
        assert gaps.iloc[0]["model_short"] == "EM"
        assert int(gaps.iloc[0]["horizon_value"]) == 0
        # No gap keyed at the removed -1 sentinel.
        assert (gaps["horizon_value"] != -1).all()
        # One WARNING for the single skipped NULL-lead row.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        assert any("1" in r.getMessage() for r in warnings)


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
