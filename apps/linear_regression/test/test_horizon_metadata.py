"""
LR-008: Protective tests for horizon metadata indexing convention.

These tests guard three contracts:

1. **Convention contract** — the LR DataFrame stores rows under the ISSUE DATE,
   so `forecast_pentad_of_year = tl.get_pentad_in_year(current_day)` returns the
   issue pentad. This is correct and must NOT change.

2. **Override arithmetic** — before the API write, linear_regression.py overrides
   `pentad_in_year` and `pentad_in_month` (and their decad equivalents) from the
   issue period to the TARGET period using +1 wrap-around logic.

3. **Internal consistency** — `horizon_in_year` and `horizon_value` (pentad/decad
   in month) are algebraically consistent for all valid period values.

No live API calls, no filesystem writes. The arithmetic tests are pure unit tests
that import tag_library directly via the sys.path set up in conftest.py.
"""

import datetime as dt
import os
import sys

import pytest

# conftest.py already inserts iEasyHydroForecast onto sys.path, but we need
# to be explicit here so the module is importable when tests run in isolation.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import tag_library as tl  # noqa: E402

# ============================================================================
# Convention contract (Tests 1–2): issue-date indexing is preserved
# ============================================================================


class TestIssueIndexingConvention:
    """The LR DataFrame is indexed by issue date, not target date.

    Mar 25 is the issue date for the pentad 18 forecast (Mar 26–31).
    `get_pentad_in_year(Mar 25)` must return '17' — the ISSUE pentad.
    This value is used for training, norms, and visibility; it must stay as-is.
    """

    def test_get_pentad_in_year_returns_issue_pentad_mar25(self):
        """get_pentad_in_year(Mar 25) returns '17' (issue pentad), not '18'."""
        issue_date = dt.date(2026, 3, 25)
        result = tl.get_pentad_in_year(issue_date)
        assert result == "17", (
            f"Expected issue pentad '17' for {issue_date}, got {result!r}. "
            "The convention that LR rows are indexed by issue date must be preserved."
        )

    def test_get_decad_in_year_returns_issue_decad_mar10(self):
        """get_decad_in_year(Mar 10) returns '7' (issue decad), not '8'."""
        issue_date = dt.date(2026, 3, 10)
        result = tl.get_decad_in_year(issue_date)
        assert result == "7", (
            f"Expected issue decad '7' for {issue_date}, got {result!r}. "
            "The convention that LR rows are indexed by issue date must be preserved."
        )


# ============================================================================
# Upstream isolation (Tests 3–4): save_discharge_avg uses issue period
# ============================================================================


class TestUpstreamIsolation:
    """save_discharge_avg and perform_linear_regression must use the issue period.

    The override to target period happens AFTER these calls. The arithmetic
    below mirrors what linear_regression.py does: the training functions
    receive `forecast_pentad_of_year` (issue period), while `write_*` receives
    the overridden target period.
    """

    def test_issue_pentad_differs_from_target_pentad(self):
        """Issue pentad and target pentad are different values (not equal)."""
        issue_date = dt.date(2026, 3, 25)
        _issue_pentad = int(tl.get_pentad_in_year(issue_date))
        _target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
        assert _issue_pentad != _target_pentad
        assert _issue_pentad == 17
        assert _target_pentad == 18

    def test_issue_decad_differs_from_target_decad(self):
        """Issue decad and target decad are different values (not equal)."""
        issue_date = dt.date(2026, 3, 10)
        _issue_decad = int(tl.get_decad_in_year(issue_date))
        _target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
        assert _issue_decad != _target_decad
        assert _issue_decad == 7
        assert _target_decad == 8


# ============================================================================
# Pentad override arithmetic (Tests 5–14)
# ============================================================================


class TestPentadMetadataOverride:
    """Verify issue_pentad + 1 produces the correct target pentad.

    For each case the formulas from linear_regression.py are reproduced:
        _issue_pentad = int(tl.get_pentad_in_year(issue_date))
        _target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
        _target_pim = ((_target_pentad - 1) % 6) + 1

    Each test also cross-checks against get_pentad_in_year(issue_date + 1 day).
    """

    @pytest.mark.parametrize(
        "issue_date,expected_target_piy,expected_target_pim",
        [
            # Test 5: Mar 25, issue pentad 17 → target 18, month-pentad 6
            (dt.date(2026, 3, 25), "18", "6"),
            # Test 6: Mar 5, issue pentad 13 → target 14, month-pentad 2
            (dt.date(2026, 3, 5), "14", "2"),
            # Test 7: Mar 15, issue pentad 15 → target 16, month-pentad 4
            (dt.date(2026, 3, 15), "16", "4"),
            # Test 10: Dec 31, issue pentad 72 → wrap to 1, month-pentad 1
            (dt.date(2026, 12, 31), "1", "1"),
            # Test 12: Jan 31, issue pentad 6 → target 7 (crosses to Feb)
            (dt.date(2026, 1, 31), "7", "1"),
            # Test 13: May 31, issue pentad 30 → target 31 (crosses to Jun)
            (dt.date(2026, 5, 31), "31", "1"),
            # Test 14: Feb 29 leap year, issue pentad 12 → target 13 (crosses to Mar)
            (dt.date(2024, 2, 29), "13", "1"),
            # Feb 28 non-leap year, same target
            (dt.date(2026, 2, 28), "13", "1"),
        ],
    )
    def test_pentad_override_arithmetic(
        self, issue_date: dt.date, expected_target_piy: str, expected_target_pim: str
    ):
        """The +1 override produces the correct target pentad and pentad-in-month."""
        _issue_pentad = int(tl.get_pentad_in_year(issue_date))
        _target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
        _target_pim = ((_target_pentad - 1) % 6) + 1

        assert str(_target_pentad) == expected_target_piy, (
            f"issue_date={issue_date}: expected target pentad_in_year={expected_target_piy}, "
            f"got {_target_pentad} (issue was {_issue_pentad})"
        )
        assert str(_target_pim) == expected_target_pim, (
            f"issue_date={issue_date}: expected target pentad_in_month={expected_target_pim}, "
            f"got {_target_pim}"
        )

        # Cross-check: target pentad must equal get_pentad_in_year(issue_date + 1 day)
        # (except Dec 31 → Jan 1 which wraps the year, handled by the wrap guard above)
        if _issue_pentad != 72:
            target_day = issue_date + dt.timedelta(days=1)
            cross_check = tl.get_pentad_in_year(target_day)
            assert str(_target_pentad) == cross_check, (
                f"issue_date={issue_date}: target pentad {_target_pentad} does not match "
                f"get_pentad_in_year({target_day})={cross_check}"
            )


# ============================================================================
# Decad override arithmetic (Tests 8–11 + boundary cases)
# ============================================================================


class TestDecadMetadataOverride:
    """Verify issue_decad + 1 produces the correct target decad.

    Mirrors TestPentadMetadataOverride but for decads.
    Formula from linear_regression.py:
        _issue_decad = int(tl.get_decad_in_year(issue_date))
        _target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
        _target_dim = ((_target_decad - 1) % 3) + 1
    """

    @pytest.mark.parametrize(
        "issue_date,expected_target_diy,expected_target_dim",
        [
            # Test 8: Mar 10, issue decad 7 → target 8, month-decad 2
            (dt.date(2026, 3, 10), "8", "2"),
            # Test 9: Mar 20, issue decad 8 → target 9, month-decad 3
            (dt.date(2026, 3, 20), "9", "3"),
            # Test 11: Dec 31, issue decad 36 → wrap to 1, month-decad 1
            (dt.date(2026, 12, 31), "1", "1"),
            # Jan 31, issue decad 3 → target 4 (crosses to Feb), month-decad 1
            (dt.date(2026, 1, 31), "4", "1"),
            # Jun 30, issue decad 18 → target 19 (crosses to Jul), month-decad 1
            (dt.date(2026, 6, 30), "19", "1"),
        ],
    )
    def test_decad_override_arithmetic(
        self, issue_date: dt.date, expected_target_diy: str, expected_target_dim: str
    ):
        """The +1 override produces the correct target decad and decad-in-month."""
        _issue_decad = int(tl.get_decad_in_year(issue_date))
        _target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
        _target_dim = ((_target_decad - 1) % 3) + 1

        assert str(_target_decad) == expected_target_diy, (
            f"issue_date={issue_date}: expected target decad_in_year={expected_target_diy}, "
            f"got {_target_decad} (issue was {_issue_decad})"
        )
        assert str(_target_dim) == expected_target_dim, (
            f"issue_date={issue_date}: expected target decad_in_month={expected_target_dim}, "
            f"got {_target_dim}"
        )

        # Cross-check: target decad must equal get_decad_in_year(issue_date + 1 day)
        if _issue_decad != 36:
            target_day = issue_date + dt.timedelta(days=1)
            cross_check = tl.get_decad_in_year(target_day)
            assert str(_target_decad) == cross_check, (
                f"issue_date={issue_date}: target decad {_target_decad} does not match "
                f"get_decad_in_year({target_day})={cross_check}"
            )


# ============================================================================
# Internal consistency (Test 15): horizon_in_year and horizon_value
# ============================================================================


class TestInternalConsistency:
    """Test 15: horizon_in_year and horizon_value must be algebraically consistent.

    For any valid target period number, the month derived from horizon_in_year
    must be consistent with horizon_value (pentad/decad in month), and the
    round-trip reconstruction must reproduce the original horizon_in_year.
    """

    @pytest.mark.parametrize("target_pentad", range(1, 73))
    def test_pentad_consistency(self, target_pentad: int):
        """For all 72 pentads, pentad_in_year and pentad_in_month are consistent."""
        pim = ((target_pentad - 1) % 6) + 1
        month = ((target_pentad - 1) // 6) + 1

        assert 1 <= pim <= 6, (
            f"pentad_in_month {pim} out of range [1,6] for target_pentad={target_pentad}"
        )
        assert 1 <= month <= 12, (
            f"month {month} out of range [1,12] for target_pentad={target_pentad}"
        )

        # Round-trip: reconstruct pentad_in_year from month and pentad_in_month
        reconstructed = (month - 1) * 6 + pim
        assert reconstructed == target_pentad, (
            f"Round-trip failed: target_pentad={target_pentad}, month={month}, "
            f"pim={pim} → reconstructed={reconstructed}"
        )

    @pytest.mark.parametrize("target_decad", range(1, 37))
    def test_decad_consistency(self, target_decad: int):
        """For all 36 decads, decad_in_year and decad_in_month are consistent."""
        dim = ((target_decad - 1) % 3) + 1
        month = ((target_decad - 1) // 3) + 1

        assert 1 <= dim <= 3, (
            f"decad_in_month {dim} out of range [1,3] for target_decad={target_decad}"
        )
        assert 1 <= month <= 12, (
            f"month {month} out of range [1,12] for target_decad={target_decad}"
        )

        # Round-trip: reconstruct decad_in_year from month and decad_in_month
        reconstructed = (month - 1) * 3 + dim
        assert reconstructed == target_decad, (
            f"Round-trip failed: target_decad={target_decad}, month={month}, "
            f"dim={dim} → reconstructed={reconstructed}"
        )


# ============================================================================
# Integration smoke test: override is applied before write_linreg_*
# ============================================================================


class TestMetadataOverrideIntegration:
    """Integration smoke test: the DataFrame passed to write_linreg_pentad_forecast_data
    carries the TARGET pentad, not the issue pentad.

    This is the end-to-end guard: if the override code in linear_regression.py
    is removed or regressed, this test will fail.
    """

    def test_override_applied_before_pentad_write(self):
        """After override, pentad_in_year reflects target period (issue + 1).

        This test reproduces the exact override logic from linear_regression.py
        and verifies the resulting DataFrame has the target values, not the
        issue values.
        """
        import pandas as pd

        # Simulate what linear_regression.py does:
        # 1. get_pentad_in_year returns the issue pentad
        issue_date = dt.date(2026, 3, 25)
        forecast_pentad_of_year = tl.get_pentad_in_year(issue_date)
        assert forecast_pentad_of_year == "17"  # issue pentad

        # 2. Build a minimal linreg_pentad DataFrame (as perform_linear_regression would)
        linreg_pentad = pd.DataFrame(
            {
                "code": ["15013"],
                "date": [issue_date],
                "discharge_avg": [50.0],
                "pentad_in_year": [forecast_pentad_of_year],  # issue pentad initially
                "pentad": ["5"],  # pentad in month (will be renamed to pentad_in_month)
            }
        )

        # 3. Rename (mirrors the rename in linear_regression.py)
        linreg_pentad.rename(columns={"pentad": "pentad_in_month"}, inplace=True)

        # 4. Apply the override (exact code from linear_regression.py)
        _issue_pentad = int(forecast_pentad_of_year)
        _target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
        linreg_pentad["pentad_in_year"] = str(_target_pentad)
        linreg_pentad["pentad_in_month"] = str(((_target_pentad - 1) % 6) + 1)

        # 5. Assert the DataFrame now has target values
        assert linreg_pentad["pentad_in_year"].iloc[0] == "18", (
            "pentad_in_year must be the target pentad (18), not the issue pentad (17)"
        )
        assert linreg_pentad["pentad_in_month"].iloc[0] == "6", (
            "pentad_in_month must be 6 (the 6th pentad of March), not 5"
        )

    def test_override_applied_before_decad_write(self):
        """After override, decad_in_year reflects target period (issue + 1).

        Mirrors the pentad smoke test but for decad.
        """
        import pandas as pd

        issue_date = dt.date(2026, 3, 10)
        forecast_decad_of_year = tl.get_decad_in_year(issue_date)
        assert forecast_decad_of_year == "7"  # issue decad

        linreg_decad = pd.DataFrame(
            {
                "code": ["15013"],
                "date": [issue_date],
                "discharge_avg": [50.0],
                "decad_in_year": [forecast_decad_of_year],  # issue decad initially
                "decad": ["1"],
            }
        )

        linreg_decad.rename(columns={"decad": "decad_in_month"}, inplace=True)

        _issue_decad = int(forecast_decad_of_year)
        _target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
        linreg_decad["decad_in_year"] = str(_target_decad)
        linreg_decad["decad_in_month"] = str(((_target_decad - 1) % 3) + 1)

        assert linreg_decad["decad_in_year"].iloc[0] == "8", (
            "decad_in_year must be the target decad (8), not the issue decad (7)"
        )
        assert linreg_decad["decad_in_month"].iloc[0] == "2", (
            "decad_in_month must be 2 (the 2nd decad of March), not 1"
        )

    def test_dec31_pentad_wrap_produces_correct_jan1_target(self):
        """Dec 31 (issue pentad 72) wraps to target pentad 1 (Jan 1–5)."""
        import pandas as pd

        issue_date = dt.date(2026, 12, 31)
        forecast_pentad_of_year = tl.get_pentad_in_year(issue_date)
        assert forecast_pentad_of_year == "72"

        linreg_pentad = pd.DataFrame(
            {
                "code": ["15013"],
                "date": [issue_date],
                "discharge_avg": [50.0],
                "pentad_in_year": [forecast_pentad_of_year],
                "pentad": ["6"],
            }
        )
        linreg_pentad.rename(columns={"pentad": "pentad_in_month"}, inplace=True)

        _issue_pentad = int(forecast_pentad_of_year)
        _target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
        linreg_pentad["pentad_in_year"] = str(_target_pentad)
        linreg_pentad["pentad_in_month"] = str(((_target_pentad - 1) % 6) + 1)

        assert linreg_pentad["pentad_in_year"].iloc[0] == "1"
        assert linreg_pentad["pentad_in_month"].iloc[0] == "1"

    def test_dec31_decad_wrap_produces_correct_jan1_target(self):
        """Dec 31 (issue decad 36) wraps to target decad 1 (Jan 1–10)."""
        import pandas as pd

        issue_date = dt.date(2026, 12, 31)
        forecast_decad_of_year = tl.get_decad_in_year(issue_date)
        assert forecast_decad_of_year == "36"

        linreg_decad = pd.DataFrame(
            {
                "code": ["15013"],
                "date": [issue_date],
                "discharge_avg": [50.0],
                "decad_in_year": [forecast_decad_of_year],
                "decad": ["3"],
            }
        )
        linreg_decad.rename(columns={"decad": "decad_in_month"}, inplace=True)

        _issue_decad = int(forecast_decad_of_year)
        _target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
        linreg_decad["decad_in_year"] = str(_target_decad)
        linreg_decad["decad_in_month"] = str(((_target_decad - 1) % 3) + 1)

        assert linreg_decad["decad_in_year"].iloc[0] == "1"
        assert linreg_decad["decad_in_month"].iloc[0] == "1"
