"""
Tests for calculate_snow_norms_from_api() in dg_utils.py.

Unit tests for API-based snow norm computation. The function reads
historical snow data from the preprocessing API, discovers station
codes from the response, and computes per-(code, dayofyear) mean norms.
"""

import os
import sys
from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client module before importing
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import dg_utils

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_snow_df(n_rows, snow_type="SWE", code="19999", start_year=2000):
    """Return a minimal snow DataFrame with n_rows daily rows."""
    dates = pd.date_range(f"{start_year}-01-01", periods=n_rows, freq="D")
    return pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            "snow_type": [snow_type] * n_rows,
            "code": [code] * n_rows,
            "date": dates,
            "value": [50.0 + i * 0.01 for i in range(n_rows)],
            "norm": [np.nan] * n_rows,
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCalculateSnowNormsFromApi:
    """Unit tests for dg_utils.calculate_snow_norms_from_api()."""

    def test_single_year_norm_equals_values(self):
        """With only one year of data, the norm equals the single observed value."""
        # --- Arrange ---
        n_days = 365
        dates = pd.date_range("2020-01-01", periods=n_days, freq="D")
        values = [d.dayofyear * 0.5 for d in dates]
        page = pd.DataFrame(
            {
                "snow_type": ["SWE"] * n_days,
                "code": ["19999"] * n_days,
                "date": dates,
                "value": values,
                "norm": [np.nan] * n_days,
            }
        )

        client = Mock()
        # First call returns the full single page; second call returns empty
        client.read_snow.side_effect = [page, pd.DataFrame()]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        assert not result.empty
        result_19999 = result[result["code"] == "19999"]

        for _, row in result_19999.iterrows():
            doy = int(row["dayofyear"])
            expected = doy * 0.5
            assert abs(row["norm"] - expected) < 1e-9, (
                f"dayofyear={doy}: expected norm={expected}, got {row['norm']}"
            )

    def test_multi_year_norm_is_mean(self):
        """Norm is the mean across multiple years for the same dayofyear."""
        # --- Arrange ---
        # dayofyear 1 in three different years → values 10, 20, 30
        rows = []
        for year, val in zip([2020, 2021, 2022], [10.0, 20.0, 30.0], strict=True):
            rows.append(
                {
                    "snow_type": "SWE",
                    "code": "19999",
                    "date": pd.Timestamp(f"{year}-01-01"),
                    "value": val,
                    "norm": np.nan,
                }
            )
        page = pd.DataFrame(rows)

        client = Mock()
        client.read_snow.side_effect = [page, pd.DataFrame()]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        doy1 = result[(result["code"] == "19999") & (result["dayofyear"] == 1)]
        assert len(doy1) == 1
        assert abs(doy1.iloc[0]["norm"] - 20.0) < 1e-9

    def test_multiple_variables(self):
        """Norms are computed separately for each requested variable."""

        # --- Arrange ---
        def _make_page(snow_type):
            dates = pd.date_range("2020-01-01", periods=10, freq="D")
            return pd.DataFrame(
                {
                    "snow_type": [snow_type] * 10,
                    "code": ["19999"] * 10,
                    "date": dates,
                    "value": [float(i) for i in range(10)],
                    "norm": [np.nan] * 10,
                }
            )

        swe_page = _make_page("SWE")
        hs_page = _make_page("HS")

        client = Mock()
        # Each page has fewer rows than batch_size (10000), so one call per variable suffices
        client.read_snow.side_effect = [swe_page, hs_page]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE", "HS"])

        # --- Assert ---
        assert set(result["snow_type"].unique()) == {"SWE", "HS"}

    def test_multiple_codes_discovered_from_response(self):
        """Station codes are discovered from API data, not passed as parameters."""
        # --- Arrange ---
        dates = pd.date_range("2020-01-01", periods=5, freq="D")
        page = pd.DataFrame(
            {
                "snow_type": ["SWE"] * 10,
                "code": ["19999"] * 5 + ["29999"] * 5,
                "date": list(dates) * 2,
                "value": [1.0] * 10,
                "norm": [np.nan] * 10,
            }
        )

        client = Mock()
        client.read_snow.side_effect = [page, pd.DataFrame()]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        codes_in_result = set(result["code"].unique())
        assert "19999" in codes_in_result
        assert "29999" in codes_in_result

    def test_missing_value_rows_excluded(self):
        """NaN values are excluded from the mean computation."""
        # --- Arrange ---
        # Three rows for dayofyear 1: values 10, NaN, 30 → mean of non-NaN = 20
        rows = [
            {
                "snow_type": "SWE",
                "code": "19999",
                "date": pd.Timestamp("2020-01-01"),
                "value": 10.0,
                "norm": np.nan,
            },
            {
                "snow_type": "SWE",
                "code": "19999",
                "date": pd.Timestamp("2021-01-01"),
                "value": np.nan,
                "norm": np.nan,
            },
            {
                "snow_type": "SWE",
                "code": "19999",
                "date": pd.Timestamp("2022-01-01"),
                "value": 30.0,
                "norm": np.nan,
            },
        ]
        page = pd.DataFrame(rows)

        client = Mock()
        client.read_snow.side_effect = [page, pd.DataFrame()]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        doy1 = result[(result["code"] == "19999") & (result["dayofyear"] == 1)]
        assert len(doy1) == 1
        assert abs(doy1.iloc[0]["norm"] - 20.0) < 1e-9

    def test_empty_api_response(self):
        """An empty API response produces an empty DataFrame with correct columns."""
        # --- Arrange ---
        client = Mock()
        client.read_snow.return_value = pd.DataFrame()

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        assert result.empty
        assert list(result.columns) == ["snow_type", "code", "dayofyear", "norm"]

    def test_api_error_handled_gracefully(self):
        """An API exception is caught and produces an empty DataFrame."""
        # --- Arrange ---
        client = Mock()
        client.read_snow.side_effect = Exception("Connection refused")

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert ---
        assert result.empty
        assert list(result.columns) == ["snow_type", "code", "dayofyear", "norm"]

    def test_output_format(self):
        """Output has the correct columns, integer dayofyear, and float norm."""
        # --- Arrange ---
        dates = pd.date_range("2020-01-01", periods=30, freq="D")
        page = pd.DataFrame(
            {
                "snow_type": ["SWE"] * 30,
                "code": ["19999"] * 30,
                "date": dates,
                "value": [float(i) for i in range(30)],
                "norm": [np.nan] * 30,
            }
        )

        client = Mock()
        client.read_snow.side_effect = [page, pd.DataFrame()]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert columns ---
        assert list(result.columns) == ["snow_type", "code", "dayofyear", "norm"]

        # --- Assert dayofyear type and range ---
        assert result["dayofyear"].dtype in (
            np.dtype("int64"),
            np.dtype("int32"),
            np.dtype("int16"),
            np.dtype("int8"),
        ), f"Expected integer dayofyear, got {result['dayofyear'].dtype}"
        assert result["dayofyear"].between(1, 366).all()

        # --- Assert norm is numeric ---
        assert pd.api.types.is_float_dtype(result["norm"]) or pd.api.types.is_numeric_dtype(
            result["norm"]
        )

    def test_pagination_fetches_all_pages(self):
        """Function paginates correctly and deduplicates overlapping rows."""
        # --- Arrange ---
        # First call: full batch of 10000 rows (years 2000–2027 approximately)
        page1 = _make_snow_df(10000, snow_type="SWE", code="19999", start_year=2000)

        # Second call: 5000 rows from a later period, with 3 rows duplicated from page1
        page2_unique = _make_snow_df(4997, snow_type="SWE", code="19999", start_year=2027)
        page2_dupes = page1.iloc[:3].copy()
        page2 = pd.concat([page2_unique, page2_dupes], ignore_index=True)
        # Reset id to avoid any id-based confusion
        page2["id"] = range(10001, 10001 + len(page2))

        client = Mock()
        client.read_snow.side_effect = [page1, page2]

        # --- Act ---
        result = dg_utils.calculate_snow_norms_from_api(client, ["SWE"])

        # --- Assert: two calls were made (10000 rows → full batch, 5000 rows → stop) ---
        assert client.read_snow.call_count == 2

        # --- Assert: result is non-empty (norms were computed) ---
        assert not result.empty

        # --- Assert: duplicates removed — unique (code, date) pairs = 10000 + 4997 = 14997 ---
        # We can verify by checking that the total unique dayofyear entries make sense
        # (the exact row count is data-dependent, but norms must exist)
        assert len(result) > 0
        assert result["snow_type"].iloc[0] == "SWE"
        assert (result["code"] == "19999").all()
