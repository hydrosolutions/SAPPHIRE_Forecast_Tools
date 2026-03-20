"""Unit tests for forecast_dashboard/src/db.py.

Tests pure helpers directly, and API-calling functions with mocked HTTP.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from src import db

# ── _convert_na_to_nan ─────────────────────────────────────────────────────


class TestConvertNaToNan:
    def test_converts_pd_na_to_np_nan(self):
        df = pd.DataFrame({"a": pd.array([1, pd.NA, 3], dtype="Int64")})
        result = db._convert_na_to_nan(df)
        assert np.isnan(result["a"].iloc[1])
        # Non-NA values preserved
        assert result["a"].iloc[0] == 1
        assert result["a"].iloc[2] == 3

    def test_preserves_regular_values(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        result = db._convert_na_to_nan(df)
        assert list(result["x"]) == [1.0, 2.0, 3.0]

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"a": pd.array([1, pd.NA], dtype="Int64")})
        db._convert_na_to_nan(df)
        assert pd.isna(df["a"].iloc[1])  # original still has pd.NA

    def test_handles_string_columns(self):
        df = pd.DataFrame({"s": pd.array(["a", pd.NA, "c"], dtype="string")})
        result = db._convert_na_to_nan(df)
        assert result["s"].iloc[0] == "a"

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.array([], dtype="Int64")})
        result = db._convert_na_to_nan(df)
        assert len(result) == 0


# ── _horizon_in_year_col ──────────────────────────────────────────────────


class TestHorizonInYearCol:
    def test_pentad(self):
        assert db._horizon_in_year_col("pentad") == "pentad_in_year"

    def test_decade(self):
        assert db._horizon_in_year_col("decade") == "decad_in_year"


# ── _resolve_station ──────────────────────────────────────────────────────


class TestResolveStation:
    def test_string_passthrough(self):
        assert db._resolve_station("15102") == "15102"

    def test_widget_with_value(self):
        widget = MagicMock()
        widget.value = "15102 - River Name"
        assert db._resolve_station(widget) == "15102"

    def test_widget_extracts_first_token(self):
        widget = MagicMock()
        widget.value = "99001 Test Station Extra Words"
        assert db._resolve_station(widget) == "99001"
