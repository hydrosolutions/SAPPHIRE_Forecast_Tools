"""
Tests for ensemble transform functions in Quantile_Mapping_OP.py.

Covers:
- transform_data_file_ensemble_member (DG ensemble CSV → long format)
- merge_ensemble_forecast (multiple CSVs → merged P+T DataFrame)
"""

import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client before importing
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

from Quantile_Mapping_OP import (
    merge_ensemble_forecast,
    transform_data_file_ensemble_member,
)

# =====================================================================
# transform_data_file_ensemble_member
# =====================================================================


class TestTransformDataFileEnsembleMember:
    """Tests for the DG ensemble CSV → long-format transform."""

    @pytest.fixture
    def dg_ensemble_dataframe(self):
        """Minimal DG ensemble DataFrame: 4 header rows + 2 data rows,
        2 bands."""
        rows = [
            ["P", "P", "P"],  # row 0: value_type in col 1
            ["unit", "mm", "mm"],
            ["h", "h", "h"],
            ["h", "h", "h"],
            ["01/01/2024", "5.0", "8.0"],
            ["02/01/2024", "6.0", "9.0"],
        ]
        return pd.DataFrame(rows, columns=["Unnamed: 0", "band_1000", "band_2000"])

    def test_basic_columns_and_row_count(self, dg_ensemble_dataframe):
        """Output has [date, <value_type>, code, name]; 2 dates x 2
        bands = 4 rows with correct values."""
        result = transform_data_file_ensemble_member(dg_ensemble_dataframe, "12345")
        assert list(result.columns) == ["date", "P", "code", "name"]
        assert len(result) == 4

        # Verify exact values
        band_1000_jan = result[
            (result["name"] == "band_1000") & (result["date"] == pd.Timestamp("2024-01-01"))
        ]
        assert band_1000_jan["P"].iloc[0] == 5.0

        band_2000_jan2 = result[
            (result["name"] == "band_2000") & (result["date"] == pd.Timestamp("2024-01-02"))
        ]
        assert band_2000_jan2["P"].iloc[0] == 9.0

    def test_value_type_read_from_row_0(self):
        """Column name comes from data_file.iloc[0].values[1]. 'T' in
        row 0 → output column named 'T'."""
        rows = [
            ["T", "T", "T"],
            ["unit", "K", "K"],
            ["h", "h", "h"],
            ["h", "h", "h"],
            ["01/01/2024", "280.0", "281.0"],
        ]
        df = pd.DataFrame(rows, columns=["Unnamed: 0", "band_1000", "band_2000"])
        result = transform_data_file_ensemble_member(df, "99999")
        assert "T" in result.columns
        assert "P" not in result.columns

    def test_first_4_rows_dropped(self, dg_ensemble_dataframe):
        """Header rows 0-3 removed; first data row date is a real
        date, not a header value."""
        result = transform_data_file_ensemble_member(dg_ensemble_dataframe, "12345")
        # All dates should be valid timestamps
        assert result["date"].dt.year.min() == 2024
        # No header strings should survive as dates
        assert len(result) == 4  # 2 dates x 2 bands

    def test_date_parsing_dayfirst(self):
        """'01/02/2024' parsed as Feb 1 (dayfirst=True)."""
        rows = [
            ["P", "P"],
            ["unit", "mm"],
            ["h", "h"],
            ["h", "h"],
            ["01/02/2024", "5.0"],
        ]
        df = pd.DataFrame(rows, columns=["Unnamed: 0", "band_1000"])
        result = transform_data_file_ensemble_member(df, "12345")
        assert result["date"].iloc[0] == pd.Timestamp("2024-02-01")

    def test_hru_code_assigned_to_all_rows(self, dg_ensemble_dataframe):
        """All rows get code == '12345'."""
        result = transform_data_file_ensemble_member(dg_ensemble_dataframe, "12345")
        assert list(result["code"].unique()) == ["12345"]

    def test_name_column_preserves_band_identifier(self, dg_ensemble_dataframe):
        """'name' column matches original column headers."""
        result = transform_data_file_ensemble_member(dg_ensemble_dataframe, "12345")
        assert set(result["name"].unique()) == {"band_1000", "band_2000"}

    def test_non_numeric_values_coerced_to_nan(self):
        """Cell 'abc' becomes NaN; numeric cell '5.0' remains 5.0."""
        rows = [
            ["P", "P", "P"],
            ["unit", "mm", "mm"],
            ["h", "h", "h"],
            ["h", "h", "h"],
            ["01/01/2024", "abc", "5.0"],
        ]
        df = pd.DataFrame(rows, columns=["Unnamed: 0", "band_1000", "band_2000"])
        result = transform_data_file_ensemble_member(df, "12345")
        band_1000 = result[result["name"] == "band_1000"]
        band_2000 = result[result["name"] == "band_2000"]
        assert pd.isna(band_1000["P"].iloc[0])
        assert band_2000["P"].iloc[0] == 5.0


# =====================================================================
# merge_ensemble_forecast
# =====================================================================


class TestMergeEnsembleForecast:
    """Tests using real CSV files in tmp_path.

    ensemble_csv_factory is a shared fixture, defined in conftest.py.
    """

    def test_empty_files_list_exits(self):
        """sys.exit(1) on empty files_downloaded."""
        with pytest.raises(SystemExit) as exc_info:
            merge_ensemble_forecast([])
        assert exc_info.value.code == 1

    def test_merge_roundtrip_with_known_values(self, ensemble_csv_factory):
        """P CSV (values [5.0, 6.0]) + T CSV (values [280.0, 281.0])
        merge correctly."""
        dates = ["01/01/2024", "02/01/2024"]
        p_file = ensemble_csv_factory("12345", 1, "tp", dates, [5.0, 6.0])
        t_file = ensemble_csv_factory("12345", 1, "2t", dates, [280.0, 281.0])
        result = merge_ensemble_forecast([p_file, t_file])
        assert "P" in result.columns
        assert "T" in result.columns
        assert len(result) == 2
        # Check exact values
        result_sorted = result.sort_values("date").reset_index(drop=True)
        assert result_sorted["P"].iloc[0] == 5.0
        assert result_sorted["P"].iloc[1] == 6.0
        assert result_sorted["T"].iloc[0] == 280.0
        assert result_sorted["T"].iloc[1] == 281.0

    def test_filename_parsing_hru_code(self, ensemble_csv_factory):
        """HRU12345 → code == '12345' in output."""
        p_file = ensemble_csv_factory("12345", 1, "tp", ["01/01/2024"], [5.0])
        t_file = ensemble_csv_factory("12345", 1, "2t", ["01/01/2024"], [280.0])
        result = merge_ensemble_forecast([p_file, t_file])
        assert result["code"].iloc[0] == "12345"

    def test_filename_parsing_ensemble_member(self, ensemble_csv_factory):
        """EM003 → ensemble_member == 3 in output."""
        p_file = ensemble_csv_factory("12345", 3, "tp", ["01/01/2024"], [5.0])
        t_file = ensemble_csv_factory("12345", 3, "2t", ["01/01/2024"], [280.0])
        result = merge_ensemble_forecast([p_file, t_file])
        assert result["ensemble_member"].iloc[0] == 3

    def test_no_precipitation_files_exits(self, ensemble_csv_factory):
        """Only _2t.csv files → sys.exit(1)."""
        t_file = ensemble_csv_factory("12345", 1, "2t", ["01/01/2024"], [280.0])
        with pytest.raises(SystemExit) as exc_info:
            merge_ensemble_forecast([t_file])
        assert exc_info.value.code == 1

    def test_no_temperature_files_exits(self, ensemble_csv_factory):
        """Only _tp.csv files → sys.exit(1)."""
        p_file = ensemble_csv_factory("12345", 1, "tp", ["01/01/2024"], [5.0])
        with pytest.raises(SystemExit) as exc_info:
            merge_ensemble_forecast([p_file])
        assert exc_info.value.code == 1

    def test_merge_produces_both_p_and_t_columns(self, ensemble_csv_factory):
        """Output has both 'P' and 'T' columns."""
        dates = ["01/01/2024"]
        p_file = ensemble_csv_factory("12345", 1, "tp", dates, [5.0])
        t_file = ensemble_csv_factory("12345", 1, "2t", dates, [280.0])
        result = merge_ensemble_forecast([p_file, t_file])
        assert "P" in result.columns
        assert "T" in result.columns

    def test_outer_merge_preserves_unmatched(self, ensemble_csv_factory):
        """P file has 3 dates, T file has 2 → 3 rows, T=NaN for
        the 3rd."""
        p_dates = ["01/01/2024", "02/01/2024", "03/01/2024"]
        t_dates = ["01/01/2024", "02/01/2024"]
        p_file = ensemble_csv_factory("12345", 1, "tp", p_dates, [5.0, 6.0, 7.0])
        t_file = ensemble_csv_factory("12345", 1, "2t", t_dates, [280.0, 281.0])
        result = merge_ensemble_forecast([p_file, t_file])
        assert len(result) == 3
        result_sorted = result.sort_values("date").reset_index(drop=True)
        assert pd.isna(result_sorted["T"].iloc[2])
        assert result_sorted["P"].iloc[2] == 7.0

    def test_multiple_ensemble_members_merged(self, ensemble_csv_factory):
        """EM001 (P=[5.0]) and EM002 (P=[8.0]) produce 2 rows with
        distinct values."""
        dates = ["01/01/2024"]
        p1 = ensemble_csv_factory("12345", 1, "tp", dates, [5.0])
        t1 = ensemble_csv_factory("12345", 1, "2t", dates, [280.0])
        p2 = ensemble_csv_factory("12345", 2, "tp", dates, [8.0])
        t2 = ensemble_csv_factory("12345", 2, "2t", dates, [281.0])
        result = merge_ensemble_forecast([p1, t1, p2, t2])
        assert len(result) == 2
        em1 = result[result["ensemble_member"] == 1]
        em2 = result[result["ensemble_member"] == 2]
        assert em1["P"].iloc[0] == 5.0
        assert em2["P"].iloc[0] == 8.0

    def test_unrecognized_variable_skipped(self, ensemble_csv_factory, tmp_path):
        """A file with unrecognized variable (e.g., _rh.csv) is
        skipped; valid P and T files still present in output."""
        dates = ["01/01/2024"]
        p_file = ensemble_csv_factory("12345", 1, "tp", dates, [5.0])
        t_file = ensemble_csv_factory("12345", 1, "2t", dates, [280.0])
        # Create an unrecognized variable file
        rh_path = tmp_path / "prefix_EM001_HRU12345_rh.csv"
        rows = [
            ["RH", "RH"],
            ["unit", "%"],
            ["h", "h"],
            ["h", "h"],
            ["01/01/2024", "50.0"],
        ]
        df = pd.DataFrame(rows, columns=["Unnamed: 0", "band_1000"])
        df.to_csv(rh_path, index=False)

        result = merge_ensemble_forecast([p_file, t_file, str(rh_path)])
        assert "P" in result.columns
        assert "T" in result.columns
        assert len(result) == 1
