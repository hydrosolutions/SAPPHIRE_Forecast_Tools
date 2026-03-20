"""Tests for ML-006: shape mismatch fix in recalculate_nan_forecasts.

Tests the merge-based update_forecast() logic that replaced the
broadcast-assignment approach which crashed when forecast and hindcast
had different row counts for the same forecast_date.
"""

import pandas as pd


def update_forecast(forecast_code, hindcast_code):
    """Extracted from recalculate_nan_forecasts.recalculate_nan_forecasts().

    Mirror of the nested function — kept in sync manually.
    Updated for ML-013: returns (forecast_code, applied_rows) tuple.
    """
    value_cols = [col for col in forecast_code.columns if "Q" in col]
    forecast_code = forecast_code.copy()
    hindcast_code = hindcast_code.copy()

    forecast_dates_flag1 = forecast_code[forecast_code["flag"].isin([1, 2])][
        "forecast_date"
    ].unique()

    # Track which rows originally had flag in [1, 2]
    original_flag12_mask = forecast_code["flag"].isin([1, 2])

    for forecast_date in forecast_dates_flag1:
        fc_mask = forecast_code["forecast_date"] == forecast_date
        hc_mask = hindcast_code["forecast_date"] == forecast_date

        if not hc_mask.any():
            continue

        fc_rows = forecast_code.loc[fc_mask].copy()
        hc_rows = hindcast_code.loc[hc_mask][["date"] + value_cols + ["flag"]].copy()

        # Align on target date — left join so only matching rows update
        merged = fc_rows[["date"]].merge(hc_rows, on="date", how="left", suffixes=("", "_hc"))
        merged = merged.set_index(fc_rows.index)

        for col in value_cols:
            hc_col = col + "_hc" if col + "_hc" in merged.columns else col
            valid = merged[hc_col].notna()
            forecast_code.loc[
                fc_mask & valid.reindex(forecast_code.index, fill_value=False),
                col,
            ] = merged.loc[valid, hc_col].values

        flag_col = "flag_hc" if "flag_hc" in merged.columns else "flag"
        valid_flag = merged[flag_col].notna()
        forecast_code.loc[
            fc_mask & valid_flag.reindex(forecast_code.index, fill_value=False),
            "flag",
        ] = merged.loc[valid_flag, flag_col].values

    # Rows that were flag=1/2 and got updated (flag changed)
    changed_mask = original_flag12_mask & ~forecast_code["flag"].isin([1, 2])
    applied_rows = forecast_code.loc[changed_mask]

    return forecast_code, applied_rows


def _make_df(dates, forecast_date, code="15013", q50=None, flag=1):
    """Helper to build a minimal forecast/hindcast DataFrame."""
    n = len(dates)
    return pd.DataFrame(
        {
            "code": [code] * n,
            "date": pd.to_datetime(dates),
            "forecast_date": pd.to_datetime([forecast_date] * n),
            "Q50": q50 if q50 is not None else [float("nan")] * n,
            "flag": [flag] * n,
        }
    )


class TestUpdateForecastShapeMismatch:
    """ML-006: update_forecast must not crash when N != M."""

    def test_same_row_count_updates_correctly(self):
        """N == M: hindcast has same target dates as forecast."""
        fc = _make_df(["2026-03-01", "2026-03-02"], "2026-03-01", flag=1)
        hc = _make_df(
            ["2026-03-01", "2026-03-02"],
            "2026-03-01",
            q50=[10.0, 20.0],
            flag=3,
        )

        result, _ = update_forecast(fc, hc)
        assert result["Q50"].tolist() == [10.0, 20.0]
        assert result["flag"].tolist() == [3, 3]

    def test_forecast_has_more_rows_than_hindcast(self):
        """N > M: forecast has more target dates — extra rows unchanged."""
        fc = _make_df(["2026-03-01", "2026-03-02", "2026-03-03"], "2026-03-01", flag=1)
        hc = _make_df(["2026-03-01", "2026-03-02"], "2026-03-01", q50=[10.0, 20.0], flag=3)

        result, _ = update_forecast(fc, hc)
        assert result["Q50"].iloc[0] == 10.0
        assert result["Q50"].iloc[1] == 20.0
        assert pd.isna(result["Q50"].iloc[2])  # extra row unchanged
        assert result["flag"].iloc[2] == 1  # flag unchanged for unmatched row

    def test_hindcast_has_more_rows_than_forecast(self):
        """N < M: hindcast has more target dates — no crash, only matching updated."""
        fc = _make_df(["2026-03-01", "2026-03-02"], "2026-03-01", flag=1)
        hc = _make_df(
            ["2026-03-01", "2026-03-02", "2026-03-03"],
            "2026-03-01",
            q50=[10.0, 20.0, 30.0],
            flag=3,
        )

        result, _ = update_forecast(fc, hc)
        assert result["Q50"].tolist() == [10.0, 20.0]
        assert len(result) == 2  # no extra rows added

    def test_no_hindcast_for_date_leaves_unchanged(self):
        """hindcast_mask.any() is False — row unchanged, no crash."""
        fc = _make_df(["2026-03-01"], "2026-03-01", flag=1)
        hc = _make_df(["2026-03-01"], "2026-03-05", q50=[99.0], flag=3)  # different forecast_date

        result, _ = update_forecast(fc, hc)
        assert pd.isna(result["Q50"].iloc[0])  # unchanged
        assert result["flag"].iloc[0] == 1  # unchanged

    def test_non_flag1_rows_not_updated(self):
        """Only flag=1 or flag=2 rows are candidates for update."""
        fc = _make_df(["2026-03-01"], "2026-03-01", flag=0)
        hc = _make_df(["2026-03-01"], "2026-03-01", q50=[10.0], flag=3)

        result, _ = update_forecast(fc, hc)
        assert pd.isna(result["Q50"].iloc[0])  # flag=0 not updated

    def test_multiple_value_cols_all_updated(self):
        """value_cols contains Q05, Q50, Q95 — all updated where dates match."""
        fc = pd.DataFrame(
            {
                "code": ["15013", "15013"],
                "date": pd.to_datetime(["2026-03-01", "2026-03-02"]),
                "forecast_date": pd.to_datetime(["2026-03-01", "2026-03-01"]),
                "Q05": [float("nan"), float("nan")],
                "Q50": [float("nan"), float("nan")],
                "Q95": [float("nan"), float("nan")],
                "flag": [1, 1],
            }
        )
        hc = pd.DataFrame(
            {
                "code": ["15013", "15013"],
                "date": pd.to_datetime(["2026-03-01", "2026-03-02"]),
                "forecast_date": pd.to_datetime(["2026-03-01", "2026-03-01"]),
                "Q05": [1.0, 2.0],
                "Q50": [10.0, 20.0],
                "Q95": [100.0, 200.0],
                "flag": [3, 3],
            }
        )

        result, _ = update_forecast(fc, hc)
        assert result["Q05"].tolist() == [1.0, 2.0]
        assert result["Q50"].tolist() == [10.0, 20.0]
        assert result["Q95"].tolist() == [100.0, 200.0]

    def test_original_not_mutated(self):
        """update_forecast must not mutate the input DataFrames."""
        fc = _make_df(["2026-03-01"], "2026-03-01", flag=1)
        hc = _make_df(["2026-03-01"], "2026-03-01", q50=[10.0], flag=3)
        fc_orig = fc.copy()
        hc_orig = hc.copy()

        update_forecast(fc, hc)
        pd.testing.assert_frame_equal(fc, fc_orig)
        pd.testing.assert_frame_equal(hc, hc_orig)
