from __future__ import annotations

import math
from datetime import date

import pandas as pd
import pytest

from forecast_skill_eval import api_readers
from forecast_skill_eval.api_readers import (
    _decad_of_year,
    _normalize_lr_forecasts,
    _pentad_of_year,
    read_forecasts,
    read_hydrograph_norms,
    read_long_forecasts,
    read_lr_forecasts,
    read_runoff_observed,
    select_point_value,
    select_quantile_band,
)


def _rows(count: int, **base: object) -> list[dict[str, object]]:
    return [base | {"row_id": index} for index in range(count)]


def _lr_row(
    *,
    date: str,
    horizon_in_year: int,
    discharge: float | None = 10.0,
    **overrides: object,
) -> dict[str, object]:
    return {
        "horizon": "pentad",
        "code": "19999",
        "date": date,
        "horizon_in_year": horizon_in_year,
        "forecasted_discharge": discharge,
    } | overrides


def test_paginated_readers_fetch_all_pages(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=_rows(
            5,
            horizon="day",
            code="19999",
            model="model-a",
            horizon_in_year=1,
            forecasted_discharge=10.0,
        ),
        long_forecasts_rows=_rows(
            5,
            horizon="month",
            code="19999",
            model="model-a",
            q=10.0,
        ),
        hydrograph_rows=_rows(5, horizon="day", code="19999", discharge=10.0),
        runoff_rows=_rows(5, horizon="day", code="19999", discharge=10.0),
    )

    short_result = read_forecasts(
        client,
        horizon="day",
        code="19999",
        model="model-a",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-05",
        limit=2,
    )
    long_result = read_long_forecasts(
        client,
        horizon="month",
        code="19999",
        model="model-a",
        horizon_value=None,
        valid_from="2024-01-01",
        valid_to="2024-05-31",
        limit=2,
    )
    norms_result = read_hydrograph_norms(
        client,
        horizon="day",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-05",
        limit=2,
    )
    observed_result = read_runoff_observed(
        client,
        horizon="day",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-05",
        limit=2,
    )

    assert len(short_result.data) == 5
    assert len(long_result.data) == 5
    assert len(norms_result.data) == 5
    assert len(observed_result.data) == 5

    method_names = [name for name, _kwargs in client.calls]
    assert method_names.count("read_short_term_forecasts") == 3
    assert method_names.count("read_long_term_forecasts") == 3
    assert method_names.count("read_hydrograph") == 3
    assert method_names.count("read_runoff") == 3
    assert "read_forecasts" not in method_names
    assert "read_long_forecasts" not in method_names
    long_calls = [kwargs for name, kwargs in client.calls if name == "read_long_term_forecasts"]
    assert {call["horizon_type"] for call in long_calls} == {"month"}
    assert all("horizon" not in call for call in long_calls)


def test_lr_forecast_reader_paginates_and_normalizes_rows(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=_rows(
            5,
            horizon="pentad",
            code="19999",
            date="2024-01-05",
            horizon_in_year=1,
            forecasted_discharge=10.0,
            flag="future-proof",
        ),
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=2,
    )

    assert len(result.data) == 5
    assert result.data["model"].tolist() == ["LR"] * 5
    assert "model_type" not in result.data.columns
    assert result.data["horizon_in_year"].tolist() == [1] * 5
    assert result.data["date"].tolist() == ["2024-01-05"] * 5
    assert result.data["forecasted_discharge"].tolist() == [10.0] * 5
    assert result.data["flag"].tolist() == ["future-proof"] * 5
    assert result.data["point_value"].tolist() == [10.0] * 5
    assert result.data["point_value_note"].tolist() == [""] * 5

    lr_calls = [kwargs for name, kwargs in client.calls if name == "read_lr_forecasts"]
    assert [call["skip"] for call in lr_calls] == [0, 2, 4]
    assert [call["limit"] for call in lr_calls] == [2, 2, 2]


def test_lr_forecast_reader_drops_horizon_zero_sentinels(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-01-05", horizon_in_year=0, discharge=999.0),
            _lr_row(date="2024-01-10", horizon_in_year=2, discharge=12.5),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=10,
    )

    assert len(result.data) == 1
    assert result.dropped_sentinels == 1
    assert result.data.iloc[0]["horizon_in_year"] == 2
    assert result.data.iloc[0]["point_value"] == 12.5
    assert result.data.iloc[0]["point_value_note"] == ""


def test_lr_forecast_reader_notes_null_forecasted_discharge(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-01-05", horizon_in_year=1, discharge=None),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=10,
    )

    assert math.isnan(result.data.iloc[0]["point_value"])
    assert "forecasted_discharge" in result.data.iloc[0]["point_value_note"]


def test_lr_target_synthesis_uses_issue_date_only_for_year(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-12-31", horizon_in_year=1, row_id="boundary"),
            _lr_row(date="2024-03-25", horizon_in_year=18, row_id="mid-year"),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-12-31",
        limit=10,
    )

    rows = result.data.set_index("row_id")
    assert pd.Timestamp(rows.loc["boundary", "target"]).year == 2025
    assert rows.loc["boundary", "horizon_in_year"] == 1
    assert pd.Timestamp(rows.loc["mid-year", "target"]).year == 2024
    assert rows.loc["mid-year", "horizon_in_year"] == 18
    assert "period_key" not in result.data.columns


def test_read_forecasts_output_remains_unchanged(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "pentad",
                "code": "19999",
                "model": "ML",
                "date": "2024-01-04",
                "target": "2024-01-05",
                "horizon_in_year": 1,
                "forecasted_discharge": 11.0,
            },
        ],
    )

    result = read_forecasts(
        client,
        horizon="pentad",
        code="19999",
        model="ML",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    row = result.data.iloc[0]
    assert row["model"] == "ML"
    assert row["date"] == "2024-01-04"
    assert row["target"] == "2024-01-05"
    assert row["point_value"] == 11.0
    assert row["point_value_note"] == ""
    assert "read_lr_forecasts" not in [name for name, _kwargs in client.calls]


def test_short_forecast_reader_drops_horizon_zero_sentinels(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "day",
                "code": "19999",
                "model": "model-a",
                "horizon_in_year": 0,
                "forecasted_discharge": 999.0,
            },
            {
                "horizon": "day",
                "code": "19999",
                "model": "model-a",
                "horizon_in_year": 3,
                "forecasted_discharge": 12.5,
            },
        ],
    )

    result = read_forecasts(
        client,
        horizon="day",
        code="19999",
        model="model-a",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    assert len(result.data) == 1
    assert result.dropped_sentinels == 1
    assert result.data.iloc[0]["horizon_in_year"] == 3
    assert result.data.iloc[0]["point_value"] == 12.5
    assert result.data.iloc[0]["point_value_note"] == ""


def test_point_value_selection_is_deterministic_for_short_and_long() -> None:
    assert select_point_value({"forecasted_discharge": 8.25}, forecast_type="short") == (
        8.25,
        "",
    )
    assert select_point_value({"q": 1.0, "q50": 2.0, "q_loc": 3.0}, forecast_type="long") == (
        1.0,
        "",
    )
    assert select_point_value({"q": None, "q50": 2.0, "q_loc": 3.0}, forecast_type="long") == (
        2.0,
        "",
    )
    assert select_point_value({"q": None, "q50": None, "q_loc": 3.0}, forecast_type="long") == (
        3.0,
        "",
    )

    value, note = select_point_value(
        {"q": None, "q50": None, "q_loc": None},
        forecast_type="long",
    )
    assert math.isnan(value)
    assert "q, q50, or q_loc" in note


def test_long_forecast_reader_adds_point_values_and_missing_note(fake_client_factory) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            {"horizon": "month", "code": "19999", "model": "model-a", "q": 1.0, "q50": 2.0},
            {"horizon": "month", "code": "19999", "model": "model-a", "q": None, "q50": 2.0},
            {"horizon": "month", "code": "19999", "model": "model-a", "q_loc": 3.0},
            {"horizon": "month", "code": "19999", "model": "model-a"},
        ],
    )

    result = read_long_forecasts(
        client,
        horizon="month",
        code="19999",
        model="model-a",
        horizon_value=None,
        valid_from="2024-01-01",
        valid_to="2024-12-31",
        limit=10,
    )

    assert result.data["point_value"].iloc[:3].tolist() == [1.0, 2.0, 3.0]
    assert math.isnan(result.data["point_value"].iloc[3])
    assert "q, q50, or q_loc" in result.data["point_value_note"].iloc[3]


def test_api_client_dependency_gate_uses_standard_skip_message() -> None:
    if not api_readers.SAPPHIRE_API_AVAILABLE:
        pytest.skip("sapphire-api-client not installed")

    assert api_readers.SapphirePostprocessingClient is not None
    assert api_readers.SapphirePreprocessingClient is not None


# ---------------------------------------------------------------------------
# Quantile-band ingestion tests (P1 — additive plumbing)
# ---------------------------------------------------------------------------


def test_select_quantile_band_long_row_returns_seven_nodes_and_grid_id() -> None:
    row = {
        "q05": 1.0,
        "q10": 2.0,
        "q25": 4.0,
        "q50": 5.0,
        "q75": 7.0,
        "q90": 8.0,
        "q95": 9.0,
        "q": 5.0,  # point-value column — must not pollute the band
    }
    band, note, grid_id = select_quantile_band(row, "long")

    assert note == ""
    assert grid_id == "long7"
    assert set(band.keys()) == {0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95}
    assert band[0.05] == pytest.approx(1.0)
    assert band[0.50] == pytest.approx(5.0)
    assert band[0.95] == pytest.approx(9.0)


def test_select_quantile_band_short_row_returns_five_nodes_with_q50_from_discharge() -> None:
    row = {
        "q05": 1.0,
        "q25": 3.0,
        "forecasted_discharge": 5.0,  # used as q50
        "q75": 7.0,
        "q95": 9.0,
    }
    band, note, grid_id = select_quantile_band(row, "short")

    assert note == ""
    assert grid_id == "short5"
    assert set(band.keys()) == {0.05, 0.25, 0.50, 0.75, 0.95}
    assert band[0.50] == pytest.approx(5.0)
    # q10 and q90 are absent from QUANTILE_SOURCE_MAP["short"]
    assert 0.10 not in band
    assert 0.90 not in band


def test_select_quantile_band_lr_shaped_row_returns_empty_band() -> None:
    # LR row: only forecasted_discharge (mapped to q50) — no q05/q25/q75/q95.
    # Even if a stray 'q' column is present it must be ignored.
    row = {
        "forecasted_discharge": 10.0,
        "q_mean": 10.0,
        "q_std_sigma": 2.0,
        "q": 10.0,  # stray point-value column — must not be used
    }
    band, note, grid_id = select_quantile_band(row, "short")

    assert band == {}
    assert note == "no_quantile_band"
    assert grid_id == ""


def test_select_quantile_band_nan_node_is_dropped() -> None:
    row = {
        "q05": float("nan"),  # absent → dropped
        "q25": 3.0,
        "forecasted_discharge": 5.0,
        "q75": 7.0,
        "q95": 9.0,
    }
    band, note, grid_id = select_quantile_band(row, "short")

    # q05 dropped; 4 remaining nodes → valid band
    assert 0.05 not in band
    assert note == ""
    assert grid_id == "short5"
    assert len(band) == 4


def test_select_quantile_band_fewer_than_two_finite_nodes_returns_empty() -> None:
    # Only forecasted_discharge present → 1 node → band-less
    row = {"forecasted_discharge": 5.0}
    band, note, grid_id = select_quantile_band(row, "short")

    assert band == {}
    assert note == "no_quantile_band"
    assert grid_id == ""


def test_read_forecasts_adds_quantile_band_and_preserves_point_value(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "pentad",
                "code": "19999",
                "model": "TFT",
                "date": "2024-01-04",
                "target": "2024-01-05",
                "horizon_in_year": 1,
                "forecasted_discharge": 11.0,
                "q05": 2.0,
                "q25": 5.0,
                "q75": 17.0,
                "q95": 22.0,
            },
        ],
    )

    result = read_forecasts(
        client,
        horizon="pentad",
        code="19999",
        model="TFT",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    row = result.data.iloc[0]
    # Point-value path byte-for-byte unchanged
    assert row["point_value"] == pytest.approx(11.0)
    assert row["point_value_note"] == ""
    # Quantile band columns present
    assert row["fc_grid_id"] == "short5"
    assert isinstance(row["quantiles"], dict)
    assert row["quantiles"][0.05] == pytest.approx(2.0)
    assert row["quantiles"][0.50] == pytest.approx(11.0)  # from forecasted_discharge
    assert row["quantiles_note"] == ""


def test_read_long_forecasts_adds_full_seven_node_quantile_band(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            {
                "horizon": "month",
                "code": "19999",
                "model": "model-a",
                "q": 5.0,
                "q05": 1.0,
                "q10": 2.0,
                "q25": 3.0,
                "q50": 5.0,
                "q75": 7.0,
                "q90": 8.0,
                "q95": 9.0,
            },
        ],
    )

    result = read_long_forecasts(
        client,
        horizon="month",
        code="19999",
        model="model-a",
        horizon_value=None,
        valid_from="2024-01-01",
        valid_to="2024-12-31",
        limit=10,
    )

    row = result.data.iloc[0]
    # Point-value path unchanged (reads from 'q' column)
    assert row["point_value"] == pytest.approx(5.0)
    assert row["point_value_note"] == ""
    # Quantile band: 7 nodes
    assert row["fc_grid_id"] == "long7"
    assert row["quantiles_note"] == ""
    assert len(row["quantiles"]) == 7
    assert row["quantiles"][0.10] == pytest.approx(2.0)
    assert row["quantiles"][0.90] == pytest.approx(8.0)


def test_read_lr_forecasts_adds_empty_quantile_band(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-01-05", horizon_in_year=1, discharge=10.0),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=10,
    )

    row = result.data.iloc[0]
    # Point-value unchanged
    assert row["point_value"] == pytest.approx(10.0)
    assert row["point_value_note"] == ""
    # Quantile band is empty (LR has no q05/q25/q75/q95)
    assert row["quantiles"] == {}
    assert row["quantiles_note"] == "no_quantile_band"
    assert row["fc_grid_id"] == ""


def test_add_quantile_band_empty_frame_adds_typed_columns(fake_client_factory) -> None:
    client = fake_client_factory(forecasts_rows=[])

    result = read_forecasts(
        client,
        horizon="pentad",
        code="19999",
        model="TFT",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    assert result.data.empty
    assert "quantiles" in result.data.columns
    assert "quantiles_note" in result.data.columns
    assert "fc_grid_id" in result.data.columns
    assert "point_value" in result.data.columns  # point path still present


# ---------------------------------------------------------------------------
# LR issue-indexing repair-on-read tests (optional, default off)
# ---------------------------------------------------------------------------


def test_period_helpers_match_tag_library_convention() -> None:
    assert _pentad_of_year(date(2022, 1, 1)) == 1
    assert _decad_of_year(date(2022, 1, 1)) == 1
    assert _pentad_of_year(date(2022, 5, 15)) == 27
    assert _decad_of_year(date(2022, 5, 15)) == 14
    assert _pentad_of_year(date(2022, 12, 31)) == 72
    assert _decad_of_year(date(2022, 12, 31)) == 36


def test_lr_repair_remaps_issue_indexed_pentad_row() -> None:
    # Issue date 2023-03-21 is pentad 17; stored horizon_in_year 17 == issue
    # period → issue-indexed → remap to target pentad 18.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-03-21",
                "horizon_in_year": 17,
                "horizon_value": 5,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=True)

    row = repaired.iloc[0]
    assert row["horizon_in_year"] == 18
    assert row["horizon_value"] == ((18 - 1) % 6) + 1 == 6
    # Non-wrap remap leaves target's year as issue-year (issue date + 1 day).
    assert pd.to_datetime(row["target"]).year == 2023


def test_lr_repair_leaves_already_target_indexed_row_untouched() -> None:
    # Issue date 2023-03-21 is pentad 17; stored horizon_in_year 18 != 17 →
    # already target-indexed → unchanged.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-03-21",
                "horizon_in_year": 18,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=True)

    assert repaired.iloc[0]["horizon_in_year"] == 18


def test_lr_repair_wraps_pentad_72_and_advances_target_year() -> None:
    # Issue date 2023-12-27 is pentad 72; issue-indexed → wrap to pentad 1 with
    # the target year advanced to the following year.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-12-27",
                "horizon_in_year": 72,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=True)

    row = repaired.iloc[0]
    assert row["horizon_in_year"] == 1
    assert pd.to_datetime(row["target"]).year == 2024


def test_lr_repair_ignores_unparseable_issue_date() -> None:
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "not-a-date",
                "horizon_in_year": 17,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=True)

    assert repaired.iloc[0]["horizon_in_year"] == 17


def test_lr_repair_remaps_issue_indexed_decade_row() -> None:
    # Issue date 2023-02-15 is decade 5; horizon_in_year 5 == issue → remap to 6.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-02-15",
                "horizon_in_year": 5,
                "horizon_value": 2,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="decade", repair_issue_indexing=True)

    row = repaired.iloc[0]
    assert row["horizon_in_year"] == 6
    assert row["horizon_value"] == ((6 - 1) % 3) + 1 == 3
    assert pd.to_datetime(row["target"]).year == 2023


def test_lr_repair_wraps_decade_36_and_advances_target_year() -> None:
    # Issue date 2023-12-25 is decade 36; issue-indexed → wrap to decade 1 with
    # the target year advanced to the following year.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-12-25",
                "horizon_in_year": 36,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="decade", repair_issue_indexing=True)

    row = repaired.iloc[0]
    assert row["horizon_in_year"] == 1
    assert pd.to_datetime(row["target"]).year == 2024


def test_lr_repair_off_is_byte_identical_on_target_indexed_row() -> None:
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-03-21",
                "horizon_in_year": 18,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    on = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=True)
    off = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=False)

    assert on["horizon_in_year"].tolist() == off["horizon_in_year"].tolist()


def test_lr_repair_off_leaves_issue_indexed_row_untouched() -> None:
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-03-21",
                "horizon_in_year": 17,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    # Repair OFF, and the pre-existing default call (no horizon / no flag), both
    # leave the stored (issue) index untouched.
    off = _normalize_lr_forecasts(df, horizon="pentad", repair_issue_indexing=False)
    default = _normalize_lr_forecasts(df)

    assert off.iloc[0]["horizon_in_year"] == 17
    assert default.iloc[0]["horizon_in_year"] == 17


def test_lr_repair_excludes_day_horizon() -> None:
    # A day-horizon call with an issue-indexed-looking row must NOT be remapped;
    # the repair is pentad/decade only.
    df = pd.DataFrame(
        [
            {
                "code": "19999",
                "date": "2023-03-21",
                "horizon_in_year": 17,
                "forecasted_discharge": 10.0,
            }
        ]
    )

    repaired = _normalize_lr_forecasts(df, horizon="day", repair_issue_indexing=True)

    assert repaired.iloc[0]["horizon_in_year"] == 17


def test_read_lr_forecasts_repair_on_shifts_period_via_reader(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2023-03-21", horizon_in_year=17, discharge=10.0),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2023-01-01",
        end_date="2023-12-31",
        limit=10,
        repair_issue_indexing=True,
    )

    assert result.data.iloc[0]["horizon_in_year"] == 18
