"""LOCKED acceptance test for milestone M2 — the PRE-CUTOVER PARITY DIFF-TEST.

Before the legacy pentad/decad writer is retired, the relocated
``preprocessing_runoff`` producer must reproduce the old writer's envelope/norm
by the SAME climatology method (criterion 7: envelope/norm byte-identical to the
old output), while the actuals equal the independently-computed expected 3sf
values.

This is a shadow-compute comparison (independently-computed expected vs new
output), NOT a live dual-write.

DISCRIMINATING FIXTURE (reviewer blocker): the historical daily series carries a
DISTINCT value per (year, day-of-year), so the pooled per-period aggregate is
NOT a constant. A constant series collapses mean/min/max/q05-q95 to that same
constant under any grouping key, any boundary off-by-one, or any quantile
method, and therefore cannot catch a parity regression. Here the expected
envelope is recomputed by mirroring the legacy pandas pipeline exactly
(``forecast_library.write_pentad_hydrograph_data``: pentad_in_year assigned from
``date + 1 day`` via ``tag_library``, historical years only, groupby().agg with
mean/min/max/``quantile`` and a final ``.round(3)``). A wrong grouping key,
off-by-one calendar boundary, or non-linear quantile method all change these
values and fail the gate.

Fake station code '19999'; no real discharge values.
"""

import calendar
import datetime as dt
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import forecast_library as fl  # noqa: E402
import tag_library as tl  # noqa: E402

try:
    import sync_short_horizon_hydrograph as shh  # noqa: E402
except Exception as exc:  # pragma: no cover
    shh = None
    _IMPORT_ERROR = exc


CODE = "19999"
TARGET_YEAR = 2026
HISTORICAL_YEARS = (2024, 2025)
SDK_CURRENT_VALUE = 24.67  # -> round_3sf == 24.7, distinct from any envelope value
SDK_PREVIOUS_VALUE = 12.34  # -> round_3sf == 12.3
ALL_CLOSED = dt.date(2027, 1, 1)
ENVELOPE_KEYS = ("mean", "min", "max", "q05", "q25", "q75", "q95")


def _require_module():
    if shh is None:
        pytest.fail(f"sync_short_horizon_hydrograph not importable — M2 missing: {_IMPORT_ERROR!r}")


def _day_value(year, day_of_year):
    """A DISTINCT value per (year, day-of-year) so no period aggregate is flat."""
    return round(day_of_year + (year - HISTORICAL_YEARS[0]) * 0.5, 3)


def _varying_year_daily(year):
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            date = dt.date(year, month, day)
            rows.append(
                {
                    "code": CODE,
                    "date": date.isoformat(),
                    "discharge": _day_value(year, date.timetuple().tm_yday),
                }
            )
    return rows


def _daily_by_year():
    return {year: _varying_year_daily(year) for year in HISTORICAL_YEARS}


def _row(records, period):
    return next(r for r in records if int(r["horizon_in_year"]) == period)


def _legacy_expected_envelope(daily_by_year, get_in_year, period):
    """Independently recompute the legacy envelope for one period.

    For each historical year (year != TARGET_YEAR) independently, take the
    per-period mean of that year's daily discharge grouped by
    ``get_in_year(date)`` (one value per (year, period)), then
    ``groupby(period).agg(mean/min/max/quantile at 0.05/0.25/0.75/0.95)`` OVER
    THOSE PER-YEAR VALUES, then ``.round(3)``.
    """
    per_year_period_means = []
    for year, recs in daily_by_year.items():
        if year == TARGET_YEAR:
            continue
        year_rows = [
            {
                "discharge": fl.round_discharge_to_float(float(r["discharge"])),
                "period": int(get_in_year(pd.Timestamp(r["date"]))),
            }
            for r in recs
        ]
        if not year_rows:
            continue
        year_df = pd.DataFrame(year_rows)
        year_period_means = year_df.groupby("period")["discharge"].mean().reset_index()
        per_year_period_means.append(year_period_means)
    df = pd.concat(per_year_period_means, ignore_index=True)
    df = df.rename(columns={"discharge": "discharge_avg"})
    grouped = (
        df.groupby("period")
        .agg(
            mean=("discharge_avg", "mean"),
            min=("discharge_avg", "min"),
            max=("discharge_avg", "max"),
            q05=("discharge_avg", lambda x: x.quantile(0.05)),
            q25=("discharge_avg", lambda x: x.quantile(0.25)),
            q75=("discharge_avg", lambda x: x.quantile(0.75)),
            q95=("discharge_avg", lambda x: x.quantile(0.95)),
        )
        .round(3)
    )
    row = grouped.loc[period]
    return {key: float(row[key]) for key in ENVELOPE_KEYS}


def test_pentad_shadow_parity_envelope_method_and_actuals():
    _require_module()
    daily = _daily_by_year()
    records = shh.build_pentad_records(
        code=CODE,
        norms=[float(p) for p in range(1, 73)],
        daily_by_year=daily,
        sdk_current={p: SDK_CURRENT_VALUE for p in range(1, 73)},
        sdk_previous={p: SDK_PREVIOUS_VALUE for p in range(1, 73)},
        target_year=TARGET_YEAR,
        today=ALL_CLOSED,
    )
    assert len(records) == 72
    # Envelope byte-identical to the legacy pandas computation across interior
    # periods that span the year (discriminates grouping / boundary / quantile).
    for period in (3, 30, 55):
        expected = _legacy_expected_envelope(daily, tl.get_pentad_in_year, period)
        row = _row(records, period)
        assert expected["min"] != expected["max"], "fixture must vary within a period"
        for key in ENVELOPE_KEYS:
            assert row[key] == expected[key], (
                f"pentad {period} {key}={row[key]!r} != {expected[key]!r}"
            )
    # Actuals are sourced independently (SDK-first) and 3sf-rounded.
    for r in records:
        assert r["current"] == fl.round_3sf(SDK_CURRENT_VALUE)
        assert r["previous"] == fl.round_3sf(SDK_PREVIOUS_VALUE)
        assert r["current"] != r["mean"]  # envelope vs actual computed separately


def test_decad_shadow_parity_envelope_method_and_actuals():
    _require_module()
    daily = _daily_by_year()
    records = shh.build_decad_records(
        code=CODE,
        norms=[float(d) for d in range(1, 37)],
        daily_by_year=daily,
        sdk_current={d: SDK_CURRENT_VALUE for d in range(1, 37)},
        sdk_previous={d: SDK_PREVIOUS_VALUE for d in range(1, 37)},
        target_year=TARGET_YEAR,
        today=ALL_CLOSED,
    )
    assert len(records) == 36
    for period in (2, 15, 30):
        expected = _legacy_expected_envelope(daily, tl.get_decad_in_year, period)
        row = _row(records, period)
        assert expected["min"] != expected["max"], "fixture must vary within a period"
        for key in ENVELOPE_KEYS:
            assert row[key] == expected[key], (
                f"decad {period} {key}={row[key]!r} != {expected[key]!r}"
            )
    for r in records:
        assert r["current"] == fl.round_3sf(SDK_CURRENT_VALUE)
        assert r["previous"] == fl.round_3sf(SDK_PREVIOUS_VALUE)
