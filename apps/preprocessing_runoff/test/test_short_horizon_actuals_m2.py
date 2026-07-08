"""LOCKED acceptance tests for milestone M2 — pentad & decad ACTUALS produced
by ``preprocessing_runoff`` (SDK-first WDFA/WDDCA + WDDA fallback, full-row
write, issue-date key).

These tests pin the M2 *contract* (behaviour + public seam), not an internal
implementation. They MUST fail until ``preprocessing_runoff`` owns the
pentad/decad row and satisfy the criteria below, and must not be weakened.

Required public seam (mirrors the existing long-horizon builder
``sync_long_horizon_hydrograph.build_monthly_records``):

    module   apps/preprocessing_runoff/sync_short_horizon_hydrograph.py
    function build_pentad_records(code, *, norms, daily_by_year,
                                  sdk_current, sdk_previous,
                                  target_year, today) -> list[dict]   (72 rows)
    function build_decad_records(...) -> list[dict]                    (36 rows)

  Inputs (all injected so the build is offline/deterministic — S27):
    norms         sequence indexed by period_in_year (norms[p-1])
    daily_by_year dict[int year -> iterable of {"date": iso, "discharge": float}]
                  daily WDDA averages; used BOTH for the climatology envelope
                  (years != target_year) AND for the WDDA fallback of
                  current (target_year) / previous (target_year-1)
    sdk_current   dict[int period_in_year -> WDFA/WDDCA value | None]  (target_year)
    sdk_previous  dict[int period_in_year -> WDFA/WDDCA value | None]  (target_year-1)
    target_year   the "current" year; previous year is target_year-1
    today         completeness reference; a period whose calendar window is not
                  fully in the past on this date is IN PROGRESS

  Each returned record is a COMPLETE hydrographs row with keys:
    horizon_type, code, date, horizon_value, horizon_in_year, day_of_year,
    norm, current, previous, mean, min, max, q05, q25, q75, q95

Rules under test:
  * current = round_3sf(SDK value) for the most-recent year; previous = same
    for the prior year — BOTH from source (SDK-first, WDDA fallback); previous
    is NOT read from stored rows and NOT gated on the M4 backfill.
  * WDDA fallback only when >= 80% of the period's calendar days are present,
    else the actual is null.
  * No finalized actual for an in-progress period.
  * Row keyed on issue_date = last day of the previous period; exactly one row
    per (horizon_type, code, issue_date); no duplicate / orphan.
  * Envelope (mean/min/max/q05-q95) + norm always populated by the relocated
    method; a null actual never nulls the envelope/norm.
  * Stored actual == its bulletin display via the shared 3sf contract.

Fake station code '19999'; no real discharge values.
"""

import calendar
import datetime as dt
import json
import os
import sys

import pandas as pd
import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import forecast_library as fl  # noqa: E402  (round_3sf + issue-date helpers)
import tag_library as tl  # noqa: E402  (period-in-year helpers, legacy env mirror)

try:
    import sync_short_horizon_hydrograph as shh  # noqa: E402
except Exception as exc:  # pragma: no cover - drives a clean FAIL pre-M2
    shh = None
    _IMPORT_ERROR = exc


CODE = "19999"
TARGET_YEAR = 2026
PREVIOUS_YEAR = 2025
ALL_CLOSED = dt.date(2027, 1, 1)  # every 2026 period is in the past

PENTAD_NORMS = [float(p) for p in range(1, 73)]
DECAD_NORMS = [float(d) for d in range(1, 37)]


def _require_module():
    if shh is None:
        pytest.fail(
            f"sync_short_horizon_hydrograph not importable — M2 writer missing: {_IMPORT_ERROR!r}"
        )


def _full_year_daily(year, value):
    """Every calendar day of ``year`` carries ``value`` (constant climatology)."""
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            rows.append(
                {"code": CODE, "date": dt.date(year, month, day).isoformat(), "discharge": value}
            )
    return rows


def _daily_for_dates(year, month, days, value):
    return [
        {"code": CODE, "date": dt.date(year, month, d).isoformat(), "discharge": value}
        for d in days
    ]


def _daily_for_dates_with_values(year, month, day_value_pairs):
    """Like ``_daily_for_dates`` but each date carries its OWN discharge value,
    including ``None`` for a dated-but-non-numeric row (present row, no reading).
    """
    return [
        {"code": CODE, "date": dt.date(year, month, d).isoformat(), "discharge": value}
        for d, value in day_value_pairs
    ]


def _varying_year_daily(year, base):
    """Every calendar day carries a DISTINCT value (day-of-year + year offset).

    A flat/constant series makes the envelope non-discriminating (mean == min ==
    max == every quantile), so the exact-envelope assertion below uses this.
    """
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            date = dt.date(year, month, day)
            rows.append(
                {
                    "code": CODE,
                    "date": date.isoformat(),
                    "discharge": round(date.timetuple().tm_yday + base, 3),
                }
            )
    return rows


def _legacy_expected_envelope(daily_by_year, get_in_year, period):
    """Independently recompute the legacy envelope for one period.

    For each historical year (year != TARGET_YEAR) independently, take the
    per-period mean of that year's daily discharge grouped by
    ``get_in_year(date)`` (one value per (year, period)), then
    ``groupby(period).agg(mean/min/max/quantile at 0.05/0.25/0.75/0.95)`` OVER
    THOSE PER-YEAR VALUES, then ``.round(3)``."""
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
    return {key: float(row[key]) for key in ("mean", "min", "max", "q05", "q25", "q75", "q95")}


def _build_pentads(**over):
    kwargs = dict(
        code=CODE,
        norms=PENTAD_NORMS,
        daily_by_year={PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0)},
        sdk_current={},
        sdk_previous={},
        target_year=TARGET_YEAR,
        today=ALL_CLOSED,
    )
    kwargs.update(over)
    return shh.build_pentad_records(**kwargs)


def _build_decads(**over):
    kwargs = dict(
        code=CODE,
        norms=DECAD_NORMS,
        daily_by_year={PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0)},
        sdk_current={},
        sdk_previous={},
        target_year=TARGET_YEAR,
        today=ALL_CLOSED,
    )
    kwargs.update(over)
    return shh.build_decad_records(**kwargs)


def _row(records, period):
    return next(r for r in records if int(r["horizon_in_year"]) == period)


ENVELOPE_KEYS = ("mean", "min", "max", "q05", "q25", "q75", "q95")
ROW_KEYS = (
    "horizon_type",
    "code",
    "date",
    "horizon_value",
    "horizon_in_year",
    "norm",
    "current",
    "previous",
    *ENVELOPE_KEYS,
)


# --------------------------------------------------------------------------
# Criterion 1 — SDK-first current & previous, both from source, 3sf-rounded
# --------------------------------------------------------------------------
def test_pentad_sdk_first_current_and_previous_from_source():
    _require_module()
    records = _build_pentads(
        sdk_current={3: 24.67},
        sdk_previous={3: 12.34},
    )
    row = _row(records, 3)
    assert row["current"] == fl.round_3sf(24.67)  # 24.7
    assert row["previous"] == fl.round_3sf(12.34)  # 12.3
    # 8.0 is the historical constant; SDK actuals must not equal the envelope.
    assert row["current"] != row["mean"]


def test_decad_sdk_first_current_and_previous_from_source():
    _require_module()
    records = _build_decads(
        sdk_current={2: 24.67},
        sdk_previous={2: 12.34},
    )
    row = _row(records, 2)
    assert row["current"] == fl.round_3sf(24.67)
    assert row["previous"] == fl.round_3sf(12.34)


def test_previous_is_from_source_independent_of_current_or_stored_rows():
    """previous is recomputed from the prior year's source even when the current
    year has no value at all — proving it is not read from stored rows and not
    gated on the M4 backfill."""
    _require_module()
    records = _build_pentads(
        sdk_current={},  # no current SDK
        daily_by_year={PREVIOUS_YEAR: []},  # no current/historical daily either
        sdk_previous={3: 12.34},
    )
    row = _row(records, 3)
    assert row["current"] is None
    assert row["previous"] == fl.round_3sf(12.34)


# --------------------------------------------------------------------------
# Criterion 2 — WDDA fallback under the >= 80% completeness rule
# --------------------------------------------------------------------------
def test_pentad_fallback_when_sdk_missing_and_coverage_at_least_80pct():
    _require_module()
    # Pentad 3 = Jan 11-15 (5 days); 4/5 = 80% present, no SDK -> daily-mean fallback.
    records = _build_pentads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates(TARGET_YEAR, 1, [11, 12, 13, 14], 5.0),
        },
    )
    row = _row(records, 3)
    assert row["current"] == fl.round_3sf(5.0)


def test_pentad_fallback_below_80pct_stores_null():
    _require_module()
    # Pentad 3 = Jan 11-15 (5 days); only 3/5 = 60% present, no SDK -> null.
    records = _build_pentads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates(TARGET_YEAR, 1, [11, 12, 13], 5.0),
        },
    )
    assert _row(records, 3)["current"] is None


def test_decad_fallback_when_sdk_missing_and_coverage_at_least_80pct():
    _require_module()
    # Decad 2 = Jan 11-20 (10 days); 8/10 = 80% present (boundary), no SDK ->
    # daily-mean fallback fills current. Proves the >= 80% SUCCESS path for decad
    # (not only the below-80% null case).
    records = _build_decads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates(TARGET_YEAR, 1, [11, 12, 13, 14, 15, 16, 17, 18], 5.0),
        },
    )
    assert _row(records, 2)["current"] == fl.round_3sf(5.0)


def test_decad_fallback_below_80pct_stores_null():
    _require_module()
    # Decad 2 = Jan 11-20 (10 days); only 6/10 = 60% present, no SDK -> null.
    records = _build_decads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates(TARGET_YEAR, 1, [11, 12, 13, 14, 15, 16], 5.0),
        },
    )
    assert _row(records, 2)["current"] is None


# --------------------------------------------------------------------------
# Criterion 2b — the >= 80% coverage gate is computed from FINITE discharge
# values, not merely dated rows. A dated-but-non-numeric (NaN/None) row must
# not count toward coverage, and must not be averaged in.
# --------------------------------------------------------------------------
def test_pentad_row_count_passes_80pct_but_finite_coverage_below_80pct_stores_null():
    _require_module()
    # Pentad 3 = Jan 11-15 (5 days); all 5 days are DATED (100% row coverage),
    # but only 1/5 = 20% carry a finite discharge value (the rest are None).
    # Row-count coverage alone would pass (5/5 >= 0.80); finite coverage must
    # gate this to null.
    records = _build_pentads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates_with_values(
                TARGET_YEAR,
                1,
                [(11, 5.0), (12, None), (13, None), (14, None), (15, None)],
            ),
        },
    )
    assert _row(records, 3)["current"] is None


def test_decad_row_count_passes_80pct_but_finite_coverage_below_80pct_stores_null():
    _require_module()
    # Decad 2 = Jan 11-20 (10 days); all 10 days are DATED (100% row coverage),
    # but only 2/10 = 20% carry a finite discharge value.
    records = _build_decads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates_with_values(
                TARGET_YEAR,
                1,
                [
                    (11, 5.0),
                    (12, 5.0),
                    (13, None),
                    (14, None),
                    (15, None),
                    (16, None),
                    (17, None),
                    (18, None),
                    (19, None),
                    (20, None),
                ],
            ),
        },
    )
    assert _row(records, 2)["current"] is None


def test_pentad_average_uses_only_finite_discharge_values_at_exact_80pct_coverage():
    _require_module()
    # Pentad 3 = Jan 11-15 (5 days); 5/5 dated but only 4/5 = 80% (boundary)
    # carry a finite value. Coverage passes on FINITE count, and the average
    # must be the mean of the 4 finite values only (5, 6, 7, 8 -> 6.5), never
    # contaminated by the None row.
    records = _build_pentads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates_with_values(
                TARGET_YEAR,
                1,
                [(11, 5.0), (12, 6.0), (13, 7.0), (14, 8.0), (15, None)],
            ),
        },
    )
    assert _row(records, 3)["current"] == fl.round_3sf(6.5)


def test_decad_average_uses_only_finite_discharge_values_at_exact_80pct_coverage():
    _require_module()
    # Decad 2 = Jan 11-20 (10 days); 10/10 dated but only 8/10 = 80% (boundary)
    # carry a finite value. Average must be the mean of those 8 finite values
    # only (5..12 -> 8.5), never contaminated by the 2 None rows.
    records = _build_decads(
        sdk_current={},
        daily_by_year={
            PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
            TARGET_YEAR: _daily_for_dates_with_values(
                TARGET_YEAR,
                1,
                [
                    (11, 5.0),
                    (12, 6.0),
                    (13, 7.0),
                    (14, 8.0),
                    (15, 9.0),
                    (16, 10.0),
                    (17, 11.0),
                    (18, 12.0),
                    (19, None),
                    (20, None),
                ],
            ),
        },
    )
    assert _row(records, 2)["current"] == fl.round_3sf((5.0 + 6 + 7 + 8 + 9 + 10 + 11 + 12) / 8)


# --------------------------------------------------------------------------
# Criterion 2 (mirror) — the SAME >= 80% WDDA fallback governs `previous`
# (prior year), computed from source exactly like `current`. Only current was
# exercised above; previous must obey the identical gate.
# --------------------------------------------------------------------------
def test_pentad_previous_fallback_when_sdk_missing_and_coverage_at_least_80pct():
    _require_module()
    # No previous SDK; PREVIOUS_YEAR pentad 3 (Jan 11-15) has 4/5 = 80% days ->
    # previous filled from the prior-year daily mean. current stays null (no
    # current SDK and no target-year daily), proving previous is from source.
    records = _build_pentads(
        sdk_current={},
        sdk_previous={},
        daily_by_year={
            PREVIOUS_YEAR: _daily_for_dates(PREVIOUS_YEAR, 1, [11, 12, 13, 14], 6.0),
        },
    )
    row = _row(records, 3)
    assert row["previous"] == fl.round_3sf(6.0)
    assert row["current"] is None


def test_pentad_previous_fallback_below_80pct_stores_null():
    _require_module()
    # PREVIOUS_YEAR pentad 3 (Jan 11-15) has only 3/5 = 60% days, no previous SDK
    # -> previous null. The envelope is still computed from those present days
    # (a null actual never nulls the envelope).
    records = _build_pentads(
        sdk_current={},
        sdk_previous={},
        daily_by_year={
            PREVIOUS_YEAR: _daily_for_dates(PREVIOUS_YEAR, 1, [11, 12, 13], 6.0),
        },
    )
    row = _row(records, 3)
    assert row["previous"] is None
    assert row["mean"] is not None  # envelope survives the null previous


def test_decad_previous_fallback_when_sdk_missing_and_coverage_at_least_80pct():
    _require_module()
    # Decad 2 (Jan 11-20) PREVIOUS_YEAR has 8/10 = 80% days, no previous SDK ->
    # previous filled from the prior-year daily mean.
    records = _build_decads(
        sdk_current={},
        sdk_previous={},
        daily_by_year={
            PREVIOUS_YEAR: _daily_for_dates(
                PREVIOUS_YEAR, 1, [11, 12, 13, 14, 15, 16, 17, 18], 6.0
            ),
        },
    )
    assert _row(records, 2)["previous"] == fl.round_3sf(6.0)


def test_decad_previous_fallback_below_80pct_stores_null():
    _require_module()
    # Decad 2 (Jan 11-20) PREVIOUS_YEAR has only 6/10 = 60% days -> previous null.
    records = _build_decads(
        sdk_current={},
        sdk_previous={},
        daily_by_year={
            PREVIOUS_YEAR: _daily_for_dates(PREVIOUS_YEAR, 1, [11, 12, 13, 14, 15, 16], 6.0),
        },
    )
    assert _row(records, 2)["previous"] is None


# --------------------------------------------------------------------------
# Criterion 3 — no finalized actual for an in-progress period
# --------------------------------------------------------------------------
def test_in_progress_pentad_has_no_finalized_current_even_with_sdk():
    _require_module()
    # today = Feb 8 2026. Pentad 8 (Feb 6-10) is in progress; pentad 7 (Feb 1-5)
    # is closed. Both are given an SDK value; only the closed one may finalize.
    records = _build_pentads(
        today=dt.date(2026, 2, 8),
        sdk_current={7: 24.67, 8: 24.67},
    )
    assert _row(records, 8)["current"] is None  # in-progress -> null
    assert _row(records, 7)["current"] == fl.round_3sf(24.67)  # closed -> finalized


def test_in_progress_decad_has_no_finalized_current_even_with_sdk():
    _require_module()
    # today = Feb 8 2026. Decad 4 (Feb 1-10) is IN PROGRESS; decad 3 (Jan 21-31)
    # is closed. Both are given an SDK value; only the closed one may finalize.
    # Decad window arithmetic (10/11-day periods, month-boundary decads) is a
    # DISTINCT calculation from pentad's, so the in-progress guard must be
    # exercised independently for decad — not inferred from the pentad case.
    records = _build_decads(
        today=dt.date(2026, 2, 8),
        sdk_current={3: 24.67, 4: 24.67},
    )
    assert _row(records, 4)["current"] is None  # Feb 1-10 in-progress -> null
    assert _row(records, 3)["current"] == fl.round_3sf(24.67)  # Jan 21-31 closed


# --------------------------------------------------------------------------
# Criterion 4 — issue-date key; exactly one row per (horizon_type, code, date)
# --------------------------------------------------------------------------
def test_pentad_rows_keyed_on_issue_date_no_duplicate_or_orphan():
    _require_module()
    records = _build_pentads()
    assert len(records) == 72
    assert {r["horizon_type"] for r in records} == {"pentad"}
    dates = [str(r["date"]) for r in records]
    assert len(set(dates)) == 72  # one row per issue date, no duplicates
    for p in range(1, 73):
        expected = fl.get_issue_date_from_pentad(p, TARGET_YEAR)
        assert dt.date.fromisoformat(str(_row(records, p)["date"])[:10]) == expected.date()


def test_decad_issue_date_named_cases():
    _require_module()
    records = _build_decads()
    assert len(records) == 36
    assert {r["horizon_type"] for r in records} == {"decade"}
    assert len({str(r["date"]) for r in records}) == 36
    # decad 2 -> Jan 10 (target year); decad 1 -> Dec 31 of the prior year.
    assert str(_row(records, 2)["date"])[:10] == "2026-01-10"
    assert str(_row(records, 1)["date"])[:10] == "2025-12-31"


# --------------------------------------------------------------------------
# Criterion 5 — complete row; a null actual never nulls the envelope/norm
# --------------------------------------------------------------------------
def test_every_row_carries_full_envelope_and_norm():
    _require_module()
    # Varying multi-year historical fixture so the envelope is discriminating
    # (a constant series collapses mean/min/max/quantiles to one value). No SDK
    # and no target-year daily -> current is null, yet the full envelope + norm
    # must still be populated by the relocated method.
    daily_by_year = {
        2024: _varying_year_daily(2024, 0.0),
        PREVIOUS_YEAR: _varying_year_daily(PREVIOUS_YEAR, 0.5),
    }
    records = _build_pentads(daily_by_year=daily_by_year)
    for r in records:
        for key in ROW_KEYS:
            assert key in r, f"missing column {key}"
    row = _row(records, 3)
    assert row["current"] is None  # null actual must not null the envelope/norm
    assert row["norm"] == PENTAD_NORMS[2]
    for key in ENVELOPE_KEYS:
        assert row[key] is not None
    # Envelope equals the legacy pandas computation exactly (criterion 5/7).
    expected = _legacy_expected_envelope(daily_by_year, tl.get_pentad_in_year, 3)
    assert expected["min"] != expected["max"], "fixture must vary within the period"
    for key in ENVELOPE_KEYS:
        assert row[key] == expected[key], f"pentad 3 {key}={row[key]!r} != {expected[key]!r}"


# --------------------------------------------------------------------------
# Criterion 8 — stored actual == bulletin/dashboard display via the shared 3sf
# contract. This is a genuine cross-check: it renders through the ACTUAL display
# seam (forecast_library.format_discharge — the M1 bulletin/dashboard formatter,
# which takes the ORIGINAL value and emits the 3sf string) and asserts the stored
# actual renders the SAME string as its source. It therefore catches a real
# divergence between store and display (e.g. a formatter that truncates instead
# of rounding half-up, or drops trailing significant zeros) — NOT idempotence of
# round_3sf on its own output.
# --------------------------------------------------------------------------
def test_stored_actual_renders_identically_to_its_source_in_the_display_formatter():
    _require_module()
    # source_current 2.565 sits on a HALF-UP boundary: a display layer that
    # truncated (or used binary round()) would render "2.56", diverging from the
    # stored 3sf value 2.57. source_previous 0.9995 -> "1.00" exercises trailing
    # significant zeros: a naive str(1.0) display would render "1.0".
    source_current = 2.565
    source_previous = 0.9995
    records = _build_pentads(
        sdk_current={3: source_current},
        sdk_previous={3: source_previous},
    )
    row = _row(records, 3)
    # Stored actual, rendered through the bulletin/dashboard formatter, must equal
    # the source rendered through the SAME formatter -> stored == displayed.
    assert fl.format_discharge(row["current"]) == fl.format_discharge(source_current)
    assert fl.format_discharge(row["previous"]) == fl.format_discharge(source_previous)
    # And the rendered strings are the expected 3sf display, not a truncation or a
    # dropped trailing zero.
    assert fl.format_discharge(row["current"]) == "2.57"
    assert fl.format_discharge(row["previous"]) == "1.00"


# --------------------------------------------------------------------------
# Negative / error paths — malformed input to the new public seam must fail
# loudly or fail safe (null), but NEVER emit a silently-wrong row. At least one
# per builder, covering distinct malformed categories (wrong-length norms,
# malformed daily date, non-numeric SDK value).
# --------------------------------------------------------------------------
class _NotANumber:
    """A value the builder can never legitimately treat as a discharge."""


def test_pentad_build_rejects_wrong_length_norms():
    _require_module()
    # norms MUST be indexed by period_in_year (72 pentads). A short list means
    # missing norm data for real periods; the builder must not fabricate a norm
    # or drop rows — it must raise. (71 != 72.)
    with pytest.raises((ValueError, IndexError, KeyError, AssertionError)):
        _build_pentads(norms=PENTAD_NORMS[:-1])


def test_decad_build_rejects_wrong_length_norms():
    _require_module()
    # 35 != 36 decads.
    with pytest.raises((ValueError, IndexError, KeyError, AssertionError)):
        _build_decads(norms=DECAD_NORMS[:-1])


def test_pentad_build_rejects_malformed_daily_date():
    _require_module()
    # No SDK -> the WDDA fallback must parse target-year daily dates. A record
    # with an unparseable date must NOT be silently dropped (which would
    # understate coverage or skew the mean into a wrong row); the builder must
    # raise rather than emit a silently-wrong actual.
    with pytest.raises((ValueError, TypeError, KeyError)):
        _build_pentads(
            sdk_current={},
            daily_by_year={
                PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0),
                TARGET_YEAR: [{"code": CODE, "date": "not-a-date", "discharge": 5.0}],
            },
        )


def test_decad_build_does_not_silently_store_a_non_numeric_sdk_value():
    _require_module()
    # A non-numeric SDK value is corrupt input. The builder may raise, or fail
    # safe (reject it -> current is None / falls back), but it must NEVER surface
    # the garbage object as the stored actual -> a silently-wrong row.
    garbage = _NotANumber()
    try:
        records = _build_decads(sdk_current={2: garbage})
    except (TypeError, ValueError, ArithmeticError):
        return  # raising is an acceptable well-defined failure
    current = _row(records, 2)["current"]
    assert current is not garbage  # never the raw garbage object
    assert not isinstance(current, _NotANumber)


# --------------------------------------------------------------------------
# Regression — the "date" field must be JSON-serializable (ISO date string),
# not a raw pandas.Timestamp. A real API write does
# ``requests.post(..., json=records)``, and ``json.dumps`` cannot serialize a
# ``pandas.Timestamp`` -> ``TypeError: Object of type Timestamp is not JSON
# serializable``. Month/quarter/season rows already emit ISO strings; this
# pins pentad/decad to the same contract.
# --------------------------------------------------------------------------
def test_pentad_and_decad_date_field_is_json_serializable():
    _require_module()
    daily_by_year = {
        2024: [{"code": CODE, "date": "2024-01-01", "discharge": 10.0}],
        2025: [{"code": CODE, "date": "2025-01-01", "discharge": 20.0}],
    }
    pentad_records = shh.build_pentad_records(
        code=CODE,
        norms=PENTAD_NORMS,
        daily_by_year=daily_by_year,
        sdk_current={},
        sdk_previous={},
        target_year=2025,
        today=dt.date(2025, 7, 1),
    )
    decad_records = shh.build_decad_records(
        code=CODE,
        norms=DECAD_NORMS,
        daily_by_year=daily_by_year,
        sdk_current={},
        sdk_previous={},
        target_year=2025,
        today=dt.date(2025, 7, 1),
    )

    for records in (pentad_records, decad_records):
        for record in records:
            assert isinstance(record["date"], str), (
                f"date must be a JSON-serializable str, got {type(record['date'])!r}: "
                f"{record['date']!r}"
            )
        # The real failure mode: json.dumps (and by extension the requests
        # `json=` kwarg used by the API client) must not raise.
        json.dumps({"data": records})
        # Exercise the actual serialization path used by
        # `client.write_hydrograph(records)` -> sapphire_api_client -> requests.
        requests.Session().prepare_request(
            requests.Request("POST", "http://x/", json={"data": records})
        )
