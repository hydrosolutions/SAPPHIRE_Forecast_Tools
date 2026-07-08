"""LOCKED regression tests: stale aggregate rows must not corrupt monthly skill.

Bug: on the dashboard month view some hydroposts show forecasts but ALL skill
columns blank for every model, while other sites display fully. The skill IS
computed and stored, but a *stray duplicate* population of skill_metrics rows
is produced at ``horizon_value == target_month``. That stray population later
activates a dashboard filter that discards the correct ``horizon_value == 0``
skill.

Root cause exercised here: stale aggregate forecast rows
(NAIVE_MEAN / ENSEMBLE_MEAN / SKILLED_MEAN) arrive at
``horizon_value = target month`` (an invalid convention) and are scored as if
they were ordinary base-model rows by the first raw-model-skill groupby in
``calculate_monthly_skill_metrics``. They must instead be excluded from that
groupby, so the aggregate skill is regenerated from the base models using the
base-model ``horizon_value`` convention (0 = no-lead sentinel here).

Convention (locked): monthly ``horizon_value`` is the lead offset when known;
0 is the legacy/no-lead sentinel; the *calendar month is never* a valid
``horizon_value``. Ensemble/aggregate models MUST use the same convention as
base models.

These tests assert the CORRECT (post-fix) behavior and therefore FAIL against
the current buggy code. Do NOT relax them to match current behavior.

Placeholder station code 19999 is used (never a real station code).
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import calculate_monthly_skill_metrics

# Placeholder station code — never a real station.
STATION = "19999"


# Target month of the forecast; also the (INVALID) horizon_value a stale
# aggregate row would carry under the buggy calendar-month fallback.
TARGET_MONTH = 8

# Base-model horizon_value convention in this scenario (no-lead sentinel).
BASE_HV = 0

QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
AGGREGATE_MODELS = {"EM", "Naive Mean", "Skilled Mean"}


def _make_observations():
    """Observations for station 19999, month 8, four years.

    Returns a frame with the columns the monthly skill routine consumes:
    ``code, year, month, month_in_year, discharge_avg, delta``.
    """
    rows = [
        (STATION, 2021, TARGET_MONTH, 100.0),
        (STATION, 2022, TARGET_MONTH, 110.0),
        (STATION, 2023, TARGET_MONTH, 120.0),
        (STATION, 2024, TARGET_MONTH, 130.0),
    ]
    df = pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])
    df["month_in_year"] = df["month"]

    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    df = df.merge(
        delta_df[["code", "month_in_year", "delta"]],
        on=["code", "month_in_year"],
        how="left",
    )
    return df


def _fcst_row(year, model_short, horizon_value, center):
    """Build one forecast row with a symmetric quantile spread around center."""
    offsets = [-20, -15, -8, 0, 8, 15, 20]
    row = {
        "code": STATION,
        "year": year,
        "month": TARGET_MONTH,
        "model_short": model_short,
        "horizon_value": horizon_value,
    }
    for qcol, off in zip(QUANTILE_COLS, offsets, strict=True):
        row[qcol] = float(center + off)
    return row


def _make_forecasts_with_stale_aggregate():
    """Two base models at hv=0 plus a STALE Naive Mean row at hv=month.

    The stale ``Naive Mean`` rows imitate previously-written aggregate rows
    that landed at ``horizon_value = target month`` (8) via the buggy
    calendar-month fallback. A correct recalc must ignore them for scoring and
    regenerate the aggregate from the base models at the base convention (0).
    """
    rows = []
    for year, base in [(2021, 102.0), (2022, 108.0), (2023, 121.0), (2024, 131.0)]:
        # Base models at the no-lead sentinel horizon_value = 0.
        rows.append(_fcst_row(year, "LR_Base", BASE_HV, base + 1.0))
        rows.append(_fcst_row(year, "LR_SM", BASE_HV, base - 1.0))
        # STALE aggregate row at the calendar-month horizon_value = 8.
        rows.append(_fcst_row(year, "Naive Mean", TARGET_MONTH, base))
    return pd.DataFrame(rows)


def test_stale_aggregate_row_at_month_produces_no_skill_at_month():
    """No skill row may exist at horizon_value == target month.

    Any skill_metrics row at ``horizon_value == 8`` is a stray artifact of
    scoring a stale aggregate row as a base model. The correct output contains
    no such row for any model.
    """
    obs = _make_observations()
    fcst = _make_forecasts_with_stale_aggregate()

    skill_stats, _joint, _timing = calculate_monthly_skill_metrics(obs, fcst)

    assert not skill_stats.empty, "expected non-empty skill metrics"
    at_month = skill_stats[skill_stats["horizon_value"] == TARGET_MONTH]
    assert at_month.empty, (
        "stale aggregate row at horizon_value == target month must not be "
        f"scored; found stray skill rows:\n{at_month.to_string()}"
    )


def test_aggregate_skill_uses_base_model_horizon_convention():
    """Regenerated aggregate skill must use the base-model horizon_value (0).

    The aggregate ("Naive Mean") is rebuilt from the base models, which live at
    horizon_value == 0, so its skill row must land at horizon_value == 0 — never
    at the calendar month.
    """
    obs = _make_observations()
    fcst = _make_forecasts_with_stale_aggregate()

    skill_stats, _joint, _timing = calculate_monthly_skill_metrics(obs, fcst)

    aggregate_rows = skill_stats[skill_stats["model_short"].isin(AGGREGATE_MODELS)]
    assert not aggregate_rows.empty, "expected a regenerated aggregate (Naive Mean) skill row"
    assert (aggregate_rows["horizon_value"] == BASE_HV).all(), (
        "aggregate skill must follow the base-model horizon_value convention "
        f"(0); got horizon_values {sorted(aggregate_rows['horizon_value'].unique())}"
    )


def test_no_duplicate_aggregate_skill_population():
    """Each (code, model, month_in_year) aggregate appears at exactly one hv.

    The bug yields two populations of the aggregate skill: the correct one at
    hv==0 and a stray one at hv==month. Deduping across horizon_value must leave
    a single row per aggregate model.
    """
    obs = _make_observations()
    fcst = _make_forecasts_with_stale_aggregate()

    skill_stats, _joint, _timing = calculate_monthly_skill_metrics(obs, fcst)

    aggregate_rows = skill_stats[skill_stats["model_short"].isin(AGGREGATE_MODELS)]
    counts = aggregate_rows.groupby(["code", "model_short", "month_in_year"])[
        "horizon_value"
    ].nunique()
    assert (counts == 1).all(), (
        "aggregate skill must not span multiple horizon_value populations; "
        f"per-group horizon_value counts:\n{counts.to_string()}"
    )


def test_base_model_skill_preserved_at_zero():
    """Base-model skill stays at horizon_value == 0 and is fully populated.

    Guards against a fix that would drop the correct base-model skill along with
    the stray aggregate rows.
    """
    obs = _make_observations()
    fcst = _make_forecasts_with_stale_aggregate()

    skill_stats, _joint, _timing = calculate_monthly_skill_metrics(obs, fcst)

    for model in ("LR_Base", "LR_SM"):
        rows = skill_stats[skill_stats["model_short"] == model]
        assert not rows.empty, f"expected skill rows for base model {model}"
        assert (rows["horizon_value"] == BASE_HV).all(), (
            f"{model} skill must stay at horizon_value == 0"
        )
        assert rows["mae"].notna().all(), f"{model} MAE must be populated"
        assert not np.isinf(rows["mae"]).any(), f"{model} MAE must be finite"
