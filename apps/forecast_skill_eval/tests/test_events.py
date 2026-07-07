"""Tests for Phase-2C: symmetric percentile-based event detection.

Covers:
- Percentile threshold computation per (code, horizon, period_key)
- min_years gate
- Event classification for low- and high-flow events
- Event column emitted in contingency pipeline output
- below_norm rows backward-compatible with pre-change behaviour
- CLI --events flag
- Config events_filter validation
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

STATION_CODE = "19999"


# ---------------------------------------------------------------------------
# Helper: build a minimal pairs DataFrame with controlled observed values
# ---------------------------------------------------------------------------


def _obs_pairs(
    *,
    code: str = STATION_CODE,
    horizon: str = "pentad",
    period_key: int = 1,
    observed_values: list[float],
    forecast_value: float = 5.0,
    model: str = "model-a",
) -> pd.DataFrame:
    """Build a minimal pairs DataFrame with one row per year."""
    rows = []
    for i, obs in enumerate(observed_values):
        year = 2010 + i
        _fc_class = "below" if forecast_value < 8.0 else "normal"
        _obs_class = "below" if obs < 8.0 else "normal"
        if _fc_class == "below" and _obs_class == "below":
            _contingency = "TP"
        elif _fc_class == "below" and _obs_class == "normal":
            _contingency = "FP"
        elif _fc_class == "normal" and _obs_class == "below":
            _contingency = "FN"
        else:
            _contingency = "TN"
        rows.append(
            {
                "horizon": horizon,
                "code": code,
                "basin": "other",
                "period_key": period_key,
                "year": year,
                "model": model,
                "regime": "hindcast",
                "season": "irrigation",
                "lead": None,
                "issue_date": None,
                "forecast_value": forecast_value,
                "observed_value": obs,
                "norm": 10.0,
                "norm_provenance": "calculated",
                "fc_class": _fc_class,
                "obs_class": _obs_class,
                "contingency": _contingency,
            }
        )
    return pd.DataFrame(rows)


def _rich_pairs(n_years: int = 15) -> pd.DataFrame:
    """Build pairs with n_years of data — enough to meet the default min_years=10 gate."""
    observed = [float(i * 2) for i in range(n_years)]
    return _obs_pairs(
        observed_values=observed,
        forecast_value=float(observed[2]),
    )


# ===========================================================================
# Block 1 — Percentile threshold computation
# ===========================================================================


def test_percentile_thresholds_correct_values_for_known_distribution() -> None:
    """Thresholds must match numpy's empirical percentile for a known distribution."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    observed = list(range(100))  # 100 distinct years, values 0..99
    pairs = _obs_pairs(observed_values=observed)

    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    key = (STATION_CODE, "pentad", 1)
    assert key in thresholds
    assert abs(thresholds[key][10.0] - float(np.percentile(observed, 10))) < 1e-9
    assert abs(thresholds[key][5.0] - float(np.percentile(observed, 5))) < 1e-9
    assert abs(thresholds[key][90.0] - float(np.percentile(observed, 90))) < 1e-9
    assert abs(thresholds[key][95.0] - float(np.percentile(observed, 95))) < 1e-9


def test_percentile_threshold_min_years_gate_excludes_thin_stations() -> None:
    """Groups with fewer distinct years than min_years must be excluded."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    observed = [1.0, 2.0, 3.0]  # 3 years — below default min_years=10
    pairs = _obs_pairs(observed_values=observed)

    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    assert (STATION_CODE, "pentad", 1) not in thresholds


def test_percentile_threshold_meets_min_years_exactly() -> None:
    """Exactly min_years distinct years must produce a threshold entry."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    observed = list(range(10))  # exactly 10 years
    pairs = _obs_pairs(observed_values=observed)

    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    assert (STATION_CODE, "pentad", 1) in thresholds


def test_percentile_threshold_deduplicates_across_models() -> None:
    """Same (code, horizon, period_key, year) from different models counts once."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    observed = list(range(10))  # 10 distinct years
    pairs_a = _obs_pairs(observed_values=observed, model="model-a")
    pairs_b = _obs_pairs(observed_values=observed, model="model-b")
    combined = pd.concat([pairs_a, pairs_b], ignore_index=True)

    thresholds = compute_percentile_thresholds(combined, min_years=10)
    single = compute_percentile_thresholds(pairs_a, min_years=10)

    key = (STATION_CODE, "pentad", 1)
    assert key in thresholds
    assert abs(thresholds[key][10.0] - single[key][10.0]) < 1e-9


def test_percentile_threshold_empty_pairs_returns_empty() -> None:
    """An empty pairs DataFrame must return an empty mapping."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    result = compute_percentile_thresholds(pd.DataFrame(), min_years=10)

    assert result == {}


def test_percentile_threshold_per_station_and_period_independent() -> None:
    """Thresholds for different (code, period_key) must be computed independently."""
    from forecast_skill_eval.events import compute_percentile_thresholds

    pairs_p1 = _obs_pairs(period_key=1, observed_values=list(range(10)), forecast_value=1.0)
    pairs_p2 = _obs_pairs(period_key=2, observed_values=list(range(100, 110)), forecast_value=1.0)
    combined = pd.concat([pairs_p1, pairs_p2], ignore_index=True)

    thresholds = compute_percentile_thresholds(combined, min_years=10)

    key1 = (STATION_CODE, "pentad", 1)
    key2 = (STATION_CODE, "pentad", 2)
    assert key1 in thresholds
    assert key2 in thresholds
    # The 10th percentile of 0..9 is different from 100..109
    assert thresholds[key1][10.0] < thresholds[key2][10.0]


# ===========================================================================
# Block 2 — Event classification via reclassify_pairs_for_event
# ===========================================================================


def test_below_norm_event_returns_pairs_unchanged() -> None:
    """reclassify_pairs_for_event with below_norm must return an identical copy."""
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    pairs = _obs_pairs(observed_values=[5.0, 6.0, 7.0])

    event = event_by_name("below_norm")
    result = reclassify_pairs_for_event(pairs, event, thresholds={})

    pd.testing.assert_frame_equal(result, pairs)


def test_low_p5_positive_class_for_value_below_5th_percentile() -> None:
    """A forecast value clearly below the 5th percentile must yield fc_class='below'."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    # 0..99; 5th percentile ~ 4.95; forecast_value=1.0 < 5th pctile
    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=1.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("low_p5")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "below").all()


def test_low_p5_negative_class_for_value_above_5th_percentile() -> None:
    """A forecast value well above the 5th percentile must yield fc_class='normal'."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=50.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("low_p5")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "normal").all()


def test_low_p10_positive_for_value_below_10th_percentile() -> None:
    """Value below 10th percentile must yield positive (below) class for low_p10."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=1.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("low_p10")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "below").all()


def test_high_p90_positive_class_for_value_above_90th_percentile() -> None:
    """A forecast value above the 90th percentile must yield fc_class='below' (positive)."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    # 0..99; 90th percentile ~ 89.1; forecast_value=99.0 > 90th pctile
    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=99.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("high_p90")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "below").all()


def test_high_p90_negative_class_for_value_below_90th_percentile() -> None:
    """A forecast value below the 90th percentile must yield fc_class='normal' (negative)."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=1.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("high_p90")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "normal").all()


def test_high_p95_positive_for_value_above_95th_percentile() -> None:
    """Value above 95th percentile must yield positive (below) class for high_p95."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    observed = list(range(100))
    pairs = _obs_pairs(observed_values=observed, forecast_value=99.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("high_p95")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert not result.empty
    assert (result["fc_class"] == "below").all()


def test_reclassify_drops_rows_with_missing_threshold() -> None:
    """Rows for a station/period without a threshold (thin data) must be dropped."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    # Only 2 years → threshold not computed with min_years=10
    pairs = _obs_pairs(observed_values=[5.0, 6.0], forecast_value=5.0)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("low_p10")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert result.empty


def test_reclassify_preserves_all_pair_columns() -> None:
    """Reclassified pairs must carry all original columns."""
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )

    pairs = _rich_pairs(n_years=15)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    event = event_by_name("low_p10")
    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert set(pairs.columns) == set(result.columns)


# ===========================================================================
# Block 3 — Event column in contingency pipeline output
# ===========================================================================


def _run_event_pipeline(
    pairs: pd.DataFrame,
    min_years: int = 10,
) -> pd.DataFrame:
    """Run the per-event contingency pipeline and return the combined output."""
    from forecast_skill_eval.contingency import count_contingencies
    from forecast_skill_eval.events import (
        ALL_EVENTS,
        compute_percentile_thresholds,
        reclassify_pairs_for_event,
    )
    from forecast_skill_eval.metrics import add_metrics

    thresholds = compute_percentile_thresholds(pairs, min_years=min_years)
    frames = []
    for event in ALL_EVENTS:
        ep = reclassify_pairs_for_event(pairs, event, thresholds)
        if ep.empty:
            continue
        ct = add_metrics(count_contingencies(ep))
        ct["event"] = event.name
        frames.append(ct)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def test_event_pipeline_produces_event_column() -> None:
    """The contingency pipeline must emit an 'event' column."""
    result = _run_event_pipeline(_rich_pairs(n_years=15))

    assert "event" in result.columns


def test_all_five_events_produced_when_sufficient_data() -> None:
    """All five event names must appear in the event column when data is sufficient."""
    from forecast_skill_eval.events import ALL_EVENT_NAMES

    result = _run_event_pipeline(_rich_pairs(n_years=15))

    assert set(ALL_EVENT_NAMES).issubset(set(result["event"].unique()))


def test_below_norm_event_only_when_thin_data() -> None:
    """With fewer years than min_years only below_norm rows appear (no percentile events)."""
    # 3 years — below min_years=10 → only below_norm survives
    pairs = _obs_pairs(observed_values=[5.0, 6.0, 7.0])
    result = _run_event_pipeline(pairs, min_years=10)

    assert not result.empty
    events_in_output = set(result["event"].unique())
    assert events_in_output == {"below_norm"}


def test_below_norm_rows_identical_to_pre_change() -> None:
    """The below_norm event rows must match what count_contingencies returns directly."""
    from forecast_skill_eval.contingency import count_contingencies
    from forecast_skill_eval.events import (
        compute_percentile_thresholds,
        event_by_name,
        reclassify_pairs_for_event,
    )
    from forecast_skill_eval.metrics import add_metrics

    pairs = _rich_pairs(n_years=15)
    thresholds = compute_percentile_thresholds(pairs, min_years=10)

    # Pre-change: direct count
    expected = add_metrics(count_contingencies(pairs))

    # Post-change: via event pipeline for below_norm
    bn_event = event_by_name("below_norm")
    bn_pairs = reclassify_pairs_for_event(pairs, bn_event, thresholds)
    actual = add_metrics(count_contingencies(bn_pairs))

    # Numeric count columns must be identical row-by-row
    for col in ("TP", "FP", "FN", "TN", "n_pairs"):
        pd.testing.assert_series_equal(
            expected[col].reset_index(drop=True),
            actual[col].reset_index(drop=True),
            check_names=False,
        )


# ===========================================================================
# Block 4 — CLI --events flag
# ===========================================================================


def test_cli_events_default_is_all_five_events() -> None:
    """Omitting --events must default to all five event names in the config."""
    from forecast_skill_eval import cli
    from forecast_skill_eval.events import ALL_EVENT_NAMES

    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)

    assert set(config.events_filter) == set(ALL_EVENT_NAMES)


def test_cli_events_flag_restricts_to_specified_events() -> None:
    """--events below_norm low_p10 must restrict events_filter to those two."""
    from forecast_skill_eval import cli

    parser = cli._parser()
    args = parser.parse_args(["--events", "below_norm", "low_p10"])
    config = cli._config_from_args(args)

    assert set(config.events_filter) == {"below_norm", "low_p10"}


def test_cli_invalid_event_name_raises() -> None:
    """An invalid event name must be rejected via config validation."""
    from forecast_skill_eval import cli

    parser = cli._parser()
    args = parser.parse_args(["--events", "invalid_event"])
    with pytest.raises(ValueError):
        cli._config_from_args(args)


def test_cli_events_single_event_accepted() -> None:
    """A single valid event name must be accepted."""
    from forecast_skill_eval import cli

    parser = cli._parser()
    args = parser.parse_args(["--events", "high_p95"])
    config = cli._config_from_args(args)

    assert config.events_filter == ("high_p95",)


# ===========================================================================
# Block 5 — Config events_filter validation
# ===========================================================================


def test_config_events_filter_defaults_to_all_five() -> None:
    """ForecastSkillEvalConfig() must default events_filter to all five events."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig
    from forecast_skill_eval.events import ALL_EVENT_NAMES

    config = ForecastSkillEvalConfig()

    assert set(config.events_filter) == set(ALL_EVENT_NAMES)


def test_config_events_filter_accepts_valid_subset() -> None:
    """A valid subset of event names must be accepted by ForecastSkillEvalConfig."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig

    config = ForecastSkillEvalConfig(events_filter=("below_norm", "high_p90"))

    assert set(config.events_filter) == {"below_norm", "high_p90"}


def test_config_events_filter_validates_unknown_event() -> None:
    """An unknown event name must raise ValueError with 'events_filter' in the message."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig

    with pytest.raises(ValueError, match="events_filter"):
        ForecastSkillEvalConfig(events_filter=("below_norm", "bogus_event"))


def test_config_events_filter_rejects_empty() -> None:
    """An empty events_filter tuple must raise ValueError."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig

    with pytest.raises(ValueError, match="events_filter"):
        ForecastSkillEvalConfig(events_filter=())


def test_config_events_filter_normalised_to_tuple() -> None:
    """events_filter must be stored as a tuple regardless of input sequence type."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig

    config = ForecastSkillEvalConfig(events_filter=["below_norm", "low_p10"])

    assert isinstance(config.events_filter, tuple)


# ===========================================================================
# Block 6 — Return-period event definitions (Phase-2D)
# ===========================================================================


def _rp_pairs(
    *,
    code: str = STATION_CODE,
    second_code: str = "29999",
    horizon: str = "pentad",
    n_years: int = 20,
    base: float = 10.0,
    step: float = 5.0,
) -> pd.DataFrame:
    """Build pairs with linearly increasing observed values for GEV fitting.

    Values are ``base + i * step`` for year *i*, giving a clear trend that
    produces well-ordered return levels across the requested return periods.
    """
    rows = []
    for i in range(n_years):
        val = base + i * step
        year = 2000 + i
        rows.append(
            {
                "horizon": horizon,
                "code": code,
                "basin": "other",
                "period_key": 1,
                "year": year,
                "model": "model-a",
                "regime": "hindcast",
                "season": "irrigation",
                "lead": None,
                "issue_date": None,
                "forecast_value": val,
                "observed_value": val,
                "norm": 50.0,
                "norm_provenance": "calculated",
                "fc_class": "normal",
                "obs_class": "normal",
                "contingency": "TN",
            }
        )
    return pd.DataFrame(rows)


def test_return_period_events_not_in_default_set() -> None:
    """rp5/rp10/rp30/rp100 must not appear in ALL_EVENT_NAMES (the default set)."""
    from forecast_skill_eval.events import ALL_EVENT_NAMES

    for name in ("rp5", "rp10", "rp30", "rp100"):
        assert name not in ALL_EVENT_NAMES, f"{name!r} must not be a default event"


def test_return_period_events_valid_in_config() -> None:
    """rp5/rp10/rp30/rp100 must be accepted by ForecastSkillEvalConfig.events_filter."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig

    config = ForecastSkillEvalConfig(events_filter=("rp5", "rp10", "rp30", "rp100"))

    assert set(config.events_filter) == {"rp5", "rp10", "rp30", "rp100"}


def test_return_period_events_valid_set_covers_rp_names() -> None:
    """VALID_EVENTS must include the four return-period event names."""
    from forecast_skill_eval.events import VALID_EVENTS

    for name in ("rp5", "rp10", "rp30", "rp100"):
        assert name in VALID_EVENTS


def test_event_by_name_resolves_rp_events() -> None:
    """event_by_name must return the correct EventDef for each rp event."""
    from forecast_skill_eval.events import event_by_name

    for name, T in (("rp5", 5.0), ("rp10", 10.0), ("rp30", 30.0), ("rp100", 100.0)):
        ev = event_by_name(name)
        assert ev.name == name
        assert ev.return_period == T
        assert ev.direction == "above"
        assert ev.percentile is None


# ===========================================================================
# Block 7 — compute_return_levels
# ===========================================================================


def test_compute_return_levels_levels_increase_with_return_period() -> None:
    """Return levels must be non-decreasing: rp5 <= rp10 <= rp30 <= rp100."""
    from forecast_skill_eval.events import compute_return_levels

    pairs = _rp_pairs(n_years=20)
    return_periods = (5.0, 10.0, 30.0, 100.0)
    levels = compute_return_levels(pairs, return_periods=return_periods, min_years=10)

    key = (STATION_CODE, "pentad", 1)
    assert key in levels, "Station must have estimable return levels with 20 years of data"

    rp5 = levels[key][5.0]
    rp10 = levels[key][10.0]
    rp30 = levels[key][30.0]
    rp100 = levels[key][100.0]

    assert rp5 <= rp10, f"rp5={rp5} must be <= rp10={rp10}"
    assert rp10 <= rp30, f"rp10={rp10} must be <= rp30={rp30}"
    assert rp30 <= rp100, f"rp30={rp30} must be <= rp100={rp100}"


def test_compute_return_levels_min_years_gate_excludes_short_record() -> None:
    """Stations with fewer distinct years than min_years must be excluded."""
    from forecast_skill_eval.events import compute_return_levels

    pairs = _rp_pairs(n_years=5)  # 5 years — below min_years=10
    levels = compute_return_levels(pairs, return_periods=(5.0, 10.0), min_years=10)

    assert (STATION_CODE, "pentad", 1) not in levels


def test_compute_return_levels_meets_min_years_exactly() -> None:
    """Exactly min_years distinct years must produce an entry."""
    from forecast_skill_eval.events import compute_return_levels

    pairs = _rp_pairs(n_years=10)  # exactly min_years=10
    levels = compute_return_levels(pairs, return_periods=(5.0, 10.0), min_years=10)

    assert (STATION_CODE, "pentad", 1) in levels


def test_compute_return_levels_degenerate_constant_series_no_raise() -> None:
    """A constant observed series must not raise — yields no entry."""
    from forecast_skill_eval.events import compute_return_levels

    # All observed values identical: GEV fit is degenerate
    pairs = _rp_pairs(n_years=20, base=42.0, step=0.0)
    # No assertion error expected — function must return gracefully
    levels = compute_return_levels(pairs, return_periods=(5.0, 10.0), min_years=10)

    assert (STATION_CODE, "pentad", 1) not in levels


def test_compute_return_levels_empty_pairs_returns_empty() -> None:
    """An empty pairs DataFrame must return an empty mapping."""
    from forecast_skill_eval.events import compute_return_levels

    levels = compute_return_levels(pd.DataFrame(), return_periods=(5.0, 10.0), min_years=10)

    assert levels == {}


def test_compute_return_levels_deduplicates_across_models() -> None:
    """Duplicate model rows for the same (code, horizon, period_key, year) count once."""
    from forecast_skill_eval.events import compute_return_levels

    single = _rp_pairs(n_years=20)
    doubled = pd.concat(
        [single.assign(model="model-a"), single.assign(model="model-b")], ignore_index=True
    )

    levels_single = compute_return_levels(single, return_periods=(5.0,), min_years=10)
    levels_doubled = compute_return_levels(doubled, return_periods=(5.0,), min_years=10)

    key = (STATION_CODE, "pentad", 1)
    assert key in levels_single
    assert key in levels_doubled
    assert abs(levels_single[key][5.0] - levels_doubled[key][5.0]) < 1e-6


def test_compute_return_levels_per_period_keys_are_independent() -> None:
    """Return levels for different period_keys must be keyed and estimated independently.

    Period A (low values) and period B (high values) must produce separate
    entries, and period A's return level must not contaminate period B's.
    """
    from forecast_skill_eval.events import compute_return_levels

    # period_key=1: low values 10..29; period_key=2: high values 100..119
    rows_p1 = []
    rows_p2 = []
    for i in range(20):
        base_row = {
            "horizon": "pentad",
            "code": STATION_CODE,
            "basin": "other",
            "year": 2000 + i,
            "model": "model-a",
            "regime": "hindcast",
            "season": "all",
            "lead": None,
            "issue_date": None,
            "forecast_value": 0.0,
            "norm": 50.0,
            "norm_provenance": "calculated",
            "fc_class": "normal",
            "obs_class": "normal",
            "contingency": "TN",
        }
        rows_p1.append({**base_row, "period_key": 1, "observed_value": 10.0 + i})
        rows_p2.append({**base_row, "period_key": 2, "observed_value": 100.0 + i})

    pairs = pd.concat([pd.DataFrame(rows_p1), pd.DataFrame(rows_p2)], ignore_index=True)
    levels = compute_return_levels(pairs, return_periods=(5.0,), min_years=10)

    key_p1 = (STATION_CODE, "pentad", 1)
    key_p2 = (STATION_CODE, "pentad", 2)
    assert key_p1 in levels, "period_key=1 must have its own return level"
    assert key_p2 in levels, "period_key=2 must have its own return level"
    # Period A (low) rp5 must be well below period B (high) rp5
    assert levels[key_p1][5.0] < levels[key_p2][5.0], (
        "Period A (low values) return level must be lower than period B (high values)"
    )


# ===========================================================================
# Block 8 — reclassify_pairs_for_rp_event
# ===========================================================================


def test_reclassify_rp_event_positive_when_above_level() -> None:
    """A value exceeding the return level must yield fc_class='below' (positive)."""
    from forecast_skill_eval.events import (
        compute_return_levels,
        event_by_name,
        reclassify_pairs_for_rp_event,
    )

    # 20 years of increasing values; rp5 level should be around the upper range
    pairs = _rp_pairs(n_years=20, base=10.0, step=5.0)
    return_levels = compute_return_levels(pairs, return_periods=(5.0,), min_years=10)

    key = (STATION_CODE, "pentad", 1)
    assert key in return_levels
    rp5_level = return_levels[key][5.0]

    # Build a pair with a forecast clearly above the return level
    extreme_pairs = pairs.copy()
    extreme_pairs["forecast_value"] = rp5_level + 100.0
    extreme_pairs["observed_value"] = rp5_level + 100.0

    event = event_by_name("rp5")
    result = reclassify_pairs_for_rp_event(extreme_pairs, event, return_levels)

    assert not result.empty
    assert (result["fc_class"] == "below").all()
    assert (result["obs_class"] == "below").all()
    assert (result["contingency"] == "TP").all()


def test_reclassify_rp_event_negative_when_below_level() -> None:
    """A value below the return level must yield fc_class='normal' (negative)."""
    from forecast_skill_eval.events import (
        compute_return_levels,
        event_by_name,
        reclassify_pairs_for_rp_event,
    )

    pairs = _rp_pairs(n_years=20, base=10.0, step=5.0)
    return_levels = compute_return_levels(pairs, return_periods=(5.0,), min_years=10)

    key = (STATION_CODE, "pentad", 1)
    rp5_level = return_levels[key][5.0]

    # Build a pair with a forecast clearly below the return level
    low_pairs = pairs.copy()
    low_pairs["forecast_value"] = rp5_level - 1000.0
    low_pairs["observed_value"] = rp5_level - 1000.0

    event = event_by_name("rp5")
    result = reclassify_pairs_for_rp_event(low_pairs, event, return_levels)

    assert not result.empty
    assert (result["fc_class"] == "normal").all()
    assert (result["obs_class"] == "normal").all()
    assert (result["contingency"] == "TN").all()


def test_reclassify_rp_event_drops_rows_with_missing_level() -> None:
    """Rows for a station with no estimable return level must be dropped."""
    from forecast_skill_eval.events import (
        event_by_name,
        reclassify_pairs_for_rp_event,
    )

    # Empty return_levels: no station has an estimable level
    pairs = _rp_pairs(n_years=5)
    event = event_by_name("rp5")
    result = reclassify_pairs_for_rp_event(pairs, event, return_levels={})

    assert result.empty


def test_reclassify_rp_event_raises_for_non_rp_event() -> None:
    """reclassify_pairs_for_rp_event must raise ValueError for a percentile event."""
    from forecast_skill_eval.events import (
        event_by_name,
        reclassify_pairs_for_rp_event,
    )

    pairs = _rp_pairs(n_years=5)
    event = event_by_name("high_p90")
    with pytest.raises(ValueError, match="return_period"):
        reclassify_pairs_for_rp_event(pairs, event, return_levels={})


def test_reclassify_rp_event_preserves_all_pair_columns() -> None:
    """Reclassified pairs must carry all original columns."""
    from forecast_skill_eval.events import (
        compute_return_levels,
        event_by_name,
        reclassify_pairs_for_rp_event,
    )

    pairs = _rp_pairs(n_years=20)
    return_levels = compute_return_levels(pairs, return_periods=(5.0,), min_years=10)

    event = event_by_name("rp5")
    result = reclassify_pairs_for_rp_event(pairs, event, return_levels)

    assert set(pairs.columns) == set(result.columns)


# ===========================================================================
# Block 9 — Equivalence tests: vectorized vs reference row-wise implementation
# ===========================================================================

# Reference row-wise implementations kept inline so the equivalence assertion
# is self-contained and does not depend on the production code path under test.


def _reference_reclassify_pairs_for_event(pairs, event, thresholds):
    """Verbatim copy of the original row-wise loop for equivalence checking."""
    from forecast_skill_eval.classifier import contingency as _contingency_ref

    if pairs.empty:
        return pairs.copy()
    if event.percentile is None and event.return_period is None:
        return pairs.copy()
    rows = []
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        try:
            period_key = int(row["period_key"])
        except (TypeError, ValueError, KeyError):
            continue
        key = (code, horizon, period_key)
        period_thresholds = thresholds.get(key)
        if period_thresholds is None:
            continue
        threshold_value = period_thresholds.get(event.percentile)
        if threshold_value is None:
            continue
        fc_val = row.get("forecast_value")
        obs_val = row.get("observed_value")
        if fc_val is None or obs_val is None:
            continue
        try:
            fc_f = float(fc_val)
            obs_f = float(obs_val)
        except (TypeError, ValueError):
            continue
        if event.direction == "below":
            fc_class = "below" if fc_f < threshold_value else "normal"
            obs_class = "below" if obs_f < threshold_value else "normal"
        else:
            fc_class = "below" if fc_f > threshold_value else "normal"
            obs_class = "below" if obs_f > threshold_value else "normal"
        new_row = dict(row)
        new_row["fc_class"] = fc_class
        new_row["obs_class"] = obs_class
        new_row["contingency"] = _contingency_ref(fc_class, obs_class)
        rows.append(new_row)
    if not rows:
        return pd.DataFrame(columns=list(pairs.columns))
    return pd.DataFrame(rows, columns=list(pairs.columns))


def _reference_reclassify_pairs_for_rp_event(pairs, event, return_levels):
    """Verbatim copy of the original row-wise rp loop for equivalence checking."""
    from forecast_skill_eval.classifier import contingency as _contingency_ref

    if pairs.empty:
        return pairs.copy()
    if event.return_period is None:
        raise ValueError("not an rp event")
    rows = []
    for row in pairs.to_dict("records"):
        code = str(row.get("code", ""))
        horizon = str(row.get("horizon", ""))
        try:
            period_key = int(row["period_key"])
        except (TypeError, ValueError, KeyError):
            continue
        key = (code, horizon, period_key)
        group_levels = return_levels.get(key)
        if group_levels is None:
            continue
        level = group_levels.get(event.return_period)
        if level is None:
            continue
        fc_val = row.get("forecast_value")
        obs_val = row.get("observed_value")
        if fc_val is None or obs_val is None:
            continue
        try:
            fc_f = float(fc_val)
            obs_f = float(obs_val)
        except (TypeError, ValueError):
            continue
        fc_class = "below" if fc_f > level else "normal"
        obs_class = "below" if obs_f > level else "normal"
        new_row = dict(row)
        new_row["fc_class"] = fc_class
        new_row["obs_class"] = obs_class
        new_row["contingency"] = _contingency_ref(fc_class, obs_class)
        rows.append(new_row)
    if not rows:
        return pd.DataFrame(columns=list(pairs.columns))
    return pd.DataFrame(rows, columns=list(pairs.columns))


def _equiv_pairs() -> pd.DataFrame:
    """Craft a fixture covering every drop/classify branch.

    All period_key values are int so dtype is consistent across both
    implementations (no mixed-type column to confuse dtype inference).
    Fake codes 19999 / 29999 — no real station identifiers.

    NaN forecast/observed values are KEPT by both implementations (the original
    loop calls float(nan) which succeeds; nan comparisons return False → "normal").
    Only Python None or non-castable strings in object columns are dropped.
    """
    COLS = [
        "code",
        "horizon",
        "basin",
        "period_key",
        "year",
        "model",
        "regime",
        "season",
        "lead",
        "issue_date",
        "forecast_value",
        "observed_value",
        "norm",
        "norm_provenance",
        "fc_class",
        "obs_class",
        "contingency",
    ]

    def row(code, period_key, fc, obs):
        return {
            "code": code,
            "horizon": "pentad",
            "basin": "test",
            "period_key": period_key,
            "year": 2010,
            "model": "LR",
            "regime": "hindcast",
            "season": "all",
            "lead": None,
            "issue_date": None,
            "forecast_value": fc,
            "observed_value": obs,
            "norm": 100.0,
            "norm_provenance": "calculated",
            "fc_class": "normal",
            "obs_class": "normal",
            "contingency": "TN",
        }

    # Threshold for (19999, pentad, 1) at p90 = 50.0 (direction="above")
    # Threshold for (19999, pentad, 3) at p5  = 20.0 (direction="below")
    rows = [
        # --- direction="above" (high_p90), threshold=50.0 ---
        row("19999", 1, fc=60.0, obs=70.0),  # both > 50  → TP
        row("19999", 1, fc=30.0, obs=30.0),  # both < 50  → TN
        row("19999", 1, fc=60.0, obs=30.0),  # fc>50, obs<50 → FP
        row("19999", 1, fc=30.0, obs=60.0),  # fc<50, obs>50 → FN
        row("19999", 1, fc=50.0, obs=70.0),  # fc==50 (not >) → normal → FN (strict >)
        row("19999", 1, fc=70.0, obs=50.0),  # obs==50 (not >) → normal → FP (strict >)
        # --- no threshold for period_key=2 → all dropped ---
        row("19999", 2, fc=60.0, obs=60.0),
        row("19999", 2, fc=30.0, obs=30.0),
        # --- NaN forecast (float64) → KEPT; nan > 50 = False → fc "normal" → FN ---
        row("19999", 1, fc=float("nan"), obs=60.0),
        # --- NaN observed (float64) → KEPT; nan > 50 = False → obs "normal" → FP ---
        row("19999", 1, fc=60.0, obs=float("nan")),
        # --- code 29999 has no threshold → dropped ---
        row("29999", 1, fc=60.0, obs=60.0),
        # --- direction="below" (low_p5), period_key=3, threshold=20.0 ---
        row("19999", 3, fc=10.0, obs=10.0),  # both < 20  → TP
        row("19999", 3, fc=30.0, obs=30.0),  # both > 20  → TN
        row("19999", 3, fc=20.0, obs=10.0),  # fc==20 (not <) → normal → FN (strict <)
        row("19999", 3, fc=10.0, obs=20.0),  # obs==20 (not <) → normal → FP (strict <)
    ]
    return pd.DataFrame(rows, columns=COLS)


def test_reclassify_equivalence_percentile_events_above() -> None:
    """Vectorized reclassify_pairs_for_event matches reference for direction='above'.

    Surviving rows: period_key==1 only (8 rows: 6 clean + 2 NaN-float rows kept).
    NaN float forecast/observed are NOT dropped — they classify as "normal"
    because float(nan) succeeds and nan > threshold returns False.
    """
    from forecast_skill_eval.events import EventDef, reclassify_pairs_for_event

    pairs = _equiv_pairs()
    thresholds = {
        ("19999", "pentad", 1): {90.0: 50.0},
        ("19999", "pentad", 3): {5.0: 20.0},
    }
    event = EventDef(name="high_p90", direction="above", percentile=90.0)

    expected = _reference_reclassify_pairs_for_event(pairs, event, thresholds)
    actual = reclassify_pairs_for_event(pairs, event, thresholds)

    # 6 clean period_key=1 rows + 2 NaN-float rows (kept, classified as "normal")
    # period_key=2 (no threshold) and code=29999 (no threshold) → dropped
    # period_key=3 rows: threshold dict has only p5=20.0, not p90 → dropped
    assert len(actual) == 8, f"expected 8 surviving rows, got {len(actual)}"
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_like=False,
    )
    # Spot-check contingency labels (rows in fixture order):
    # TP, TN, FP, FN, FN(fc=50 at thresh), FP(obs=50 at thresh),
    # FN(nan fc → normal, obs>50 → below), FP(fc>50 → below, nan obs → normal)
    assert list(actual["contingency"]) == ["TP", "TN", "FP", "FN", "FN", "FP", "FN", "FP"]
    assert list(actual["fc_class"]) == [
        "below",
        "normal",
        "below",
        "normal",
        "normal",
        "below",
        "normal",
        "below",
    ]
    assert list(actual["obs_class"]) == [
        "below",
        "normal",
        "normal",
        "below",
        "below",
        "normal",
        "below",
        "normal",
    ]


def test_reclassify_equivalence_percentile_events_below() -> None:
    """Vectorized reclassify_pairs_for_event matches reference for direction='below'."""
    from forecast_skill_eval.events import EventDef, reclassify_pairs_for_event

    pairs = _equiv_pairs()
    thresholds = {
        ("19999", "pentad", 1): {90.0: 50.0},
        ("19999", "pentad", 3): {5.0: 20.0},
    }
    event = EventDef(name="low_p5", direction="below", percentile=5.0)

    expected = _reference_reclassify_pairs_for_event(pairs, event, thresholds)
    actual = reclassify_pairs_for_event(pairs, event, thresholds)

    # Only period_key==3 rows survive (4 rows).
    # period_key=1 rows: threshold dict has only p90=50.0, not p5 → dropped.
    # period_key=2: no threshold → dropped. code=29999: no threshold → dropped.
    assert len(actual) == 4, f"expected 4 surviving rows, got {len(actual)}"
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_like=False,
    )
    # TP, TN, FN (fc==20 at threshold → normal), FP (obs==20 → normal)
    assert list(actual["contingency"]) == ["TP", "TN", "FN", "FP"]
    assert list(actual["fc_class"]) == ["below", "normal", "normal", "below"]
    assert list(actual["obs_class"]) == ["below", "normal", "below", "normal"]


def test_reclassify_equivalence_bad_period_key_dropped() -> None:
    """A non-castable period_key value causes that row to be dropped silently."""
    from forecast_skill_eval.events import EventDef, reclassify_pairs_for_event

    # Use object-dtype period_key with one bad value; all-int rows should survive.
    rows = [
        {
            "code": "19999",
            "horizon": "pentad",
            "basin": "t",
            "period_key": 1,
            "year": 2010,
            "model": "LR",
            "regime": "h",
            "season": "all",
            "lead": None,
            "issue_date": None,
            "forecast_value": 60.0,
            "observed_value": 60.0,
            "norm": 100.0,
            "norm_provenance": "c",
            "fc_class": "normal",
            "obs_class": "normal",
            "contingency": "TN",
        },
        {
            "code": "19999",
            "horizon": "pentad",
            "basin": "t",
            "period_key": "bad",
            "year": 2011,
            "model": "LR",
            "regime": "h",
            "season": "all",
            "lead": None,
            "issue_date": None,
            "forecast_value": 60.0,
            "observed_value": 60.0,
            "norm": 100.0,
            "norm_provenance": "c",
            "fc_class": "normal",
            "obs_class": "normal",
            "contingency": "TN",
        },
    ]
    pairs = pd.DataFrame(rows)
    thresholds = {("19999", "pentad", 1): {90.0: 50.0}}
    event = EventDef(name="high_p90", direction="above", percentile=90.0)

    result = reclassify_pairs_for_event(pairs, event, thresholds)

    assert len(result) == 1, "bad period_key row must be dropped"
    assert result.iloc[0]["contingency"] == "TP"


def test_reclassify_equivalence_rp_event() -> None:
    """Vectorized reclassify_pairs_for_rp_event matches reference row-wise output."""
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_rp_event

    # 20-year pairs; return level for rp5 is well-defined
    pairs = _rp_pairs(n_years=20, base=10.0, step=5.0)

    from forecast_skill_eval.events import compute_return_levels

    return_levels = compute_return_levels(pairs, return_periods=(5.0,), min_years=10)
    rp5_level = return_levels[(STATION_CODE, "pentad", 1)][5.0]

    # Mix: half clearly above, half clearly below
    above = pairs.copy()
    above["forecast_value"] = rp5_level + 100.0
    above["observed_value"] = rp5_level + 100.0

    below = pairs.copy()
    below["forecast_value"] = rp5_level - 100.0
    below["observed_value"] = rp5_level - 100.0

    mixed = pd.concat([above, below], ignore_index=True)

    event = event_by_name("rp5")

    expected = _reference_reclassify_pairs_for_rp_event(mixed, event, return_levels)
    actual = reclassify_pairs_for_rp_event(mixed, event, return_levels)

    assert len(actual) == len(expected)
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_like=False,
    )
    # Above-level half → TP; below-level half → TN
    assert (actual.iloc[: len(above)]["contingency"] == "TP").all()
    assert (actual.iloc[len(above) :]["contingency"] == "TN").all()


# ===========================================================================
# Block 10 — Norm-factor event: below_norm_100 (plain below-norm at 1.0 × norm)
# ===========================================================================


def test_below_norm_100_registered_and_not_a_default_event() -> None:
    """below_norm_100 must resolve with factor=1.0, be VALID, but not a default."""
    from forecast_skill_eval.events import (
        ALL_EVENT_NAMES,
        VALID_EVENTS,
        event_by_name,
    )

    event = event_by_name("below_norm_100")
    assert event.factor == 1.0
    assert event.direction == "below"
    assert event.percentile is None
    assert event.return_period is None
    assert "below_norm_100" in VALID_EVENTS
    assert "below_norm_100" not in ALL_EVENT_NAMES


def test_below_norm_event_reclassify_is_byte_identical() -> None:
    """REGRESSION: below_norm reclassification returns a frame-equal copy.

    Proves the 0.80 × norm output is unchanged by the new norm-factor branch:
    a hand-built pairs frame is returned identical (values and dtypes)."""
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    pairs = _obs_pairs(observed_values=[9.0, 5.0, 12.0], forecast_value=9.0)

    event = event_by_name("below_norm")
    result = reclassify_pairs_for_event(pairs, event, thresholds={})

    pd.testing.assert_frame_equal(result, pairs)


def test_below_norm_100_flips_rows_between_080_and_100_norm() -> None:
    """A value in [0.80×norm, 1.0×norm) is 'normal' at below_norm but 'below' at 100.

    norm=10 → below_norm threshold 8.0, below_norm_100 threshold 10.0.  A value
    of 9.0 sits between the two.  The two events must classify the SAME rows
    (identical row count / n_pairs), differing only in the TP/FP/FN/TN split.
    """
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    # Row 0: fc=9, obs=9  → below_norm: TN;  below_norm_100: TP
    # Row 1: fc=9, obs=5  → below_norm: FN;  below_norm_100: TP
    # Row 2: fc=9, obs=12 → below_norm: TN;  below_norm_100: FP
    pairs = _obs_pairs(observed_values=[9.0, 5.0, 12.0], forecast_value=9.0)

    bn = reclassify_pairs_for_event(pairs, event_by_name("below_norm"), thresholds={})
    bn100 = reclassify_pairs_for_event(pairs, event_by_name("below_norm_100"), thresholds={})

    # Identical row set / n_pairs — norm-factor event drops no rows here.
    assert len(bn) == len(bn100) == len(pairs)

    # below_norm leaves the in-between rows classified as normal at 0.80.
    assert list(bn["fc_class"]) == ["normal", "normal", "normal"]
    assert list(bn["obs_class"]) == ["normal", "below", "normal"]

    # below_norm_100 flips forecast to below for all three (all fc=9 < 10),
    # and observed to below wherever obs < 10.
    assert list(bn100["fc_class"]) == ["below", "below", "below"]
    assert list(bn100["obs_class"]) == ["below", "below", "normal"]
    assert list(bn100["contingency"]) == ["TP", "TP", "FP"]


def test_below_norm_100_drops_rows_with_nonpositive_or_nonfinite_norm() -> None:
    """Rows with norm <= 0 or non-finite norm are dropped (classifier → None)."""
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    pairs = _obs_pairs(observed_values=[9.0, 5.0, 12.0], forecast_value=9.0)
    # Corrupt norm on two rows: one zero, one NaN.
    pairs = pairs.copy()
    pairs.loc[1, "norm"] = 0.0
    pairs.loc[2, "norm"] = float("nan")

    result = reclassify_pairs_for_event(pairs, event_by_name("below_norm_100"), thresholds={})

    # Only the row with a finite, positive norm survives.
    assert len(result) == 1
    assert result.iloc[0]["contingency"] == "TP"
