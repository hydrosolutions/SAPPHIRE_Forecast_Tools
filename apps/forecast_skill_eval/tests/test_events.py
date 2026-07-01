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

    key = (STATION_CODE, "pentad")
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

    assert (STATION_CODE, "pentad") not in levels


def test_compute_return_levels_meets_min_years_exactly() -> None:
    """Exactly min_years distinct years must produce an entry."""
    from forecast_skill_eval.events import compute_return_levels

    pairs = _rp_pairs(n_years=10)  # exactly min_years=10
    levels = compute_return_levels(pairs, return_periods=(5.0, 10.0), min_years=10)

    assert (STATION_CODE, "pentad") in levels


def test_compute_return_levels_degenerate_constant_series_no_raise() -> None:
    """A constant observed series must not raise — yields no entry."""
    from forecast_skill_eval.events import compute_return_levels

    # All observed values identical: GEV fit is degenerate
    pairs = _rp_pairs(n_years=20, base=42.0, step=0.0)
    # No assertion error expected — function must return gracefully
    levels = compute_return_levels(pairs, return_periods=(5.0, 10.0), min_years=10)

    assert (STATION_CODE, "pentad") not in levels


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

    key = (STATION_CODE, "pentad")
    assert key in levels_single
    assert key in levels_doubled
    assert abs(levels_single[key][5.0] - levels_doubled[key][5.0]) < 1e-6


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

    key = (STATION_CODE, "pentad")
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

    key = (STATION_CODE, "pentad")
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
