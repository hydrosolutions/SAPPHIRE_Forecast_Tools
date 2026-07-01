from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from forecast_skill_eval.artifacts import write_artifacts
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import count_contingencies
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.metrics import add_metrics
from forecast_skill_eval.orchestrator import HorizonCoverage, ResultsBundle
from forecast_skill_eval.prob_metrics import PROB_METRIC_COLUMNS, PROB_RELIABILITY_COLUMNS

STATION_CODE = "19999"


def test_artifacts_are_written_with_readable_summary_and_config(tmp_path: Path) -> None:
    config = ForecastSkillEvalConfig(
        threshold=0.75,
        horizons=["day", "month"],
        model_filter=["model-a"],
        station_filter=[STATION_CODE],
        start_date="2024-01-01",
        end_date="2024-12-31",
        output_dir=tmp_path,
        provenance_by_horizon={"day": "calculated", "month": "official"},
        min_years=12,
    )
    ledger = ExclusionLedger()
    ledger.add(stage="norm", reason="missing_norm", code=STATION_CODE, period_key=1, year=2024)
    pairs = _pairs()
    metrics = add_metrics(count_contingencies(pairs))
    bundle = ResultsBundle(
        pairs=pairs,
        contingency_metrics=metrics,
        baselines=_baselines(),
        exclusion_ledger=ledger,
        horizon_summary=(
            HorizonCoverage("day", n_pairs=3),
            HorizonCoverage("month", n_pairs=0, skipped=True, skip_reason="empty pairs"),
        ),
    )

    artifact_dir = write_artifacts(config, bundle, run_id="fixed-run")

    assert artifact_dir == tmp_path / "fixed-run"
    for name in ("pairs", "contingency_metrics", "baselines", "exclusion_ledger"):
        assert (artifact_dir / f"{name}.csv").exists()
    assert (artifact_dir / "run_config.json").exists()
    assert (artifact_dir / "summary.md").exists()

    captured = json.loads((artifact_dir / "run_config.json").read_text())
    assert captured["threshold"] == 0.75
    assert captured["horizons"] == ["day", "month"]
    assert captured["model_filter"] == ["model-a"]
    assert captured["station_filter"] == [STATION_CODE]
    assert captured["start_date"] == "2024-01-01"
    assert captured["end_date"] == "2024-12-31"
    assert captured["provenance_by_horizon"]["day"] == "calculated"
    assert captured["min_years"] == 12

    summary = (artifact_dir / "summary.md").read_text()
    assert "## Per-Horizon Coverage" in summary
    assert "| day | 3 | no |  |" in summary
    assert "| month | 0 | yes | empty pairs |" in summary
    assert "## Exclusion Ledger Totals" in summary
    assert "| norm | missing_norm | 1 |" in summary
    assert "## Headline Pooled Metrics" in summary
    # No ``event`` column in this pre-Phase-2C fixture → rows default to below_norm.
    assert "| day | below_norm | model-a | all | calculated |" in summary
    assert "HSS" in summary
    assert "PSS" in summary
    assert "HSS_undefined" in summary
    assert "station_pod_min" in summary
    assert "calculated_norm" in summary
    assert "hindcast_regime" in summary
    assert "## Norm Provenance" in summary
    assert "calculated" in summary


# --- Fix 2: headline section populated for long-term per-lead pooled rows ---


def test_headline_section_populated_for_long_term_per_lead_rows(tmp_path: Path) -> None:
    """After Fix 2, long-term pooled rows carry a concrete lead (not NaN).
    The headline section must still be populated for these rows.
    """
    config = ForecastSkillEvalConfig(
        threshold=0.75,
        horizons=["month"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
        provenance_by_horizon={"month": "official"},
    )
    # Construct metrics that simulate Fix 2 output: per-lead pooled rows, no lead=None.
    metrics = pd.DataFrame(
        [
            {
                "horizon": "month",
                "model": "model-b",
                "regime": "all",
                "code": "POOLED",
                "basin": "all",
                "norm_provenance": "all",
                "lead": 1,  # concrete lead — not None
                "TP": 1,
                "FP": 0,
                "FN": 1,
                "TN": 0,
                "n_pairs": 2,
                "pod": 0.5,
                "far": 0.0,
                "base_rate": 0.5,
                "hss": 0.0,
                "hss_undefined": False,
                "pss": 0.0,
                "pss_undefined": False,
            },
            {
                "horizon": "month",
                "model": "model-b",
                "regime": "all",
                "code": STATION_CODE,
                "basin": "other",
                "norm_provenance": "all",
                "lead": 1,
                "TP": 1,
                "FP": 0,
                "FN": 1,
                "TN": 0,
                "n_pairs": 2,
                "pod": 0.5,
                "far": 0.0,
                "base_rate": 0.5,
                "hss": 0.0,
                "hss_undefined": False,
                "pss": 0.0,
                "pss_undefined": False,
            },
        ]
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=metrics,
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("month", n_pairs=2),),
    )

    artifact_dir = write_artifacts(config, bundle, run_id="lt-lead-run")
    summary = (artifact_dir / "summary.md").read_text()

    assert "## Headline Pooled Metrics" in summary
    assert "No pooled metrics available." not in summary
    assert "| month |" in summary


# --- Phase-2C: event-aware summary sections ---


def _event_metrics_two_events() -> pd.DataFrame:
    """Metrics with two events where the same station has different POD per event.

    Station 19999 has POD=0.20 for below_norm and POD=0.90 for high_p90.  If the
    per-station distribution pooled across events, the below_norm pooled row would
    show a spread spanning both values; correct behaviour restricts to below_norm.
    """
    common = {
        "horizon": "day",
        "model": "model-a",
        "regime": "all",
        "basin": "all",
        "norm_provenance": "all",
        "lead": None,
        "TP": 1,
        "FP": 0,
        "FN": 1,
        "TN": 0,
        "n_pairs": 2,
        "far": 0.0,
        "base_rate": 0.5,
        "hss": 0.0,
        "hss_undefined": False,
        "pss": 0.0,
        "pss_undefined": False,
    }
    rows = [
        # below_norm: pooled + one station row with POD 0.20
        {**common, "code": "POOLED", "pod": 0.20, "event": "below_norm"},
        {**common, "code": STATION_CODE, "basin": "other", "pod": 0.20, "event": "below_norm"},
        # high_p90: pooled + one station row with POD 0.90
        {**common, "code": "POOLED", "pod": 0.90, "event": "high_p90"},
        {**common, "code": STATION_CODE, "basin": "other", "pod": 0.90, "event": "high_p90"},
    ]
    return pd.DataFrame(rows)


def _write_summary(tmp_path: Path, metrics: pd.DataFrame) -> str:
    config = ForecastSkillEvalConfig(
        horizons=["day"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=metrics,
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("day", n_pairs=2),),
    )
    artifact_dir = write_artifacts(config, bundle, run_id="event-run")
    return (artifact_dir / "summary.md").read_text()


def test_below_norm_station_pod_distribution_excludes_other_events(tmp_path: Path) -> None:
    """The below_norm pooled row's station POD must reflect ONLY below_norm rows.

    Proves bug #1 fixed: without the event guard, the below_norm distribution
    would pool the high_p90 station POD (0.90) alongside below_norm (0.20).
    """
    from forecast_skill_eval.artifacts import _headline_pooled_rows, _station_pod_distribution

    metrics = _event_metrics_two_events()
    pooled = _headline_pooled_rows(metrics)

    below_norm_row = pooled[pooled["event"].eq("below_norm")].iloc[0].to_dict()
    high_p90_row = pooled[pooled["event"].eq("high_p90")].iloc[0].to_dict()

    below_dist = _station_pod_distribution(metrics, below_norm_row)
    high_dist = _station_pod_distribution(metrics, high_p90_row)

    # below_norm distribution must contain only the 0.20 station POD.
    assert below_dist == {"min": 0.20, "median": 0.20, "max": 0.20}
    # high_p90 distribution must contain only the 0.90 station POD.
    assert high_dist == {"min": 0.90, "median": 0.90, "max": 0.90}


def test_summary_tables_have_event_column_and_one_row_per_event(tmp_path: Path) -> None:
    """Headline + station-distribution tables must carry an event column and label
    one row per event."""
    summary = _write_summary(tmp_path, _event_metrics_two_events())

    # Header carries the event column (placed right after horizon).
    assert "| horizon | event | model | regime | norm_provenance | lead | n_pairs |" in summary
    assert "| horizon | event | model | regime | norm_provenance | lead | min_pod |" in summary

    # One labelled pooled row per event in each table.
    assert "| day | below_norm | model-a | all | all |" in summary
    assert "| day | high_p90 | model-a | all | all |" in summary


def test_summary_event_rows_carry_distinct_station_pod(tmp_path: Path) -> None:
    """Each event's per-station POD distribution must be rendered independently."""
    summary = _write_summary(tmp_path, _event_metrics_two_events())

    # below_norm station POD (0.200) and high_p90 (0.900) both appear, per event.
    assert "0.200" in summary
    assert "0.900" in summary


# --- _station_pod_distributions precompute helper ---


def _multi_group_metrics() -> pd.DataFrame:
    """Metrics frame with two horizons, two models, two leads, two events,
    and two per-station codes so the equivalence test spans many group keys.

    Layout:
      - horizon=day,   model=model-a, lead=NaN, event=below_norm  → POD 0.30 (19999), 0.60 (29999)
      - horizon=day,   model=model-a, lead=NaN, event=high_p90    → POD 0.10 (19999), 0.80 (29999)
      - horizon=month, model=model-b, lead=1,   event=below_norm  → POD 0.50 (19999)
      - horizon=month, model=model-b, lead=2,   event=below_norm  → POD 0.70 (19999)
    Each group has a matching POOLED row.
    """
    base = {
        "regime": "all",
        "basin": "all",
        "norm_provenance": "all",
        "TP": 1,
        "FP": 0,
        "FN": 1,
        "TN": 0,
        "n_pairs": 2,
        "far": 0.0,
        "base_rate": 0.5,
        "hss": 0.0,
        "hss_undefined": False,
        "pss": 0.0,
        "pss_undefined": False,
    }
    rows = [
        # day / model-a / lead=None / below_norm
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": "POOLED",
            "pod": 0.45,
            "event": "below_norm",
        },
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": STATION_CODE,
            "pod": 0.30,
            "event": "below_norm",
        },
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": "29999",
            "pod": 0.60,
            "event": "below_norm",
        },
        # day / model-a / lead=None / high_p90
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": "POOLED",
            "pod": 0.45,
            "event": "high_p90",
        },
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": STATION_CODE,
            "pod": 0.10,
            "event": "high_p90",
        },
        {
            **base,
            "horizon": "day",
            "model": "model-a",
            "lead": None,
            "code": "29999",
            "pod": 0.80,
            "event": "high_p90",
        },
        # month / model-b / lead=1 / below_norm
        {
            **base,
            "horizon": "month",
            "model": "model-b",
            "lead": 1,
            "code": "POOLED",
            "pod": 0.50,
            "event": "below_norm",
        },
        {
            **base,
            "horizon": "month",
            "model": "model-b",
            "lead": 1,
            "code": STATION_CODE,
            "pod": 0.50,
            "event": "below_norm",
        },
        # month / model-b / lead=2 / below_norm
        {
            **base,
            "horizon": "month",
            "model": "model-b",
            "lead": 2,
            "code": "POOLED",
            "pod": 0.70,
            "event": "below_norm",
        },
        {
            **base,
            "horizon": "month",
            "model": "model-b",
            "lead": 2,
            "code": STATION_CODE,
            "pod": 0.70,
            "event": "below_norm",
        },
    ]
    return pd.DataFrame(rows)


def test_precomputed_distributions_match_per_row_function() -> None:
    """For every pooled row, the precomputed lookup must equal _station_pod_distribution."""
    from forecast_skill_eval.artifacts import (
        _distribution_key,
        _headline_pooled_rows,
        _station_pod_distribution,
        _station_pod_distributions,
    )

    metrics = _multi_group_metrics()
    distributions = _station_pod_distributions(metrics)
    pooled = _headline_pooled_rows(metrics)

    assert not pooled.empty, "fixture must have pooled rows"
    for row in pooled.to_dict("records"):
        expected = _station_pod_distribution(metrics, row)
        key = _distribution_key(row)
        actual = distributions.get(key, {})
        assert actual == expected, (
            f"Mismatch for key {key}: precomputed={actual}, per-row={expected}"
        )


def test_event_isolation_preserved_in_precomputed_distributions() -> None:
    """A station with different POD per event must yield event-specific results."""
    from forecast_skill_eval.artifacts import (
        _distribution_key,
        _headline_pooled_rows,
        _station_pod_distributions,
    )

    metrics = _multi_group_metrics()
    distributions = _station_pod_distributions(metrics)
    pooled = _headline_pooled_rows(metrics)

    below_row = (
        pooled[(pooled["horizon"] == "day") & (pooled["event"] == "below_norm")].iloc[0].to_dict()
    )
    high_row = (
        pooled[(pooled["horizon"] == "day") & (pooled["event"] == "high_p90")].iloc[0].to_dict()
    )

    below_dist = distributions.get(_distribution_key(below_row), {})
    high_dist = distributions.get(_distribution_key(high_row), {})

    # below_norm: stations 0.30 and 0.60 only
    assert abs(below_dist["min"] - 0.30) < 1e-9
    assert abs(below_dist["max"] - 0.60) < 1e-9

    # high_p90: stations 0.10 and 0.80 only (must not include below_norm values)
    assert abs(high_dist["min"] - 0.10) < 1e-9
    assert abs(high_dist["max"] - 0.80) < 1e-9


def test_precomputed_distributions_no_event_column() -> None:
    """A frame without an event column must still produce a valid lookup (below_norm key)."""
    from forecast_skill_eval.artifacts import (
        _distribution_key,
        _headline_pooled_rows,
        _station_pod_distributions,
    )

    # Strip the event column from the fixture
    metrics = _multi_group_metrics().drop(columns=["event"])
    # Re-filter to a single horizon/model/lead so the groups are clear
    metrics = metrics[metrics["horizon"] == "day"].copy()

    distributions = _station_pod_distributions(metrics)
    assert distributions, "must produce at least one group for no-event frame"

    pooled = _headline_pooled_rows(metrics)
    for row in pooled.to_dict("records"):
        key = _distribution_key(row)
        # event component of key must default to "below_norm"
        assert key[-1] == "below_norm", f"expected below_norm event in key, got {key[-1]}"
        # distribution must be present and non-empty
        assert distributions.get(key), f"missing distribution for key {key}"


def test_precomputed_distributions_nan_lead_separate_from_integer_lead() -> None:
    """NaN-lead (short-term) groups must not merge with integer-lead groups."""
    from forecast_skill_eval.artifacts import _station_pod_distributions

    base = {
        "horizon": "month",
        "model": "model-x",
        "regime": "all",
        "norm_provenance": "all",
        "event": "below_norm",
        "n_pairs": 1,
        "far": 0.0,
        "base_rate": 0.5,
        "hss": 0.0,
        "hss_undefined": False,
        "pss": 0.0,
        "pss_undefined": False,
        "TP": 1,
        "FP": 0,
        "FN": 0,
        "TN": 0,
    }
    rows = [
        # NaN lead station → POD 0.20
        {**base, "code": STATION_CODE, "lead": None, "pod": 0.20},
        # lead=1 station → POD 0.80
        {**base, "code": STATION_CODE, "lead": 1, "pod": 0.80},
        # POOLED rows (needed for groupby input; won't be grouped into distributions)
        {**base, "code": "POOLED", "lead": None, "pod": 0.20},
        {**base, "code": "POOLED", "lead": 1, "pod": 0.80},
    ]
    metrics = pd.DataFrame(rows)
    distributions = _station_pod_distributions(metrics)

    nan_lead_key = ("month", "model-x", "all", "all", None, "below_norm")
    int_lead_key = ("month", "model-x", "all", "all", 1, "below_norm")

    assert nan_lead_key in distributions, "NaN-lead group must have its own entry"
    assert int_lead_key in distributions, "integer-lead group must have its own entry"
    assert abs(distributions[nan_lead_key]["min"] - 0.20) < 1e-9
    assert abs(distributions[int_lead_key]["min"] - 0.80) < 1e-9
    # The two groups must be distinct (no cross-contamination)
    assert distributions[nan_lead_key] != distributions[int_lead_key]


def _pairs() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _pair("model-a", "TP", norm_provenance="calculated", regime="operational"),
            _pair("model-a", "FN", norm_provenance="calculated", regime="hindcast"),
            _pair("model-a", "FP", norm_provenance="official", regime="hindcast"),
            _pair("model-b", "TN", norm_provenance="official", regime="operational"),
        ]
    )


def _pair(
    model: str,
    contingency: str,
    *,
    norm_provenance: str,
    regime: str,
) -> dict[str, object]:
    return {
        "horizon": "day",
        "code": STATION_CODE,
        "basin": "other",
        "period_key": 1,
        "year": 2024,
        "model": model,
        "regime": regime,
        "lead": None,
        "issue_date": "2024-01-01",
        "forecast_value": 7.0,
        "observed_value": 7.0,
        "norm": 10.0,
        "norm_provenance": norm_provenance,
        "fc_class": "below",
        "obs_class": "below",
        "contingency": contingency,
    }


def _baselines() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "horizon": "day",
                "model": "climatology",
                "regime": "all",
                "code": "POOLED",
                "basin": "all",
                "norm_provenance": "all",
                "lead": None,
                "TP": 0,
                "FP": 0,
                "FN": 1,
                "TN": 1,
                "n_pairs": 2,
                "pod": 0.0,
                "far": 0.0,
                "baseline": "climatology",
                "comparison_model": "model-a",
                "is_proxy": False,
                "n_matched": 2,
            }
        ]
    )


# ---------------------------------------------------------------------------
# Probabilistic artifact tests
# ---------------------------------------------------------------------------


def _prob_metrics_frame() -> pd.DataFrame:
    """Minimal non-empty prob_metrics DataFrame with required columns."""
    row: dict[str, object] = {col: math.nan for col in PROB_METRIC_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "all",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "event": "distribution",
            "fc_grid_id": "short5",
            "n_pairs": 3,
            "crps": 1.5,
            "crpss": 0.2,
        }
    )
    return pd.DataFrame([row])


def _prob_reliability_frame() -> pd.DataFrame:
    """Minimal non-empty prob_reliability DataFrame with required columns."""
    row: dict[str, object] = {
        "horizon": "pentad",
        "model": "model-a",
        "regime": "all",
        "season": "all",
        "code": "POOLED",
        "basin": "all",
        "norm_provenance": "all",
        "lead": None,
        "fc_grid_id": "short5",
        "nominal_level": 0.90,
        "observed_frequency": 0.88,
        "n": 3,
    }
    return pd.DataFrame([row])


def _bundle_with_prob(tmp_path: Path) -> tuple[ForecastSkillEvalConfig, ResultsBundle]:
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("pentad", n_pairs=3),),
        prob_metrics=_prob_metrics_frame(),
        prob_reliability=_prob_reliability_frame(),
    )
    return config, bundle


def _bundle_without_prob(tmp_path: Path) -> tuple[ForecastSkillEvalConfig, ResultsBundle]:
    """Bundle with empty prob frames (flag-off default)."""
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("pentad", n_pairs=0),),
        # Default empty frames — flag OFF
    )
    return config, bundle


def test_prob_artifacts_written_when_frames_non_empty(tmp_path: Path) -> None:
    """With non-empty prob frames, prob_metrics.csv and prob_reliability.csv are written."""
    config, bundle = _bundle_with_prob(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="prob-on")

    assert (artifact_dir / "prob_metrics.csv").exists(), "prob_metrics.csv must be written"
    assert (artifact_dir / "prob_reliability.csv").exists(), "prob_reliability.csv must be written"

    written_metrics = pd.read_csv(artifact_dir / "prob_metrics.csv")
    assert list(written_metrics.columns) == list(PROB_METRIC_COLUMNS)
    assert len(written_metrics) == 1

    written_reliability = pd.read_csv(artifact_dir / "prob_reliability.csv")
    assert list(written_reliability.columns) == list(PROB_RELIABILITY_COLUMNS)
    assert len(written_reliability) == 1


def test_prob_artifacts_not_written_when_frames_empty(tmp_path: Path) -> None:
    """With empty prob frames (flag OFF), no prob CSV files are created."""
    config, bundle = _bundle_without_prob(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="prob-off")

    assert not (artifact_dir / "prob_metrics.csv").exists(), (
        "prob_metrics.csv must NOT be written when frame is empty"
    )
    assert not (artifact_dir / "prob_reliability.csv").exists(), (
        "prob_reliability.csv must NOT be written when frame is empty"
    )


def test_existing_artifacts_unaffected_by_prob_flag(tmp_path: Path) -> None:
    """Standard artifacts (pairs, contingency, baselines, ledger) are written
    regardless of whether prob frames are empty or not."""
    config, bundle = _bundle_without_prob(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="prob-off-std")

    for name in ("pairs", "contingency_metrics", "baselines", "exclusion_ledger"):
        assert (artifact_dir / f"{name}.csv").exists(), (
            f"{name}.csv must be written even when prob flag is off"
        )
    assert (artifact_dir / "run_config.json").exists()
    assert (artifact_dir / "summary.md").exists()


def test_summary_includes_prob_section_when_frame_non_empty(tmp_path: Path) -> None:
    """summary.md must include the Probabilistic Metrics section listing row counts
    and artifact names when prob_metrics is non-empty."""
    config, bundle = _bundle_with_prob(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="prob-summary")
    summary = (artifact_dir / "summary.md").read_text()

    assert "## Probabilistic Metrics" in summary
    assert "Distribution score rows: 1" in summary
    assert "prob_metrics.csv" in summary


def test_summary_prob_section_shows_not_computed_when_empty(tmp_path: Path) -> None:
    """summary.md must indicate that probabilistic metrics were not computed when
    the prob frames are empty (flag OFF)."""
    config, bundle = _bundle_without_prob(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="prob-off-summary")
    summary = (artifact_dir / "summary.md").read_text()

    assert "## Probabilistic Metrics" in summary
    assert "not computed" in summary


# ---------------------------------------------------------------------------
# Phase-4 value-metric artifacts (SAPPHIRE_SKILL_VALUE)
# ---------------------------------------------------------------------------

from forecast_skill_eval.continuous_metrics import (  # noqa: E402
    CONTINUOUS_METRIC_COLUMNS,
    SEASONAL_VOLUME_COLUMNS,
    SEASONAL_VOLUME_SUMMARY_COLUMNS,
)
from forecast_skill_eval.economic_value import (  # noqa: E402
    ECONOMIC_VALUE_COLUMNS,
    ECONOMIC_VALUE_SUMMARY_COLUMNS,
)


def _continuous_metrics_frame() -> pd.DataFrame:
    row: dict[str, object] = {col: math.nan for col in CONTINUOUS_METRIC_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "all",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "n_pairs": 12,
            "bias": 0.5,
            "mae": 1.0,
        }
    )
    return pd.DataFrame([row])


def _seasonal_volume_frame() -> pd.DataFrame:
    row: dict[str, object] = {col: math.nan for col in SEASONAL_VOLUME_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "code": STATION_CODE,
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "year": 2024,
            "n_periods": 36,
            "expected_periods": 36,
            "season_complete": True,
            "season_volume_m3_fc": 1.0e6,
            "season_volume_m3_obs": 1.1e6,
            "seasonal_volume_error": -0.09,
        }
    )
    return pd.DataFrame([row])


def _seasonal_volume_summary_frame() -> pd.DataFrame:
    row: dict[str, object] = {col: math.nan for col in SEASONAL_VOLUME_SUMMARY_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "code": STATION_CODE,
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "n_years": 1,
            "seasonal_volume_error_mean": -0.09,
            "seasonal_volume_error_median": -0.09,
        }
    )
    return pd.DataFrame([row])


def _economic_value_frame() -> pd.DataFrame:
    row: dict[str, object] = {col: math.nan for col in ECONOMIC_VALUE_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "all",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "event": "below_norm",
            "n_pairs": 12,
            "base_rate_s": 0.3,
            "hit_rate_H": 0.7,
            "pofd_F": 0.2,
            "alpha": 0.3,
            "value": 0.5,
        }
    )
    return pd.DataFrame([row])


def _economic_value_summary_frame() -> pd.DataFrame:
    row: dict[str, object] = {col: math.nan for col in ECONOMIC_VALUE_SUMMARY_COLUMNS}
    row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "all",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "event": "below_norm",
            "n_pairs": 12,
            "base_rate_s": 0.3,
            "hit_rate_H": 0.7,
            "pofd_F": 0.2,
            "v_max": 0.5,
            "alpha_star": 0.3,
        }
    )
    return pd.DataFrame([row])


def _bundle_with_value(tmp_path: Path) -> tuple[ForecastSkillEvalConfig, ResultsBundle]:
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("pentad", n_pairs=12),),
        continuous_metrics=_continuous_metrics_frame(),
        seasonal_volume=_seasonal_volume_frame(),
        seasonal_volume_summary=_seasonal_volume_summary_frame(),
        economic_value=_economic_value_frame(),
        economic_value_summary=_economic_value_summary_frame(),
    )
    return config, bundle


def _bundle_without_value(tmp_path: Path) -> tuple[ForecastSkillEvalConfig, ResultsBundle]:
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
        output_dir=tmp_path,
    )
    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(HorizonCoverage("pentad", n_pairs=0),),
        # Default empty value frames — flag OFF
    )
    return config, bundle


def test_value_artifacts_written_when_frames_non_empty(tmp_path: Path) -> None:
    """With non-empty value frames, all five value CSVs are written."""
    config, bundle = _bundle_with_value(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="value-on")

    for name in (
        "continuous_metrics",
        "seasonal_volume",
        "seasonal_volume_summary",
        "economic_value",
        "economic_value_summary",
    ):
        assert (artifact_dir / f"{name}.csv").exists(), f"{name}.csv must be written"

    written = pd.read_csv(artifact_dir / "continuous_metrics.csv")
    assert list(written.columns) == list(CONTINUOUS_METRIC_COLUMNS)


def test_value_artifacts_not_written_when_frames_empty(tmp_path: Path) -> None:
    """With empty value frames (flag OFF), no value CSV files are created."""
    config, bundle = _bundle_without_value(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="value-off")

    for name in (
        "continuous_metrics",
        "seasonal_volume",
        "seasonal_volume_summary",
        "economic_value",
        "economic_value_summary",
    ):
        assert not (artifact_dir / f"{name}.csv").exists(), (
            f"{name}.csv must NOT be written when frame is empty"
        )


def test_existing_artifacts_unaffected_by_value_flag(tmp_path: Path) -> None:
    """Standard artifacts are written regardless of the value frames."""
    config, bundle = _bundle_without_value(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="value-off-std")

    for name in ("pairs", "contingency_metrics", "baselines", "exclusion_ledger"):
        assert (artifact_dir / f"{name}.csv").exists()
    assert (artifact_dir / "run_config.json").exists()
    assert (artifact_dir / "summary.md").exists()


def test_summary_includes_value_section_when_frames_non_empty(tmp_path: Path) -> None:
    """summary.md must include the Value Metrics section with counts and filenames."""
    config, bundle = _bundle_with_value(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="value-summary")
    summary = (artifact_dir / "summary.md").read_text()

    assert "## Value Metrics" in summary
    assert "Continuous-metric groups: 1" in summary
    assert "continuous_metrics.csv" in summary
    assert "complete seasons" in summary


def test_summary_value_section_shows_not_computed_when_empty(tmp_path: Path) -> None:
    """summary.md must indicate value metrics were not computed when frames empty."""
    config, bundle = _bundle_without_value(tmp_path)
    artifact_dir = write_artifacts(config, bundle, run_id="value-off-summary")
    summary = (artifact_dir / "summary.md").read_text()

    assert "## Value Metrics" in summary
    assert "not computed" in summary
