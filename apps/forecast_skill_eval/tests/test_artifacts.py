from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from forecast_skill_eval.artifacts import write_artifacts
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import count_contingencies
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.metrics import add_metrics
from forecast_skill_eval.orchestrator import HorizonCoverage, ResultsBundle

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
