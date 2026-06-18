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
    assert "| day | model-a | all | calculated |" in summary
    assert "HSS" in summary
    assert "PSS" in summary
    assert "HSS_undefined" in summary
    assert "station_pod_min" in summary
    assert "calculated_norm" in summary
    assert "hindcast_regime" in summary
    assert "## Norm Provenance" in summary
    assert "calculated" in summary


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
