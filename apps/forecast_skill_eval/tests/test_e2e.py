from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from forecast_skill_eval.artifacts import write_artifacts
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.orchestrator import run

STATION_CODE = "19999"


def test_e2e_fake_client_multi_horizon_artifacts(
    fake_client_factory,
    tmp_path: Path,
) -> None:
    client = fake_client_factory(
        forecasts_rows=_pentad_forecasts(),
        long_forecasts_rows=_month_forecasts(),
        hydrograph_rows=_hydrograph_norms(),
        runoff_rows=[
            *_pentad_observed_history(),
            *_daily_rows(year=2024, month=4, value=90.0),
        ],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad", "month"],
        model_filter=["ML", "LR", "LR_Base"],
        station_filter=[STATION_CODE],
        start_date="2010-01-01",
        end_date="2024-12-31",
        output_dir=tmp_path,
        provenance_by_horizon={"pentad": "calculated", "month": "official"},
        min_years=3,
    )

    bundle = run(config, client, run_id="fake-client-e2e")
    artifact_dir = write_artifacts(config, bundle, run_id="fake-client-e2e")

    assert artifact_dir == tmp_path / "fake-client-e2e"
    assert set(bundle.pairs["horizon"]) == {"pentad", "month"}
    assert _n_pairs(bundle.pairs, "pentad") == 4
    assert _n_pairs(bundle.pairs, "month") == 3

    ledger_counts = bundle.exclusion_ledger.counts_by_stage_reason()
    assert ledger_counts[("pair", "forecast_missing")] == 1
    assert ledger_counts[("pair", "observed_unmatched")] == 1
    assert ledger_counts[("pair", "forecast_rolling_window")] == 1

    assert set(bundle.pairs["norm_provenance"]) == {"calculated", "official"}
    assert (
        _n_pairs(
            bundle.pairs[
                bundle.pairs["horizon"].eq("pentad")
                & bundle.pairs["norm_provenance"].eq("calculated")
            ],
            "pentad",
        )
        == 4
    )
    assert (
        _n_pairs(
            bundle.pairs[
                bundle.pairs["horizon"].eq("month") & bundle.pairs["norm_provenance"].eq("official")
            ],
            "month",
        )
        == 3
    )

    metrics = bundle.contingency_metrics
    assert _has_metric_row(metrics, horizon="pentad", code=STATION_CODE, provenance="all")
    assert _has_metric_row(metrics, horizon="pentad", code="POOLED", provenance="all")
    assert _has_metric_row(metrics, horizon="month", code=STATION_CODE, provenance="all")
    assert _has_metric_row(metrics, horizon="month", code="POOLED", provenance="all")
    assert _has_metric_row(metrics, horizon="pentad", code="POOLED", provenance="calculated")
    assert _has_metric_row(metrics, horizon="month", code="POOLED", provenance="official")

    proxy_rows = bundle.baselines[
        bundle.baselines["baseline"].eq("operational_proxy") & bundle.baselines["is_proxy"].eq(True)
    ]
    assert not proxy_rows.empty
    assert proxy_rows["n_matched"].astype(int).gt(0).all()

    for name in ("pairs", "contingency_metrics", "baselines", "exclusion_ledger"):
        assert (artifact_dir / f"{name}.csv").exists()
    assert (artifact_dir / "run_config.json").exists()
    assert (artifact_dir / "summary.md").exists()

    written_config = json.loads((artifact_dir / "run_config.json").read_text())
    assert written_config["horizons"] == ["pentad", "month"]
    assert written_config["station_filter"] == [STATION_CODE]
    assert written_config["provenance_by_horizon"]["pentad"] == "calculated"
    assert written_config["provenance_by_horizon"]["month"] == "official"

    written_pairs = pd.read_csv(artifact_dir / "pairs.csv")
    assert set(written_pairs["horizon"]) == {"pentad", "month"}

    written_ledger = pd.read_csv(artifact_dir / "exclusion_ledger.csv")
    assert {
        "forecast_missing",
        "observed_unmatched",
        "forecast_rolling_window",
    }.issubset(set(written_ledger["reason"]))

    summary = (artifact_dir / "summary.md").read_text()
    for heading in (
        "## Per-Horizon Coverage",
        "## Exclusion Ledger Totals",
        "## Headline Pooled Metrics",
        "## Per-Station POD Distribution",
        "## Norm Provenance",
    ):
        assert heading in summary
    assert "| pentad | 4 | no |  |" in summary
    assert "| month | 3 | no |  |" in summary
    assert "| pair | forecast_missing | 1 |" in summary
    assert "| pair | observed_unmatched | 1 |" in summary
    assert "| pair | forecast_rolling_window | 1 |" in summary
    assert "| pentad | calculated | 4 |" in summary
    assert "| month | official | 3 |" in summary


def _pentad_forecasts() -> list[dict[str, object]]:
    return [
        _short_forecast(target="2024-01-01", model="ML", value=70.0),
        _short_forecast(target="2024-01-01", model="LR", value=90.0),
        _short_forecast(target="2024-01-06", model="ML", value=70.0),
        _short_forecast(target="2024-01-06", model="LR", value=70.0),
        _short_forecast(target="2024-01-11", model="ML", value=None),
        _short_forecast(target="2024-01-16", model="ML", value=70.0),
    ]


def _month_forecasts() -> list[dict[str, object]]:
    return [
        _long_forecast(
            valid_from="2024-04-01",
            valid_to="2024-04-30",
            model="ML",
            lead=2,
            value=70.0,
        ),
        _long_forecast(
            valid_from="2024-04-01",
            valid_to="2024-04-30",
            model="LR_Base",
            lead=2,
            value=90.0,
        ),
        _long_forecast(
            valid_from="2024-04-01",
            valid_to="2024-04-30",
            model="LR_Base",
            lead=1,
            value=70.0,
        ),
        _long_forecast(
            valid_from="2024-04-15",
            valid_to="2024-05-14",
            model="ML",
            lead=3,
            value=70.0,
        ),
    ]


def _short_forecast(*, target: str, model: str, value: float | None) -> dict[str, object]:
    return {
        "horizon": "pentad",
        "code": STATION_CODE,
        "target": target,
        "horizon_in_year": int(target[8:10]),
        "date": "2023-12-25",
        "model": model,
        "forecasted_discharge": value,
    }


def _long_forecast(
    *,
    valid_from: str,
    valid_to: str,
    model: str,
    lead: int,
    value: float,
) -> dict[str, object]:
    return {
        "horizon": "month",
        "code": STATION_CODE,
        "date": "2024-02-01",
        "valid_from": valid_from,
        "valid_to": valid_to,
        "horizon_value": lead,
        "model": model,
        "q": value,
    }


def _hydrograph_norms() -> list[dict[str, object]]:
    return [
        {
            "horizon": "month",
            "code": STATION_CODE,
            "horizon_in_year": 4,
            "norm": 100.0,
        }
    ]


def _pentad_observed_history() -> list[dict[str, object]]:
    rows = [
        _short_observed(year=2024, period=1, value=70.0),
        _short_observed(year=2024, period=6, value=90.0),
        _short_observed(year=2024, period=11, value=70.0),
    ]
    for year in (2021, 2022, 2023):
        for period in (1, 6, 11, 16):
            rows.append(_short_observed(year=year, period=period, value=100.0))
    return rows


def _short_observed(*, year: int, period: int, value: float) -> dict[str, object]:
    return {
        "horizon": "pentad",
        "code": STATION_CODE,
        "horizon_in_year": period,
        "year": year,
        "discharge": value,
    }


def _daily_rows(*, year: int, month: int, value: float) -> list[dict[str, object]]:
    rows = []
    day = date(year, month, 1)
    while day.month == month:
        rows.append(
            {
                "horizon": "day",
                "code": STATION_CODE,
                "date": day.isoformat(),
                "discharge": value,
            }
        )
        day += timedelta(days=1)
    return rows


def _n_pairs(pairs: pd.DataFrame, horizon: str) -> int:
    return int(pairs[pairs["horizon"].eq(horizon)].shape[0])


def _has_metric_row(
    metrics: pd.DataFrame,
    *,
    horizon: str,
    code: str,
    provenance: str,
) -> bool:
    return bool(
        (
            metrics["horizon"].eq(horizon)
            & metrics["code"].eq(code)
            & metrics["norm_provenance"].eq(provenance)
        ).any()
    )
