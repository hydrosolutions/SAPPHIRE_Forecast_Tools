from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from forecast_skill_eval.cli import _SapphireClientBundle
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.orchestrator import run
from forecast_skill_eval.prob_metrics import PROB_METRIC_COLUMNS, PROB_RELIABILITY_COLUMNS

STATION_CODE = "19999"


def test_orchestrates_short_long_mix_and_records_empty_horizon(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "decade",
                "code": STATION_CODE,
                "date": "2024-01-01",
                "target": "2024-01-01",
                "horizon_in_year": 1,
                "model_type": "model-a",
                "forecasted_discharge": 7.0,
                "flag": 0,
            }
        ],
        long_forecasts_rows=[
            {
                "horizon": "month",
                "code": STATION_CODE,
                "date": "2023-12-25",  # day 25: operational issue day
                "valid_from": "2024-04-01",
                "valid_to": "2024-04-30",
                "horizon_value": 2,
                "model_type": "model-a",
                "q": 7.0,
            }
        ],
        runoff_rows=[
            {
                "horizon": "decade",
                "code": STATION_CODE,
                "horizon_in_year": 1,
                "year": 2024,
                "discharge": 7.0,
            },
            *_daily_rows(year=2024, month=4, value=7.0),
        ],
        hydrograph_rows=[
            {
                "horizon": "decade",
                "code": STATION_CODE,
                "horizon_in_year": 1,
                "norm": 10.0,
            },
            {
                "horizon": "month",
                "code": STATION_CODE,
                "horizon_in_year": 4,
                "norm": 10.0,
            },
        ],
    )
    config = ForecastSkillEvalConfig(
        horizons=["decade", "month", "pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="test-run")

    assert set(bundle.pairs["horizon"]) == {"decade", "month"}
    assert len(bundle.pairs) == 2
    assert set(bundle.pairs["regime"]) == {"operational", "hindcast"}
    assert set(bundle.contingency_metrics["model"]) == {"model-a"}
    assert {"all", "operational", "hindcast"}.issubset(set(bundle.contingency_metrics["regime"]))
    assert "climatology" in set(bundle.baselines["baseline"])
    assert bundle.exclusion_ledger.entries == ()

    summary = {coverage.horizon: coverage for coverage in bundle.horizon_summary}
    assert set(summary) == {"decade", "month", "pentad"}
    assert summary["decade"].n_pairs == 1
    assert summary["month"].n_pairs == 1
    assert summary["pentad"].n_pairs == 0
    assert summary["pentad"].skipped is True
    assert summary["pentad"].skip_reason == "empty pairs"


def test_orchestrator_builds_matched_lr_operational_proxy_baseline(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "date": "2024-01-01",
                "target": "2024-01-02",
                "horizon_in_year": 1,
                "model": "TFT",
                "forecasted_discharge": 9.0,
                "flag": 0,
            }
        ],
        lr_forecasts_rows=[
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "date": "2024-01-01",
                "horizon_in_year": 1,
                "forecasted_discharge": 7.0,
            }
        ],
        runoff_rows=[
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "horizon_in_year": 1,
                "year": 2024,
                "discharge": 7.0,
            },
        ],
        hydrograph_rows=[
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "horizon_in_year": 1,
                "norm": 10.0,
                "count": 30,
            },
        ],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="test-run")

    assert set(bundle.pairs["model"]) == {"TFT", "LR"}
    assert set(bundle.pairs["regime"]) == {"operational"}
    assert bundle.horizon_summary[0].n_pairs == 2
    proxy_rows = bundle.baselines[
        bundle.baselines["baseline"].eq("operational_proxy")
        & bundle.baselines["model"].eq("LR")
        & bundle.baselines["comparison_model"].eq("TFT")
        & bundle.baselines["regime"].eq("all")
        & bundle.baselines["season"].eq("all")  # aggregate over all seasons
        & bundle.baselines["code"].eq(STATION_CODE)
        & bundle.baselines["basin"].eq("all")
        & bundle.baselines["norm_provenance"].eq("all")
        & bundle.baselines["lead"].isna()
    ]
    assert len(proxy_rows) == 1
    proxy = proxy_rows.iloc[0]
    assert proxy["is_proxy"] is True
    assert int(proxy["n_matched"]) == 1
    assert int(proxy["n_pairs"]) == 1


def test_sapphire_client_bundle_delegates_lr_reads_to_postprocessing() -> None:
    class StubPostprocessingClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def read_lr_forecasts(self, **kwargs: object) -> dict[str, object]:
            self.calls.append(dict(kwargs))
            return {"delegated": True, **kwargs}

    postprocessing = StubPostprocessingClient()
    bundle = _SapphireClientBundle(
        postprocessing=postprocessing,
        preprocessing=object(),
    )
    kwargs: dict[str, object] = {
        "horizon": "pentad",
        "code": STATION_CODE,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "skip": 5,
        "limit": 10,
    }

    result = bundle.read_lr_forecasts(**kwargs)

    assert postprocessing.calls == [kwargs]
    assert result == {"delegated": True, **kwargs}


def test_orchestrator_skips_failed_horizon_and_continues(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = fake_client_factory()
    config = ForecastSkillEvalConfig(horizons=["pentad", "decade"])

    def fake_build_pairs(config, client, horizon):  # noqa: ANN001
        if horizon == "pentad":
            raise RuntimeError("reader unavailable")
        return build_pairs(config, client, horizon)

    from forecast_skill_eval import orchestrator
    from forecast_skill_eval.pairs import build_pairs

    monkeypatch.setattr(orchestrator, "build_pairs", fake_build_pairs)

    bundle = run(config, client, run_id="test-run")

    summary = {coverage.horizon: coverage for coverage in bundle.horizon_summary}
    assert summary["pentad"].skipped is True
    assert summary["pentad"].n_pairs == 0
    assert summary["pentad"].skip_reason == "RuntimeError: reader unavailable"
    assert summary["decade"].skipped is True
    assert summary["decade"].skip_reason == "empty pairs"
    assert bundle.exclusion_ledger.counts_by_stage_reason() == {("horizon", "horizon_error"): 1}


# ---------------------------------------------------------------------------
# Norm-factor event: below_norm_100 in the contingency pipeline
# ---------------------------------------------------------------------------


def _below_norm_pairs() -> pd.DataFrame:
    """Build a small pentad pairs frame classified at 0.80 × norm (norm=10)."""
    rows = []
    specs = [(9.0, 9.0), (9.0, 5.0), (5.0, 5.0), (9.0, 12.0)]
    for i, (fc, obs) in enumerate(specs):
        fc_class = "below" if fc < 8.0 else "normal"
        obs_class = "below" if obs < 8.0 else "normal"
        if fc_class == "below" and obs_class == "below":
            cont = "TP"
        elif fc_class == "below":
            cont = "FP"
        elif obs_class == "below":
            cont = "FN"
        else:
            cont = "TN"
        rows.append(
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "basin": "other",
                "period_key": 1,
                "year": 2010 + i,
                "model": "model-a",
                "regime": "hindcast",
                "season": "irrigation",
                "lead": None,
                "issue_date": None,
                "forecast_value": fc,
                "observed_value": obs,
                "norm": 10.0,
                "norm_provenance": "calculated",
                "fc_class": fc_class,
                "obs_class": obs_class,
                "contingency": cont,
            }
        )
    return pd.DataFrame(rows)


def test_compute_event_contingencies_includes_below_norm_100_when_requested() -> None:
    """With both events requested, both appear and share identical n_pairs per group,
    while below_norm rows are unchanged vs a below_norm-only run."""
    from forecast_skill_eval.orchestrator import _compute_event_contingencies

    pairs = _below_norm_pairs()

    both = _compute_event_contingencies(pairs, {}, ("below_norm", "below_norm_100"))
    bn_only = _compute_event_contingencies(pairs, {}, ("below_norm",))

    # Both events present in the combined output.
    assert set(both["event"].unique()) == {"below_norm", "below_norm_100"}

    group_keys = [
        "horizon",
        "model",
        "regime",
        "season",
        "code",
        "basin",
        "norm_provenance",
        "lead",
    ]

    bn_rows = both[both["event"] == "below_norm"].reset_index(drop=True)
    bn100_rows = both[both["event"] == "below_norm_100"].reset_index(drop=True)

    # Identical n_pairs per matching group (same row set, differing only in split).
    merged = bn_rows.merge(
        bn100_rows,
        on=group_keys,
        suffixes=("_bn", "_bn100"),
        how="inner",
    )
    assert len(merged) == len(bn_rows)
    assert (merged["n_pairs_bn"] == merged["n_pairs_bn100"]).all()

    # below_norm rows are byte-identical whether or not below_norm_100 is requested.
    bn_only_rows = bn_only[bn_only["event"] == "below_norm"].reset_index(drop=True)
    pd.testing.assert_frame_equal(bn_rows, bn_only_rows)


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


# ---------------------------------------------------------------------------
# SAPPHIRE_SKILL_PROB flag tests
# ---------------------------------------------------------------------------

_SECOND_STATION = "29999"

# A pentad forecast row that carries a quantile band so the pair is scorable.
_FORECAST_WITH_BAND = {
    "horizon": "pentad",
    "code": STATION_CODE,
    "date": "2024-01-01",
    "target": "2024-01-02",
    "horizon_in_year": 1,
    "model_type": "model-a",
    "forecasted_discharge": 7.0,
    "q05": 4.0,
    "q25": 6.0,
    "q75": 8.0,
    "q95": 10.0,
    "flag": 0,
}

_RUNOFF_PENTAD = {
    "horizon": "pentad",
    "code": STATION_CODE,
    "horizon_in_year": 1,
    "year": 2024,
    "discharge": 7.0,
}

_HYDROGRAPH_PENTAD = {
    "horizon": "pentad",
    "code": STATION_CODE,
    "horizon_in_year": 1,
    "norm": 10.0,
    "count": 30,
}


def test_prob_metrics_populated_when_flag_on(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With SAPPHIRE_SKILL_PROB=1, prob_metrics and prob_reliability are non-empty
    and carry the expected column schema."""
    monkeypatch.setenv("SAPPHIRE_SKILL_PROB", "1")

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="prob-on")

    # Frames must be non-empty.
    assert not bundle.prob_metrics.empty, "prob_metrics must be non-empty when flag is ON"
    assert not bundle.prob_reliability.empty, "prob_reliability must be non-empty when flag is ON"

    # All required columns must be present.
    for col in PROB_METRIC_COLUMNS:
        assert col in bundle.prob_metrics.columns, f"prob_metrics missing column '{col}'"
    for col in PROB_RELIABILITY_COLUMNS:
        assert col in bundle.prob_reliability.columns, f"prob_reliability missing column '{col}'"

    # Distribution event rows must be present.
    assert "distribution" in bundle.prob_metrics["event"].values, (
        "prob_metrics must contain 'distribution' event rows"
    )


def test_prob_metrics_empty_when_flag_off(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With SAPPHIRE_SKILL_PROB absent (default OFF), both prob frames are empty
    and existing contingency/baseline outputs are identical to the flag-off baseline."""
    monkeypatch.delenv("SAPPHIRE_SKILL_PROB", raising=False)

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="prob-off")

    # Both prob frames must be empty when flag is off.
    assert bundle.prob_metrics.empty, "prob_metrics must be empty when flag is OFF"
    assert bundle.prob_reliability.empty, "prob_reliability must be empty when flag is OFF"

    # Existing contingency and baseline outputs must still be populated.
    assert not bundle.contingency_metrics.empty, (
        "contingency_metrics must remain populated when flag is OFF"
    )
    assert not bundle.baselines.empty, "baselines must remain populated when flag is OFF"
    assert set(bundle.contingency_metrics["model"]) == {"model-a"}, (
        "contingency_metrics model set must be unchanged"
    )
    assert "climatology" in set(bundle.baselines["baseline"]), (
        "climatology baseline must be present when flag is OFF"
    )


def test_prob_flag_truthiness_accepts_true_string(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SAPPHIRE_SKILL_PROB='true' (case-insensitive) is treated as enabled."""
    monkeypatch.setenv("SAPPHIRE_SKILL_PROB", "true")

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="prob-true-str")
    assert not bundle.prob_metrics.empty, "SAPPHIRE_SKILL_PROB='true' must enable prob metrics"


def test_prob_flag_off_leaves_contingency_byte_identical(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag OFF must produce the same contingency/baseline outputs as a baseline run.

    Runs the orchestrator twice (both times flag OFF) against identical inputs
    and asserts the contingency DataFrame's model and code columns are identical,
    proving the flag pathway has zero effect on the existing pipeline.
    """
    monkeypatch.delenv("SAPPHIRE_SKILL_PROB", raising=False)

    client_a = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    client_b = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle_a = run(config, client_a, run_id="run-a")
    bundle_b = run(config, client_b, run_id="run-b")

    # Both contingency frames must have the same shape and model/code values.
    assert bundle_a.contingency_metrics.shape == bundle_b.contingency_metrics.shape
    assert sorted(bundle_a.contingency_metrics["model"].tolist()) == sorted(
        bundle_b.contingency_metrics["model"].tolist()
    )
    assert bundle_a.prob_metrics.empty
    assert bundle_b.prob_metrics.empty


# ---------------------------------------------------------------------------
# SAPPHIRE_SKILL_VALUE flag tests (Phase-4 value metrics)
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


def test_value_metrics_populated_when_flag_on(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With SAPPHIRE_SKILL_VALUE=1, continuous_metrics and economic_value are
    non-empty and carry the expected column schema."""
    monkeypatch.setenv("SAPPHIRE_SKILL_VALUE", "1")

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="value-on")

    assert not bundle.continuous_metrics.empty, (
        "continuous_metrics must be non-empty when flag is ON"
    )
    assert not bundle.economic_value.empty, "economic_value must be non-empty when flag is ON"
    for col in CONTINUOUS_METRIC_COLUMNS:
        assert col in bundle.continuous_metrics.columns, (
            f"continuous_metrics missing column '{col}'"
        )
    for col in ECONOMIC_VALUE_COLUMNS:
        assert col in bundle.economic_value.columns, f"economic_value missing column '{col}'"
    # A single pentad pair is below the variance-metric floor → ledger records it.
    reasons = {reason for (_stage, reason) in bundle.exclusion_ledger.counts_by_stage_reason()}
    stages = {stage for (stage, _reason) in bundle.exclusion_ledger.counts_by_stage_reason()}
    assert "value" in stages
    assert "min_pairs_gate" in reasons


def test_value_metrics_empty_when_flag_off(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With SAPPHIRE_SKILL_VALUE absent (default OFF), all five value frames are
    empty with the correct columns and existing outputs remain populated."""
    monkeypatch.delenv("SAPPHIRE_SKILL_VALUE", raising=False)

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="value-off")

    assert bundle.continuous_metrics.empty
    assert bundle.seasonal_volume.empty
    assert bundle.seasonal_volume_summary.empty
    assert bundle.economic_value.empty
    assert bundle.economic_value_summary.empty
    assert list(bundle.continuous_metrics.columns) == list(CONTINUOUS_METRIC_COLUMNS)
    assert list(bundle.seasonal_volume.columns) == list(SEASONAL_VOLUME_COLUMNS)
    assert list(bundle.seasonal_volume_summary.columns) == list(SEASONAL_VOLUME_SUMMARY_COLUMNS)
    assert list(bundle.economic_value.columns) == list(ECONOMIC_VALUE_COLUMNS)
    assert list(bundle.economic_value_summary.columns) == list(ECONOMIC_VALUE_SUMMARY_COLUMNS)

    # Existing outputs unaffected by the (off) value flag.
    assert not bundle.contingency_metrics.empty
    assert not bundle.baselines.empty
    # No value-stage ledger entries when the flag is off.
    stages = {stage for (stage, _reason) in bundle.exclusion_ledger.counts_by_stage_reason()}
    assert "value" not in stages


def test_value_flag_off_leaves_contingency_byte_identical(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag OFF must leave contingency/baseline outputs identical to a baseline run."""
    monkeypatch.delenv("SAPPHIRE_SKILL_VALUE", raising=False)

    client_a = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    client_b = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle_a = run(config, client_a, run_id="value-off-a")
    bundle_b = run(config, client_b, run_id="value-off-b")

    pd.testing.assert_frame_equal(bundle_a.contingency_metrics, bundle_b.contingency_metrics)
    pd.testing.assert_frame_equal(bundle_a.baselines, bundle_b.baselines)
    assert bundle_a.continuous_metrics.empty
    assert bundle_b.economic_value.empty


def test_value_flag_truthiness_accepts_true_string(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SAPPHIRE_SKILL_VALUE='true' (case-insensitive) is treated as enabled."""
    monkeypatch.setenv("SAPPHIRE_SKILL_VALUE", "true")

    client = fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
    )

    bundle = run(config, client, run_id="value-true-str")
    assert not bundle.continuous_metrics.empty, (
        "SAPPHIRE_SKILL_VALUE='true' must enable value metrics"
    )


# ---------------------------------------------------------------------------
# P1b: below_norm_100 full-parity (economic value / baselines / prob) invariant
# ---------------------------------------------------------------------------


def _band_client(fake_client_factory):
    return fake_client_factory(
        forecasts_rows=[_FORECAST_WITH_BAND],
        runoff_rows=[_RUNOFF_PENTAD],
        hydrograph_rows=[_HYDROGRAPH_PENTAD],
    )


def test_economic_value_includes_below_norm_100_when_requested(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SAPPHIRE_SKILL_VALUE", "1")
    config = ForecastSkillEvalConfig(
        horizons=["pentad"],
        station_filter=[STATION_CODE],
        events_filter=("below_norm", "below_norm_100"),
    )
    bundle = run(config, _band_client(fake_client_factory), run_id="ev-100")

    assert {"below_norm", "below_norm_100"} <= set(bundle.economic_value["event"].unique())
    assert {"below_norm", "below_norm_100"} <= set(bundle.economic_value_summary["event"].unique())
    # 0.80 rows are ordered first (deterministic).
    events_in_order = bundle.economic_value["event"].tolist()
    first_100 = events_in_order.index("below_norm_100")
    assert all(e == "below_norm" for e in events_in_order[:first_100])


def test_below_norm_100_is_purely_additive_across_all_frames(
    fake_client_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ABSOLUTE INVARIANT: adding below_norm_100 leaves the below_norm slice
    of every output frame (contingency, prob, economic value + summary,
    baselines) byte-identical to a below_norm-only run."""
    monkeypatch.setenv("SAPPHIRE_SKILL_PROB", "1")
    monkeypatch.setenv("SAPPHIRE_SKILL_VALUE", "1")

    only = run(
        ForecastSkillEvalConfig(
            horizons=["pentad"],
            station_filter=[STATION_CODE],
            events_filter=("below_norm",),
        ),
        _band_client(fake_client_factory),
        run_id="only",
    )
    both = run(
        ForecastSkillEvalConfig(
            horizons=["pentad"],
            station_filter=[STATION_CODE],
            events_filter=("below_norm", "below_norm_100"),
        ),
        _band_client(fake_client_factory),
        run_id="both",
    )

    def _bn(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["event"] == "below_norm"].reset_index(drop=True)

    def _nonfactor_prob(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["event"].isin(["distribution", "below_norm"])].reset_index(drop=True)

    # Byte-identical below_norm slice across all four frame families.
    pd.testing.assert_frame_equal(_bn(only.contingency_metrics), _bn(both.contingency_metrics))
    pd.testing.assert_frame_equal(_bn(only.baselines), _bn(both.baselines))
    pd.testing.assert_frame_equal(_bn(only.economic_value), _bn(both.economic_value))
    pd.testing.assert_frame_equal(
        _bn(only.economic_value_summary), _bn(both.economic_value_summary)
    )
    pd.testing.assert_frame_equal(
        _nonfactor_prob(only.prob_metrics), _nonfactor_prob(both.prob_metrics)
    )

    # And the both-run actually carries the new below_norm_100 rows everywhere.
    assert "below_norm_100" in set(both.contingency_metrics["event"])
    assert "below_norm_100" in set(both.baselines["event"])
    assert "below_norm_100" in set(both.economic_value["event"])
    assert "below_norm_100" in set(both.economic_value_summary["event"])
    assert "below_norm_100" in set(both.prob_metrics["event"])
