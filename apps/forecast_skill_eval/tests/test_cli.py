from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from forecast_skill_eval import cli
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.orchestrator import ResultsBundle


def test_cli_parses_args_into_one_config_and_prints_artifact_path(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    calls: dict[str, Any] = {}

    def fake_build_client(config: ForecastSkillEvalConfig) -> object:
        calls["build_config"] = config
        return object()

    def fake_run(config: ForecastSkillEvalConfig, client: object, run_id: str) -> ResultsBundle:
        calls["run_config"] = config
        calls["client"] = client
        calls["run_id"] = run_id
        return ResultsBundle(
            pairs=pd.DataFrame(),
            contingency_metrics=pd.DataFrame(),
            baselines=pd.DataFrame(),
            exclusion_ledger=ExclusionLedger(),
            horizon_summary=(),
        )

    def fake_write_artifacts(
        config: ForecastSkillEvalConfig,
        bundle: ResultsBundle,
        run_id: str,
    ) -> Path:
        calls["write_config"] = config
        calls["bundle"] = bundle
        calls["write_run_id"] = run_id
        return tmp_path / run_id

    monkeypatch.setattr(cli, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(cli, "_build_client", fake_build_client)
    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "write_artifacts", fake_write_artifacts)

    exit_code = cli.main(
        [
            "--threshold",
            "0.65",
            "--horizons",
            "decad,month",
            "--models",
            "model-a,model-b",
            "--stations",
            "19999",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--output-dir",
            str(tmp_path),
            "--provenance",
            "decad=custom-official",
            "--min-years",
            "12",
            "--operational-start",
            "2024-02-01",
            "--operational-flags",
            "10",
            "--hindcast-flags",
            "11,14",
            "--nan-exclude-flags",
            "13",
            "--error-flags",
            "12",
            "--run-id",
            "fixed-run",
        ]
    )

    assert exit_code == 0
    config = calls["run_config"]
    assert calls["build_config"] is config
    assert calls["write_config"] is config
    assert config.threshold == 0.65
    assert config.horizons == ("decade", "month")
    assert config.model_filter == ("model-a", "model-b")
    assert config.station_filter == ("19999",)
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-12-31"
    assert config.output_dir == tmp_path
    assert config.provenance_by_horizon["decade"] == "custom-official"
    assert config.min_years == 12
    assert config.operational_start == "2024-02-01"
    assert config.operational_flags == (10,)
    assert config.hindcast_flags == (11, 14)
    assert config.nan_exclude_flags == (13,)
    assert config.error_flags == (12,)
    assert calls["run_id"] == "fixed-run"
    assert calls["write_run_id"] == "fixed-run"
    assert str(tmp_path / "fixed-run") in capsys.readouterr().out


def test_cli_skips_with_message_when_api_client_is_unavailable(
    monkeypatch,
    capsys,
) -> None:
    def fail_build_client(_config: ForecastSkillEvalConfig) -> object:
        raise AssertionError("client construction should be gated")

    monkeypatch.setattr(cli, "SAPPHIRE_API_AVAILABLE", False)
    monkeypatch.setattr(cli, "_build_client", fail_build_client)

    exit_code = cli.main(["--run-id", "fixed-run"])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "SAPPHIRE API client is unavailable" in captured.err


def test_operational_issue_days_cli_arg_parses_into_config() -> None:
    parser = cli._parser()
    args = parser.parse_args(["--operational-issue-days", "1", "10", "25"])
    config = cli._config_from_args(args)
    assert config.operational_issue_days == (1, 10, 25)


def test_operational_issue_days_cli_default_is_empty() -> None:
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.operational_issue_days == ()


def test_regime_source_cli_default_is_auto() -> None:
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.regime_source == "auto"


def test_regime_source_cli_arg_parses_into_config() -> None:
    parser = cli._parser()
    for value in ("auto", "flag", "date"):
        args = parser.parse_args(["--regime-source", value])
        config = cli._config_from_args(args)
        assert config.regime_source == value


def test_short_term_gate_flags_default_off_in_cli() -> None:
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.short_term_issue_before_target is False
    assert config.short_term_dedup_one_per_target is False


def test_short_term_gate_flags_parse_into_config() -> None:
    parser = cli._parser()
    args = parser.parse_args(
        [
            "--short-term-issue-before-target",
            "--short-term-dedup-one-per-target",
        ]
    )
    config = cli._config_from_args(args)
    assert config.short_term_issue_before_target is True
    assert config.short_term_dedup_one_per_target is True


def test_forecast_only_env_enables_both_short_term_gates(monkeypatch) -> None:
    monkeypatch.setenv("SAPPHIRE_SKILL_FORECAST_ONLY", "1")
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.short_term_issue_before_target is True
    assert config.short_term_dedup_one_per_target is True


def test_forecast_only_env_unset_leaves_gates_off(monkeypatch) -> None:
    monkeypatch.delenv("SAPPHIRE_SKILL_FORECAST_ONLY", raising=False)
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.short_term_issue_before_target is False
    assert config.short_term_dedup_one_per_target is False


def test_lr_repair_cli_default_is_off(monkeypatch) -> None:
    monkeypatch.delenv("SAPPHIRE_SKILL_LR_REPAIR", raising=False)
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.short_term_lr_repair_issue_indexing is False


def test_lr_repair_cli_flag_enables_repair(monkeypatch) -> None:
    monkeypatch.delenv("SAPPHIRE_SKILL_LR_REPAIR", raising=False)
    parser = cli._parser()
    args = parser.parse_args(["--short-term-lr-repair"])
    config = cli._config_from_args(args)
    assert config.short_term_lr_repair_issue_indexing is True


def test_lr_repair_env_enables_repair(monkeypatch) -> None:
    monkeypatch.setenv("SAPPHIRE_SKILL_LR_REPAIR", "1")
    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.short_term_lr_repair_issue_indexing is True


def test_apply_season_filter_passes_prob_frames_through(tmp_path: Path) -> None:
    """_apply_season_filter must slice prob_metrics / prob_reliability on 'season'
    the same way it slices contingency_metrics and baselines."""
    import math

    from forecast_skill_eval.cli import _apply_season_filter
    from forecast_skill_eval.ledger import ExclusionLedger
    from forecast_skill_eval.orchestrator import ResultsBundle
    from forecast_skill_eval.prob_metrics import PROB_METRIC_COLUMNS, PROB_RELIABILITY_COLUMNS

    prob_row: dict[str, object] = {col: math.nan for col in PROB_METRIC_COLUMNS}
    prob_row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "irrigation",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "event": "distribution",
            "fc_grid_id": "short5",
            "n_pairs": 2,
        }
    )
    prob_row_non_irr: dict[str, object] = {**prob_row, "season": "non_irrigation"}
    prob_metrics = pd.DataFrame([prob_row, prob_row_non_irr])

    rel_row: dict[str, object] = {col: math.nan for col in PROB_RELIABILITY_COLUMNS}
    rel_row.update(
        {
            "horizon": "pentad",
            "model": "model-a",
            "regime": "all",
            "season": "irrigation",
            "code": "POOLED",
            "basin": "all",
            "norm_provenance": "all",
            "lead": None,
            "fc_grid_id": "short5",
            "nominal_level": 0.90,
            "observed_frequency": 0.88,
            "n": 2,
        }
    )
    rel_row_non_irr: dict[str, object] = {**rel_row, "season": "non_irrigation"}
    prob_reliability = pd.DataFrame([rel_row, rel_row_non_irr])

    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(),
        prob_metrics=prob_metrics,
        prob_reliability=prob_reliability,
    )

    filtered = _apply_season_filter(bundle, "irrigation")

    assert list(filtered.prob_metrics["season"]) == ["irrigation"], (
        "prob_metrics must be filtered to irrigation season"
    )
    assert list(filtered.prob_reliability["season"]) == ["irrigation"], (
        "prob_reliability must be filtered to irrigation season"
    )


def test_apply_season_filter_all_passes_prob_frames_unchanged() -> None:
    """_apply_season_filter with season='all' must return the bundle unchanged."""
    import math

    from forecast_skill_eval.cli import _apply_season_filter
    from forecast_skill_eval.ledger import ExclusionLedger
    from forecast_skill_eval.orchestrator import ResultsBundle
    from forecast_skill_eval.prob_metrics import PROB_METRIC_COLUMNS

    prob_row: dict[str, object] = {col: math.nan for col in PROB_METRIC_COLUMNS}
    prob_row.update({"season": "irrigation", "horizon": "pentad", "model": "m", "code": "POOLED"})
    prob_metrics = pd.DataFrame([prob_row])

    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(),
        prob_metrics=prob_metrics,
        prob_reliability=pd.DataFrame(),
    )

    filtered = _apply_season_filter(bundle, "all")

    # With "all", the bundle is returned unchanged — same object.
    assert filtered is bundle, "_apply_season_filter('all') must return the bundle unchanged"


def test_apply_season_filter_threads_value_frames_non_empty() -> None:
    """_apply_season_filter must thread all five Phase-4 value frames through the
    reconstruction. Season-keyed frames are sliced; season-less frames pass through
    untouched — and every frame must survive non-empty (guards a dropped field)."""
    import math

    from forecast_skill_eval.cli import _apply_season_filter
    from forecast_skill_eval.continuous_metrics import (
        CONTINUOUS_METRIC_COLUMNS,
        SEASONAL_VOLUME_COLUMNS,
        SEASONAL_VOLUME_SUMMARY_COLUMNS,
    )
    from forecast_skill_eval.economic_value import (
        ECONOMIC_VALUE_COLUMNS,
        ECONOMIC_VALUE_SUMMARY_COLUMNS,
    )
    from forecast_skill_eval.ledger import ExclusionLedger
    from forecast_skill_eval.orchestrator import ResultsBundle

    def _row(columns: tuple[str, ...], season: str | None) -> dict[str, object]:
        row: dict[str, object] = {col: math.nan for col in columns}
        row.update({"horizon": "pentad", "model": "model-a", "code": "POOLED"})
        if season is not None:
            row["season"] = season
        return row

    # Season-keyed frames: one irrigation + one non_irrigation row each.
    continuous = pd.DataFrame(
        [
            _row(CONTINUOUS_METRIC_COLUMNS, "irrigation"),
            _row(CONTINUOUS_METRIC_COLUMNS, "non_irrigation"),
        ]
    )
    economic = pd.DataFrame(
        [_row(ECONOMIC_VALUE_COLUMNS, "irrigation"), _row(ECONOMIC_VALUE_COLUMNS, "non_irrigation")]
    )
    economic_summary = pd.DataFrame(
        [
            _row(ECONOMIC_VALUE_SUMMARY_COLUMNS, "irrigation"),
            _row(ECONOMIC_VALUE_SUMMARY_COLUMNS, "non_irrigation"),
        ]
    )
    # Season-less frames: must pass through untouched (no "season" column).
    seasonal = pd.DataFrame([_row(SEASONAL_VOLUME_COLUMNS, None)])
    seasonal_summary = pd.DataFrame([_row(SEASONAL_VOLUME_SUMMARY_COLUMNS, None)])

    bundle = ResultsBundle(
        pairs=pd.DataFrame(),
        contingency_metrics=pd.DataFrame(),
        baselines=pd.DataFrame(),
        exclusion_ledger=ExclusionLedger(),
        horizon_summary=(),
        continuous_metrics=continuous,
        seasonal_volume=seasonal,
        seasonal_volume_summary=seasonal_summary,
        economic_value=economic,
        economic_value_summary=economic_summary,
    )

    filtered = _apply_season_filter(bundle, "irrigation")

    # Season-keyed frames sliced to irrigation, still non-empty.
    assert list(filtered.continuous_metrics["season"]) == ["irrigation"]
    assert list(filtered.economic_value["season"]) == ["irrigation"]
    assert list(filtered.economic_value_summary["season"]) == ["irrigation"]
    # Season-less frames threaded through non-empty (not reset to the empty default).
    assert not filtered.seasonal_volume.empty
    assert not filtered.seasonal_volume_summary.empty


def test_build_client_delegates_to_real_sapphire_clients(monkeypatch) -> None:
    constructed: list[tuple[str, str]] = []

    class FakePostprocessingClient:
        def __init__(self, *, base_url: str) -> None:
            constructed.append(("post", base_url))

        def read_short_term_forecasts(self, **kwargs: object) -> str:
            return f"short:{kwargs['horizon']}"

        def read_long_term_forecasts(self, **kwargs: object) -> str:
            return f"long:{kwargs['horizon_type']}"

    class FakePreprocessingClient:
        def __init__(self, *, base_url: str) -> None:
            constructed.append(("pre", base_url))

        def read_hydrograph(self, **kwargs: object) -> str:
            return f"hydro:{kwargs['horizon']}"

        def read_runoff(self, **kwargs: object) -> str:
            return f"runoff:{kwargs['horizon']}"

    monkeypatch.setattr(cli, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(cli, "SapphirePostprocessingClient", FakePostprocessingClient)
    monkeypatch.setattr(cli, "SapphirePreprocessingClient", FakePreprocessingClient)

    client = cli._build_client(ForecastSkillEvalConfig(base_url="https://example.test"))

    assert constructed == [
        ("post", "https://example.test"),
        ("pre", "https://example.test"),
    ]
    assert client.read_short_term_forecasts(horizon="day") == "short:day"
    assert client.read_long_term_forecasts(horizon_type="month") == "long:month"
    assert client.read_hydrograph(horizon="day") == "hydro:day"
    assert client.read_runoff(horizon="day") == "runoff:day"
