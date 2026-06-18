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
