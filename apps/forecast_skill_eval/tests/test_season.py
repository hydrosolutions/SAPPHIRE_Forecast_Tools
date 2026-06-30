"""Tests for Phase-2A: seasonal disaggregation (irrigation Apr–Sep).

Tests cover:
- Season label derivation per horizon and period_key
- Contingency emits season strata (all / irrigation / non_irrigation)
- CLI --season flag filters output rows
"""

from __future__ import annotations

import pandas as pd
import pytest

from forecast_skill_eval.contingency import count_contingencies
from forecast_skill_eval.pairs import _season_label

STATION_CODE = "19999"

# ---------------------------------------------------------------------------
# Season label derivation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("horizon", "period_key", "year", "expected"),
    [
        # month horizon: period_key IS the month
        ("month", 4, 2024, "irrigation"),  # April
        ("month", 7, 2024, "irrigation"),  # July
        ("month", 9, 2024, "irrigation"),  # September
        ("month", 1, 2024, "non_irrigation"),  # January
        ("month", 3, 2024, "non_irrigation"),  # March
        ("month", 10, 2024, "non_irrigation"),  # October
        ("month", 12, 2024, "non_irrigation"),  # December
        # quarter horizon: Q2=Apr-Jun(irrigation), Q3=Jul-Sep(irrigation)
        ("quarter", 2, 2024, "irrigation"),  # Q2
        ("quarter", 3, 2024, "irrigation"),  # Q3
        ("quarter", 1, 2024, "non_irrigation"),  # Q1
        ("quarter", 4, 2024, "non_irrigation"),  # Q4
        # season horizon: always covers Apr-Sep → irrigation
        ("season", 1, 2024, "irrigation"),
        # day horizon: derived from day-of-year
        # 2023 is a non-leap year: Jan=31, Feb=28, Mar=31 → days 1-90; Apr 1 = day 91
        # 2024 is a leap year: Jan=31, Feb=29, Mar=31 → days 1-91; Apr 1 = day 92
        ("day", 91, 2023, "irrigation"),   # Apr 1 in non-leap year 2023
        ("day", 92, 2024, "irrigation"),   # Apr 1 in leap year 2024
        ("day", 273, 2024, "irrigation"),  # Sep 30 in leap year 2024
        ("day", 1, 2024, "non_irrigation"),   # Jan 1
        ("day", 31, 2024, "non_irrigation"),  # Jan 31
        ("day", 91, 2024, "non_irrigation"),  # Mar 31 in leap year 2024
        ("day", 365, 2023, "non_irrigation"),  # Dec 31 in non-leap year
        # pentad horizon: 6 pentads per month; pentad 19=start of Apr
        ("pentad", 19, 2024, "irrigation"),   # Apr (month 4)
        ("pentad", 36, 2024, "irrigation"),   # Jun (month 6)
        ("pentad", 37, 2024, "irrigation"),   # Jul (month 7)
        ("pentad", 54, 2024, "irrigation"),   # Sep (month 9)
        ("pentad", 1, 2024, "non_irrigation"),   # Jan
        ("pentad", 18, 2024, "non_irrigation"),  # Mar
        ("pentad", 55, 2024, "non_irrigation"),  # Oct
        ("pentad", 72, 2024, "non_irrigation"),  # Dec
        # decade horizon: 3 decades per month; decade 10=start of Apr
        ("decade", 10, 2024, "irrigation"),   # Apr (month 4)
        ("decade", 27, 2024, "irrigation"),   # Sep (month 9)
        ("decade", 1, 2024, "non_irrigation"),   # Jan
        ("decade", 9, 2024, "non_irrigation"),   # Mar
        ("decade", 28, 2024, "non_irrigation"),  # Oct
        ("decade", 36, 2024, "non_irrigation"),  # Dec
    ],
)
def test_season_label_derivation(
    horizon: str,
    period_key: int,
    year: int,
    expected: str,
) -> None:
    assert _season_label(horizon, period_key, year) == expected


def test_season_label_returns_non_irrigation_for_unknown_horizon() -> None:
    result = _season_label("unsupported_horizon", 1, 2024)
    assert result in ("irrigation", "non_irrigation")


# ---------------------------------------------------------------------------
# Pairs carry a season column
# ---------------------------------------------------------------------------


def test_pairs_dataframe_has_season_column(fake_client_factory) -> None:
    """build_pairs must emit a 'season' column on the pairs DataFrame."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig
    from forecast_skill_eval.pairs import build_pairs

    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "date": "2024-01-01",
                "target": "2024-04-01",  # April → irrigation
                "horizon_in_year": 92,   # day 92 = Apr 1 in leap year 2024
                "model_type": "model-a",
                "forecasted_discharge": 7.0,
            }
        ],
        runoff_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "horizon_in_year": 92,
                "year": 2024,
                "discharge": 7.0,
            }
        ],
        hydrograph_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "horizon_in_year": 92,
                "norm": 10.0,
                "count": 30,
            }
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, _ = build_pairs(config, client, "day")

    assert "season" in pairs.columns
    # day 92 in 2024 (leap year) = April 1 → irrigation
    assert pairs.iloc[0]["season"] == "irrigation"


def test_pairs_january_day_gets_non_irrigation(fake_client_factory) -> None:
    """A January target period must get season=non_irrigation."""
    from forecast_skill_eval.config import ForecastSkillEvalConfig
    from forecast_skill_eval.pairs import build_pairs

    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "date": "2024-01-01",
                "target": "2024-01-15",  # January → non_irrigation
                "horizon_in_year": 15,
                "model_type": "model-a",
                "forecasted_discharge": 7.0,
            }
        ],
        runoff_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "horizon_in_year": 15,
                "year": 2024,
                "discharge": 7.0,
            }
        ],
        hydrograph_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "horizon_in_year": 15,
                "norm": 10.0,
                "count": 30,
            }
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, _ = build_pairs(config, client, "day")

    assert "season" in pairs.columns
    assert pairs.iloc[0]["season"] == "non_irrigation"


# ---------------------------------------------------------------------------
# Contingency emits season strata
# ---------------------------------------------------------------------------


def _season_pair(
    horizon: str,
    model: str,
    code: str,
    provenance: str,
    contingency_label: str,
    season: str,
    *,
    lead: int | None = None,
    regime: str = "operational",
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": code,
        "basin": "other",
        "period_key": 1,
        "year": 2024,
        "model": model,
        "regime": regime,
        "lead": lead,
        "norm_provenance": provenance,
        "season": season,
        "contingency": contingency_label,
    }


def test_contingency_emits_season_all_plus_each_stratum() -> None:
    """count_contingencies must emit rows for season=all, irrigation, non_irrigation."""
    pairs = pd.DataFrame(
        [
            _season_pair("day", "model-a", STATION_CODE, "calculated", "TP", "irrigation"),
            _season_pair("day", "model-a", STATION_CODE, "calculated", "FN", "non_irrigation"),
        ]
    )

    counts = count_contingencies(pairs)

    seasons_in_output = set(counts["season"].unique())
    assert "all" in seasons_in_output
    assert "irrigation" in seasons_in_output
    assert "non_irrigation" in seasons_in_output


def test_contingency_season_all_pools_both_seasons() -> None:
    """season='all' row must pool pairs from irrigation and non_irrigation."""
    pairs = pd.DataFrame(
        [
            _season_pair("day", "model-a", STATION_CODE, "calculated", "TP", "irrigation"),
            _season_pair("day", "model-a", STATION_CODE, "calculated", "FN", "non_irrigation"),
        ]
    )

    counts = count_contingencies(pairs)

    all_pooled = counts[
        (counts["code"] == "POOLED")
        & (counts["norm_provenance"] == "all")
        & (counts["regime"] == "all")
        & (counts["season"] == "all")
        & (counts["basin"] == "all")
    ]
    assert len(all_pooled) == 1
    row = all_pooled.iloc[0]
    assert int(row["n_pairs"]) == 2
    assert int(row["TP"]) == 1
    assert int(row["FN"]) == 1


def test_contingency_irrigation_slice_only_contains_irrigation_pairs() -> None:
    """The irrigation season row must only count irrigation-season pairs."""
    # Use "calculated" as provenance (not "all" which is the aggregate sentinel).
    pairs = pd.DataFrame(
        [
            _season_pair("day", "model-a", STATION_CODE, "calculated", "TP", "irrigation"),
            _season_pair("day", "model-a", STATION_CODE, "calculated", "FP", "irrigation"),
            _season_pair("day", "model-a", STATION_CODE, "calculated", "FN", "non_irrigation"),
        ]
    )

    counts = count_contingencies(pairs)

    irr_pooled = counts[
        (counts["code"] == "POOLED")
        & (counts["norm_provenance"] == "all")
        & (counts["regime"] == "all")
        & (counts["season"] == "irrigation")
        & (counts["basin"] == "all")
    ]
    assert len(irr_pooled) == 1
    row = irr_pooled.iloc[0]
    assert int(row["n_pairs"]) == 2
    assert int(row["TP"]) == 1
    assert int(row["FP"]) == 1
    assert int(row["FN"]) == 0


def test_contingency_output_has_season_column() -> None:
    """count_contingencies output must always include the season column."""
    pairs = pd.DataFrame(
        [
            _season_pair("day", "model-a", STATION_CODE, "calculated", "TP", "irrigation"),
        ]
    )

    counts = count_contingencies(pairs)

    assert "season" in counts.columns


# ---------------------------------------------------------------------------
# CLI --season flag
# ---------------------------------------------------------------------------


def test_cli_season_arg_parses_into_config() -> None:
    from forecast_skill_eval import cli

    parser = cli._parser()
    args = parser.parse_args(["--season", "irrigation"])
    config = cli._config_from_args(args)
    assert config.season_filter == "irrigation"


def test_cli_season_default_is_all() -> None:
    from forecast_skill_eval import cli

    parser = cli._parser()
    args = parser.parse_args([])
    config = cli._config_from_args(args)
    assert config.season_filter == "all"


def test_cli_season_filter_restricts_contingency_output(
    monkeypatch,
    tmp_path,
) -> None:
    """--season irrigation must restrict output to season=irrigation rows only."""
    import pandas as pd

    from forecast_skill_eval import cli
    from forecast_skill_eval.config import ForecastSkillEvalConfig
    from forecast_skill_eval.ledger import ExclusionLedger
    from forecast_skill_eval.orchestrator import ResultsBundle

    captured_bundle: dict[str, ResultsBundle] = {}

    def fake_build_client(config: ForecastSkillEvalConfig) -> object:
        return object()

    def fake_run(
        config: ForecastSkillEvalConfig, client: object, run_id: str
    ) -> ResultsBundle:
        # Return a bundle with both season strata in contingency and baselines
        contingency = pd.DataFrame(
            [
                {
                    "horizon": "day",
                    "model": "model-a",
                    "regime": "all",
                    "season": "all",
                    "code": "POOLED",
                    "basin": "all",
                    "norm_provenance": "all",
                    "lead": None,
                    "TP": 2,
                    "FP": 0,
                    "FN": 0,
                    "TN": 2,
                    "n_pairs": 4,
                },
                {
                    "horizon": "day",
                    "model": "model-a",
                    "regime": "all",
                    "season": "irrigation",
                    "code": "POOLED",
                    "basin": "all",
                    "norm_provenance": "all",
                    "lead": None,
                    "TP": 1,
                    "FP": 0,
                    "FN": 0,
                    "TN": 1,
                    "n_pairs": 2,
                },
                {
                    "horizon": "day",
                    "model": "model-a",
                    "regime": "all",
                    "season": "non_irrigation",
                    "code": "POOLED",
                    "basin": "all",
                    "norm_provenance": "all",
                    "lead": None,
                    "TP": 1,
                    "FP": 0,
                    "FN": 0,
                    "TN": 1,
                    "n_pairs": 2,
                },
            ]
        )
        baselines = pd.DataFrame(columns=["season", "baseline"])
        return ResultsBundle(
            pairs=pd.DataFrame(),
            contingency_metrics=contingency,
            baselines=baselines,
            exclusion_ledger=ExclusionLedger(),
            horizon_summary=(),
        )

    def fake_write_artifacts(
        config: ForecastSkillEvalConfig,
        bundle: ResultsBundle,
        run_id: str,
    ) -> object:
        captured_bundle["bundle"] = bundle
        return tmp_path / run_id

    monkeypatch.setattr(cli, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(cli, "_build_client", fake_build_client)
    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "write_artifacts", fake_write_artifacts)

    cli.main(["--season", "irrigation", "--run-id", "test-run"])

    bundle = captured_bundle["bundle"]
    seasons_in_output = set(bundle.contingency_metrics["season"].unique())
    assert seasons_in_output == {"irrigation"}, (
        f"Expected only 'irrigation' but got {seasons_in_output}"
    )
