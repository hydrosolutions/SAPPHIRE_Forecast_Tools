from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from forecast_skill_eval.api_readers import (
    SAPPHIRE_API_AVAILABLE,
    SapphirePostprocessingClient,
    SapphirePreprocessingClient,
)
from forecast_skill_eval.artifacts import write_artifacts
from forecast_skill_eval.config import (
    DEFAULT_BASE_URL,
    DEFAULT_ERROR_FLAGS,
    DEFAULT_EVENTS,
    DEFAULT_HINDCAST_FLAGS,
    DEFAULT_HORIZONS,
    DEFAULT_NAN_EXCLUDE_FLAGS,
    DEFAULT_OPERATIONAL_FLAGS,
    DEFAULT_OPERATIONAL_ISSUE_DAYS,
    ForecastSkillEvalConfig,
)
from forecast_skill_eval.orchestrator import ResultsBundle, run

API_UNAVAILABLE_MESSAGE = "SAPPHIRE API client is unavailable; skipping forecast skill evaluation."


@dataclass(frozen=True)
class _SapphireClientBundle:
    postprocessing: Any
    preprocessing: Any

    def read_short_term_forecasts(self, **kwargs: object) -> Any:
        """Delegate short-term forecast reads to the postprocessing client."""
        return self.postprocessing.read_short_term_forecasts(**kwargs)

    def read_lr_forecasts(self, **kwargs: object) -> Any:
        """Delegate LR forecast reads to the postprocessing client."""
        return self.postprocessing.read_lr_forecasts(**kwargs)

    def read_long_term_forecasts(self, **kwargs: object) -> Any:
        """Delegate long-term forecast reads to the postprocessing client."""
        return self.postprocessing.read_long_term_forecasts(**kwargs)

    def read_hydrograph(self, **kwargs: object) -> Any:
        """Delegate hydrograph norm reads to the preprocessing client."""
        return self.preprocessing.read_hydrograph(**kwargs)

    def read_runoff(self, **kwargs: object) -> Any:
        """Delegate observed runoff reads to the preprocessing client."""
        return self.preprocessing.read_runoff(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the forecast-skill evaluation CLI.

    Args:
        argv: Optional argument vector for tests.

    Returns:
        Process exit status.
    """
    args = _parser().parse_args(argv)
    run_id = args.run_id or _default_run_id()
    config = _config_from_args(args)

    if not SAPPHIRE_API_AVAILABLE:
        print(API_UNAVAILABLE_MESSAGE, file=_stderr())
        return 0

    client = _build_client(config)
    bundle = run(config, client, run_id)
    bundle = _apply_season_filter(bundle, config.season_filter)
    artifact_dir = write_artifacts(config, bundle, run_id)
    print(artifact_dir)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run SAPPHIRE forecast skill evaluation.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--threshold", type=float, default=0.80)
    parser.add_argument("--horizons", nargs="+", default=list(DEFAULT_HORIZONS))
    parser.add_argument("--models", nargs="+", dest="model_filter")
    parser.add_argument("--stations", nargs="+", dest="station_filter")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts"))
    parser.add_argument("--provenance", action="append", default=[], metavar="HORIZON=SOURCE")
    parser.add_argument("--min-years", type=int, default=10)
    parser.add_argument("--operational-start", default="2024-01-01")
    parser.add_argument(
        "--regime-source",
        choices=["auto", "flag", "date"],
        default="auto",
        help=(
            "Regime selection strategy. 'auto' (default) auto-picks flags when "
            "flag presence is meaningful, else issue date; 'flag' forces "
            "flag-based assignment; 'date' forces issue-date-based assignment."
        ),
    )
    parser.add_argument(
        "--operational-flags",
        nargs="+",
        default=list(DEFAULT_OPERATIONAL_FLAGS),
        metavar="FLAG",
    )
    parser.add_argument(
        "--hindcast-flags",
        nargs="+",
        default=list(DEFAULT_HINDCAST_FLAGS),
        metavar="FLAG",
    )
    parser.add_argument(
        "--nan-exclude-flags",
        nargs="+",
        default=list(DEFAULT_NAN_EXCLUDE_FLAGS),
        metavar="FLAG",
    )
    parser.add_argument(
        "--error-flags",
        nargs="+",
        default=list(DEFAULT_ERROR_FLAGS),
        metavar="FLAG",
    )
    parser.add_argument(
        "--operational-issue-days",
        nargs="+",
        default=list(DEFAULT_OPERATIONAL_ISSUE_DAYS),
        metavar="DAY",
    )
    parser.add_argument(
        "--events",
        nargs="+",
        default=list(DEFAULT_EVENTS),
        dest="events_filter",
        metavar="EVENT",
        help=(
            "Binary events to evaluate. "
            "Valid values: below_norm low_p10 low_p5 high_p90 high_p95. "
            "Default: all five events. "
            "Optional: below_norm_100 (plain below-norm at 1.0x norm)."
        ),
    )
    parser.add_argument(
        "--season",
        choices=["all", "irrigation", "non_irrigation"],
        default="all",
        dest="season_filter",
        help=(
            "Season filter for output rows: 'all' emits all season strata, "
            "'irrigation' restricts to Apr–Sep, 'non_irrigation' restricts to Oct–Mar."
        ),
    )
    parser.add_argument(
        "--short-term-issue-before-target",
        action="store_true",
        help=(
            "Short-term correctness gate (default off): drop day/pentad/decade "
            "forecasts issued on or after their target period start (leakage / "
            "mislabelled rows)."
        ),
    )
    parser.add_argument(
        "--short-term-dedup-one-per-target",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Short-term correctness gate (default ON): keep only the latest "
            "issue per (code, period_key, year, model) for day/pentad/decade, "
            "matching the operational one-pair-per-target convention. Pass "
            "--no-short-term-dedup-one-per-target to opt out."
        ),
    )
    parser.add_argument(
        "--short-term-lr-repair",
        action="store_true",
        help=(
            "LR repair-on-read (default off): correct historical issue-indexed LR "
            "pentad/decade forecasts to target-indexed at read time."
        ),
    )
    parser.add_argument(
        "--long-term-derive-lead",
        action="store_true",
        help=(
            "Long-term correctness gate (default off): for quarter/season, derive "
            "the true forecast lead (months from issue date to target-period start) "
            "instead of the overloaded stored horizon_value, dedup to one forecast "
            "per (code, target, year, model) at the smallest lead, and stratify "
            "quarter output per target quarter (Q1–Q4). Month is unchanged."
        ),
    )
    parser.add_argument("--run-id")
    return parser


def _config_from_args(args: argparse.Namespace) -> ForecastSkillEvalConfig:
    # SAPPHIRE_SKILL_FORECAST_ONLY=1/true forces BOTH short-term correctness gates
    # ON, mirroring the existing SAPPHIRE_SKILL_* env-flag convention.  The CLI
    # flags are OR-ed with it so either mechanism can enable a gate.  Note
    # short_term_dedup_one_per_target now defaults ON (D4/#7): its arg is a
    # BooleanOptionalAction defaulting True, so the OR keeps it ON by default and
    # honours the --no-... opt-out, while FORECAST_ONLY still force-enables it.
    forecast_only = _env_flag("SAPPHIRE_SKILL_FORECAST_ONLY")
    lr_repair = _env_flag("SAPPHIRE_SKILL_LR_REPAIR")
    lt_lead = _env_flag("SAPPHIRE_SKILL_LT_LEAD")
    return ForecastSkillEvalConfig(
        base_url=args.base_url,
        threshold=args.threshold,
        horizons=_split_values(args.horizons),
        model_filter=_split_optional_values(args.model_filter),
        station_filter=_split_optional_values(args.station_filter),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        provenance_by_horizon=_provenance_overrides(args.provenance),
        min_years=args.min_years,
        operational_start=args.operational_start,
        regime_source=args.regime_source,
        operational_flags=_split_int_values(args.operational_flags),
        hindcast_flags=_split_int_values(args.hindcast_flags),
        nan_exclude_flags=_split_int_values(args.nan_exclude_flags),
        error_flags=_split_int_values(args.error_flags),
        operational_issue_days=_split_int_values(args.operational_issue_days),
        events_filter=_split_values(args.events_filter),
        season_filter=args.season_filter,
        short_term_issue_before_target=(bool(args.short_term_issue_before_target) or forecast_only),
        short_term_dedup_one_per_target=(
            bool(args.short_term_dedup_one_per_target) or forecast_only
        ),
        short_term_lr_repair_issue_indexing=(bool(args.short_term_lr_repair) or lr_repair),
        long_term_derive_lead=(bool(args.long_term_derive_lead) or lt_lead),
    )


def _env_flag(name: str) -> bool:
    import os

    return os.environ.get(name, "").strip().lower() in {"1", "true"}


def _split_optional_values(values: Sequence[str] | None) -> list[str] | None:
    if values is None:
        return None
    return _split_values(values)


def _split_values(values: Sequence[str]) -> list[str]:
    split: list[str] = []
    for value in values:
        split.extend(part.strip() for part in value.split(",") if part.strip())
    return split


def _split_int_values(values: Sequence[object]) -> list[int]:
    split: list[int] = []
    for value in values:
        if isinstance(value, int):
            split.append(value)
            continue
        split.extend(int(part.strip()) for part in str(value).split(",") if part.strip())
    return split


def _provenance_overrides(values: Sequence[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--provenance values must use HORIZON=SOURCE")
        horizon, source = value.split("=", 1)
        overrides[horizon.strip()] = source.strip()
    return overrides


def _build_client(config: ForecastSkillEvalConfig) -> _SapphireClientBundle:
    if (
        not SAPPHIRE_API_AVAILABLE
        or SapphirePostprocessingClient is None
        or SapphirePreprocessingClient is None
    ):
        raise RuntimeError(API_UNAVAILABLE_MESSAGE)
    return _SapphireClientBundle(
        postprocessing=SapphirePostprocessingClient(base_url=config.base_url),
        preprocessing=SapphirePreprocessingClient(base_url=config.base_url),
    )


def _default_run_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _apply_season_filter(bundle: ResultsBundle, season_filter: str) -> ResultsBundle:
    """Return a copy of the bundle with contingency and baselines filtered by season.

    When ``season_filter`` is ``"all"`` the bundle is returned unchanged.  Otherwise
    only rows whose ``season`` column matches the requested value are kept.  Frames
    that lack a ``season`` column are passed through unmodified.
    """
    if season_filter == "all":
        return bundle

    def _filter(frame: object) -> object:
        import pandas as pd

        if not isinstance(frame, pd.DataFrame):
            return frame
        if frame.empty or "season" not in frame.columns:
            return frame
        return frame[frame["season"] == season_filter].reset_index(drop=True)

    return ResultsBundle(
        pairs=bundle.pairs,
        contingency_metrics=_filter(bundle.contingency_metrics),
        baselines=_filter(bundle.baselines),
        exclusion_ledger=bundle.exclusion_ledger,
        horizon_summary=bundle.horizon_summary,
        prob_metrics=_filter(bundle.prob_metrics),
        prob_reliability=_filter(bundle.prob_reliability),
        continuous_metrics=_filter(bundle.continuous_metrics),
        seasonal_volume=_filter(bundle.seasonal_volume),
        seasonal_volume_summary=_filter(bundle.seasonal_volume_summary),
        economic_value=_filter(bundle.economic_value),
        economic_value_summary=_filter(bundle.economic_value_summary),
    )


def _stderr() -> Any:
    import sys

    return sys.stderr


if __name__ == "__main__":
    raise SystemExit(main())
