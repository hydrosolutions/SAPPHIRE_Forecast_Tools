from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pandas as pd

from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import POOLED_CODE
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.orchestrator import HorizonCoverage, ResultsBundle

LEDGER_COLUMNS = ("stage", "reason", "code", "period_key", "year")


def write_artifacts(
    config: ForecastSkillEvalConfig,
    bundle: ResultsBundle,
    run_id: str,
) -> Path:
    """Persist a forecast-skill result bundle under ``output_dir/run_id``.

    Args:
        config: Resolved configuration captured once at CLI startup.
        bundle: Full result bundle returned by the orchestrator.
        run_id: Artifact directory name.

    Returns:
        Path to the artifact directory.
    """
    artifact_dir = config.output_dir / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    parquet_available = _parquet_engine_available()
    _write_table(bundle.pairs, artifact_dir / "pairs", parquet_available=parquet_available)
    _write_table(
        bundle.contingency_metrics,
        artifact_dir / "contingency_metrics",
        parquet_available=parquet_available,
    )
    _write_table(bundle.baselines, artifact_dir / "baselines", parquet_available=parquet_available)
    _write_table(
        _ledger_frame(bundle.exclusion_ledger),
        artifact_dir / "exclusion_ledger",
        parquet_available=parquet_available,
    )
    (artifact_dir / "run_config.json").write_text(
        json.dumps(_config_record(config), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "summary.md").write_text(_summary_markdown(config, bundle), encoding="utf-8")
    return artifact_dir


def _write_table(data: pd.DataFrame, stem: Path, *, parquet_available: bool) -> None:
    data.to_csv(stem.with_suffix(".csv"), index=False)
    if parquet_available:
        _parquet_frame(data).to_parquet(stem.with_suffix(".parquet"), index=False)


def _parquet_engine_available() -> bool:
    return (
        importlib.util.find_spec("pyarrow") is not None
        or importlib.util.find_spec("fastparquet") is not None
    )


def _parquet_frame(data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy()
    for column in frame.columns:
        if frame[column].dtype == "object":
            frame[column] = frame[column].map(_parquet_scalar)
    return frame


def _parquet_scalar(value: object) -> object | None:
    if _is_missing(value):
        return None
    if isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _ledger_frame(ledger: ExclusionLedger) -> pd.DataFrame:
    rows = [
        {
            "stage": entry.stage,
            "reason": entry.reason,
            "code": entry.code,
            "period_key": entry.period_key,
            "year": entry.year,
        }
        for entry in ledger.entries
    ]
    return pd.DataFrame(rows, columns=LEDGER_COLUMNS)


def _config_record(config: ForecastSkillEvalConfig) -> dict[str, Any]:
    return {
        "base_url": config.base_url,
        "threshold": config.threshold,
        "horizons": list(config.horizons),
        "model_filter": _optional_list(config.model_filter),
        "station_filter": _optional_list(config.station_filter),
        "start_date": config.start_date,
        "end_date": config.end_date,
        "output_dir": str(config.output_dir),
        "provenance_by_horizon": dict(sorted(config.provenance_by_horizon.items())),
        "min_years": config.min_years,
        "operational_start": config.operational_start,
    }


def _optional_list(values: tuple[str, ...] | None) -> list[str] | None:
    if values is None:
        return None
    return list(values)


def _summary_markdown(config: ForecastSkillEvalConfig, bundle: ResultsBundle) -> str:
    lines = ["# Forecast Skill Evaluation Summary", ""]
    lines.extend(_coverage_section(bundle.horizon_summary))
    lines.extend(_regime_source_section(bundle.horizon_summary))
    lines.extend(_ledger_section(bundle.exclusion_ledger))
    lines.extend(_headline_section(bundle.contingency_metrics))
    lines.extend(_station_distribution_section(bundle.contingency_metrics))
    lines.extend(_norm_provenance_section(config, bundle.pairs))
    return "\n".join(lines).rstrip() + "\n"


def _coverage_section(coverage: tuple[HorizonCoverage, ...]) -> list[str]:
    lines = [
        "## Per-Horizon Coverage",
        "",
        "| horizon | n_pairs | skipped | reason |",
        "| --- | ---: | --- | --- |",
    ]
    for item in coverage:
        skipped = "yes" if item.skipped else "no"
        lines.append(f"| {item.horizon} | {item.n_pairs} | {skipped} | {item.skip_reason} |")
    lines.append("")
    return lines


def _regime_source_section(coverage: tuple[HorizonCoverage, ...]) -> list[str]:
    lines = ["## Regime Source", ""]
    if not coverage:
        lines.extend(["No regime source rows available.", ""])
        return lines

    lines.extend(["| horizon | source | reason |", "| --- | --- | --- |"])
    for item in coverage:
        lines.append(
            "| "
            f"{item.horizon} | {_format_text(item.regime_source)} | "
            f"{_format_text(item.regime_reason)} |"
        )
    lines.append("")
    return lines


def _ledger_section(ledger: ExclusionLedger) -> list[str]:
    lines = ["## Exclusion Ledger Totals", ""]
    counts = ledger.counts_by_stage_reason()
    if not counts:
        lines.extend(["No exclusions recorded.", ""])
        return lines

    lines.extend(["| stage | reason | count |", "| --- | --- | ---: |"])
    for (stage, reason), count in sorted(counts.items()):
        lines.append(f"| {stage} | {reason} | {count} |")
    lines.append("")
    return lines


def _headline_section(metrics: pd.DataFrame) -> list[str]:
    lines = ["## Headline Pooled Metrics", ""]
    pooled = _headline_pooled_rows(metrics)
    if pooled.empty:
        lines.extend(["No pooled metrics available.", ""])
        return lines

    lines.extend(
        [
            "| horizon | event | model | regime | norm_provenance | lead | n_pairs | "
            "base_rate | POD | FAR | FN_count | HSS | HSS_undefined | PSS | "
            "PSS_undefined | station_pod_min | station_pod_median | "
            "station_pod_max | flags |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | "
            "---: | --- | ---: | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in pooled.to_dict("records"):
        distribution = _station_pod_distribution(metrics, row)
        lines.append(
            "| "
            f"{row['horizon']} | {_event_value(row)} | {row['model']} | {row['regime']} | "
            f"{row['norm_provenance']} | "
            f"{_format_lead(row.get('lead'))} | {_format_count(row.get('n_pairs'))} | "
            f"{_format_metric(row.get('base_rate'))} | "
            f"{_format_metric(row.get('pod'))} | {_format_metric(row.get('far'))} | "
            f"{_format_count(row.get('FN'))} | "
            f"{_format_metric(row.get('hss'))} | "
            f"{_format_bool(row.get('hss_undefined'))} | "
            f"{_format_metric(row.get('pss'))} | "
            f"{_format_bool(row.get('pss_undefined'))} | "
            f"{_format_metric(distribution.get('min'))} | "
            f"{_format_metric(distribution.get('median'))} | "
            f"{_format_metric(distribution.get('max'))} | "
            f"{_headline_flags(row)} |"
        )
    lines.append("")
    return lines


def _station_distribution_section(metrics: pd.DataFrame) -> list[str]:
    lines = ["## Per-Station POD Distribution", ""]
    pooled = _headline_pooled_rows(metrics)
    if pooled.empty:
        lines.extend(["No station POD distribution available.", ""])
        return lines

    lines.extend(
        [
            "| horizon | event | model | regime | norm_provenance | lead | min_pod | "
            "median_pod | max_pod |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in pooled.to_dict("records"):
        distribution = _station_pod_distribution(metrics, row)
        lines.append(
            "| "
            f"{row['horizon']} | {_event_value(row)} | {row['model']} | {row['regime']} | "
            f"{row['norm_provenance']} | "
            f"{_format_lead(row.get('lead'))} | "
            f"{_format_metric(distribution.get('min'))} | "
            f"{_format_metric(distribution.get('median'))} | "
            f"{_format_metric(distribution.get('max'))} |"
        )
    lines.append("")
    return lines


def _norm_provenance_section(config: ForecastSkillEvalConfig, pairs: pd.DataFrame) -> list[str]:
    lines = ["## Norm Provenance", ""]
    calculated_config = [
        horizon
        for horizon in config.horizons
        if config.provenance_by_horizon.get(horizon) == "calculated"
    ]
    calculated_pairs = _calculated_pair_horizons(pairs)
    lines.append(
        f"Configured `calculated` norm horizons: {_format_list(calculated_config) or 'none'}"
    )
    lines.append(
        f"Horizons with `calculated` pair norms: {_format_list(calculated_pairs) or 'none'}"
    )
    lines.append("")

    if pairs.empty or "norm_provenance" not in pairs:
        lines.extend(["No norm provenance rows available.", ""])
        return lines

    breakdown = (
        pairs.groupby(["horizon", "norm_provenance"], dropna=False)
        .size()
        .reset_index(name="n_pairs")
        .sort_values(["horizon", "norm_provenance"], kind="stable")
    )
    lines.extend(["| horizon | norm_provenance | n_pairs |", "| --- | --- | ---: |"])
    for row in breakdown.to_dict("records"):
        lines.append(
            "| "
            f"{row['horizon']} | {_format_text(row['norm_provenance'])} | "
            f"{_format_count(row['n_pairs'])} |"
        )
    lines.append("")
    return lines


def _headline_pooled_rows(metrics: pd.DataFrame) -> pd.DataFrame:
    required = {"code", "regime", "norm_provenance", "lead"}
    if metrics.empty or not required.issubset(metrics.columns):
        return pd.DataFrame()

    pooled = metrics[metrics["code"].eq(POOLED_CODE)].copy()
    if pooled.empty:
        return pooled
    # ``event`` is optional — metrics built before Phase-2C lack this column.
    # When present, sort by it so each event's pooled row is retained and
    # ordered deterministically; when absent, behaviour is unchanged.
    sort_keys = ["horizon", "model", "regime", "norm_provenance"]
    if "event" in pooled.columns:
        sort_keys.append("event")
    return pooled.sort_values(sort_keys, kind="stable").reset_index(drop=True)


def _station_pod_distribution(
    metrics: pd.DataFrame,
    pooled_row: dict[str, object],
) -> dict[str, Any]:
    required = {"horizon", "model", "regime", "code", "norm_provenance", "lead", "pod"}
    if metrics.empty or not required.issubset(metrics.columns):
        return {}

    predicate = (
        metrics["horizon"].eq(pooled_row["horizon"])
        & metrics["model"].eq(pooled_row["model"])
        & metrics["regime"].eq(pooled_row["regime"])
        & metrics["code"].ne(POOLED_CODE)
        & metrics["norm_provenance"].eq(pooled_row["norm_provenance"])
        & _matching_lead(metrics["lead"], pooled_row.get("lead"))
    )
    # When the metrics frame carries an ``event`` column, a pooled row's station
    # distribution must only pool station rows for the SAME event.  Without this
    # guard, Phase-2C's five events would be merged into one distribution.
    if "event" in metrics.columns:
        predicate &= metrics["event"].eq(_event_value(pooled_row))
    station_rows = metrics[predicate]
    pods = pd.to_numeric(station_rows["pod"], errors="coerce").dropna()
    if pods.empty:
        return {}
    return {"min": pods.min(), "median": pods.median(), "max": pods.max()}


def _event_value(row: dict[str, object]) -> str:
    """Return the row's event label, defaulting to ``below_norm`` when absent.

    Metrics frames produced before Phase-2C have no ``event`` column; those rows
    represent the original below-norm decision, so they are treated as a single
    implicit ``below_norm`` group.
    """
    value = row.get("event")
    if _is_missing(value):
        return "below_norm"
    return str(value)


def _matching_lead(values: pd.Series, lead: object) -> pd.Series:
    if _is_missing(lead):
        return values.isna()
    return values.eq(lead)


def _calculated_pair_horizons(pairs: pd.DataFrame) -> list[str]:
    if pairs.empty or "norm_provenance" not in pairs:
        return []
    calculated = pairs[pairs["norm_provenance"].eq("calculated")]
    return sorted(str(value) for value in calculated["horizon"].dropna().unique())


def _format_list(values: list[str]) -> str:
    return ", ".join(values)


def _format_metric(value: object) -> str:
    if _is_missing(value):
        return "n/a"
    return f"{float(value):.3f}"


def _format_bool(value: object) -> str:
    if _is_missing(value):
        return "n/a"
    return "yes" if bool(value) else "no"


def _headline_flags(row: dict[str, object]) -> str:
    flags: list[str] = []
    if row.get("norm_provenance") == "calculated":
        flags.append("calculated_norm")
    if row.get("regime") == "hindcast":
        flags.append("hindcast_regime")
    return ", ".join(flags)


def _format_count(value: object) -> str:
    if _is_missing(value):
        return "0"
    return str(int(value))


def _format_lead(value: object) -> str:
    if _is_missing(value):
        return "all"
    return str(int(value))


def _format_text(value: object) -> str:
    if _is_missing(value):
        return "unknown"
    text = str(value)
    return text if text else "unknown"


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False
