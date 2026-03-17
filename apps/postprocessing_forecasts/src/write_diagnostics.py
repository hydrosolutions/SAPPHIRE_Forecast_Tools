"""DEBUG-level write diagnostics for postprocessing forecasts.

Logs detailed data summaries before each CSV/API write, guarded by
``logger.isEnabledFor(logging.DEBUG)`` so there is zero overhead at
INFO and above.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Column that identifies the period for each horizon type
_PERIOD_COLUMN = {
    "pentad": "pentad_in_year",
    "decad": "decad_in_year",
    "month": "month_in_year",
    "quarter": "quarter_in_year",
    "season": "season_in_year",
}


def diagnose_forecast_data(
    data: pd.DataFrame | None,
    horizon_type: str,
    label: str,
) -> None:
    """Log a DEBUG summary of forecast data before a write.

    Skips all computation when the logger is not at DEBUG level.

    Args:
        data: Forecast DataFrame (may be None or empty).
        horizon_type: One of "pentad", "decad", "month".
        label: Human-readable label for the log block header.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
        logger.debug("=== Write Diagnostics: %s (empty) ===", label)
        return

    lines: list[str] = []
    nrows = len(data)
    lines.append(f"=== Write Diagnostics: {label} ({nrows} rows) ===")

    # Stations & models
    n_stations = data["code"].nunique() if "code" in data.columns else 0
    if "model_short" in data.columns:
        models = sorted(data["model_short"].unique())
        lines.append(
            f"  Stations: {n_stations} unique | Models: {', '.join(models)} ({len(models)})"
        )
    else:
        lines.append(f"  Stations: {n_stations} unique")

    # Date range
    if "date" in data.columns:
        dates = pd.to_datetime(data["date"], errors="coerce").dropna()
        if not dates.empty:
            lines.append(
                f"  Date range: {dates.min().strftime('%Y-%m-%d')} "
                f".. {dates.max().strftime('%Y-%m-%d')}"
            )

    # Discharge stats
    if "forecasted_discharge" in data.columns:
        col = data["forecasted_discharge"]
        n_nan = int(col.isna().sum())
        n_zero = int((col == 0).sum())
        n_neg = int((col < 0).sum())
        valid = col.dropna()
        if not valid.empty:
            lines.append(
                f"  forecasted_discharge: min={valid.min():.2f} "
                f"max={valid.max():.2f} | "
                f"NaN={n_nan} zero={n_zero} negative={n_neg}"
            )
        else:
            lines.append(f"  forecasted_discharge: all NaN ({n_nan})")

    # Per-model row counts
    if "model_short" in data.columns:
        counts = data["model_short"].value_counts().sort_index()
        parts = [f"{m}={c}" for m, c in counts.items()]
        lines.append(f"  Per-model rows: {', '.join(parts)}")

    # Ensemble composition
    if "model_short" in data.columns and "composition" in data.columns:
        ensemble_models = data[data["composition"].notna()]["model_short"].unique()
        if len(ensemble_models) > 0:
            comp_parts = []
            for m in sorted(ensemble_models):
                subset = data[data["model_short"] == m]
                compositions = subset["composition"].dropna().unique()
                comp_str = "; ".join(sorted(compositions))
                comp_parts.append(f"{m}='{comp_str}' ({len(subset)})")
            lines.append(f"  Ensemble composition: {' | '.join(comp_parts)}")

    # Completeness at latest period
    period_col = _PERIOD_COLUMN.get(horizon_type)
    if (
        period_col
        and period_col in data.columns
        and "code" in data.columns
        and "model_short" in data.columns
    ):
        latest_period = data[period_col].max()
        latest = data[data[period_col] == latest_period]
        n_sta = latest["code"].nunique()
        n_mod = latest["model_short"].nunique()
        expected = n_sta * n_mod
        actual = len(latest.drop_duplicates(subset=["code", "model_short"]))
        lines.append(
            f"  Completeness at latest period: "
            f"{n_sta} stations x {n_mod} models = "
            f"{expected} expected, {actual} actual"
        )

    logger.debug("\n".join(lines))


def diagnose_skill_metrics(
    data: pd.DataFrame | None,
    horizon_type: str,
    label: str,
) -> None:
    """Log a DEBUG summary of skill metrics before a write.

    Args:
        data: Skill metrics DataFrame (may be None or empty).
        horizon_type: One of "pentad", "decad", "month".
        label: Human-readable label for the log block header.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
        logger.debug("=== Write Diagnostics: %s (empty) ===", label)
        return

    lines: list[str] = []
    nrows = len(data)
    lines.append(f"=== Write Diagnostics: {label} ({nrows} rows) ===")

    # Stations & models
    n_stations = data["code"].nunique() if "code" in data.columns else 0
    if "model_short" in data.columns:
        models = sorted(data["model_short"].unique())
        lines.append(
            f"  Stations: {n_stations} unique | Models: {', '.join(models)} ({len(models)})"
        )
    else:
        lines.append(f"  Stations: {n_stations} unique")

    # Period range
    period_col = _PERIOD_COLUMN.get(horizon_type)
    if period_col and period_col in data.columns:
        col = data[period_col]
        lines.append(f"  Period range: {period_col} {int(col.min())}..{int(col.max())}")

    # n_pairs summary
    if "n_pairs" in data.columns:
        np_col = data["n_pairs"].dropna()
        if not np_col.empty:
            low_conf = int((np_col < 3).sum())
            lines.append(
                f"  n_pairs: min={int(np_col.min())} "
                f"max={int(np_col.max())} | "
                f"low-confidence (n<3): {low_conf} rows"
            )

    # NSE summary
    if "nse" in data.columns:
        nse_col = data["nse"].dropna()
        if not nse_col.empty:
            worse = int((nse_col < 0).sum())
            lines.append(
                f"  nse: min={nse_col.min():.2f} "
                f"max={nse_col.max():.2f} | "
                f"worse-than-climatology (NSE<0): {worse} rows"
            )

    # sdivsigma summary
    if "sdivsigma" in data.columns:
        sd_col = data["sdivsigma"].dropna()
        if not sd_col.empty:
            high_bias = int((sd_col > 2.0).sum())
            lines.append(
                f"  sdivsigma: min={sd_col.min():.2f} "
                f"max={sd_col.max():.2f} | "
                f"high-bias (>2.0): {high_bias} rows"
            )

    # NaN counts per metric column
    metric_cols = [
        "sdivsigma",
        "nse",
        "delta",
        "accuracy",
        "mae",
        "n_pairs",
        "crps",
        "pbias",
        "kgelf",
        "nse_log",
    ]
    present = [c for c in metric_cols if c in data.columns]
    if present:
        nan_parts = [f"{c}={int(data[c].isna().sum())}" for c in present]
        lines.append(f"  NaN counts: {' '.join(nan_parts)}")

    # Per-model row counts
    if "model_short" in data.columns:
        counts = data["model_short"].value_counts().sort_index()
        parts = [f"{m}={c}" for m, c in counts.items()]
        lines.append(f"  Per-model rows: {', '.join(parts)}")

    logger.debug("\n".join(lines))


def diagnose_daily_skill_metrics(
    fdc_metrics: pd.DataFrame | None,
    threshold_metrics: pd.DataFrame | None,
    label: str = "daily skill metrics",
) -> None:
    """Log a DEBUG summary of daily (Tier 2) skill metrics before a write.

    Args:
        fdc_metrics: FDC metrics DataFrame (fhv, flv columns).
        threshold_metrics: Threshold metrics DataFrame (f1, csi columns).
        label: Human-readable label for the log block header.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    fdc_empty = fdc_metrics is None or (isinstance(fdc_metrics, pd.DataFrame) and fdc_metrics.empty)
    thr_empty = threshold_metrics is None or (
        isinstance(threshold_metrics, pd.DataFrame) and threshold_metrics.empty
    )

    if fdc_empty and thr_empty:
        logger.debug("=== Write Diagnostics: %s (empty) ===", label)
        return

    lines: list[str] = []
    lines.append(f"=== Write Diagnostics: {label} ===")

    # FDC section
    if not fdc_empty:
        n_rows = len(fdc_metrics)
        n_sta = fdc_metrics["code"].nunique() if "code" in fdc_metrics.columns else 0
        models = (
            sorted(fdc_metrics["model_short"].unique())
            if "model_short" in fdc_metrics.columns
            else []
        )
        lines.append(f"  FDC: {n_rows} rows | {n_sta} stations | Models: {', '.join(models)}")
        for col_name in ("fhv", "flv"):
            if col_name in fdc_metrics.columns:
                col = fdc_metrics[col_name].dropna()
                if not col.empty:
                    lines.append(f"    {col_name}: min={col.min():.1f} max={col.max():.1f}")

    # Threshold section
    if not thr_empty:
        n_rows = len(threshold_metrics)
        n_sta = threshold_metrics["code"].nunique() if "code" in threshold_metrics.columns else 0
        models = (
            sorted(threshold_metrics["model_short"].unique())
            if "model_short" in threshold_metrics.columns
            else []
        )
        lines.append(f"  Threshold: {n_rows} rows | {n_sta} stations | Models: {', '.join(models)}")

        if "threshold_type" in threshold_metrics.columns:
            types = sorted(threshold_metrics["threshold_type"].unique())
            lines.append(f"    types: {', '.join(str(t) for t in types)}")

        if "n_years" in threshold_metrics.columns:
            ny = threshold_metrics["n_years"].dropna()
            if not ny.empty:
                lines.append(f"    n_years: {int(ny.min())}..{int(ny.max())}")

        for col_name in ("f1", "csi"):
            if col_name in threshold_metrics.columns:
                col = threshold_metrics[col_name].dropna()
                if not col.empty:
                    lines.append(f"    {col_name}: {col.min():.2f}..{col.max():.2f}")

    logger.debug("\n".join(lines))
