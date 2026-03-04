"""Skill metric calculations for postprocessing forecasts.

Extracted from forecast_library.py — these functions are exclusively
used by postprocessing_forecasts.
"""

import datetime as dt
import logging
import os
from contextlib import contextmanager

import numpy as np
import pandas as pd
from src.postprocessing_tools import forecast_target_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metric registry — single source of truth for all point metrics
# ---------------------------------------------------------------------------

METRIC_REGISTRY = {
    "sdivsigma": {
        "min_points": 2,
        "higher_is_better": False,
        "env_var": "ieasyhydroforecast_efficiency_threshold",
        "default_threshold": 0.6,
    },
    "nse": {
        "min_points": 2,
        "higher_is_better": True,
        "env_var": "ieasyhydroforecast_nse_threshold",
        "default_threshold": 0.8,
    },
    "mae": {
        "min_points": 1,
        "higher_is_better": False,
        "env_var": None,  # no threshold filtering
        "default_threshold": None,
    },
    "n_pairs": {
        "min_points": 1,
        "higher_is_better": None,  # metadata, not a skill metric
        "env_var": None,
        "default_threshold": None,
    },
    "delta": {
        "min_points": 1,
        "higher_is_better": None,  # metadata
        "env_var": None,
        "default_threshold": None,
    },
    "accuracy": {
        "min_points": 1,
        "higher_is_better": True,
        "env_var": "ieasyhydroforecast_accuracy_threshold",
        "default_threshold": 0.8,
    },
    "pbias": {
        "min_points": 2,
        "higher_is_better": None,  # informational (closer to 0 is better)
        "env_var": None,
        "default_threshold": None,
    },
    "kgelf": {
        "min_points": 10,
        "higher_is_better": True,
        "env_var": None,
        "default_threshold": None,
    },
    "nse_log": {
        "min_points": 2,
        "higher_is_better": True,
        "env_var": None,
        "default_threshold": None,
    },
}

# Tier 2 daily metrics — computed by calculate_daily_skill_metrics(),
# NOT by calculate_all_skill_metrics().  Separate registry because
# these are per-(code, model) across all days, not per-period.
DAILY_METRIC_REGISTRY = {
    "fhv": {
        "min_points": 50,
        "higher_is_better": None,  # informational (closer to 0 is better)
    },
    "flv": {
        "min_points": 10,
        "higher_is_better": None,  # informational (closer to 0 is better)
    },
}

METRIC_ORDER = list(METRIC_REGISTRY.keys())

THRESHOLD_METRICS = {
    name: entry for name, entry in METRIC_REGISTRY.items() if entry["env_var"] is not None
}


# ---------------------------------------------------------------------------
# Individual metric helpers
# ---------------------------------------------------------------------------


def sdivsigma_nse(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the forecast efficacy and the Nash-Sutcliffe Efficiency (NSE) for the observed and simulated data.

    NSE = 1 - s/sigma

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast efficacy and the NSE value.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(
            f"DataFrame is missing one or more required columns: {observed_col, simulated_col}"
        )

    # Convert to numpy arrays for faster computation
    # Use float64 for better numerical stability
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=["sdivsigma", "nse"])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 2:  # Need at least 2 points for std calculation
        logger.info("Not enough data points for sdivsigma_nse calculation.")
        return pd.Series([np.nan, np.nan], index=["sdivsigma", "nse"])

    # Calculate mean once for reuse
    observed_mean = np.mean(observed)

    # Count the number of data points
    n = len(observed)

    # Calculate denominators
    denominator_nse = np.sum((observed - observed_mean) ** 2)
    # sigma: Standard deviation of the observed data
    denominator_sdivsigma = np.std(observed, ddof=1)  # ddof=1 for sample std

    # Check for numerical stability
    if denominator_nse < 1e-10 or denominator_sdivsigma < 1e-10:
        logger.debug("Numerical stability issue in sdivsigma_nse:")
        logger.debug(f"denominator_nse: {denominator_nse}")
        logger.debug(f"denominator_sdivsigma: {denominator_sdivsigma}")
        return pd.Series([np.nan, np.nan], index=["sdivsigma", "nse"])

    try:
        # Calculate differences once for reuse
        differences = observed - simulated

        # Calculate NSE
        numerator_nse = np.sum(differences**2)
        nse_value = 1 - (numerator_nse / denominator_nse)

        # Calculate sdivsigma
        # s: Average of squared differences between observed and simulated data
        numerator_sdivsigma = np.sqrt(np.sum(differences**2) / (n - 1))
        # s/sigma: Efficacy of the model
        sdivsigma = numerator_sdivsigma / denominator_sdivsigma

        # Sanity checks
        if not (-np.inf < nse_value < np.inf) or not (0 <= sdivsigma < np.inf):
            return pd.Series([np.nan, np.nan], index=["sdivsigma", "nse"])

        return pd.Series([sdivsigma, nse_value], index=["sdivsigma", "nse"])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in sdivsigma_nse: {str(e)}")
        return pd.Series([np.nan, np.nan], index=["sdivsigma", "nse"])


def forecast_accuracy_hydromet(
    data: pd.DataFrame, observed_col: str, simulated_col: str, delta_col: str
):
    """
    Calculate the forecast accuracy for the observed and simulated data.

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A pandas Series containing the forecast accuracy.

    Raises:
        ValueError: If the input data is missing one or more required columns.

    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col, delta_col]):
        raise ValueError(
            f"DataFrame is missing one or more required columns: {observed_col, simulated_col, delta_col}"
        )

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)
    delta_values = data[delta_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated) | np.isnan(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=["delta", "accuracy"])

    # Also drop rows where observed, simulated or delta_valus is inf
    mask = mask & ~(np.isinf(observed) | np.isinf(simulated) | np.isinf(delta_values))
    if not np.any(mask):
        return pd.Series([np.nan, np.nan], index=["delta", "accuracy"])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]
    delta_values = delta_values[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, np.nan], index=["delta", "accuracy"])

    try:
        # Calculate absolute differences once
        abs_diff = np.abs(observed - simulated)

        # Calculate accuracy using vectorized operations
        accuracy = np.mean(abs_diff <= delta_values)

        # Delta is constant per (code, period_in_year) by design.
        # Use first value; warn if they vary unexpectedly.
        delta = delta_values[0]
        delta_range = np.ptp(delta_values)
        if delta_range > 1e-6:
            logger.warning(
                "Delta values vary within group: range=%.6f, "
                "min=%.6f, max=%.6f (using first value %.6f)",
                delta_range,
                delta_values.min(),
                delta_values.max(),
                delta,
            )

        # Sanity checks
        if not (0 <= accuracy <= 1) or not (0 <= delta < np.inf):
            return pd.Series([np.nan, np.nan], index=["delta", "accuracy"])

        return pd.Series([delta, accuracy], index=["delta", "accuracy"])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in forecast_accuracy_hydromet: {str(e)}")
        return pd.Series([np.nan, np.nan], index=["delta", "accuracy"])


def mae(data: pd.DataFrame, observed_col: str, simulated_col: str):
    """
    Calculate the mean average error between observed and simulated data

    Args:
        data (pandas.DataFrame): The input data containing the observed and simulated data.
        observed_col (str): The name of the column containing the observed data.
        simulated_col (str): The name of the column containing the simulated data.

    Returns:
        pandas.Series: A series containing:
            - mae: mean average error between observed and simulated data
            - n_pairs: number of valid observed-simulated pairs used in calculation

    Raises:
        ValueError: If the input data is missing one or more required columns.
    """
    # Test the input. Make sure that the DataFrame contains the required columns
    if not all(column in data.columns for column in [observed_col, simulated_col]):
        raise ValueError(
            f"DataFrame is missing one or more required columns: {observed_col, simulated_col}"
        )

    # Convert to numpy arrays for faster computation
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)

    # Check for empty data after dropping NaNs
    mask = ~(np.isnan(observed) | np.isnan(simulated))
    if not np.any(mask):
        return pd.Series([np.nan, 0], index=["mae", "n_pairs"])

    # Filter arrays using mask
    observed = observed[mask]
    simulated = simulated[mask]

    # Early return if not enough data points
    if len(observed) < 1:
        return pd.Series([np.nan, 0], index=["mae", "n_pairs"])

    try:
        # Calculate MAE using vectorized operations
        mae_value = np.mean(np.abs(observed - simulated))

        # Sanity check
        if not (0 <= mae_value < np.inf):  # MAE must be non-negative
            return pd.Series([np.nan, 0], index=["mae", "n_pairs"])

        return pd.Series([mae_value, len(observed)], index=["mae", "n_pairs"])

    except (RuntimeWarning, FloatingPointError) as e:
        logger.debug(f"Numerical computation error in mae: {str(e)}")
        return pd.Series([np.nan, 0], index=["mae", "n_pairs"])


def pbias(obs: np.ndarray, sim: np.ndarray) -> float:
    """Percent Bias: 100 * SUM(obs - sim) / SUM(obs).

    Positive PBIAS indicates model underestimation (sim < obs),
    negative indicates overestimation (sim > obs).

    Args:
        obs: Observed values (NaN-free).
        sim: Simulated values (NaN-free).

    Returns:
        PBIAS as a percentage. NaN if < 2 points or SUM(obs) near zero.
    """
    if len(obs) < 2:
        return np.nan
    sum_obs = np.sum(obs)
    if abs(sum_obs) < 1e-10:
        return np.nan
    return 100.0 * np.sum(obs - sim) / sum_obs


def _kge(obs: np.ndarray, sim: np.ndarray) -> float:
    """Kling-Gupta Efficiency: 1 - sqrt((r-1)^2 + (a-1)^2 + (b-1)^2).

    Internal helper for kge_lf(). Not registered in METRIC_REGISTRY.

    Uses sample standard deviation (ddof=1) rather than the population
    std (ddof=0) in Gupta et al. (2009).  The difference is negligible
    for n >= 10 (kge_lf's minimum).

    Args:
        obs: Observed values (NaN-free, >= 2 points).
        sim: Simulated values (NaN-free, same length as obs).

    Returns:
        KGE value. NaN if std(obs) or mean(obs) near zero.
    """
    if len(obs) < 2:
        return np.nan
    obs_mean = np.mean(obs)
    sim_mean = np.mean(sim)
    obs_std = np.std(obs, ddof=1)
    sim_std = np.std(sim, ddof=1)
    if obs_std < 1e-10 or abs(obs_mean) < 1e-10:
        return np.nan
    # Pearson correlation
    r = np.corrcoef(obs, sim)[0, 1]
    if not np.isfinite(r):
        return np.nan
    alpha = sim_std / obs_std
    beta = sim_mean / obs_mean
    return 1.0 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)


def kge_lf(obs: np.ndarray, sim: np.ndarray) -> float:
    """Low-flow KGE: average of KGE(Q) and KGE(1/(Q+eps)).

    Epsilon = mean(obs) / 100 prevents division by zero in the
    inverse transformation, giving extra weight to low flows.

    Args:
        obs: Observed values (NaN-free).
        sim: Simulated values (NaN-free).

    Returns:
        KGElf value. NaN if < 10 points or mean(obs) near zero.
    """
    if len(obs) < 10:
        return np.nan
    obs_mean = np.mean(obs)
    if abs(obs_mean) < 1e-10:
        return np.nan
    eps = obs_mean / 100.0
    if np.any((obs + eps) <= 0) or np.any((sim + eps) <= 0):
        return np.nan
    kge_direct = _kge(obs, sim)
    kge_inv = _kge(1.0 / (obs + eps), 1.0 / (sim + eps))
    if np.isnan(kge_direct) or np.isnan(kge_inv):
        return np.nan
    return (kge_direct + kge_inv) / 2.0


def nse_log(obs: np.ndarray, sim: np.ndarray) -> float:
    """NSE on log-transformed flows: NSE(log(obs+eps), log(sim+eps)).

    Epsilon = mean(obs) / 100 prevents log(0). Gives extra weight
    to low-flow performance compared to standard NSE.

    Args:
        obs: Observed values (NaN-free).
        sim: Simulated values (NaN-free).

    Returns:
        NSE_log value. NaN if < 2 points, mean(obs) near zero,
        or constant log(obs).
    """
    if len(obs) < 2:
        return np.nan
    obs_mean = np.mean(obs)
    if abs(obs_mean) < 1e-10:
        return np.nan
    eps = obs_mean / 100.0
    if np.any((obs + eps) <= 0) or np.any((sim + eps) <= 0):
        return np.nan
    log_obs = np.log(obs + eps)
    log_sim = np.log(sim + eps)
    log_obs_mean = np.mean(log_obs)
    denom = np.sum((log_obs - log_obs_mean) ** 2)
    if denom < 1e-10:
        return np.nan
    numer = np.sum((log_obs - log_sim) ** 2)
    return 1.0 - numer / denom


# ---------------------------------------------------------------------------
# Tier 2: FDC-based and threshold-based daily metrics
# ---------------------------------------------------------------------------


def fdc_fhv(obs: np.ndarray, sim: np.ndarray) -> float:
    """FDC High Volume bias (%). Top 2% of sorted discharge.

    Measures how well the model reproduces high-flow volume.
    Positive = overestimation, negative = underestimation.
    Yilmaz et al. (2008), Eq. 2.

    Args:
        obs: Observed daily discharge (NaN-free).
        sim: Simulated daily discharge (NaN-free, same length).

    Returns:
        FHV as a percentage. NaN if <50 points or sum(obs_high) near 0.
    """
    if len(obs) < 50:
        return np.nan
    n = len(obs)
    # Top 2% threshold index (at least 1 point)
    k = max(int(np.ceil(n * 0.02)), 1)
    obs_sorted = np.sort(obs)[::-1]  # descending
    sim_sorted = np.sort(sim)[::-1]
    obs_high = obs_sorted[:k]
    sim_high = sim_sorted[:k]
    sum_obs_high = np.sum(obs_high)
    if abs(sum_obs_high) < 1e-10:
        return np.nan
    return 100.0 * (np.sum(sim_high) - sum_obs_high) / sum_obs_high


def fdc_flv(obs: np.ndarray, sim: np.ndarray) -> float:
    """FDC Low Volume bias (%). Bottom 30% of log-FDC.

    Measures how well the model reproduces low-flow volume using
    the log-transformed flow duration curve.
    Positive = overestimation, negative = underestimation.
    Yilmaz et al. (2008), Eq. 4.

    Args:
        obs: Observed daily discharge (NaN-free).
        sim: Simulated daily discharge (NaN-free, same length).

    Returns:
        FLV as a percentage. NaN if <10 points, or any low-flow <= 0.
    """
    if len(obs) < 10:
        return np.nan
    n = len(obs)
    # Bottom 30% threshold index (at least 1 point)
    k = max(int(np.floor(n * 0.3)), 1)
    obs_sorted = np.sort(obs)[::-1]  # descending
    sim_sorted = np.sort(sim)[::-1]
    # Bottom 30% = last k elements of descending sort
    obs_low = obs_sorted[-k:]
    sim_low = sim_sorted[-k:]
    if np.any(obs_low <= 0) or np.any(sim_low <= 0):
        return np.nan
    log_obs_low = np.log(obs_low)
    log_sim_low = np.log(sim_low)
    sum_log_obs = np.sum(log_obs_low)
    if abs(sum_log_obs) < 1e-10:
        return np.nan
    return 100.0 * (np.sum(log_sim_low) - sum_log_obs) / sum_log_obs


def estimate_return_period_thresholds(
    annual_maxima: np.ndarray,
    return_periods: tuple[int, ...] = (2, 5),
) -> dict[int, float]:
    """GEV fit to annual maxima → return-period thresholds.

    Uses scipy.stats.genextreme to fit a Generalized Extreme Value
    distribution and compute discharge thresholds for given return
    periods.

    Args:
        annual_maxima: Array of annual maximum discharge values.
        return_periods: Tuple of return periods in years.

    Returns:
        Dict mapping return period → threshold discharge.
        Empty dict if <15 years of data or fit fails.
    """
    from scipy.stats import genextreme

    if len(annual_maxima) < 15:
        return {}
    # Remove NaN
    am = annual_maxima[~np.isnan(annual_maxima)]
    if len(am) < 15:
        return {}
    # Check for constant data (GEV fit would fail)
    if np.ptp(am) < 1e-10:
        return {}
    try:
        shape, loc, scale = genextreme.fit(am)
        result = {}
        for rp in return_periods:
            # Exceedance probability = 1/rp
            # CDF quantile = 1 - 1/rp
            result[rp] = float(genextreme.ppf(1.0 - 1.0 / rp, shape, loc=loc, scale=scale))
        return result
    except Exception:
        logger.debug("GEV fit failed for annual maxima", exc_info=True)
        return {}


def binary_contingency(
    obs: np.ndarray,
    sim: np.ndarray,
    threshold: float,
    above: bool = True,
) -> dict:
    """Binary contingency table + F1/precision/recall/CSI.

    Classifies each (obs, sim) pair as exceedance or non-exceedance
    relative to *threshold*, then computes the contingency table.

    Args:
        obs: Observed values (NaN-free).
        sim: Simulated values (NaN-free, same length).
        threshold: Discharge threshold for event definition.
        above: If True, event = value >= threshold (floods).
               If False, event = value <= threshold (low-flow).

    Returns:
        Dict with keys: tp, fp, fn, tn, f1, precision, recall, csi.
        Metrics are NaN when undefined (e.g., no observed events).
    """
    if above:
        obs_event = obs >= threshold
        sim_event = sim >= threshold
    else:
        obs_event = obs <= threshold
        sim_event = sim <= threshold

    tp = int(np.sum(obs_event & sim_event))
    fp = int(np.sum(~obs_event & sim_event))
    fn = int(np.sum(obs_event & ~sim_event))
    tn = int(np.sum(~obs_event & ~sim_event))

    precision = tp / (tp + fp) if (tp + fp) > 0 else np.nan
    recall = tp / (tp + fn) if (tp + fn) > 0 else np.nan
    if np.isnan(precision) or np.isnan(recall) or (precision + recall) == 0:
        f1 = np.nan
    else:
        f1 = 2.0 * precision * recall / (precision + recall)
    csi = tp / (tp + fp + fn) if (tp + fp + fn) > 0 else np.nan

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "f1": f1,
        "precision": precision,
        "recall": recall,
        "csi": csi,
    }


def lowflow_quantiles(obs: np.ndarray) -> dict[str, float]:
    """Q90 (10th percentile) and Q95 (5th percentile) of daily flow.

    In hydrology, Q90 means the flow exceeded 90% of the time, i.e.,
    the 10th percentile. Similarly Q95 = 5th percentile.

    Args:
        obs: Observed daily discharge (NaN-free).

    Returns:
        Dict with 'q90' and 'q95' keys. Empty dict if <365 points.
    """
    if len(obs) < 365:
        return {}
    return {
        "q90": float(np.percentile(obs, 10)),
        "q95": float(np.percentile(obs, 5)),
    }


def calculate_daily_skill_metrics(
    obs_df: pd.DataFrame,
    sim_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate all Tier 2 daily skill metrics per (code, model_short).

    Joins observations and simulations on (code, date), then computes:
    - FDC metrics: FHV, FLV
    - Threshold metrics: GEV-based return period F1/CSI, low-flow F1/CSI

    Args:
        obs_df: DataFrame with columns [code, date, discharge_avg].
        sim_df: DataFrame with columns [code, date, model_short,
                forecasted_discharge].

    Returns:
        Tuple of (fdc_metrics_df, threshold_metrics_df).

        fdc_metrics_df columns: [code, model_short, fhv, flv]
        threshold_metrics_df columns: [code, model_short, threshold_type,
            threshold_value, f1, precision, recall, csi, tp, fp, fn, tn,
            n_years]
    """
    fdc_records = []
    threshold_records = []

    empty_fdc = pd.DataFrame(columns=["code", "model_short", "fhv", "flv"])
    empty_threshold = pd.DataFrame(
        columns=[
            "code",
            "model_short",
            "threshold_type",
            "threshold_value",
            "f1",
            "precision",
            "recall",
            "csi",
            "tp",
            "fp",
            "fn",
            "tn",
            "n_years",
        ]
    )

    if obs_df.empty or sim_df.empty:
        return empty_fdc, empty_threshold

    # Ensure date columns are datetime
    obs = obs_df.copy()
    sim = sim_df.copy()
    obs["date"] = pd.to_datetime(obs["date"])
    sim["date"] = pd.to_datetime(sim["date"])

    # Merge on (code, date) — inner join
    merged = pd.merge(
        sim[["code", "date", "model_short", "forecasted_discharge"]],
        obs[["code", "date", "discharge_avg"]],
        on=["code", "date"],
        how="inner",
    )

    if merged.empty:
        return empty_fdc, empty_threshold

    # Pre-compute GEV thresholds and low-flow quantiles per code
    # (based on observations only)
    gev_cache = {}  # code -> {2: threshold, 5: threshold}
    lowflow_cache = {}  # code -> {'q90': val, 'q95': val}
    for code, grp in obs.groupby("code"):
        obs_vals = grp["discharge_avg"].dropna().to_numpy(dtype=np.float64)
        # Annual maxima for GEV
        years = grp.set_index("date")["discharge_avg"].dropna()
        if not years.empty:
            annual_max = years.resample("YE").max().dropna().to_numpy(dtype=np.float64)
            gev_cache[code] = estimate_return_period_thresholds(annual_max)
        else:
            gev_cache[code] = {}
        lowflow_cache[code] = lowflow_quantiles(obs_vals)

    # Compute metrics per (code, model_short)
    for (code, model), grp in merged.groupby(["code", "model_short"]):
        # Drop NaN pairs
        valid = grp.dropna(subset=["discharge_avg", "forecasted_discharge"])
        if valid.empty:
            continue

        obs_arr = valid["discharge_avg"].to_numpy(dtype=np.float64)
        sim_arr = valid["forecasted_discharge"].to_numpy(dtype=np.float64)

        # FDC metrics
        fhv_val = fdc_fhv(obs_arr, sim_arr)
        flv_val = fdc_flv(obs_arr, sim_arr)
        fdc_records.append(
            {
                "code": code,
                "model_short": model,
                "fhv": fhv_val,
                "flv": flv_val,
            }
        )

        # GEV-based flood thresholds
        gev_thresholds = gev_cache.get(code, {})
        n_years = len(
            obs[obs["code"] == code]
            .set_index("date")["discharge_avg"]
            .dropna()
            .resample("YE")
            .max()
            .dropna()
        )
        min_events = {2: 5, 5: 3}
        for rp, thresh in gev_thresholds.items():
            cont = binary_contingency(obs_arr, sim_arr, thresh, above=True)
            # Check minimum observed exceedance events
            n_obs_events = cont["tp"] + cont["fn"]
            min_req = min_events.get(rp, 3)
            if n_obs_events < min_req:
                cont = {
                    k: (np.nan if k in ("f1", "precision", "recall", "csi") else v)
                    for k, v in cont.items()
                }
            threshold_records.append(
                {
                    "code": code,
                    "model_short": model,
                    "threshold_type": f"flood_{rp}yr",
                    "threshold_value": thresh,
                    "n_years": n_years,
                    **cont,
                }
            )

        # Low-flow thresholds
        lf = lowflow_cache.get(code, {})
        for lf_name, lf_thresh in [("q90", lf.get("q90")), ("q95", lf.get("q95"))]:
            if lf_thresh is None:
                continue
            cont = binary_contingency(obs_arr, sim_arr, lf_thresh, above=False)
            # Minimum 30 below-threshold observations
            n_below = cont["tp"] + cont["fn"]
            if n_below < 30:
                cont = {
                    k: (np.nan if k in ("f1", "precision", "recall", "csi") else v)
                    for k, v in cont.items()
                }
            threshold_records.append(
                {
                    "code": code,
                    "model_short": model,
                    "threshold_type": f"lowflow_{lf_name}",
                    "threshold_value": lf_thresh,
                    "n_years": n_years,
                    **cont,
                }
            )

    fdc_df = pd.DataFrame(fdc_records) if fdc_records else empty_fdc
    threshold_df = pd.DataFrame(threshold_records) if threshold_records else empty_threshold

    return fdc_df, threshold_df


# ---------------------------------------------------------------------------
# Combined single-pass metric calculation
# ---------------------------------------------------------------------------


def calculate_all_skill_metrics(
    data: pd.DataFrame,
    observed_col: str,
    simulated_col: str,
    delta_col: str,
) -> pd.Series:
    """Calculate all skill metrics in a single pass over the data.

    Combines sdivsigma_nse(), mae(), forecast_accuracy_hydromet(),
    and the informational metrics (pbias, kgelf, nse_log) into one
    function to avoid repeated groupby/merge overhead.

    Args:
        data: DataFrame containing observed, simulated, and delta columns.
        observed_col: Column name for observed values.
        simulated_col: Column name for simulated values.
        delta_col: Column name for delta (tolerance) values.

    Returns:
        pd.Series with keys:
            sdivsigma, nse, mae, n_pairs, delta, accuracy,
            pbias, kgelf, nse_log
    """
    nan_result = pd.Series(
        [0 if name == "n_pairs" else np.nan for name in METRIC_ORDER],
        index=METRIC_ORDER,
    )

    # Validate required columns
    required = [observed_col, simulated_col, delta_col]
    if not all(col in data.columns for col in required):
        raise ValueError(f"DataFrame is missing required columns: {required}")

    # Convert to numpy arrays (float64 for numerical stability)
    observed = data[observed_col].to_numpy(dtype=np.float64)
    simulated = data[simulated_col].to_numpy(dtype=np.float64)
    delta_values = data[delta_col].to_numpy(dtype=np.float64)

    # Common NaN/inf mask for all metrics
    mask = (
        ~np.isnan(observed)
        & ~np.isnan(simulated)
        & ~np.isnan(delta_values)
        & ~np.isinf(observed)
        & ~np.isinf(simulated)
        & ~np.isinf(delta_values)
    )
    if not np.any(mask):
        return nan_result

    obs = observed[mask]
    sim = simulated[mask]
    deltas = delta_values[mask]
    n = len(obs)

    # --- MAE + n_pairs (need >= 1 point) ---
    if n < 1:
        return nan_result

    differences = obs - sim
    abs_diff = np.abs(differences)

    try:
        mae_value = float(np.mean(abs_diff))
        if not (0 <= mae_value < np.inf):
            mae_value = np.nan
    except (RuntimeWarning, FloatingPointError):
        mae_value = np.nan

    # --- Accuracy + delta (need >= 1 point) ---
    try:
        accuracy = float(np.mean(abs_diff <= deltas))
        # Delta is constant per (code, period_in_year) by design.
        # Use first value; warn if they vary unexpectedly.
        delta = float(deltas[0])
        delta_range = float(np.ptp(deltas))
        if delta_range > 1e-6:
            logger.warning(
                "Delta values vary within group: range=%.6f, "
                "min=%.6f, max=%.6f (using first value %.6f)",
                delta_range,
                float(np.min(deltas)),
                float(np.max(deltas)),
                delta,
            )
        if not (0 <= accuracy <= 1) or not (0 <= delta < np.inf):
            accuracy = np.nan
            delta = np.nan
    except (RuntimeWarning, FloatingPointError):
        accuracy = np.nan
        delta = np.nan

    # --- sdivsigma + NSE (need >= 2 points for std) ---
    if n < 2:
        return pd.Series(
            [np.nan, np.nan, mae_value, n, delta, accuracy, np.nan, np.nan, np.nan],
            index=METRIC_ORDER,
        )

    try:
        obs_mean = np.mean(obs)
        denominator_nse = np.sum((obs - obs_mean) ** 2)
        denominator_sdivsigma = np.std(obs, ddof=1)

        if denominator_nse < 1e-10 or denominator_sdivsigma < 1e-10:
            sdivsigma = np.nan
            nse_value = np.nan
        else:
            numerator_nse = np.sum(differences**2)
            nse_value = 1 - (numerator_nse / denominator_nse)
            numerator_sdivsigma = np.sqrt(np.sum(differences**2) / (n - 1))
            sdivsigma = numerator_sdivsigma / denominator_sdivsigma

            if not (-np.inf < nse_value < np.inf) or not (0 <= sdivsigma < np.inf):
                sdivsigma = np.nan
                nse_value = np.nan
    except (RuntimeWarning, FloatingPointError):
        sdivsigma = np.nan
        nse_value = np.nan

    # --- Informational metrics (pbias, kgelf, nse_log) ---
    try:
        pbias_value = pbias(obs, sim)
    except (RuntimeWarning, FloatingPointError):
        pbias_value = np.nan

    try:
        kgelf_value = kge_lf(obs, sim)
    except (RuntimeWarning, FloatingPointError):
        kgelf_value = np.nan

    try:
        nse_log_value = nse_log(obs, sim)
    except (RuntimeWarning, FloatingPointError):
        nse_log_value = np.nan

    return pd.Series(
        [
            sdivsigma,
            nse_value,
            mae_value,
            n,
            delta,
            accuracy,
            pbias_value,
            kgelf_value,
            nse_log_value,
        ],
        index=METRIC_ORDER,
    )


# ---------------------------------------------------------------------------
# CRPS (Continuous Ranked Probability Score)
# ---------------------------------------------------------------------------


def calculate_crps(
    observed: np.ndarray,
    quantile_forecasts: np.ndarray,
    quantile_levels: np.ndarray,
) -> float:
    """Continuous Ranked Probability Score from quantile forecasts.

    Uses trapezoidal integration of quantile (pinball) losses:
        CRPS = (1/N) * sum_i trapz(rho_tau(y_i - q_ij), tau_j)
    where rho_tau(u) = u * (tau - 1{u<0}) is the pinball loss.

    Args:
        observed: shape (N,) — observed values.
        quantile_forecasts: shape (N, K) — forecasted quantiles.
        quantile_levels: shape (K,) — e.g. [0.05, 0.10, ..., 0.95].

    Returns:
        Mean CRPS across valid observations (lower is better).
        Returns NaN if no valid observations.
    """
    observed = np.asarray(observed, dtype=np.float64)
    quantile_forecasts = np.asarray(quantile_forecasts, dtype=np.float64)
    quantile_levels = np.asarray(quantile_levels, dtype=np.float64)

    # Mask out NaN observations
    valid = ~np.isnan(observed)
    if not np.any(valid):
        return np.nan

    obs = observed[valid]
    qf = quantile_forecasts[valid]

    # Compute pinball loss for each (observation, quantile_level) pair
    # errors shape: (N, K)
    errors = obs[:, np.newaxis] - qf

    # rho_tau(u) = u * tau  if u >= 0
    #            = u * (tau - 1)  if u < 0
    pinball = np.where(
        errors >= 0,
        errors * quantile_levels[np.newaxis, :],
        errors * (quantile_levels[np.newaxis, :] - 1.0),
    )

    # Integrate pinball loss over quantile levels for each observation
    # using trapezoidal rule
    crps_per_obs = np.trapezoid(pinball, quantile_levels, axis=1)

    return float(np.mean(crps_per_obs))


# ---------------------------------------------------------------------------
# Monthly skill metric pipeline
# ---------------------------------------------------------------------------

# Quantile columns and levels for long-term forecasts
_QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
_QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])

# Columns carried through when appending ensemble rows to joint_forecasts
_ENSEMBLE_JOINT_COLS = (
    [
        "code",
        "year",
        "month",
        "month_in_year",
        "forecasted_discharge",
        "model_short",
        "composition",
    ]
    + _QUANTILE_COLS
    + ["valid_from", "valid_to", "date", "flag"]
)


def _append_to_joint(
    joint_forecasts: pd.DataFrame,
    ensemble_df: pd.DataFrame,
) -> pd.DataFrame:
    """Append ensemble rows to joint_forecasts, carrying quantile + date cols.

    Only includes columns that actually exist in ensemble_df.  Adds
    a 'composition' column to joint_forecasts if missing.
    """
    cols = [c for c in _ENSEMBLE_JOINT_COLS if c in ensemble_df.columns]
    if not cols:
        return joint_forecasts
    if "composition" not in joint_forecasts.columns:
        joint_forecasts = joint_forecasts.copy()
        joint_forecasts["composition"] = ""
    return pd.concat(
        [joint_forecasts, ensemble_df[cols]],
        ignore_index=True,
    )


def calculate_monthly_skill_metrics(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    timing_stats=None,
) -> tuple:
    """Calculate monthly skill metrics for long-term forecasts.

    Point metrics (Q50 vs observed): NSE, MAE, accuracy, sdivsigma.
    Probabilistic metric: CRPS (using Q05-Q95 quantile distribution).

    Args:
        observations: [code, year, month, month_in_year,
                       discharge_avg, delta]
        forecasts: [code, year, month, model_short,
                    q50, q05, q10, q25, q75, q90, q95]
        timing_stats: Optional timing collector (passed through).

    Returns:
        (skill_stats_df, joint_forecasts_df, timing_stats)
        skill_stats_df columns: [month_in_year, code, model_short,
            sdivsigma, nse, delta, accuracy, mae, n_pairs, crps]
    """
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    empty_stats = pd.DataFrame(
        columns=["month_in_year", "code", "model_short"] + METRIC_ORDER + ["crps"]
    )
    empty_joint = pd.DataFrame()

    # Guard: empty inputs
    if observations.empty or forecasts.empty:
        return empty_stats, empty_joint, timing_stats

    # --- 1. Merge forecasts with observations on [code, year, month] ---
    merged = pd.merge(
        forecasts,
        observations[["code", "year", "month", "month_in_year", "discharge_avg", "delta"]],
        on=["code", "year", "month"],
        how="inner",
    )
    merged["forecasted_discharge"] = merged["q50"].astype(float)

    if merged.empty:
        # No overlap — Naive Mean needs merged data (forecast+obs pairs)
        merged_empty = pd.DataFrame()
        return _add_naive_mean(
            empty_stats,
            merged_empty,
            observations,
            timing_stats,
            empty_joint,
        )

    # --- 2. Point metrics per (month_in_year, code, model_short) ---
    skill_stats = (
        merged.groupby(["month_in_year", "code", "model_short"])[
            ["discharge_avg", "forecasted_discharge", "delta"]
        ]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    # --- 3. CRPS per group ---
    crps_records = []
    for (miy, code, model), grp in merged.groupby(["month_in_year", "code", "model_short"]):
        obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
        qf_cols = [c for c in _QUANTILE_COLS if c in grp.columns]
        if len(qf_cols) == len(_QUANTILE_COLS):
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
        else:
            crps_val = np.nan
        crps_records.append(
            {
                "month_in_year": miy,
                "code": code,
                "model_short": model,
                "crps": crps_val,
            }
        )

    crps_df = pd.DataFrame(crps_records)
    skill_stats = skill_stats.merge(
        crps_df,
        on=["month_in_year", "code", "model_short"],
        how="left",
    )

    # --- 4. Ensemble mean (EM) ---
    joint_forecasts = forecasts.copy()
    # Ensure month_in_year is present (forecasts may only have 'month')
    if "month_in_year" not in joint_forecasts.columns and "month" in joint_forecasts.columns:
        joint_forecasts["month_in_year"] = joint_forecasts["month"]
    skill_stats_filtered = filter_for_highly_skilled_forecasts(skill_stats)

    merge_keys = ["month_in_year", "code", "model_short"]
    skilled_merged = merged.merge(
        skill_stats_filtered[merge_keys].drop_duplicates(),
        on=merge_keys,
        how="inner",
    )
    # Exclude existing EM / baselines from ensemble input
    skilled_merged = skilled_merged[
        ~skilled_merged["model_short"].isin(["EM", "Naive Mean", "Skilled Mean"])
    ].copy()
    skilled_merged = skilled_merged.dropna(subset=["forecasted_discharge"]).copy()

    n_models = forecasts["model_short"].nunique()
    if n_models > 1 and not skilled_merged.empty:
        # Build aggregation: mean of q50 + all available quantile cols
        em_agg_dict = {
            "month_in_year": "first",
            "forecasted_discharge": "mean",
            "model_short": composition_agg,
        }
        for qcol in _QUANTILE_COLS:
            if qcol in skilled_merged.columns:
                em_agg_dict[qcol] = "mean"
        for dcol in ("valid_from", "valid_to", "date"):
            if dcol in skilled_merged.columns:
                em_agg_dict[dcol] = "first"

        em_avg = skilled_merged.groupby(["year", "month", "code"]).agg(em_agg_dict).reset_index()
        em_avg = em_avg.rename(columns={"model_short": "composition"})
        em_avg["model_short"] = "EM"

        # Discard single-model "ensembles"
        em_avg = em_avg[em_avg["composition"].apply(is_multi_model_composition)].copy()

        if not em_avg.empty:
            # Compute EM skill metrics
            em_with_obs = pd.merge(
                em_avg,
                observations[["code", "year", "month", "month_in_year", "discharge_avg", "delta"]],
                on=["code", "year", "month"],
                how="inner",
                suffixes=("", "_obs"),
            )
            # Resolve month_in_year collision
            if "month_in_year_obs" in em_with_obs.columns:
                em_with_obs = em_with_obs.drop(columns=["month_in_year_obs"])

            em_skill = (
                em_with_obs.groupby(["month_in_year", "code", "model_short", "composition"])[
                    ["discharge_avg", "forecasted_discharge", "delta"]
                ]
                .apply(
                    calculate_all_skill_metrics,
                    observed_col="discharge_avg",
                    simulated_col="forecasted_discharge",
                    delta_col="delta",
                )
                .reset_index()
            )

            # Compute CRPS from aggregated quantiles
            qf_cols = [c for c in _QUANTILE_COLS if c in em_with_obs.columns]
            if len(qf_cols) == len(_QUANTILE_COLS):
                em_crps = []
                for (miy, code, model), grp in em_with_obs.groupby(
                    ["month_in_year", "code", "model_short"]
                ):
                    obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
                    qf = grp[qf_cols].to_numpy(dtype=np.float64)
                    crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
                    em_crps.append(
                        {
                            "month_in_year": miy,
                            "code": code,
                            "model_short": model,
                            "crps": crps_val,
                        }
                    )
                em_crps_df = pd.DataFrame(em_crps)
                em_skill = em_skill.merge(
                    em_crps_df,
                    on=["month_in_year", "code", "model_short"],
                    how="left",
                )
            else:
                em_skill["crps"] = np.nan

            skill_stats = pd.concat([skill_stats, em_skill], ignore_index=True)

            # Add EM rows to joint_forecasts
            em_avg["flag"] = 0
            joint_forecasts = _append_to_joint(joint_forecasts, em_avg)

    # --- 4b. Skilled Mean baseline ---
    skill_stats, joint_forecasts = _add_skilled_mean(
        skill_stats, merged, observations, timing_stats, joint_forecasts
    )

    # --- 5. Naive Mean baseline ---
    return _add_naive_mean(skill_stats, merged, observations, timing_stats, joint_forecasts)


def _add_naive_mean(
    skill_stats: pd.DataFrame,
    merged: pd.DataFrame,
    observations: pd.DataFrame,
    timing_stats,
    joint_forecasts: pd.DataFrame,
) -> tuple:
    """Add Naive Mean (unweighted model average) to skill_stats.

    Naive Mean = simple average of ALL model forecasts, regardless of
    skill thresholds (unlike EM/Skilled Mean which filter).  Baselines
    (EM, Naive Mean, Skilled Mean) are excluded from the model pool.
    Single-model groups are discarded.

    Quantiles (q05-q95) are averaged across models (same as EM).
    CRPS is computed from the aggregated quantile distribution.
    """
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    if merged.empty or observations.empty:
        return skill_stats, joint_forecasts, timing_stats

    # Exclude baselines from the model pool
    excluded = {"EM", "Naive Mean", "Skilled Mean"}
    pool = merged[~merged["model_short"].isin(excluded)].copy()
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()

    if pool.empty:
        return skill_stats, joint_forecasts, timing_stats

    # Build aggregation dict: mean of q50 + all available quantile cols
    agg_dict = {
        "month_in_year": "first",
        "forecasted_discharge": "mean",
        "model_short": composition_agg,
    }
    for qcol in _QUANTILE_COLS:
        if qcol in pool.columns:
            agg_dict[qcol] = "mean"
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            agg_dict[dcol] = "first"

    naive_avg = pool.groupby(["year", "month", "code"]).agg(agg_dict).reset_index()
    naive_avg = naive_avg.rename(columns={"model_short": "composition"})
    naive_avg["model_short"] = "Naive Mean"

    # Discard single-model groups (need >=2 models)
    naive_avg = naive_avg[naive_avg["composition"].apply(is_multi_model_composition)].copy()

    if naive_avg.empty:
        return skill_stats, joint_forecasts, timing_stats

    # Merge with observations to compute point metrics
    naive_with_obs = pd.merge(
        naive_avg,
        observations[["code", "year", "month", "month_in_year", "discharge_avg", "delta"]],
        on=["code", "year", "month"],
        how="inner",
        suffixes=("", "_obs"),
    )
    if "month_in_year_obs" in naive_with_obs.columns:
        naive_with_obs = naive_with_obs.drop(columns=["month_in_year_obs"])

    if naive_with_obs.empty:
        return skill_stats, joint_forecasts, timing_stats

    # Compute point metrics
    naive_skill = (
        naive_with_obs.groupby(["month_in_year", "code", "model_short", "composition"])[
            ["discharge_avg", "forecasted_discharge", "delta"]
        ]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    # Compute CRPS from aggregated quantiles
    qf_cols = [c for c in _QUANTILE_COLS if c in naive_with_obs.columns]
    if len(qf_cols) == len(_QUANTILE_COLS):
        crps_records = []
        for (miy, code, model), grp in naive_with_obs.groupby(
            ["month_in_year", "code", "model_short"]
        ):
            obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
            crps_records.append(
                {
                    "month_in_year": miy,
                    "code": code,
                    "model_short": model,
                    "crps": crps_val,
                }
            )
        crps_df = pd.DataFrame(crps_records)
        naive_skill = naive_skill.merge(
            crps_df,
            on=["month_in_year", "code", "model_short"],
            how="left",
        )
    else:
        naive_skill["crps"] = np.nan

    parts = [df for df in [skill_stats, naive_skill] if not df.empty]
    if parts:
        skill_stats = pd.concat(parts, ignore_index=True)
    else:
        skill_stats = naive_skill

    # Add Naive Mean rows to joint_forecasts
    naive_avg["flag"] = 0
    joint_forecasts = _append_to_joint(joint_forecasts, naive_avg)

    return skill_stats, joint_forecasts, timing_stats


def _add_skilled_mean(
    skill_stats: pd.DataFrame,
    merged: pd.DataFrame,
    observations: pd.DataFrame,
    timing_stats,
    joint_forecasts: pd.DataFrame,
) -> tuple:
    """Add Skilled Mean (inverse-MAE weighted) to skill_stats.

    Skilled Mean = weighted average of threshold-filtered models'
    q50 forecasts, where w_i = 1 / (MAE_i + eps).
    eps = mean(MAE) / 100 to avoid division by zero.

    Only models passing the same threshold filter as EM are included.
    EM, Naive Mean, and Skilled Mean themselves are excluded from
    the model pool. Single-model groups are discarded.

    CRPS is NaN (point forecast only, no quantile distribution).
    """
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    if skill_stats.empty or merged.empty:
        return skill_stats, joint_forecasts

    # 1. Filter for highly skilled models (same pool as EM)
    filtered = filter_for_highly_skilled_forecasts(skill_stats)

    # 2. Exclude baselines from the model pool
    excluded = {"EM", "Naive Mean", "Skilled Mean"}
    filtered = filtered[~filtered["model_short"].isin(excluded)].copy()
    if filtered.empty:
        return skill_stats, joint_forecasts

    # 3. Extract MAE per (month_in_year, code, model_short)
    mae_df = filtered[["month_in_year", "code", "model_short", "mae"]].copy()
    mae_df = mae_df.dropna(subset=["mae"])
    if mae_df.empty:
        return skill_stats, joint_forecasts

    # 4. Compute weights: w_i = 1 / (MAE_i + eps)
    mean_mae = mae_df["mae"].mean()
    eps = mean_mae / 100.0 if mean_mae > 0 else 1e-10
    mae_df["weight"] = 1.0 / (mae_df["mae"] + eps)

    # Get qualifying models per (month_in_year, code)
    qualifying_keys = mae_df[["month_in_year", "code", "model_short"]].drop_duplicates()

    # Filter merged to qualifying models only
    sm_merged = merged.merge(
        qualifying_keys,
        on=["month_in_year", "code", "model_short"],
        how="inner",
    )
    sm_merged = sm_merged.dropna(subset=["forecasted_discharge"]).copy()
    if sm_merged.empty:
        return skill_stats, joint_forecasts

    # Attach weights from mae_df
    sm_merged = sm_merged.merge(
        mae_df[["month_in_year", "code", "model_short", "weight"]],
        on=["month_in_year", "code", "model_short"],
        how="left",
    )

    # 5. Compute weighted mean per (code, year, month) — vincentization
    #    for forecasted_discharge and all available quantile cols
    def _weighted_mean_col(group, col):
        w = sm_merged.loc[group.index, "weight"].to_numpy()
        d = group.to_numpy()
        return np.average(d, weights=w)

    sm_agg_dict = {
        "month_in_year": ("month_in_year", "first"),
        "forecasted_discharge": (
            "forecasted_discharge",
            lambda x: _weighted_mean_col(x, "forecasted_discharge"),
        ),
        "composition": ("model_short", composition_agg),
    }

    # Weighted mean for each quantile column (vincentization)
    available_qcols = [qc for qc in _QUANTILE_COLS if qc in sm_merged.columns]
    for qcol in available_qcols:
        sm_agg_dict[qcol] = (
            qcol,
            lambda x, _c=qcol: _weighted_mean_col(x, _c),
        )

    # Carry date columns through (take first — same target period)
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in sm_merged.columns:
            sm_agg_dict[dcol] = (dcol, "first")

    sm_avg = (
        sm_merged.groupby(["year", "month", "code"])
        .agg(
            **sm_agg_dict,
        )
        .reset_index()
    )
    sm_avg["model_short"] = "Skilled Mean"

    # 6. Discard single-model groups
    sm_avg = sm_avg[sm_avg["composition"].apply(is_multi_model_composition)].copy()

    if sm_avg.empty:
        return skill_stats, joint_forecasts

    # 7. Merge with observations, compute point metrics
    sm_with_obs = pd.merge(
        sm_avg,
        observations[["code", "year", "month", "month_in_year", "discharge_avg", "delta"]],
        on=["code", "year", "month"],
        how="inner",
        suffixes=("", "_obs"),
    )
    if "month_in_year_obs" in sm_with_obs.columns:
        sm_with_obs = sm_with_obs.drop(columns=["month_in_year_obs"])

    sm_skill = (
        sm_with_obs.groupby(["month_in_year", "code", "model_short", "composition"])[
            ["discharge_avg", "forecasted_discharge", "delta"]
        ]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    # 8. Compute CRPS from aggregated quantiles (vincentized)
    qf_cols = [c for c in _QUANTILE_COLS if c in sm_with_obs.columns]
    if len(qf_cols) == len(_QUANTILE_COLS):
        sm_crps = []
        for (miy, code, model), grp in sm_with_obs.groupby(
            ["month_in_year", "code", "model_short"]
        ):
            obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
            sm_crps.append(
                {
                    "month_in_year": miy,
                    "code": code,
                    "model_short": model,
                    "crps": crps_val,
                }
            )
        sm_crps_df = pd.DataFrame(sm_crps)
        sm_skill = sm_skill.merge(
            sm_crps_df,
            on=["month_in_year", "code", "model_short"],
            how="left",
        )
    else:
        sm_skill["crps"] = np.nan

    # 9. Append to skill_stats
    skill_stats = pd.concat([skill_stats, sm_skill], ignore_index=True)

    # Add Skilled Mean rows to joint_forecasts
    sm_avg["flag"] = 0
    joint_forecasts = _append_to_joint(joint_forecasts, sm_avg)

    return skill_stats, joint_forecasts


# ---------------------------------------------------------------------------
# Threshold filtering
# ---------------------------------------------------------------------------


def filter_for_highly_skilled_forecasts(
    skill_stats: pd.DataFrame,
    **overrides,
) -> pd.DataFrame:
    """Filter skill metrics to models passing all thresholds.

    Thresholds read from env vars per THRESHOLD_METRICS registry.
    A threshold set to 'False' disables that filter.

    Args:
        skill_stats: DataFrame with metric columns.
        **overrides: metric_name=value to override env var lookup.
            E.g. filter_for_highly_skilled_forecasts(df, sdivsigma=0.5)
    """
    result = skill_stats.copy()
    for name, entry in THRESHOLD_METRICS.items():
        threshold = overrides.get(name)
        if threshold is None:
            threshold = os.getenv(entry["env_var"], entry["default_threshold"])
        if str(threshold) == "False":
            continue
        threshold = float(threshold)
        if entry["higher_is_better"]:
            result = result[result[name] > threshold].copy()
        else:
            result = result[result[name] < threshold].copy()
    return result


# ---------------------------------------------------------------------------
# Full pentad / decade skill metric pipelines
# ---------------------------------------------------------------------------


def calculate_skill_metrics(
    config, observed: pd.DataFrame, simulated: pd.DataFrame, timing_stats=None
):
    """Calculate skill metrics for a short-term horizon (pentad or decad).

    Parameterized by *config* so that a single implementation handles
    both pentad and decad horizons.

    Args:
        config: ShortTermHorizonConfig with horizon-specific parameters.
        observed: DataFrame with observed data.
        simulated: DataFrame with simulated data.
        timing_stats: Optional TimingStats collector.

    Returns:
        (skill_stats, joint_forecasts, timing_stats)
    """
    horizon = config.name.capitalize()
    period_col = config.period_col
    period_in_month_col = config.period_in_month_col
    get_period_func = config.get_period_func

    if timing_stats is None:

        @contextmanager
        def timer(stats, section):
            yield

    else:

        @contextmanager
        def timer(stats, section):
            stats.start(section)
            try:
                yield
            finally:
                stats.end(section)

    # Import composition helpers from ensemble_calculator
    from src.ensemble_calculator import composition_agg, is_multi_model_composition

    # Test the input. Make sure that the DataFrames contain the required columns
    if not all(
        column in observed.columns
        for column in ["code", "date", "discharge_avg", "model_short", "delta"]
    ):
        raise ValueError(
            f"Observed DataFrame is missing one or more required columns: {['code', 'date', 'discharge_avg', 'model_short', 'delta']}"
        )
    if not all(
        column in simulated.columns
        for column in ["code", "date", period_col, "forecasted_discharge", "model_short"]
    ):
        raise ValueError(
            f"Simulated DataFrame is missing one or more required columns: {['code', 'date', period_col, 'forecasted_discharge', 'model_short']}"
        )

    # Local functions
    def test_for_tuples(df):
        # Identify tuples in each cell
        is_tuple = df.apply(lambda col: col.map(lambda x: isinstance(x, tuple)))
        # Check if there are any True values in is_tuple
        contains_tuples = is_tuple.any(axis=1).any()
        # Test if there are any tuples in the DataFrame
        if contains_tuples:
            logger.debug("There are tuples after the merge.")
            rows_with_tuples = df[is_tuple.any(axis=1)]
            logger.debug(rows_with_tuples)
        else:
            logger.debug("No tuples found after the merge.")

    latest_date_temp = simulated["date"].max()
    unique_models = simulated["model_short"].unique()
    latest_models = simulated[simulated["date"] == latest_date_temp]["model_short"].unique()
    logger.debug(
        "Calculating %s skill metrics — latest date: %s, models: %s, models at latest date: %s",
        horizon,
        latest_date_temp,
        unique_models,
        latest_models,
    )

    with timer(timing_stats, f"calculate_skill_metrics_{config.name} - Filter data"):
        _default_start = dt.date.today().year - 20
        min_year = int(os.getenv("SAPPHIRE_SKILL_METRICS_START_YEAR", _default_start))
        observed = observed[observed["date"].dt.year >= min_year]
        simulated = simulated[simulated["date"].dt.year >= min_year]

    # Merge the observed and simulated DataFrames
    with timer(timing_stats, f"calculate_skill_metrics_{config.name} - Initially merge data"):
        skill_metrics_df = pd.merge(
            simulated, observed[["code", "date", "discharge_avg", "delta"]], on=["code", "date"]
        )
        test_for_tuples(skill_metrics_df)

    # Calculate all skill metrics in a single pass per group
    with timer(
        timing_stats, f"calculate_skill_metrics_{config.name} - Calculate all skill metrics"
    ):
        skill_stats = (
            skill_metrics_df.groupby([period_col, "code", "model_short"])[
                ["discharge_avg", "forecasted_discharge", "delta"]
            ]
            .apply(
                calculate_all_skill_metrics,
                observed_col="discharge_avg",
                simulated_col="forecasted_discharge",
                delta_col="delta",
            )
            .reset_index()
        )
        test_for_tuples(skill_stats)

    with timer(
        timing_stats,
        f"calculate_skill_metrics_{config.name} - Calculate ensemble skill metrics for highly skilled forecasts",
    ):
        skill_stats_ensemble = filter_for_highly_skilled_forecasts(skill_stats)

        merge_keys = [period_col, "code", "model_short"]
        skill_metrics_df_ensemble = skill_metrics_df.merge(
            skill_stats_ensemble[merge_keys].drop_duplicates(),
            on=merge_keys,
            how="inner",
        )
        # Filter out rows where forecasted_discharge is NaN
        skill_metrics_df_ensemble = skill_metrics_df_ensemble.dropna(
            subset=["forecasted_discharge"]
        ).copy()

        # Drop columns with model_short == NE (neural ensemble)
        skill_metrics_df_ensemble = skill_metrics_df_ensemble[
            skill_metrics_df_ensemble["model_short"] != "NE"
        ].copy()

        # Perform the aggregations and keep only the unique combinations
        # Group by period + date + code so forecasts from different periods
        # are never averaged together.
        skill_metrics_df_ensemble_avg = (
            skill_metrics_df_ensemble.groupby([period_col, "date", "code"])
            .agg(
                {
                    "forecasted_discharge": "mean",
                    "model_short": composition_agg,
                }
            )
            .reset_index()
        )
        # model_short now holds the composition string
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg.rename(
            columns={"model_short": "composition"}
        )
        skill_metrics_df_ensemble_avg["model_short"] = "EM"

        # Discard single-model or empty ensembles
        skill_metrics_df_ensemble_avg = skill_metrics_df_ensemble_avg[
            skill_metrics_df_ensemble_avg["composition"].apply(is_multi_model_composition)
        ].copy()

        number_of_models = simulated["model_short"].nunique()
        logger.debug("%s number_of_models: %d", horizon, number_of_models)
        if number_of_models > 1 and not skill_metrics_df_ensemble_avg.empty:
            # Now recalculate the skill metrics for the ensemble
            ensemble_skill_metrics_df = pd.merge(
                skill_metrics_df_ensemble_avg,
                observed[["code", "date", "discharge_avg", "delta"]],
                on=["code", "date"],
            )

            # Single-pass ensemble skill metrics
            ensemble_skill_stats = (
                ensemble_skill_metrics_df.groupby(
                    [period_col, "code", "model_short", "composition"]
                )[["discharge_avg", "forecasted_discharge", "delta"]]
                .apply(
                    calculate_all_skill_metrics,
                    observed_col="discharge_avg",
                    simulated_col="forecasted_discharge",
                    delta_col="delta",
                )
                .reset_index()
            )

            # Append the ensemble skill metrics to the skill metrics
            skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ignore_index=True)

            # Calculate period in month (production date -> target period)
            ensemble_skill_metrics_df[period_in_month_col] = forecast_target_date(
                ensemble_skill_metrics_df["date"]
            ).apply(get_period_func)

            # Ensure simulated has composition column for the outer merge
            if "composition" not in simulated.columns:
                simulated = simulated.copy()
                simulated["composition"] = ""

            # Join the two dataframes
            join_cols = [
                "code",
                "date",
                period_in_month_col,
                period_col,
                "forecasted_discharge",
                "model_short",
                "composition",
            ]
            joint_forecasts = pd.merge(
                simulated, ensemble_skill_metrics_df[join_cols], on=join_cols, how="outer"
            )
        else:
            joint_forecasts = simulated.copy()

    return skill_stats, joint_forecasts, timing_stats


# ===================================================================
# Quarterly skill metrics
# ===================================================================


def calculate_quarterly_skill_metrics(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    timing_stats=None,
) -> tuple:
    """Calculate quarterly skill metrics for long-term forecasts.

    Same pattern as calculate_monthly_skill_metrics() but grouped by
    quarter_in_year instead of month_in_year.

    Args:
        observations: [code, year, quarter_in_year, discharge_avg, delta]
        forecasts: [code, year, quarter_in_year, model_short,
                    q50, q05, q10, q25, q75, q90, q95]
        timing_stats: Optional timing collector.

    Returns:
        (skill_stats_df, joint_forecasts_df, timing_stats)
    """
    return _calculate_aggregated_skill_metrics(
        observations,
        forecasts,
        period_col="quarter_in_year",
        time_group_cols=["year", "quarter_in_year", "code"],
        merge_cols=["code", "year", "quarter_in_year"],
        timing_stats=timing_stats,
    )


# ===================================================================
# Seasonal skill metrics
# ===================================================================


def calculate_seasonal_skill_metrics(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    timing_stats=None,
) -> tuple:
    """Calculate seasonal skill metrics for long-term forecasts.

    Same pattern as quarterly but grouped by season_year.
    season_in_year is always 1 (single season per year).

    Args:
        observations: [code, season_year, season_in_year,
                       discharge_avg, delta]
        forecasts: [code, season_year, season_in_year, model_short,
                    q50, q05, q10, q25, q75, q90, q95]
        timing_stats: Optional timing collector.

    Returns:
        (skill_stats_df, joint_forecasts_df, timing_stats)
    """
    return _calculate_aggregated_skill_metrics(
        observations,
        forecasts,
        period_col="season_in_year",
        time_group_cols=["season_year", "code"],
        merge_cols=["code", "season_year"],
        timing_stats=timing_stats,
    )


# ===================================================================
# Shared implementation for quarterly/seasonal skill metrics
# ===================================================================


def _calculate_aggregated_skill_metrics(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    period_col: str,
    time_group_cols: list[str],
    merge_cols: list[str],
    timing_stats=None,
) -> tuple:
    """Shared implementation for quarterly and seasonal skill metrics.

    Follows the same structure as calculate_monthly_skill_metrics():
    merge, point metrics, CRPS, EM, Skilled Mean, Naive Mean.
    """
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    metric_group_cols = [period_col, "code", "model_short"]

    empty_stats = pd.DataFrame(
        columns=[period_col, "code", "model_short"] + METRIC_ORDER + ["crps"]
    )
    empty_joint = pd.DataFrame()

    if observations.empty or forecasts.empty:
        return empty_stats, empty_joint, timing_stats

    # --- 1. Merge forecasts with observations ---
    # Build the set of observation columns we need, ensuring no duplicates.
    # period_col may already be in merge_cols (e.g. quarter_in_year).
    obs_need = set(merge_cols) | {period_col, "discharge_avg", "delta"}
    available_obs_cols = [c for c in observations.columns if c in obs_need]

    merged = pd.merge(
        forecasts,
        observations[available_obs_cols],
        on=merge_cols,
        how="inner",
        suffixes=("", "_obs"),
    )
    # Drop any _obs suffix columns created by the merge
    obs_suffix = f"{period_col}_obs"
    if obs_suffix in merged.columns:
        merged = merged.drop(columns=[obs_suffix])

    if "forecasted_discharge" not in merged.columns and "q50" in merged.columns:
        merged["forecasted_discharge"] = merged["q50"].astype(float)

    if merged.empty:
        return empty_stats, empty_joint, timing_stats

    # --- 2. Point metrics per group ---
    skill_stats = (
        merged.groupby(metric_group_cols)[["discharge_avg", "forecasted_discharge", "delta"]]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    # --- 3. CRPS per group ---
    crps_records = []
    for group_key, grp in merged.groupby(metric_group_cols):
        if isinstance(group_key, tuple) and len(group_key) == 3:
            piy, code, model = group_key
        else:
            piy, code, model = 1, group_key[0], group_key[1]
        obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
        qf_cols = [c for c in _QUANTILE_COLS if c in grp.columns]
        if len(qf_cols) == len(_QUANTILE_COLS):
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
        else:
            crps_val = np.nan
        crps_records.append(
            {
                period_col: piy,
                "code": code,
                "model_short": model,
                "crps": crps_val,
            }
        )

    crps_df = pd.DataFrame(crps_records)
    skill_stats = skill_stats.merge(crps_df, on=metric_group_cols, how="left")

    # --- 4. Ensemble Mean (EM) ---
    joint_forecasts = forecasts.copy()

    skill_stats_filtered = filter_for_highly_skilled_forecasts(skill_stats)

    skilled_merged = merged.merge(
        skill_stats_filtered[metric_group_cols].drop_duplicates(),
        on=metric_group_cols,
        how="inner",
    )
    baselines = {"EM", "Naive Mean", "Skilled Mean"}
    skilled_merged = skilled_merged[~skilled_merged["model_short"].isin(baselines)].copy()
    skilled_merged = skilled_merged.dropna(subset=["forecasted_discharge"]).copy()

    n_models = forecasts["model_short"].nunique()
    if n_models > 1 and not skilled_merged.empty:
        em_agg_dict = {
            "forecasted_discharge": "mean",
            "model_short": composition_agg,
        }
        if period_col not in time_group_cols:
            em_agg_dict[period_col] = "first"
        for qcol in _QUANTILE_COLS:
            if qcol in skilled_merged.columns:
                em_agg_dict[qcol] = "mean"
        for dcol in ("valid_from", "valid_to", "date"):
            if dcol in skilled_merged.columns:
                em_agg_dict[dcol] = "first"

        em_avg = skilled_merged.groupby(time_group_cols).agg(em_agg_dict).reset_index()
        em_avg = em_avg.rename(columns={"model_short": "composition"})
        em_avg["model_short"] = "EM"

        em_avg = em_avg[em_avg["composition"].apply(is_multi_model_composition)].copy()

        if not em_avg.empty:
            em_with_obs = pd.merge(
                em_avg,
                observations[available_obs_cols],
                on=merge_cols,
                how="inner",
                suffixes=("", "_obs"),
            )
            if obs_suffix in em_with_obs.columns:
                em_with_obs = em_with_obs.drop(columns=[obs_suffix])

            em_skill = (
                em_with_obs.groupby([period_col, "code", "model_short", "composition"])[
                    ["discharge_avg", "forecasted_discharge", "delta"]
                ]
                .apply(
                    calculate_all_skill_metrics,
                    observed_col="discharge_avg",
                    simulated_col="forecasted_discharge",
                    delta_col="delta",
                )
                .reset_index()
            )

            qf_cols = [c for c in _QUANTILE_COLS if c in em_with_obs.columns]
            if len(qf_cols) == len(_QUANTILE_COLS):
                em_crps = []
                for group_key, grp in em_with_obs.groupby([period_col, "code", "model_short"]):
                    piy, code, model = group_key
                    obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
                    qf = grp[qf_cols].to_numpy(dtype=np.float64)
                    crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
                    em_crps.append(
                        {
                            period_col: piy,
                            "code": code,
                            "model_short": model,
                            "crps": crps_val,
                        }
                    )
                em_crps_df = pd.DataFrame(em_crps)
                em_skill = em_skill.merge(
                    em_crps_df,
                    on=[period_col, "code", "model_short"],
                    how="left",
                )
            else:
                em_skill["crps"] = np.nan

            skill_stats = pd.concat([skill_stats, em_skill], ignore_index=True)
            em_avg["flag"] = 0
            joint_forecasts = _append_to_joint(joint_forecasts, em_avg)

    # --- 4b. Skilled Mean ---
    skill_stats, joint_forecasts = _add_skilled_mean_aggregated(
        skill_stats,
        merged,
        observations,
        available_obs_cols,
        merge_cols,
        time_group_cols,
        period_col,
        joint_forecasts,
    )

    # --- 5. Naive Mean ---
    return _add_naive_mean_aggregated(
        skill_stats,
        merged,
        observations,
        available_obs_cols,
        merge_cols,
        time_group_cols,
        period_col,
        timing_stats,
        joint_forecasts,
    )


def _add_naive_mean_aggregated(
    skill_stats,
    merged,
    observations,
    obs_cols,
    merge_cols,
    time_group_cols,
    period_col,
    timing_stats,
    joint_forecasts,
):
    """Naive Mean for quarterly/seasonal: unweighted all-model average."""
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    if merged.empty or observations.empty:
        return skill_stats, joint_forecasts, timing_stats

    excluded = {"EM", "Naive Mean", "Skilled Mean"}
    pool = merged[~merged["model_short"].isin(excluded)].copy()
    pool = pool.dropna(subset=["forecasted_discharge"]).copy()
    if pool.empty:
        return skill_stats, joint_forecasts, timing_stats

    agg_dict = {
        "forecasted_discharge": "mean",
        "model_short": composition_agg,
    }
    if period_col not in time_group_cols:
        agg_dict[period_col] = "first"
    for qcol in _QUANTILE_COLS:
        if qcol in pool.columns:
            agg_dict[qcol] = "mean"
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in pool.columns:
            agg_dict[dcol] = "first"

    naive_avg = pool.groupby(time_group_cols).agg(agg_dict).reset_index()
    naive_avg = naive_avg.rename(columns={"model_short": "composition"})
    naive_avg["model_short"] = "Naive Mean"
    naive_avg = naive_avg[naive_avg["composition"].apply(is_multi_model_composition)].copy()

    if naive_avg.empty:
        return skill_stats, joint_forecasts, timing_stats

    naive_with_obs = pd.merge(
        naive_avg,
        observations[obs_cols],
        on=merge_cols,
        how="inner",
        suffixes=("", "_obs"),
    )
    obs_suffix = f"{period_col}_obs"
    if obs_suffix in naive_with_obs.columns:
        naive_with_obs = naive_with_obs.drop(columns=[obs_suffix])

    if naive_with_obs.empty:
        return skill_stats, joint_forecasts, timing_stats

    naive_skill = (
        naive_with_obs.groupby([period_col, "code", "model_short", "composition"])[
            ["discharge_avg", "forecasted_discharge", "delta"]
        ]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    qf_cols = [c for c in _QUANTILE_COLS if c in naive_with_obs.columns]
    if len(qf_cols) == len(_QUANTILE_COLS):
        crps_records = []
        for group_key, grp in naive_with_obs.groupby([period_col, "code", "model_short"]):
            piy, code, model = group_key
            obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
            crps_records.append(
                {
                    period_col: piy,
                    "code": code,
                    "model_short": model,
                    "crps": crps_val,
                }
            )
        crps_df = pd.DataFrame(crps_records)
        naive_skill = naive_skill.merge(
            crps_df,
            on=[period_col, "code", "model_short"],
            how="left",
        )
    else:
        naive_skill["crps"] = np.nan

    parts = [df for df in [skill_stats, naive_skill] if not df.empty]
    skill_stats = pd.concat(parts, ignore_index=True) if parts else naive_skill

    naive_avg["flag"] = 0
    joint_forecasts = _append_to_joint(joint_forecasts, naive_avg)
    return skill_stats, joint_forecasts, timing_stats


def _add_skilled_mean_aggregated(
    skill_stats,
    merged,
    observations,
    obs_cols,
    merge_cols,
    time_group_cols,
    period_col,
    joint_forecasts,
):
    """Skilled Mean for quarterly/seasonal: 1/MAE weighted average."""
    from src.ensemble_calculator import (
        composition_agg,
        is_multi_model_composition,
    )

    if skill_stats.empty or merged.empty:
        return skill_stats, joint_forecasts

    filtered = filter_for_highly_skilled_forecasts(skill_stats)
    excluded = {"EM", "Naive Mean", "Skilled Mean"}
    filtered = filtered[~filtered["model_short"].isin(excluded)].copy()
    if filtered.empty:
        return skill_stats, joint_forecasts

    metric_group_cols = [period_col, "code", "model_short"]
    mae_df = filtered[metric_group_cols + ["mae"]].copy()
    mae_df = mae_df.dropna(subset=["mae"])
    if mae_df.empty:
        return skill_stats, joint_forecasts

    mean_mae = mae_df["mae"].mean()
    eps = mean_mae / 100.0 if mean_mae > 0 else 1e-10
    mae_df["weight"] = 1.0 / (mae_df["mae"] + eps)

    qualifying_keys = mae_df[metric_group_cols].drop_duplicates()
    sm_merged = merged.merge(qualifying_keys, on=metric_group_cols, how="inner")
    sm_merged = sm_merged.dropna(subset=["forecasted_discharge"]).copy()
    if sm_merged.empty:
        return skill_stats, joint_forecasts

    sm_merged = sm_merged.merge(
        mae_df[metric_group_cols + ["weight"]],
        on=metric_group_cols,
        how="left",
    )

    def _weighted_mean_col(group, col):
        w = sm_merged.loc[group.index, "weight"].to_numpy()
        d = group.to_numpy()
        return np.average(d, weights=w)

    sm_agg_dict = {
        "forecasted_discharge": (
            "forecasted_discharge",
            lambda x: _weighted_mean_col(x, "forecasted_discharge"),
        ),
        "composition": ("model_short", composition_agg),
    }
    # Only aggregate period_col if it's not already a groupby key
    if period_col not in time_group_cols:
        sm_agg_dict[period_col] = (period_col, "first")
    for qcol in [qc for qc in _QUANTILE_COLS if qc in sm_merged.columns]:
        sm_agg_dict[qcol] = (
            qcol,
            lambda x, _c=qcol: _weighted_mean_col(x, _c),
        )
    for dcol in ("valid_from", "valid_to", "date"):
        if dcol in sm_merged.columns:
            sm_agg_dict[dcol] = (dcol, "first")

    sm_avg = sm_merged.groupby(time_group_cols).agg(**sm_agg_dict).reset_index()
    sm_avg["model_short"] = "Skilled Mean"
    sm_avg = sm_avg[sm_avg["composition"].apply(is_multi_model_composition)].copy()

    if sm_avg.empty:
        return skill_stats, joint_forecasts

    sm_with_obs = pd.merge(
        sm_avg,
        observations[obs_cols],
        on=merge_cols,
        how="inner",
        suffixes=("", "_obs"),
    )
    obs_suffix = f"{period_col}_obs"
    if obs_suffix in sm_with_obs.columns:
        sm_with_obs = sm_with_obs.drop(columns=[obs_suffix])

    sm_skill = (
        sm_with_obs.groupby([period_col, "code", "model_short", "composition"])[
            ["discharge_avg", "forecasted_discharge", "delta"]
        ]
        .apply(
            calculate_all_skill_metrics,
            observed_col="discharge_avg",
            simulated_col="forecasted_discharge",
            delta_col="delta",
        )
        .reset_index()
    )

    qf_cols = [c for c in _QUANTILE_COLS if c in sm_with_obs.columns]
    if len(qf_cols) == len(_QUANTILE_COLS):
        sm_crps = []
        for group_key, grp in sm_with_obs.groupby([period_col, "code", "model_short"]):
            piy, code, model = group_key
            obs_arr = grp["discharge_avg"].to_numpy(dtype=np.float64)
            qf = grp[qf_cols].to_numpy(dtype=np.float64)
            crps_val = calculate_crps(obs_arr, qf, _QUANTILE_LEVELS)
            sm_crps.append(
                {
                    period_col: piy,
                    "code": code,
                    "model_short": model,
                    "crps": crps_val,
                }
            )
        sm_crps_df = pd.DataFrame(sm_crps)
        sm_skill = sm_skill.merge(
            sm_crps_df,
            on=[period_col, "code", "model_short"],
            how="left",
        )
    else:
        sm_skill["crps"] = np.nan

    skill_stats = pd.concat([skill_stats, sm_skill], ignore_index=True)
    sm_avg["flag"] = 0
    joint_forecasts = _append_to_joint(joint_forecasts, sm_avg)
    return skill_stats, joint_forecasts
