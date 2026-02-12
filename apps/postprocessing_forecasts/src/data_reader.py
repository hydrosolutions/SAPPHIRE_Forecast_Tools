"""Read pre-calculated skill metrics from CSV or API.

Used by the operational and maintenance entry points to avoid
recalculating skill metrics from scratch.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Reverse mapping: model_short -> model_long
# Matches the model_mapping in setup_library.py and model_type_map in
# forecast_library.py so that API records get a model_long column.
MODEL_SHORT_TO_LONG = {
    "LR": "Linear regression (LR)",
    "TFT": "Temporal Fusion Transformer (TFT)",
    "TiDE": "Time-series Dense Encoder (TiDE)",
    "TSMixer": "Time-Series Mixer (TSMixer)",
    "ARIMA": "AutoRegressive Integrated Moving Average (ARIMA)",
    "RRMAMBA": "Rainfall-Runoff Mamba (RRMAMBA)",
    "RRAM": "Rainfall-Runoff Mamba (RRAM)",
    "EM": "Ensemble Mean (EM)",
    "NE": "Neural Ensemble (NE)",
}

# API model_type -> CSV model_short (inverted model_type_map)
API_MODEL_TYPE_TO_SHORT = {
    "LR": "LR",
    "TFT": "TFT",
    "TiDE": "TiDE",
    "TSMixer": "TSMixer",
    "EM": "EM",
    "NE": "NE",
    "RRAM": "RRAM",
}

try:
    from sapphire_api_client.postprocessing import (
        SapphirePostprocessingClient,
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False


def read_skill_metrics(horizon_type: str) -> pd.DataFrame:
    """Read pre-calculated skill metrics from CSV (primary) or API (fallback).

    Args:
        horizon_type: 'pentad' or 'decad'

    Returns:
        DataFrame with columns: [pentad_in_year|decad_in_year, code,
        model_long, model_short, sdivsigma, nse, delta, accuracy, mae,
        n_pairs]

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad"):
        raise ValueError(
            f"horizon_type must be 'pentad' or 'decad', got: {horizon_type}"
        )

    df = _read_skill_metrics_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from CSV (%s)", len(df), horizon_type
        )
        return df

    logger.info(
        "CSV skill metrics empty or missing for %s, trying API",
        horizon_type,
    )
    df = _read_skill_metrics_api(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from API (%s)", len(df), horizon_type
        )
        return df

    logger.warning("No skill metrics available for %s", horizon_type)
    return pd.DataFrame()


def _read_skill_metrics_csv(horizon_type: str) -> pd.DataFrame | None:
    """Read skill metrics from CSV file.

    Returns None if the file doesn't exist or can't be read.
    """
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")

    if horizon_type == "pentad":
        filename = os.getenv(
            "ieasyforecast_pentadal_skill_metrics_file", ""
        )
    else:
        filename = os.getenv(
            "ieasyforecast_decadal_skill_metrics_file", ""
        )

    if not intermediate_path or not filename:
        logger.debug(
            "Skill metrics env vars not set for %s", horizon_type
        )
        return None

    filepath = os.path.join(intermediate_path, filename)
    if not os.path.exists(filepath):
        logger.debug("Skill metrics CSV not found: %s", filepath)
        return None

    try:
        df = pd.read_csv(filepath)
        # Ensure code is string
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.replace(
                r"\.0$", "", regex=True
            )
        return df
    except Exception as e:
        logger.error("Failed to read skill metrics CSV %s: %s", filepath, e)
        return None


def _read_skill_metrics_api(horizon_type: str) -> pd.DataFrame | None:
    """Read skill metrics from SAPPHIRE postprocessing API.

    Returns None if the API is unavailable or returns no data.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.debug("sapphire-api-client not installed, skipping API read")
        return None

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower()
    if api_enabled == "false":
        logger.debug("SAPPHIRE_API_ENABLED=false, skipping API read")
        return None

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")

    try:
        client = SapphirePostprocessingClient(base_url=api_url)
        if not client.is_ready():
            logger.warning("Postprocessing API not ready at %s", api_url)
            return None

        # Read all skill metrics for this horizon; paginate if needed
        all_records = []
        skip = 0
        batch_size = 1000
        while True:
            df_batch = client.read_skill_metrics(
                horizon=horizon_type, skip=skip, limit=batch_size
            )
            if df_batch is None or df_batch.empty:
                break
            all_records.append(df_batch)
            if len(df_batch) < batch_size:
                break
            skip += batch_size

        if not all_records:
            return None

        df = pd.concat(all_records, ignore_index=True)
        return _normalize_api_skill_metrics(df, horizon_type)

    except Exception as e:
        logger.error("Failed to read skill metrics from API: %s", e)
        return None


def _normalize_api_skill_metrics(
    df: pd.DataFrame, horizon_type: str
) -> pd.DataFrame:
    """Convert API column names to CSV-compatible column names.

    API returns: horizon_in_year, model_type, code, sdivsigma, nse,
                 delta, accuracy, mae, n_pairs
    CSV expects: pentad_in_year|decad_in_year, model_short, model_long,
                 code, sdivsigma, nse, delta, accuracy, mae, n_pairs
    """
    period_col = (
        "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    )

    # Rename API columns
    rename_map = {
        "horizon_in_year": period_col,
        "model_type": "model_short",
    }
    df = df.rename(columns=rename_map)

    # Map model_short to model_long (API doesn't return model_long)
    if "model_short" in df.columns:
        df["model_long"] = df["model_short"].map(MODEL_SHORT_TO_LONG)
        # Fall back to "Unknown (<short>)" for unmapped models
        mask = df["model_long"].isna()
        if mask.any():
            df.loc[mask, "model_long"] = df.loc[mask, "model_short"].apply(
                lambda s: f"Unknown ({s})"
            )

    # Ensure code is string
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(
            r"\.0$", "", regex=True
        )

    return df
