"""Model-name normalization helpers for postprocessing forecast labels."""

from __future__ import annotations

import pandas as pd

_MODEL_ALIASES = {
    "EM": "ENSEMBLE_MEAN",
    "ENSEMBLE_MEAN": "ENSEMBLE_MEAN",
    "NAIVE_MEAN": "NAIVE_MEAN",
    "SKILLED_MEAN": "SKILLED_MEAN",
}

AGGREGATED_EM_RAW_MODELS = frozenset({"LR_BASE", "LR_SM"})
AGGREGATED_ENSEMBLE_MODELS = frozenset({"ENSEMBLE_MEAN", "NAIVE_MEAN", "SKILLED_MEAN"})
AGGREGATED_SUPPORTED_MODELS = AGGREGATED_EM_RAW_MODELS | AGGREGATED_ENSEMBLE_MODELS


def canonical_model_short(model_short: object) -> str:
    """Return a case-insensitive canonical model identifier."""
    if pd.isna(model_short):
        return ""
    key = str(model_short).strip().upper().replace(" ", "_")
    return _MODEL_ALIASES.get(key, key)


def canonical_model_short_series(model_shorts: pd.Series) -> pd.Series:
    """Vectorized canonical model identifiers for model_short columns."""
    return (
        model_shorts.astype("string")
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
        .replace(_MODEL_ALIASES)
        .fillna("")
    )
