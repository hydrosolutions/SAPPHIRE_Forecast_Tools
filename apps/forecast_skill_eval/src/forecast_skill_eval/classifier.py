from __future__ import annotations

import math
from typing import Literal

ClassLabel = Literal["below", "normal"]
ContingencyLabel = Literal["TP", "FP", "FN", "TN"]


def classify(
    value: float | None,
    threshold: float,
    norm: float | None,
) -> ClassLabel | None:
    """Classify a finite discharge value against a thresholded norm."""
    if value is None or norm is None:
        return None
    if not math.isfinite(value) or not math.isfinite(norm) or not math.isfinite(threshold):
        return None
    if norm <= 0:
        return None
    return "below" if value < threshold * norm else "normal"


def contingency(
    forecast_class: str,
    observed_class: str,
) -> ContingencyLabel:
    """Return the contingency-cell label with ``below`` as the positive class."""
    if forecast_class == "below" and observed_class == "below":
        return "TP"
    if forecast_class == "below" and observed_class == "normal":
        return "FP"
    if forecast_class == "normal" and observed_class == "below":
        return "FN"
    if forecast_class == "normal" and observed_class == "normal":
        return "TN"
    raise ValueError(
        f"Unsupported class pair: forecast={forecast_class!r}, observed={observed_class!r}"
    )
