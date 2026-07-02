from __future__ import annotations

import math

import pytest

from forecast_skill_eval.classifier import classify, contingency


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (7.999, "below"),
        (8.0, "normal"),
        (8.001, "normal"),
    ],
)
def test_classify_uses_strict_below_threshold(value: float, expected: str) -> None:
    assert classify(value, threshold=0.8, norm=10.0) == expected


@pytest.mark.parametrize(
    ("value", "norm"),
    [
        (1.0, None),
        (1.0, 0.0),
        (1.0, -1.0),
        (1.0, math.nan),
        (1.0, math.inf),
        (None, 10.0),
        (math.nan, 10.0),
        (math.inf, 10.0),
    ],
)
def test_classify_returns_none_for_unclassifiable_inputs(
    value: float | None,
    norm: float | None,
) -> None:
    assert classify(value, threshold=0.8, norm=norm) is None


@pytest.mark.parametrize(
    ("forecast_class", "observed_class", "expected"),
    [
        ("below", "below", "TP"),
        ("below", "normal", "FP"),
        ("normal", "below", "FN"),
        ("normal", "normal", "TN"),
    ],
)
def test_contingency_uses_below_as_positive_class(
    forecast_class: str,
    observed_class: str,
    expected: str,
) -> None:
    assert contingency(forecast_class, observed_class) == expected
