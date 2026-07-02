from __future__ import annotations

import math

import pytest

from forecast_skill_eval.metrics import metrics_from_counts


def test_metrics_match_hand_computed_confusion_matrix() -> None:
    metrics = metrics_from_counts({"TP": 3, "FP": 1, "FN": 2, "TN": 4})

    assert metrics["base_rate"] == pytest.approx(0.5)
    assert metrics["pod"] == pytest.approx(0.6)
    assert metrics["far"] == pytest.approx(0.25)
    assert metrics["pofd"] == pytest.approx(0.2)
    assert metrics["csi"] == pytest.approx(0.5)
    assert metrics["frequency_bias"] == pytest.approx(0.8)
    assert metrics["hss"] == pytest.approx(0.4)
    assert metrics["pss"] == pytest.approx(0.4)
    assert metrics["pod_ci_lower"] == pytest.approx(0.2307242812760128)
    assert metrics["pod_ci_upper"] == pytest.approx(0.882379225767352)
    assert metrics["far_ci_lower"] == pytest.approx(0.04558726080970055)
    assert metrics["far_ci_upper"] == pytest.approx(0.6993581574175981)


def test_zero_denominators_emit_nan_and_undefined_flags() -> None:
    metrics = metrics_from_counts({"TP": 0, "FP": 0, "FN": 0, "TN": 0})

    for name in ("pod", "far", "pofd", "hss", "pss"):
        assert math.isnan(metrics[name])
        assert metrics[f"{name}_undefined"] is True
    assert math.isnan(metrics["pod_ci_lower"])
    assert math.isnan(metrics["pod_ci_upper"])
    assert metrics["pod_ci_undefined"] is True
    assert metrics["far_ci_undefined"] is True


def test_hss_and_pss_are_undefined_when_observed_positives_are_zero() -> None:
    metrics = metrics_from_counts({"TP": 0, "FP": 1, "FN": 0, "TN": 4})

    assert math.isnan(metrics["hss"])
    assert metrics["hss_undefined"] is True
    assert math.isnan(metrics["pss"])
    assert metrics["pss_undefined"] is True
    assert metrics["far"] == pytest.approx(1.0)
    assert metrics["pofd"] == pytest.approx(0.2)
