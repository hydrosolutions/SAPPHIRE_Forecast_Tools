from __future__ import annotations

import pandas as pd

from forecast_skill_eval.baselines import (
    build_climatology_baseline,
    build_operational_proxy_baseline,
)

STATION_CODE = "19999"


def test_climatology_baseline_is_always_normal() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "model-a", STATION_CODE, 1, 2024, "calculated", "TP"),
            _pair("day", "model-a", STATION_CODE, 2, 2024, "calculated", "FP"),
            _pair("day", "model-a", STATION_CODE, 3, 2024, "calculated", "FN"),
            _pair("day", "model-a", STATION_CODE, 4, 2024, "calculated", "TN"),
        ]
    )

    baseline = build_climatology_baseline(pairs)

    row = _one_row(baseline, code=STATION_CODE, model="climatology")
    assert row["baseline"] == "climatology"
    assert row["comparison_model"] == "model-a"
    assert row["is_proxy"] is False
    assert _cells(row) == {"TP": 0, "FP": 0, "FN": 2, "TN": 2, "n_pairs": 4}
    assert row["hss"] == 0
    assert row["pss"] == 0


def test_operational_proxy_uses_lr_intersection_for_short_term_matching() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "TFT", STATION_CODE, 1, 2024, "calculated", "TP"),
            _pair("day", "TFT", STATION_CODE, 2, 2024, "calculated", "FN"),
            _pair("day", "TFT", STATION_CODE, 3, 2024, "calculated", "TN"),
            _pair("day", "LR", STATION_CODE, 1, 2024, "calculated", "TN"),
            _pair("day", "LR", STATION_CODE, 2, 2024, "calculated", "FP"),
            _pair("day", "LR", STATION_CODE, 4, 2024, "calculated", "TP"),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    candidate = _one_row(baseline, code=STATION_CODE, model="TFT")
    assert candidate["comparison_model"] == "TFT"
    assert candidate["is_proxy"] is False
    assert int(candidate["n_matched"]) == 2
    assert _cells(candidate) == {"TP": 1, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 2}

    proxy = _one_row(baseline, code=STATION_CODE, model="LR")
    assert proxy["comparison_model"] == "TFT"
    assert proxy["is_proxy"] is True
    assert int(proxy["n_matched"]) == 2
    assert _cells(proxy) == {"TP": 0, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 2}


def test_operational_proxy_uses_lr_base_and_lead_for_long_term_matching() -> None:
    pairs = pd.DataFrame(
        [
            _pair("month", "candidate", STATION_CODE, 4, 2024, "official", "TP", lead=1),
            _pair("month", "candidate", STATION_CODE, 4, 2024, "official", "FN", lead=2),
            _pair("month", "LR_Base", STATION_CODE, 4, 2024, "official", "TN", lead=1),
            _pair("month", "LR_Base", STATION_CODE, 4, 2024, "official", "FP", lead=3),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    candidate = _one_row(baseline, code=STATION_CODE, model="candidate")
    assert int(candidate["n_matched"]) == 1
    assert _cells(candidate) == {"TP": 1, "FP": 0, "FN": 0, "TN": 0, "n_pairs": 1}

    proxy = _one_row(baseline, code=STATION_CODE, model="LR_Base")
    assert proxy["is_proxy"] is True
    assert int(proxy["n_matched"]) == 1
    assert _cells(proxy) == {"TP": 0, "FP": 0, "FN": 0, "TN": 1, "n_pairs": 1}


def test_operational_proxy_matches_within_regime() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "candidate", STATION_CODE, 1, 2024, "calculated", "TP"),
            _pair("day", "LR", STATION_CODE, 1, 2024, "calculated", "FP", regime="hindcast"),
            _pair("day", "LR", STATION_CODE, 2, 2024, "calculated", "TN"),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    assert baseline.empty


def _pair(
    horizon: str,
    model: str,
    code: str,
    period_key: int,
    year: int,
    provenance: str,
    contingency: str,
    *,
    lead: int | None = None,
    regime: str = "operational",
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": code,
        "period_key": period_key,
        "year": year,
        "model": model,
        "regime": regime,
        "lead": lead,
        "norm_provenance": provenance,
        "contingency": contingency,
    }


def _one_row(
    frame: pd.DataFrame,
    *,
    code: str,
    model: str,
) -> pd.Series:
    selected = frame[
        (frame["code"] == code)
        & (frame["model"] == model)
        & (frame["regime"] == "all")
        & (frame["norm_provenance"] == "all")
        & (frame["lead"].isna())
    ]
    assert len(selected) == 1
    return selected.iloc[0]


def _cells(row: pd.Series) -> dict[str, int]:
    return {label: int(row[label]) for label in ("TP", "FP", "FN", "TN", "n_pairs")}
