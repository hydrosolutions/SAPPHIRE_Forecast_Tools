from __future__ import annotations

import pandas as pd

from forecast_skill_eval.contingency import count_contingencies

STATION_CODE = "19999"


def test_counts_include_provenance_rollups_station_and_pooled() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "model-a", STATION_CODE, "calculated", "TP"),
            _pair("day", "model-a", STATION_CODE, "calculated", "FP"),
            _pair("day", "model-a", STATION_CODE, "official", "TN"),
            _pair("day", "model-a", "20000", "calculated", "FN"),
        ]
    )

    counts = count_contingencies(pairs)

    calculated = _one_row(counts, code=STATION_CODE, provenance="calculated")
    assert _cells(calculated) == {"TP": 1, "FP": 1, "FN": 0, "TN": 0, "n_pairs": 2}

    all_station = _one_row(counts, code=STATION_CODE, provenance="all")
    assert _cells(all_station) == {"TP": 1, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 3}

    pooled = _one_row(counts, code="POOLED", provenance="all")
    assert _cells(pooled) == {"TP": 1, "FP": 1, "FN": 1, "TN": 1, "n_pairs": 4}
    assert set(counts["code"]) == {STATION_CODE, "20000", "POOLED"}


def test_counts_include_regime_rollups_station_and_pooled() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "model-a", STATION_CODE, "calculated", "TP", regime="operational"),
            _pair("day", "model-a", STATION_CODE, "calculated", "FP", regime="hindcast"),
            _pair("day", "model-a", "20000", "calculated", "FN", regime="hindcast"),
        ]
    )

    counts = count_contingencies(pairs)

    operational = _one_row(counts, code=STATION_CODE, provenance="all", regime="operational")
    assert _cells(operational) == {"TP": 1, "FP": 0, "FN": 0, "TN": 0, "n_pairs": 1}

    hindcast = _one_row(counts, code="POOLED", provenance="calculated", regime="hindcast")
    assert _cells(hindcast) == {"TP": 0, "FP": 1, "FN": 1, "TN": 0, "n_pairs": 2}

    all_regimes = _one_row(counts, code="POOLED", provenance="calculated", regime="all")
    assert _cells(all_regimes) == {"TP": 1, "FP": 1, "FN": 1, "TN": 0, "n_pairs": 3}
    assert {"all", "operational", "hindcast"}.issubset(set(counts["regime"]))


def test_long_term_counts_include_per_lead_breakdown() -> None:
    pairs = pd.DataFrame(
        [
            _pair("month", "model-b", STATION_CODE, "official", "TP", lead=1),
            _pair("month", "model-b", STATION_CODE, "official", "FN", lead=1),
            _pair("month", "model-b", STATION_CODE, "official", "FP", lead=2),
            _pair("month", "model-b", STATION_CODE, "official", "TN", lead=2),
        ]
    )

    counts = count_contingencies(pairs)

    all_leads = _one_row(counts, code=STATION_CODE, provenance="all")
    assert pd.isna(all_leads["lead"])
    assert _cells(all_leads) == {"TP": 1, "FP": 1, "FN": 1, "TN": 1, "n_pairs": 4}

    lead_one = _one_row(counts, code=STATION_CODE, provenance="all", lead=1)
    assert _cells(lead_one) == {"TP": 1, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 2}

    lead_two = _one_row(counts, code="POOLED", provenance="official", lead=2)
    assert _cells(lead_two) == {"TP": 0, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 2}


def _pair(
    horizon: str,
    model: str,
    code: str,
    provenance: str,
    contingency: str,
    *,
    lead: int | None = None,
    regime: str = "operational",
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": code,
        "period_key": 1,
        "year": 2024,
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
    provenance: str,
    lead: int | None = None,
    regime: str = "all",
) -> pd.Series:
    selected = frame[
        (frame["code"] == code)
        & (frame["regime"] == regime)
        & (frame["norm_provenance"] == provenance)
        & (frame["lead"].isna() if lead is None else frame["lead"].eq(lead))
    ]
    assert len(selected) == 1
    return selected.iloc[0]


def _cells(row: pd.Series) -> dict[str, int]:
    return {label: int(row[label]) for label in ("TP", "FP", "FN", "TN", "n_pairs")}
