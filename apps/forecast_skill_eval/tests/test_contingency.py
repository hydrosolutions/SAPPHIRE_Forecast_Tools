from __future__ import annotations

import pandas as pd

from forecast_skill_eval.baselines import build_operational_proxy_baseline
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

    calculated = _one_row(counts, code=STATION_CODE, provenance="calculated", basin="other")
    assert _cells(calculated) == {"TP": 1, "FP": 1, "FN": 0, "TN": 0, "n_pairs": 2}

    all_station = _one_row(counts, code=STATION_CODE, provenance="all")
    assert _cells(all_station) == {"TP": 1, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 3}

    pooled = _one_row(counts, code="POOLED", provenance="all")
    assert _cells(pooled) == {"TP": 1, "FP": 1, "FN": 1, "TN": 1, "n_pairs": 4}
    assert set(counts["code"]) == {STATION_CODE, "20000", "POOLED"}


def test_counts_include_basin_rollups_station_and_pooled() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "model-a", "15999", "calculated", "TP"),
            _pair("day", "model-a", "15999", "calculated", "FP"),
            _pair("day", "model-a", "16999", "calculated", "FN"),
            _pair("day", "model-a", "17999", "official", "TN"),
            _pair("day", "model-a", "19999", "official", "TP"),
        ]
    )

    counts = count_contingencies(pairs)

    chu_station = _one_row(counts, code="15999", provenance="all", basin="chu_kyrgyz")
    assert _cells(chu_station) == {"TP": 1, "FP": 1, "FN": 0, "TN": 0, "n_pairs": 2}

    syr_pooled = _one_row(counts, code="POOLED", provenance="all", basin="syr_darya")
    assert _cells(syr_pooled) == {"TP": 0, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 1}

    all_basins = _one_row(counts, code="POOLED", provenance="all", basin="all")
    assert _cells(all_basins) == {"TP": 2, "FP": 1, "FN": 1, "TN": 1, "n_pairs": 5}
    assert {"all", "chu_kyrgyz", "syr_darya", "amu_darya", "other"}.issubset(set(counts["basin"]))


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
    # Long-term horizons emit per-lead rows only; the lead-agnostic row is gone.
    pairs = pd.DataFrame(
        [
            _pair("month", "model-b", STATION_CODE, "official", "TP", lead=1),
            _pair("month", "model-b", STATION_CODE, "official", "FN", lead=1),
            _pair("month", "model-b", STATION_CODE, "official", "FP", lead=2),
            _pair("month", "model-b", STATION_CODE, "official", "TN", lead=2),
        ]
    )

    counts = count_contingencies(pairs)

    # No lead-agnostic row for long-term horizons.
    lead_agnostic = counts[
        counts["code"].eq(STATION_CODE)
        & counts["norm_provenance"].eq("all")
        & counts["lead"].isna()
    ]
    assert lead_agnostic.empty

    lead_one = _one_row(counts, code=STATION_CODE, provenance="all", lead=1)
    assert _cells(lead_one) == {"TP": 1, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 2}

    lead_two = _one_row(counts, code="POOLED", provenance="official", lead=2)
    assert _cells(lead_two) == {"TP": 0, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 2}


# --- Fix 2: long-term per-lead stratification ---


def test_long_term_emits_per_lead_rows_only_not_lead_agnostic() -> None:
    # month horizon with two distinct leads must produce per-lead rows
    # and NO lead-agnostic (lead=NaN) row.
    pairs = pd.DataFrame(
        [
            _pair("month", "model-b", STATION_CODE, "official", "TP", lead=0),
            _pair("month", "model-b", STATION_CODE, "official", "FP", lead=0),
            _pair("month", "model-b", STATION_CODE, "official", "FN", lead=1),
            _pair("month", "model-b", STATION_CODE, "official", "TN", lead=1),
        ]
    )

    counts = count_contingencies(pairs)

    # Per-lead rows exist and are correct.
    lead_zero = _one_row(counts, code=STATION_CODE, provenance="all", lead=0)
    assert _cells(lead_zero) == {"TP": 1, "FP": 1, "FN": 0, "TN": 0, "n_pairs": 2}

    lead_one = _one_row(counts, code=STATION_CODE, provenance="all", lead=1)
    assert _cells(lead_one) == {"TP": 0, "FP": 0, "FN": 1, "TN": 1, "n_pairs": 2}

    # No lead-agnostic row for long-term horizons.
    lead_agnostic = counts[
        counts["code"].eq(STATION_CODE)
        & counts["norm_provenance"].eq("all")
        & counts["lead"].isna()
    ]
    assert lead_agnostic.empty, "Long-term horizons must not produce lead-agnostic rows"


def test_long_term_nan_lead_counts_as_its_own_group() -> None:
    # month horizon with NaN-lead records alongside a concrete lead;
    # NaN-lead records must not be merged into the concrete-lead group.
    pairs = pd.DataFrame(
        [
            _pair("month", "model-b", STATION_CODE, "official", "TP", lead=None),
            _pair("month", "model-b", STATION_CODE, "official", "FN", lead=1),
        ]
    )

    counts = count_contingencies(pairs)

    # NaN-lead group is its own row with only its own records.
    nan_lead = _one_row(counts, code=STATION_CODE, provenance="all", lead=None)
    assert _cells(nan_lead) == {"TP": 1, "FP": 0, "FN": 0, "TN": 0, "n_pairs": 1}

    # Concrete-lead group has only its own record.
    lead_one = _one_row(counts, code=STATION_CODE, provenance="all", lead=1)
    assert _cells(lead_one) == {"TP": 0, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 1}


def test_short_term_still_emits_lead_agnostic_row() -> None:
    # day horizon (short-term) must continue emitting the single lead-agnostic row.
    pairs = pd.DataFrame(
        [
            _pair("day", "model-a", STATION_CODE, "official", "TP"),
            _pair("day", "model-a", STATION_CODE, "official", "FN"),
        ]
    )

    counts = count_contingencies(pairs)

    station_row = _one_row(counts, code=STATION_CODE, provenance="all")
    assert pd.isna(station_row["lead"])
    assert _cells(station_row) == {"TP": 1, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 2}


def test_operational_proxy_baseline_builds_and_carries_basin() -> None:
    pairs = pd.DataFrame(
        [
            _pair("day", "TFT", "15999", "calculated", "TP"),
            _pair("day", "TFT", "16999", "calculated", "FN"),
            _pair("day", "LR", "15999", "calculated", "TN"),
            _pair("day", "LR", "16999", "calculated", "FP"),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    assert not baseline.empty
    assert "basin" in baseline.columns
    candidate = _one_row(
        baseline,
        code="POOLED",
        provenance="all",
        basin="all",
        model="TFT",
    )
    assert candidate["comparison_model"] == "TFT"
    assert candidate["is_proxy"] is False
    assert int(candidate["n_matched"]) == 2


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
        "basin": _basin_for_test_code(code),
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
    basin: str = "all",
    model: str | None = None,
) -> pd.Series:
    selected = frame[
        (frame["code"] == code)
        & (frame["basin"] == basin)
        & (frame["regime"] == regime)
        & (frame["norm_provenance"] == provenance)
        & (frame["lead"].isna() if lead is None else frame["lead"].eq(lead))
    ]
    if model is not None:
        selected = selected[selected["model"] == model]
    assert len(selected) == 1
    return selected.iloc[0]


def _cells(row: pd.Series) -> dict[str, int]:
    return {label: int(row[label]) for label in ("TP", "FP", "FN", "TN", "n_pairs")}


def _basin_for_test_code(code: str) -> str:
    return {
        "15": "chu_kyrgyz",
        "16": "syr_darya",
        "17": "amu_darya",
    }.get(code[:2], "other")
