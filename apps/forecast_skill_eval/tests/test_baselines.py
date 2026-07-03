from __future__ import annotations

import pandas as pd

from forecast_skill_eval.baselines import (
    build_climatology_baseline,
    build_operational_proxy_baseline,
    build_persistence_baseline,
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
    # After Fix 2, long-term baselines carry a concrete lead (not NaN);
    # matching is on lead=1 (the only shared lead between candidate and LR_Base).
    pairs = pd.DataFrame(
        [
            _pair("month", "candidate", STATION_CODE, 4, 2024, "official", "TP", lead=1),
            _pair("month", "candidate", STATION_CODE, 4, 2024, "official", "FN", lead=2),
            _pair("month", "LR_Base", STATION_CODE, 4, 2024, "official", "TN", lead=1),
            _pair("month", "LR_Base", STATION_CODE, 4, 2024, "official", "FP", lead=3),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    candidate = _one_row(baseline, code=STATION_CODE, model="candidate", lead=1)
    assert int(candidate["n_matched"]) == 1
    assert _cells(candidate) == {"TP": 1, "FP": 0, "FN": 0, "TN": 0, "n_pairs": 1}

    proxy = _one_row(baseline, code=STATION_CODE, model="LR_Base", lead=1)
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
    issue_date: str | None = None,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": code,
        "basin": "other",
        "period_key": period_key,
        "year": year,
        "model": model,
        "regime": regime,
        "lead": lead,
        "norm_provenance": provenance,
        "contingency": contingency,
        "issue_date": issue_date,
    }


def _one_row(
    frame: pd.DataFrame,
    *,
    code: str,
    model: str,
    lead: int | None = None,
) -> pd.Series:
    selected = frame[
        (frame["code"] == code)
        & (frame["basin"] == "all")
        & (frame["model"] == model)
        & (frame["regime"] == "all")
        & (frame["norm_provenance"] == "all")
        & (frame["lead"].isna() if lead is None else frame["lead"].eq(lead))
    ]
    assert len(selected) == 1
    return selected.iloc[0]


def _cells(row: pd.Series) -> dict[str, int]:
    return {label: int(row[label]) for label in ("TP", "FP", "FN", "TN", "n_pairs")}


# ---------------------------------------------------------------------------
# Persistence baseline (Phase 2B)
# ---------------------------------------------------------------------------


def test_operational_proxy_equalizes_reissued_model_pairs() -> None:
    """Re-issued model forecasts for a matched key must not inflate the model
    side relative to the single-issue proxy side.

    Both emitted rows must carry exactly one pair per matched key, so their
    n_pairs are equal and equal to the number of matched keys.
    """
    pairs = pd.DataFrame(
        [
            # Two re-issues of the model forecast for the SAME matched key.
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TP",
                issue_date="2024-01-01",
            ),
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "FP",
                issue_date="2024-01-05",
            ),
            # A single proxy pair for the same key.
            _pair(
                "day",
                "LR",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TN",
                issue_date="2024-01-03",
            ),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    model_row = _one_row(baseline, code=STATION_CODE, model="TFT")
    proxy_row = _one_row(baseline, code=STATION_CODE, model="LR")

    # One matched key → one pair per side → equal n_pairs.
    assert int(model_row["n_pairs"]) == 1
    assert int(proxy_row["n_pairs"]) == 1
    assert int(model_row["n_pairs"]) == int(proxy_row["n_pairs"])
    # The representative kept for the model side is the LATEST issue_date
    # (2024-01-05 → FP), not the earlier 2024-01-01 → TP.
    assert _cells(model_row) == {"TP": 0, "FP": 1, "FN": 0, "TN": 0, "n_pairs": 1}
    assert _cells(proxy_row) == {"TP": 0, "FP": 0, "FN": 0, "TN": 1, "n_pairs": 1}


def test_operational_proxy_keeps_latest_issue_date_representative() -> None:
    """The kept representative per key is the row with the latest issue_date,
    regardless of the order rows appear in the frame."""
    pairs = pd.DataFrame(
        [
            # Latest issue_date first in the frame, oldest last, to prove the
            # choice is driven by issue_date and not by row order.
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TN",
                issue_date="2024-03-01",
            ),
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TP",
                issue_date="2024-01-01",
            ),
            _pair(
                "day",
                "LR",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "FP",
                issue_date="2024-02-01",
            ),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    model_row = _one_row(baseline, code=STATION_CODE, model="TFT")
    # 2024-03-01 → TN wins over 2024-01-01 → TP.
    assert _cells(model_row) == {"TP": 0, "FP": 0, "FN": 0, "TN": 1, "n_pairs": 1}


def test_operational_proxy_single_pair_per_key_unchanged_by_collapse() -> None:
    """With exactly one pair per key on both sides the collapse is a no-op:
    the contingency cells match the pre-fix result exactly."""
    pairs = pd.DataFrame(
        [
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TP",
                issue_date="2024-01-01",
            ),
            _pair(
                "day",
                "TFT",
                STATION_CODE,
                2,
                2024,
                "calculated",
                "FN",
                issue_date="2024-01-02",
            ),
            _pair(
                "day",
                "LR",
                STATION_CODE,
                1,
                2024,
                "calculated",
                "TN",
                issue_date="2024-01-01",
            ),
            _pair(
                "day",
                "LR",
                STATION_CODE,
                2,
                2024,
                "calculated",
                "FP",
                issue_date="2024-01-02",
            ),
        ]
    )

    baseline = build_operational_proxy_baseline(pairs)

    model_row = _one_row(baseline, code=STATION_CODE, model="TFT")
    proxy_row = _one_row(baseline, code=STATION_CODE, model="LR")

    assert _cells(model_row) == {"TP": 1, "FP": 0, "FN": 1, "TN": 0, "n_pairs": 2}
    assert _cells(proxy_row) == {"TP": 0, "FP": 1, "FN": 0, "TN": 1, "n_pairs": 2}


def _pair_with_observed(
    horizon: str,
    model: str,
    code: str,
    period_key: int,
    year: int,
    provenance: str,
    contingency_label: str,
    observed_value: float,
    norm: float,
    *,
    lead: int | None = None,
    regime: str = "operational",
    fc_class: str = "below",
    obs_class: str = "below",
    season: str = "irrigation",
) -> dict[str, object]:
    """Build a full pair row including observed_value, norm, and classification."""
    return {
        "horizon": horizon,
        "code": code,
        "basin": "other",
        "period_key": period_key,
        "year": year,
        "model": model,
        "regime": regime,
        "lead": lead,
        "norm_provenance": provenance,
        "contingency": contingency_label,
        "observed_value": observed_value,
        "norm": norm,
        "fc_class": fc_class,
        "obs_class": obs_class,
        "forecast_value": observed_value,  # placeholder; will be overridden
        "season": season,
        "issue_date": "2024-01-01",
    }


def test_persistence_baseline_prior_below_norm_predicts_below_norm_tp() -> None:
    """When lag-1 observed < 0.80×norm and current observed is also below → TP."""
    # period_key=2: prior is (code, horizon, 1, 2024) with value 6.0
    # norm=10.0 → threshold=8.0; 6.0 < 8.0 → persistence predicts "below"
    # current obs_class="below" → TP
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TP",
                observed_value=6.0,
                norm=10.0,
                fc_class="below",
                obs_class="below",
            ),
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                2,
                2024,
                "official",
                "TP",
                observed_value=7.0,
                norm=10.0,
                fc_class="below",
                obs_class="below",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    assert not baseline.empty
    assert "persistence" in set(baseline["baseline"])
    # Find the POOLED row for period_key=2 (the one that can have a lag-1)
    model_rows = baseline[baseline["comparison_model"] == "model-a"]
    assert not model_rows.empty
    # period_key=2 can use period_key=1 as lag-1 (value=6.0 < 8.0 → "below" → TP)
    # period_key=1 has no lag-1 in the data → excluded
    # So we get n_matched = 1 (only period_key=2 is scoreable)
    pooled = model_rows[
        (model_rows["code"] == "POOLED")
        & (model_rows["norm_provenance"] == "all")
        & (model_rows["regime"] == "all")
    ]
    assert len(pooled) >= 1
    row = pooled.iloc[0]
    assert int(row["TP"]) >= 1  # at least one TP (period_key=2 with below-norm persistence)


def test_persistence_baseline_prior_above_norm_predicts_normal_fn() -> None:
    """When lag-1 observed >= 0.80×norm but current is below → FN (missed event)."""
    # period_key=1 observed=9.0 → above 8.0 threshold → persistence predicts "normal"
    # period_key=2 current obs_class="below" → FN
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                2,
                2024,
                "official",
                "FN",
                observed_value=5.0,
                norm=10.0,
                fc_class="normal",
                obs_class="below",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    assert not baseline.empty
    pooled = baseline[
        (baseline["code"] == "POOLED")
        & (baseline["norm_provenance"] == "all")
        & (baseline["regime"] == "all")
        & (baseline["comparison_model"] == "model-a")
    ]
    assert len(pooled) >= 1
    row = pooled.iloc[0]
    # lag-1 value=9.0 >= 0.80×10.0=8.0 → persistence="normal"; obs="below" → FN
    assert int(row["FN"]) >= 1


def test_persistence_baseline_first_period_excluded_when_no_lag1_available() -> None:
    """Pairs at period_key=1 with no prior-year observed must be excluded (no lag-1)."""
    # Only period_key=1 exists; no lag-1 in the data → all pairs excluded
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TP",
                observed_value=6.0,
                norm=10.0,
                fc_class="below",
                obs_class="below",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    # Period_key=1 has no lag-1 → baseline should be empty
    assert baseline.empty


def test_persistence_baseline_emits_persistence_label() -> None:
    """build_persistence_baseline rows must carry baseline='persistence'."""
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                2,
                2024,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    assert not baseline.empty
    assert set(baseline["baseline"]) == {"persistence"}


def test_persistence_baseline_appears_in_baselines_csv_schema() -> None:
    """BASELINE_COLUMNS must include the same key columns as climatology."""
    from forecast_skill_eval.baselines import BASELINE_COLUMNS

    required = ("baseline", "comparison_model", "is_proxy", "n_matched", "TP", "FP", "FN", "TN")
    for col in required:
        assert col in BASELINE_COLUMNS, f"Expected {col!r} in BASELINE_COLUMNS"


def test_persistence_baseline_month_lag1_uses_prior_month() -> None:
    """For month horizon, period_key=2's lag-1 is period_key=1 of the same year."""
    # month=1 observed=6.0; month=2 uses 6.0 as persistence forecast
    # norm=10.0 → threshold=8.0; 6.0 < 8.0 → below → TP if obs=below
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "month",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TP",
                observed_value=6.0,
                norm=10.0,
                fc_class="below",
                obs_class="below",
            ),
            _pair_with_observed(
                "month",
                "model-a",
                STATION_CODE,
                2,
                2024,
                "official",
                "TP",
                observed_value=6.0,
                norm=10.0,
                fc_class="below",
                obs_class="below",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    assert not baseline.empty
    # Should have at least one row (period_key=2 can use period_key=1 as lag-1)
    pooled = baseline[
        (baseline["code"] == "POOLED")
        & (baseline["norm_provenance"] == "all")
        & (baseline["regime"] == "all")
        & (baseline["comparison_model"] == "model-a")
    ]
    assert not pooled.empty


def test_persistence_baseline_cross_year_lag1() -> None:
    """period_key=1 can use prior year's last period as lag-1 if available."""
    # prior year Dec (month=12 of year 2023); current Jan (month=1, 2024)
    pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "month",
                "model-a",
                STATION_CODE,
                12,
                2023,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
            _pair_with_observed(
                "month",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "FN",
                observed_value=5.0,
                norm=10.0,
                fc_class="normal",
                obs_class="below",
            ),
        ]
    )

    baseline = build_persistence_baseline(pairs)

    # month=1 of 2024 can use month=12 of 2023 as lag-1 (9.0 >= 8.0 → "normal")
    # obs_class="below" → FN
    assert not baseline.empty
    pooled = baseline[
        (baseline["code"] == "POOLED")
        & (baseline["norm_provenance"] == "all")
        & (baseline["regime"] == "all")
        & (baseline["comparison_model"] == "model-a")
    ]
    assert not pooled.empty
    row = pooled.iloc[0]
    # lag-1 value=9.0 → normal; obs=below → FN
    assert int(row["FN"]) >= 1


def test_persistence_baseline_wired_in_orchestrator() -> None:
    """The orchestrator ResultsBundle must include persistence rows in baselines."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from forecast_skill_eval.config import ForecastSkillEvalConfig
    from forecast_skill_eval.ledger import ExclusionLedger
    from forecast_skill_eval.orchestrator import run

    # Build a minimal pairs df that has lag-1 coverage for at least one pair.
    # period_key=2 can use period_key=1 as lag-1, so persistence is computable.
    mock_pairs = pd.DataFrame(
        [
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                1,
                2024,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
            _pair_with_observed(
                "pentad",
                "model-a",
                STATION_CODE,
                2,
                2024,
                "official",
                "TN",
                observed_value=9.0,
                norm=10.0,
                fc_class="normal",
                obs_class="normal",
            ),
        ]
    )

    config = ForecastSkillEvalConfig(horizons=["pentad"])
    fake_ledger = ExclusionLedger()

    with patch("forecast_skill_eval.orchestrator.build_pairs") as mock_build_pairs:
        mock_build_pairs.return_value = (mock_pairs, fake_ledger)
        client = MagicMock()
        result = run(config, client, "test-run")

    baseline_labels = set(result.baselines.get("baseline", pd.Series([])).unique())
    assert "persistence" in baseline_labels, (
        f"Expected 'persistence' in baselines but got: {baseline_labels}"
    )


# ---------------------------------------------------------------------------
# P1b: event column / below_norm_100 parity
# ---------------------------------------------------------------------------


def _full_pair(
    *,
    period_key: int,
    year: int,
    forecast_value: float,
    observed_value: float,
    norm: float = 10.0,
    model: str = "model-a",
    threshold: float = 0.80,
) -> dict[str, object]:
    """Build a complete pentad pair row classified at ``threshold × norm``."""
    fc_class = "below" if forecast_value < threshold * norm else "normal"
    obs_class = "below" if observed_value < threshold * norm else "normal"
    if fc_class == "below" and obs_class == "below":
        cont = "TP"
    elif fc_class == "below":
        cont = "FP"
    elif obs_class == "below":
        cont = "FN"
    else:
        cont = "TN"
    return {
        "horizon": "pentad",
        "code": STATION_CODE,
        "basin": "other",
        "period_key": period_key,
        "year": year,
        "model": model,
        "regime": "operational",
        "season": "irrigation",
        "lead": None,
        "norm_provenance": "calculated",
        "forecast_value": forecast_value,
        "observed_value": observed_value,
        "norm": norm,
        "fc_class": fc_class,
        "obs_class": obs_class,
        "contingency": cont,
    }


def _flip_pairs() -> pd.DataFrame:
    """Pairs where obs=9 (norm=10) is normal at 0.80 but below at 1.0 × norm."""
    return pd.DataFrame(
        [
            _full_pair(period_key=1, year=2010, forecast_value=6.0, observed_value=9.0),
            _full_pair(period_key=2, year=2011, forecast_value=6.0, observed_value=5.0),
            _full_pair(period_key=3, year=2012, forecast_value=6.0, observed_value=12.0),
            _full_pair(period_key=4, year=2013, forecast_value=6.0, observed_value=7.0),
        ]
    )


def test_baseline_event_column_defaults_to_below_norm() -> None:
    from forecast_skill_eval.baselines import BASELINE_COLUMNS

    assert "event" in BASELINE_COLUMNS
    baseline = build_climatology_baseline(_flip_pairs())
    assert set(baseline["event"].unique()) == {"below_norm"}


def test_climatology_below_norm_metric_values_unchanged_by_event_column() -> None:
    """Adding the constant event column must not perturb the metric cells."""
    baseline = build_climatology_baseline(_flip_pairs())
    pooled = baseline[baseline["code"] == STATION_CODE].iloc[0]
    # obs below at 0.80×10=8: obs in {9,5,12,7} -> below {5,7} -> FN=2, TN=2.
    assert _cells(pooled) == {"TP": 0, "FP": 0, "FN": 2, "TN": 2, "n_pairs": 4}


def test_climatology_below_norm_100_tagged_and_differs() -> None:
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    pairs = _flip_pairs()
    pairs_100 = reclassify_pairs_for_event(pairs, event_by_name("below_norm_100"), {})
    baseline_100 = build_climatology_baseline(pairs_100, event="below_norm_100")

    assert set(baseline_100["event"].unique()) == {"below_norm_100"}
    pooled = baseline_100[baseline_100["code"] == STATION_CODE].iloc[0]
    # obs below at 1.0×10=10: obs in {9,5,12,7} -> below {9,5,7} -> FN=3, TN=1.
    assert _cells(pooled) == {"TP": 0, "FP": 0, "FN": 3, "TN": 1, "n_pairs": 4}


def test_persistence_and_operational_thread_event_label() -> None:
    from forecast_skill_eval.events import event_by_name, reclassify_pairs_for_event

    pairs = _flip_pairs()
    pairs_100 = reclassify_pairs_for_event(pairs, event_by_name("below_norm_100"), {})
    persistence_100 = build_persistence_baseline(pairs_100, threshold=1.0, event="below_norm_100")
    if not persistence_100.empty:
        assert set(persistence_100["event"].unique()) == {"below_norm_100"}
    # Default persistence rows carry below_norm.
    persistence = build_persistence_baseline(pairs)
    if not persistence.empty:
        assert set(persistence["event"].unique()) == {"below_norm"}
