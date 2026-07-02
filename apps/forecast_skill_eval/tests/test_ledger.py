from __future__ import annotations

from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.observed_truth import ObservedTruthLedgerEntry


def test_ledger_records_entries_and_counts_by_stage_reason() -> None:
    ledger = ExclusionLedger()

    ledger.add(
        stage="pair",
        reason="forecast_missing",
        code="19999",
        period_key=4,
        year=2024,
    )
    ledger.add(stage="pair", reason="forecast_missing")
    ledger.add(stage="norm", reason="norm_duplicate_conflict")

    assert len(ledger.entries) == 3
    assert ledger.entries[0].code == "19999"
    assert ledger.counts_by_stage_reason() == {
        ("pair", "forecast_missing"): 2,
        ("norm", "norm_duplicate_conflict"): 1,
    }


def test_ledger_merge_folds_norm_and_observed_stage_reasons() -> None:
    ledger = ExclusionLedger()
    norm_ledger = ExclusionLedger()
    norm_ledger.add(stage="norm", reason="norm_unavailable_lt_min_years")

    ledger.add(stage="pair", reason="observed_unmatched")
    ledger.merge(norm_ledger)
    ledger.merge(
        [
            ObservedTruthLedgerEntry(
                reason="observed_missing",
                code="19999",
                period_key=3,
                year=2024,
            )
        ],
        stage="observed",
    )

    assert ledger.counts_by_stage_reason() == {
        ("pair", "observed_unmatched"): 1,
        ("norm", "norm_unavailable_lt_min_years"): 1,
        ("observed", "observed_missing"): 1,
    }
