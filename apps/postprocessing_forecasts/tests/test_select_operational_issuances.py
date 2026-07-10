"""Tests for `select_operational_issuances` (M1 P1 lead-aware skill).

TDD -- these tests define the contract of the pure selection step that
turns raw (possibly many-issuances-per-target) long-forecast rows into
exactly one row per (code, model, target_year, target_period), applied
after normalization and before aggregation/skill/ensemble generation.

See doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md (P1) and
the "Round-2 review refinements" section for the exact contract:
require-not-prefer matching, canonical-model-name baseline exclusion,
derive-then-filter (never trust a stored horizon_value), and a
deterministic tie-break.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from long_term_horizon_resolver import OperationalSchedule
from src.data_reader import select_operational_issuances

CODE = "19999"


def _row(
    code=CODE,
    model="LR_Base",
    year=2024,
    month=6,
    date="2024-05-25",
    valid_from="2024-06-01",
    horizon_value=99,
    marker="",
):
    """Build one raw long-forecast row for selection tests."""
    return {
        "code": code,
        "model_short": model,
        "year": year,
        "month": month,
        "date": date,
        "valid_from": valid_from,
        "horizon_value": horizon_value,
        "forecasted_discharge": 100.0,
        "marker": marker,
    }


MONTH_1_SCHEDULE = {
    "month_1": OperationalSchedule(mode="month_1", lead_time=1, issue_day=25),
}


def _select(rows, schedules=None):
    df = pd.DataFrame(rows)
    return select_operational_issuances(
        df,
        schedules or MONTH_1_SCHEDULE,
        target_year_col="year",
        target_period_col="month",
    )


class TestBasicSelection:
    def test_selects_the_single_matching_operational_row(self):
        rows = [_row(date="2024-05-25", valid_from="2024-06-01", marker="operational")]
        result = _select(rows)

        assert len(result) == 1
        assert result.iloc[0]["marker"] == "operational"
        assert result.iloc[0]["horizon_value"] == 1

    def test_multi_model_multi_target_selects_one_row_each(self):
        """2 models x 2 monthly targets in one year -> exactly 4 selected

        rows, one per (model, target month), each carrying extra
        non-operational (backfill) noise rows that must be dropped.
        """
        rows = []
        for model in ("LR_Base", "LR_SM"):
            for month in (6, 7):
                valid_from = f"2024-{month:02d}-01"
                issue = pd.Timestamp(valid_from) - pd.DateOffset(months=1)
                issue = issue.replace(day=25)
                rows.append(
                    _row(
                        model=model,
                        year=2024,
                        month=month,
                        date=str(issue.date()),
                        valid_from=valid_from,
                        marker=f"operational-{model}-{month}",
                    )
                )
                # backfill noise: same target, wrong issue day/lead
                rows.append(
                    _row(
                        model=model,
                        year=2024,
                        month=month,
                        date=f"2024-{month:02d}-15",
                        valid_from=valid_from,
                        marker=f"backfill-{model}-{month}",
                    )
                )

        result = _select(rows)

        assert len(result) == 4
        assert set(result["marker"]) == {
            "operational-LR_Base-6",
            "operational-LR_Base-7",
            "operational-LR_SM-6",
            "operational-LR_SM-7",
        }
        assert (result["horizon_value"] == 1).all()

    def test_horizon_value_is_overwritten_with_derived_lead_not_trusted_input(self):
        """The selector must derive the lead from date/valid_from and

        overwrite horizon_value -- never trust whatever value the row
        already carries.
        """
        rows = [_row(date="2024-05-25", valid_from="2024-06-01", horizon_value=999, marker="op")]
        result = _select(rows)

        assert result.iloc[0]["horizon_value"] == 1


class TestRequireNotPrefer:
    def test_row_matching_lead_but_wrong_issue_day_is_dropped(self):
        """Lead matches (1 month) but issue day does not (25 required, got

        20) -- REQUIRE both, not prefer; no fallback to a near-miss.
        """
        rows = [_row(date="2024-05-20", valid_from="2024-06-01", marker="near-miss")]
        result = _select(rows)

        assert result.empty

    def test_row_matching_issue_day_but_wrong_lead_is_dropped(self):
        rows = [_row(date="2024-04-25", valid_from="2024-06-01", marker="wrong-lead")]
        result = _select(rows)

        assert result.empty

    def test_no_operational_candidate_drops_target_unit_and_logs(self, caplog):
        """A (code, model, target) with rows present but NONE matching the

        configured schedule is DROPPED entirely -- no fallback to the
        closest backfill row.
        """
        rows = [
            _row(date="2024-05-01", valid_from="2024-06-01", marker="backfill-1"),
            _row(date="2024-05-10", valid_from="2024-06-01", marker="backfill-2"),
        ]
        with caplog.at_level("INFO"):
            result = _select(rows)

        assert result.empty
        assert "dropped 1 target unit" in caplog.text

    def test_backfill_only_year_excluded_operational_year_kept(self):
        """One target year has ONLY backfill issuances (dropped); another

        target year in the same series has a genuine operational
        issuance (kept).
        """
        rows = [
            # 2023-06 target: backfill only, no operational candidate
            _row(year=2023, month=6, date="2023-05-01", valid_from="2023-06-01", marker="bf-2023"),
            # 2024-06 target: genuine operational issuance
            _row(year=2024, month=6, date="2024-05-25", valid_from="2024-06-01", marker="op-2024"),
        ]
        result = _select(rows)

        assert len(result) == 1
        assert result.iloc[0]["marker"] == "op-2024"


class TestDeterministicTie:
    def test_duplicate_matching_rows_same_date_keeps_last_input_row(self):
        """Two rows in the same group both exactly match the schedule and

        share the identical issue date (e.g. a duplicate write) -- the
        tie-break must be deterministic: keep the LAST row in input
        order (stable sort).
        """
        rows = [
            _row(date="2024-05-25", valid_from="2024-06-01", marker="first-run"),
            _row(date="2024-05-25", valid_from="2024-06-01", marker="second-run"),
        ]

        result = _select(rows)

        assert len(result) == 1
        assert result.iloc[0]["marker"] == "second-run"

    def test_duplicate_matching_rows_different_dates_prefer_latest(self):
        """Two rows in the same group both match the schedule (same

        derived lead + issue day) but were stamped with different
        intraday timestamps -- the row with the LATEST date wins.
        """
        rows = [
            _row(date="2024-05-25 00:00:00", valid_from="2024-06-01", marker="early"),
            _row(date="2024-05-25 23:00:00", valid_from="2024-06-01", marker="late"),
        ]
        df = pd.DataFrame(rows)

        result = select_operational_issuances(
            df, MONTH_1_SCHEDULE, target_year_col="year", target_period_col="month"
        )

        assert len(result) == 1
        assert result.iloc[0]["marker"] == "late"


class TestBaselineRowsDropped:
    @pytest.mark.parametrize("model_short", ["EM", "Naive_Mean", "Skilled_Mean", "SKILLED_MEAN"])
    def test_baseline_rows_are_dropped_from_output(self, model_short):
        """LOCKED regression (HIGH defect #2): baseline/ensemble rows

        (EM/Naive/Skilled Mean) carry no issue date and are recomputed
        downstream -- they must be DROPPED from the selector output, not
        passed through. Only the raw operational model row survives.
        """
        rows = [
            _row(
                model=model_short,
                date="2024-01-01",
                valid_from="2024-06-01",
                horizon_value=-1,
                marker="baseline",
            ),
            _row(date="2024-05-25", valid_from="2024-06-01", marker="operational"),
        ]
        result = _select(rows)

        assert len(result) == 1
        assert result.iloc[0]["marker"] == "operational"
        assert result.iloc[0]["horizon_value"] == 1
        assert "baseline" not in set(result["marker"])

    def test_only_baseline_rows_returns_empty(self):
        rows = [_row(model="EM", date="2024-01-01", valid_from="2024-06-01", marker="baseline")]
        result = _select(rows)

        assert result.empty


class TestDistinctLeadsForSameTarget:
    def test_month_0_and_month_1_same_target_month_kept_as_two_rows(self):
        """LOCKED regression (CRITICAL defect #1): two configured monthly

        modes for the SAME target month -- month_0 (lead 0, issued day 10
        of the target month) and month_1 (lead 1, issued day 25 of the
        prior month) -- both match their schedules and must survive as
        TWO SEPARATE rows (lead 0 and lead 1), NOT collapse into one.
        """
        schedules = {
            "month_0": OperationalSchedule(mode="month_0", lead_time=0, issue_day=10),
            "month_1": OperationalSchedule(mode="month_1", lead_time=1, issue_day=25),
        }
        rows = [
            # month_0: lead 0, issued 2024-06-10, targets 2024-06
            _row(date="2024-06-10", valid_from="2024-06-01", marker="lead0"),
            # month_1: lead 1, issued 2024-05-25, targets 2024-06
            _row(date="2024-05-25", valid_from="2024-06-01", marker="lead1"),
        ]
        df = pd.DataFrame(rows)
        result = select_operational_issuances(
            df, schedules, target_year_col="year", target_period_col="month"
        )

        assert len(result) == 2
        assert set(result["marker"]) == {"lead0", "lead1"}
        assert set(result["horizon_value"]) == {0, 1}
        lead0_row = result[result["marker"] == "lead0"].iloc[0]
        lead1_row = result[result["marker"] == "lead1"].iloc[0]
        assert lead0_row["horizon_value"] == 0
        assert lead1_row["horizon_value"] == 1


class TestSeasonalLeadKey:
    def test_seasonal_derived_lead_written_into_season_in_year_and_horizon_value(self):
        """LOCKED regression (HIGH defect #4): a seasonal row issued

        2024-01-25 targeting valid_from 2024-04-01 has derived lead 3.
        Both the seasonal lead key column (`season_in_year`) AND
        `horizon_value` must reflect lead 3 -- not the stored sentinel 0
        the API round-trips through season_in_year.
        """
        schedules = {
            "seasonal_january": OperationalSchedule(
                mode="seasonal_january", lead_time=3, issue_day=25
            ),
        }
        df = pd.DataFrame(
            [
                {
                    "code": CODE,
                    "model_short": "LR_Base",
                    "season_year": 2024,
                    "season_in_year": 0,  # stored sentinel from the API
                    "date": "2024-01-25",
                    "valid_from": "2024-04-01",
                    "horizon_value": 0,
                    "forecasted_discharge": 100.0,
                }
            ]
        )
        result = select_operational_issuances(
            df,
            schedules,
            target_year_col="season_year",
            target_period_col=None,
            lead_output_cols=("horizon_value", "season_in_year"),
        )

        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 3
        assert result.iloc[0]["season_in_year"] == 3

    def test_two_seasonal_leads_same_season_year_kept_separate(self):
        """Two configured seasonal leads (Jan issue -> lead 3, Apr issue

        -> lead 0) targeting the same season_year survive as two rows
        keyed by distinct season_in_year leads.
        """
        schedules = {
            "seasonal_january": OperationalSchedule(
                mode="seasonal_january", lead_time=3, issue_day=25
            ),
            "seasonal_april": OperationalSchedule(mode="seasonal_april", lead_time=0, issue_day=25),
        }
        df = pd.DataFrame(
            [
                {
                    "code": CODE,
                    "model_short": "LR_Base",
                    "season_year": 2024,
                    "season_in_year": 0,
                    "date": "2024-01-25",
                    "valid_from": "2024-04-01",
                    "horizon_value": 0,
                    "forecasted_discharge": 100.0,
                },
                {
                    "code": CODE,
                    "model_short": "LR_Base",
                    "season_year": 2024,
                    "season_in_year": 0,
                    "date": "2024-04-25",
                    "valid_from": "2024-04-01",
                    "horizon_value": 0,
                    "forecasted_discharge": 110.0,
                },
            ]
        )
        result = select_operational_issuances(
            df,
            schedules,
            target_year_col="season_year",
            target_period_col=None,
            lead_output_cols=("horizon_value", "season_in_year"),
        )

        assert len(result) == 2
        assert set(result["season_in_year"]) == {0, 3}
        assert set(result["horizon_value"]) == {0, 3}


class TestInputGuards:
    def test_empty_input_returns_empty(self):
        df = pd.DataFrame()
        result = select_operational_issuances(
            df, MONTH_1_SCHEDULE, target_year_col="year", target_period_col="month"
        )
        assert result.empty

    def test_missing_required_column_returns_input_unchanged(self, caplog):
        df = pd.DataFrame([{"code": CODE, "model_short": "LR_Base"}])
        with caplog.at_level("WARNING"):
            result = select_operational_issuances(
                df, MONTH_1_SCHEDULE, target_year_col="year", target_period_col="month"
            )

        pd.testing.assert_frame_equal(result, df)
        assert "missing required column" in caplog.text
