"""Tests for the dated long-term recovery path (lt_recovery).

Contracts under test:

- the no-overwrite guard refuses when ANY member row exists for the target
  (horizon_type, horizon_value, effective_date), and passes when none does;
- guard and read-back both fail closed on a query error;
- a run that writes only flag=2 (missing/all-NaN) rows FAILS;
- only the current and previous calendar month are recoverable, the date must
  be the configured issue date, and a future date is refused;
- an empty station list FAILS (exit 1) before any query is issued — it is a
  deployment gap, not a decline;
- the operational flag assignment is unchanged when no recovery flag is given;
- EXIT_REFUSED (2) is "declined": rows already exist, or an operator-input
  refusal (bad/missing/future date, outside the window, no scheduled issue
  date, no forecast mode). This is NOT proof the month is complete or
  healthy — an existing-row decline can mean a single station's row exists
  and the rest of the month is missing, refused as a whole. EXIT_FAILED (1)
  covers two different situations: a stage 1 failure (empty station scope,
  missing member-model configuration, a RecoveryQueryError, or an
  unexpected exception before the guard could decide) means nothing was
  written and the month is still missing; a stage 2/3 failure means the
  forecast may have run and rows may or may not have been written, so the
  database state must be checked before retrying. The two log messages are
  distinguishable without the exit code (LTF-011).

Station codes are synthetic (19999 / 19998).
"""

import logging
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lt_recovery import (  # noqa: E402
    EXIT_FAILED,
    EXIT_OK,
    EXIT_REFUSED,
    MISSING_VALUE_FLAG,
    OPERATIONAL_FLAG,
    RECOVERY_FLAG,
    RecoveryError,
    RecoveryMisconfigured,
    RecoveryQueryError,
    RecoveryRefused,
    apply_success_flag,
    check_recovery_window,
    check_station_codes,
    count_member_rows,
    member_model_names,
    member_model_types,
    parse_issue_date,
    resolve_scheduled_models,
    run_recovery,
)

STATION = "19999"
OTHER_STATION = "19998"

ALL_MONTHS = list(range(1, 13))


# ─────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────


class FakeConfig:
    """Minimal stand-in for a loaded ForecastConfig."""

    def __init__(
        self,
        models=("LR_Base", "SM_GBT"),
        issue_day=1,
        forecast_months=None,
        horizon_type="month",
        horizon_value=1,
    ):
        self._models = list(models)
        self._issue_day = issue_day
        self._forecast_months = forecast_months or {}
        self._horizon_type = horizon_type
        self._horizon_value = horizon_value

    def get_models_to_run(self):
        return list(self._models)

    def get_operational_issue_day(self):
        return self._issue_day

    def get_forecast_months(self, model_name):
        return self._forecast_months.get(model_name, ALL_MONTHS)

    def get_horizon_type(self):
        return self._horizon_type

    def get_operational_month_lead_time(self):
        return self._horizon_value


class FakeClient:
    """In-memory long_forecasts store with the read API's filter semantics."""

    def __init__(self, rows=None, error=None):
        self.rows = list(rows or [])
        self.error = error
        self.calls = []

    def read_long_term_forecasts(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        model = kwargs.get("model")
        horizon_type = kwargs.get("horizon_type")
        horizon_value = kwargs.get("horizon_value")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        skip = kwargs.get("skip", 0)
        limit = kwargs.get("limit", 100)
        # Every filter the real API honours is honoured here, so a query that
        # loses its horizon scoping shows up as wrong counts rather than
        # passing silently.
        matching = [
            row
            for row in self.rows
            if row.get("model_type") == model
            and (horizon_type is None or row.get("horizon_type") == horizon_type)
            and (horizon_value is None or row.get("horizon_value") == horizon_value)
            and (start_date is None or row.get("date") >= start_date)
            and (end_date is None or row.get("date") <= end_date)
        ]
        page = matching[skip : skip + limit]
        return pd.DataFrame(page)


def make_row(
    model_type="LR_Base",
    code=STATION,
    date="2026-08-01",
    flag=0,
    q=12.5,
    horizon_type="month",
    horizon_value=1,
):
    return {
        "horizon_type": horizon_type,
        "horizon_value": horizon_value,
        "code": code,
        "date": date,
        "model_type": model_type,
        "flag": flag,
        "q": q,
    }


def build_run_recovery_kwargs(
    *,
    config=None,
    client=None,
    station_codes=(STATION,),
    issue_date="2026-08-01",
    forecast_mode="month_0",
    now="2026-08-30",
    on_run=None,
):
    """Assemble run_recovery kwargs plus a record of what the run did."""
    config = config or FakeConfig()
    client = client if client is not None else FakeClient()
    calls = []

    def run_forecast_fn(**kwargs):
        calls.append(kwargs)
        if on_run is not None:
            on_run(client)

    kwargs = dict(
        issue_date=issue_date,
        forecast_mode=forecast_mode,
        run_forecast_fn=run_forecast_fn,
        station_codes_fn=lambda: list(station_codes),
        config_factory=lambda mode: config,
        client_factory=lambda: client,
        now=pd.Timestamp(now),
    )
    return kwargs, calls, client


def write_recovered_rows(client, models=("LR_Base", "SM_GBT"), date="2026-08-01"):
    """Simulate a successful recovery run writing flag=1 rows."""
    for model in models:
        client.rows.append(make_row(model_type=model, date=date, flag=RECOVERY_FLAG, q=9.9))


# ─────────────────────────────────────────────────────────────────
# Members
# ─────────────────────────────────────────────────────────────────


class TestMemberSelection:
    def test_aggregates_excluded(self):
        config = FakeConfig(models=["LR_Base", "EM", "Skilled Mean", "Naive Mean", "GBT"])
        assert member_model_names(config) == ["LR_Base", "GBT"]

    def test_aggregate_matching_ignores_case_and_underscores(self):
        config = FakeConfig(models=["em", "Skilled_Mean", "naive mean", "LR_Base"])
        assert member_model_names(config) == ["LR_Base"]

    def test_model_types_deduplicated_and_ordered(self):
        config = FakeConfig(models=["LR_Base", "SM_GBT", "LR_Base"])
        assert member_model_types(config) == ["LR_Base", "SM_GBT"]

    def test_mode_with_only_aggregates_fails(self):
        """Missing member-model configuration is a deployment gap, not a
        decline: it exits 1 (RecoveryMisconfigured), not 2. REGRESSION: this
        exits 2 against the pre-LTF-011 code — it must fail if reverted."""
        config = FakeConfig(models=["EM", "Skilled Mean"])
        kwargs, calls, client = build_run_recovery_kwargs(config=config)
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []
        assert client.calls == []


# ─────────────────────────────────────────────────────────────────
# Date validation
# ─────────────────────────────────────────────────────────────────


class TestIssueDateParsing:
    def test_parses_iso(self):
        assert parse_issue_date("2026-08-01") == pd.Timestamp("2026-08-01")

    @pytest.mark.parametrize(
        "value",
        [
            "",
            None,
            "  ",
            "01.08.2026",
            "2026/08/01",
            "August",
            "2026-8-1",  # not zero-padded: the docstring says strict ISO
            "2026-08-01T00:00:00",
        ],
    )
    def test_rejects_non_iso(self, value):
        with pytest.raises(RecoveryRefused, match="ISO"):
            parse_issue_date(value)

    @pytest.mark.parametrize("value", ["2026-13-01", "2026-02-31", "2026-00-10"])
    def test_rejects_impossible_calendar_dates(self, value):
        with pytest.raises(RecoveryRefused, match="calendar date"):
            parse_issue_date(value)


class TestRecoveryWindow:
    def test_current_month_allowed(self):
        check_recovery_window(pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-30"))

    def test_previous_month_allowed(self):
        check_recovery_window(pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-30"))

    def test_previous_month_across_year_boundary(self):
        check_recovery_window(pd.Timestamp("2025-12-01"), pd.Timestamp("2026-01-10"))

    def test_two_months_back_refused(self):
        with pytest.raises(RecoveryRefused, match="outside the recovery window"):
            check_recovery_window(pd.Timestamp("2026-06-01"), pd.Timestamp("2026-08-30"))

    def test_future_refused(self):
        with pytest.raises(RecoveryRefused, match="in the future"):
            check_recovery_window(pd.Timestamp("2026-09-01"), pd.Timestamp("2026-08-30"))


class TestScheduledIssueDate:
    def test_exact_issue_date_accepted(self):
        config = FakeConfig(models=["LR_Base"], issue_day=1)
        assert resolve_scheduled_models(config, pd.Timestamp("2026-08-01")) == ["LR_Base"]

    def test_near_miss_is_refused_not_snapped(self):
        """A date 2 days late would be snapped by run_forecast — refuse it."""
        config = FakeConfig(models=["LR_Base"], issue_day=1)
        with pytest.raises(RecoveryRefused, match="would snap"):
            resolve_scheduled_models(config, pd.Timestamp("2026-08-03"))

    def test_far_miss_is_refused_as_non_issue_date(self):
        config = FakeConfig(models=["LR_Base"], issue_day=1)
        with pytest.raises(RecoveryRefused, match="not a scheduled issue date"):
            resolve_scheduled_models(config, pd.Timestamp("2026-08-15"))

    def test_month_outside_model_forecast_months_is_refused(self):
        config = FakeConfig(models=["LR_Base"], issue_day=1, forecast_months={"LR_Base": [3, 4]})
        with pytest.raises(RecoveryRefused, match="not a scheduled issue date"):
            resolve_scheduled_models(config, pd.Timestamp("2026-08-01"))

    def test_short_month_clamp_for_issue_day_31(self):
        """issue_day=31 in February resolves to the 28th, and only the 28th."""
        config = FakeConfig(models=["LR_Base"], issue_day=31)
        assert resolve_scheduled_models(config, pd.Timestamp("2026-02-28")) == ["LR_Base"]
        with pytest.raises(RecoveryRefused):
            resolve_scheduled_models(config, pd.Timestamp("2026-02-27"))

    def test_seasonal_model_skips_while_monthly_model_runs(self):
        """A member outside its forecast months is skipped, not a blocker."""
        config = FakeConfig(
            models=["LR_Base", "SM_GBT"],
            issue_day=1,
            forecast_months={"SM_GBT": [3]},
        )
        assert resolve_scheduled_models(config, pd.Timestamp("2026-08-01")) == ["LR_Base"]


class TestStationCodes:
    def test_non_empty_codes_pass(self):
        assert check_station_codes([19999, " 19998 "]) == ["19999", "19998"]

    @pytest.mark.parametrize("codes", [None, [], ["", "  "]])
    def test_empty_is_misconfigured_not_refused(self, codes):
        """An empty station list is a deployment gap (RecoveryMisconfigured,
        exit 1), not an operator decline (RecoveryRefused, exit 2)."""
        with pytest.raises(RecoveryMisconfigured, match="Station list is empty"):
            check_station_codes(codes)


# ─────────────────────────────────────────────────────────────────
# Counting
# ─────────────────────────────────────────────────────────────────


class TestCountMemberRows:
    def _count(self, client, **overrides):
        params = dict(
            horizon_type="month",
            horizon_value=1,
            effective_date=pd.Timestamp("2026-08-01"),
            model_types=["LR_Base", "SM_GBT"],
            station_codes=[STATION],
        )
        params.update(overrides)
        return count_member_rows(client, **params)

    def test_counts_rows_for_configured_stations_only(self):
        client = FakeClient(
            [
                make_row(code=STATION),
                make_row(code=OTHER_STATION),
            ]
        )
        assert self._count(client) == 1

    def test_counts_every_member_model(self):
        client = FakeClient([make_row(model_type="LR_Base"), make_row(model_type="SM_GBT")])
        assert self._count(client) == 2

    def test_ignores_other_dates(self):
        client = FakeClient([make_row(date="2026-07-01")])
        assert self._count(client) == 0

    def test_flag_filter(self):
        client = FakeClient(
            [
                make_row(model_type="LR_Base", flag=RECOVERY_FLAG),
                make_row(model_type="SM_GBT", flag=MISSING_VALUE_FLAG),
            ]
        )
        assert self._count(client, flags={RECOVERY_FLAG}) == 1

    def test_require_value_skips_null_q(self):
        client = FakeClient(
            [
                make_row(model_type="LR_Base", flag=RECOVERY_FLAG, q=None),
                make_row(model_type="SM_GBT", flag=RECOVERY_FLAG, q=3.0),
            ]
        )
        assert self._count(client, flags={RECOVERY_FLAG}, require_value=True) == 1

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_require_value_rejects_non_finite_q(self, value):
        """Infinity is not a forecast: an upstream divide-by-zero can persist it."""
        client = FakeClient([make_row(model_type="LR_Base", flag=RECOVERY_FLAG, q=value)])
        assert self._count(client, flags={RECOVERY_FLAG}, require_value=True) == 0

    def test_require_value_accepts_zero(self):
        """Zero discharge is a real value and must not be mistaken for missing."""
        client = FakeClient([make_row(model_type="LR_Base", flag=RECOVERY_FLAG, q=0.0)])
        assert self._count(client, flags={RECOVERY_FLAG}, require_value=True) == 1

    def test_ignores_other_horizon_types(self):
        client = FakeClient([make_row(horizon_type="quarter")])
        assert self._count(client) == 0

    def test_ignores_other_horizon_values(self):
        client = FakeClient([make_row(horizon_value=5)])
        assert self._count(client) == 0

    def test_query_is_scoped_to_the_mode_horizon(self):
        """Every query must carry horizon_type and horizon_value."""
        client = FakeClient([make_row()])
        self._count(client)
        assert client.calls, "no query was issued"
        for call in client.calls:
            assert call["horizon_type"] == "month"
            assert call["horizon_value"] == 1

    def test_query_error_raises(self):
        client = FakeClient(error=RuntimeError("boom"))
        with pytest.raises(RecoveryQueryError):
            self._count(client)

    def test_pagination_reads_every_page(self):
        client = FakeClient([make_row(code=str(19900 + i)) for i in range(7)])
        codes = [str(19900 + i) for i in range(7)]
        assert self._count(client, model_types=["LR_Base"], station_codes=codes, page_size=3) == 7


# ─────────────────────────────────────────────────────────────────
# Flag override
# ─────────────────────────────────────────────────────────────────


class TestApplySuccessFlag:
    def _frame(self):
        return pd.DataFrame({"Q_LR_Base": [1.0, None, 3.0]})

    def test_operational_path_unchanged(self):
        """REGRESSION: with no recovery flag the values are exactly 0 and 2."""
        frame = apply_success_flag(self._frame(), "Q_LR_Base")
        assert list(frame["flag"]) == [
            OPERATIONAL_FLAG,
            MISSING_VALUE_FLAG,
            OPERATIONAL_FLAG,
        ]

    def test_recovery_flag_applied_to_values_only(self):
        frame = apply_success_flag(self._frame(), "Q_LR_Base", RECOVERY_FLAG)
        assert list(frame["flag"]) == [RECOVERY_FLAG, MISSING_VALUE_FLAG, RECOVERY_FLAG]

    def test_explicit_none_matches_operational(self):
        frame = apply_success_flag(self._frame(), "Q_LR_Base", None)
        assert list(frame["flag"]) == [
            OPERATIONAL_FLAG,
            MISSING_VALUE_FLAG,
            OPERATIONAL_FLAG,
        ]


# ─────────────────────────────────────────────────────────────────
# End-to-end orchestration
# ─────────────────────────────────────────────────────────────────


class TestRunRecoveryGuard:
    def test_refuses_when_member_row_exists(self):
        client = FakeClient([make_row(model_type="LR_Base")])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == [], "the forecast must not run when the guard refuses"

    def test_refuses_on_a_flag_two_row(self):
        """A partial month, even one full of missing values, is refused."""
        client = FakeClient([make_row(model_type="SM_GBT", flag=MISSING_VALUE_FLAG, q=None)])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []

    def test_row_for_another_station_does_not_trip_the_guard(self):
        client = FakeClient([make_row(code=OTHER_STATION)])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client, on_run=write_recovered_rows)
        assert run_recovery(**kwargs) == EXIT_OK
        assert len(calls) == 1

    def test_row_for_another_month_does_not_trip_the_guard(self):
        client = FakeClient([make_row(date="2026-07-01")])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client, on_run=write_recovered_rows)
        assert run_recovery(**kwargs) == EXIT_OK

    def test_row_for_another_horizon_does_not_trip_the_guard(self):
        """A quarter row on the same date must not block a month recovery."""
        client = FakeClient([make_row(horizon_type="quarter"), make_row(horizon_value=5)])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client, on_run=write_recovered_rows)
        assert run_recovery(**kwargs) == EXIT_OK
        assert len(calls) == 1

    def test_guard_query_error_fails_without_running(self):
        """A RecoveryQueryError (API unreachable/failed) could not be
        attempted — it exits 1, not 2, even though the forecast never ran."""
        client = FakeClient(error=RuntimeError("connection reset"))
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []


class TestRunRecoverySuccess:
    def test_passes_recovery_flag_to_the_forecast(self):
        kwargs, calls, _ = build_run_recovery_kwargs(on_run=write_recovered_rows)
        assert run_recovery(**kwargs) == EXIT_OK
        assert len(calls) == 1
        assert calls[0]["recovery_flag"] == RECOVERY_FLAG
        assert calls[0]["forecast_all"] is True
        assert calls[0]["models_to_run"] == []
        assert calls[0]["forecast_mode"] == "month_0"

    def test_partial_coverage_still_succeeds(self):
        """Success criterion is 'some rows written', deliberately loose."""
        kwargs, calls, _ = build_run_recovery_kwargs(
            on_run=lambda client: write_recovered_rows(client, models=["LR_Base"])
        )
        assert run_recovery(**kwargs) == EXIT_OK

    def test_previous_month_recovered(self):
        kwargs, calls, _ = build_run_recovery_kwargs(
            issue_date="2026-07-01",
            now="2026-08-30",
            on_run=lambda client: write_recovered_rows(client, date="2026-07-01"),
        )
        assert run_recovery(**kwargs) == EXIT_OK


class TestRunRecoveryReadBack:
    def test_run_writing_nothing_fails(self):
        kwargs, calls, _ = build_run_recovery_kwargs()
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert len(calls) == 1, "the forecast ran; only the read-back failed"

    def test_only_flag_two_rows_fails(self):
        def write_missing(client):
            client.rows.append(make_row(model_type="LR_Base", flag=MISSING_VALUE_FLAG, q=None))
            client.rows.append(make_row(model_type="SM_GBT", flag=MISSING_VALUE_FLAG, q=None))

        kwargs, calls, _ = build_run_recovery_kwargs(on_run=write_missing)
        assert run_recovery(**kwargs) == EXIT_FAILED

    def test_operational_flag_rows_do_not_satisfy_the_read_back(self):
        """Only rows carrying the recovery flag count as recovered."""

        def write_operational(client):
            client.rows.append(make_row(model_type="LR_Base", flag=OPERATIONAL_FLAG))

        kwargs, _, _ = build_run_recovery_kwargs(on_run=write_operational)
        assert run_recovery(**kwargs) == EXIT_FAILED

    def test_recovery_flag_without_value_does_not_count(self):
        def write_valueless(client):
            client.rows.append(make_row(model_type="LR_Base", flag=RECOVERY_FLAG, q=None))

        kwargs, _, _ = build_run_recovery_kwargs(on_run=write_valueless)
        assert run_recovery(**kwargs) == EXIT_FAILED

    def test_recovery_flag_with_infinite_value_does_not_count(self):
        def write_infinite(client):
            client.rows.append(make_row(model_type="LR_Base", flag=RECOVERY_FLAG, q=float("inf")))

        kwargs, _, _ = build_run_recovery_kwargs(on_run=write_infinite)
        assert run_recovery(**kwargs) == EXIT_FAILED

    def test_rows_for_another_horizon_do_not_satisfy_the_read_back(self):
        def write_wrong_horizon(client):
            client.rows.append(make_row(model_type="LR_Base", flag=RECOVERY_FLAG, horizon_value=5))

        kwargs, _, _ = build_run_recovery_kwargs(on_run=write_wrong_horizon)
        assert run_recovery(**kwargs) == EXIT_FAILED

    def test_read_back_query_error_fails_closed(self):
        def break_client(client):
            write_recovered_rows(client)
            client.error = RuntimeError("gateway timeout")

        kwargs, calls, _ = build_run_recovery_kwargs(on_run=break_client)
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert len(calls) == 1

    def test_forecast_exception_fails(self):
        def explode(_client):
            raise RuntimeError("model blew up")

        kwargs, _, _ = build_run_recovery_kwargs(on_run=explode)
        assert run_recovery(**kwargs) == EXIT_FAILED


class TestRunRecoveryRefusals:
    """Operator-input refusals and 'already done': all exit 2.

    A refusal is not proof the system is healthy: the existing-row case in
    particular can mean a single station's row exists and the rest of the
    month is missing, refused as a whole (see G1 in the LTF-011 follow-up
    review).
    """

    def test_future_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date="2026-09-01", now="2026-08-30")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_out_of_window_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date="2026-06-01", now="2026-08-30")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_non_issue_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date="2026-08-15")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_near_miss_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date="2026-08-03")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_missing_forecast_mode_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(forecast_mode="")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_malformed_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date="01.08.2026")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_missing_issue_date_refused(self):
        kwargs, calls, client = build_run_recovery_kwargs(issue_date=None)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_already_exists_message_says_nothing_was_run(self, caplog):
        """C2: the refusal log line keeps saying nothing was run, and its
        specific reason ('already exist') is distinguishable from an
        operator-input decline without needing the exit code."""
        caplog.set_level(logging.ERROR, logger="long_term_forecasting")
        client = FakeClient([make_row(model_type="LR_Base")])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert "nothing was run" in caplog.text
        assert "already exist" in caplog.text
        assert calls == []

    def test_operator_declined_message_differs_from_already_exists(self, caplog):
        """The two refusal causes ('already exist' vs 'declined input') read
        differently even though they share exit code 2."""
        caplog.set_level(logging.ERROR, logger="long_term_forecasting")
        kwargs, calls, client = build_run_recovery_kwargs(forecast_mode="")
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert "No forecast mode supplied" in caplog.text
        assert "already exist" not in caplog.text

    def test_refusal_is_not_shadowed_by_the_broader_error_handler(self):
        """Handler-order pin (G6): RecoveryRefused subclasses RecoveryError,
        so this only returns EXIT_REFUSED because `except RecoveryRefused`
        precedes `except RecoveryError` in run_recovery. Swap the two
        `except` clauses (or merge them into one) and a decline would be
        caught by the broader RecoveryError handler instead, and this
        assertion would flip to EXIT_FAILED. Confirmed by hand: reversing
        the order of the two `except` clauses in run_recovery makes this
        test fail. Contrast with test_query_error_fails_not_refuses below,
        which is NOT order-sensitive because RecoveryQueryError is never a
        RecoveryRefused."""
        client = FakeClient([make_row(model_type="LR_Base")])
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []


class TestRunRecoveryStage1Failures:
    """Could-not-be-attempted causes: all exit 1, month still missing.

    REGRESSION: every case here exits 2 (EXIT_REFUSED) against the
    pre-LTF-011 code, where a single `except RecoveryError` /
    `except Exception` pair returned EXIT_REFUSED for everything. Each test
    must fail if C1 is reverted.
    """

    def test_empty_station_list_fails_before_any_query(self):
        kwargs, calls, client = build_run_recovery_kwargs(station_codes=())
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []
        assert client.calls == [], "no query may be issued without org scoping"

    def test_config_load_failure_fails(self):
        def broken_factory(_mode):
            raise FileNotFoundError("month_9.json missing")

        kwargs, calls, client = build_run_recovery_kwargs()
        kwargs["config_factory"] = broken_factory
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []
        assert client.calls == []

    def test_config_load_failure_message_names_exception_type(self, caplog):
        """C2: the failure path must say the recovery could not be attempted
        and name the exception type, so a log reader can tell it apart from
        a refusal without the exit code."""
        caplog.set_level(logging.ERROR, logger="long_term_forecasting")

        def broken_factory(_mode):
            raise FileNotFoundError("month_9.json missing")

        kwargs, calls, client = build_run_recovery_kwargs()
        kwargs["config_factory"] = broken_factory
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert "could not be attempted" in caplog.text
        assert "FileNotFoundError" in caplog.text

    def test_client_construction_failure_fails(self):
        """RecoveryQueryError (API unavailable/disabled/not ready) exits 1."""

        def broken_client():
            raise RecoveryQueryError("API not ready")

        kwargs, calls, _ = build_run_recovery_kwargs()
        kwargs["client_factory"] = broken_client
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []

    def test_query_error_fails_not_refuses(self):
        """Classification pin, NOT an order pin: RecoveryQueryError is a
        RecoveryError but never a RecoveryRefused, so it always lands in the
        `except RecoveryError` handler and returns EXIT_FAILED regardless of
        which of the two `except` clauses run_recovery lists first --
        swapping their order does not change this result (verified by
        hand). The order-sensitive test is
        TestRunRecoveryRefusals.test_refusal_is_not_shadowed_by_the_broader_error_handler,
        which uses a RecoveryRefused instead."""

        def broken_client():
            raise RecoveryQueryError("API not ready")

        kwargs, calls, _ = build_run_recovery_kwargs()
        kwargs["client_factory"] = broken_client
        result = run_recovery(**kwargs)
        assert result == EXIT_FAILED
        assert result != EXIT_REFUSED

    def test_unexpected_exception_in_stage_one_fails_not_refuses(self):
        """Regression guard: an exception stage 1 does not anticipate must
        exit 1, not 2. Catches someone re-widening the bare `except
        Exception` back onto EXIT_REFUSED."""

        def broken_station_codes_fn():
            raise KeyError("ieasyforecast_config_file_station_selection")

        kwargs, calls, client = build_run_recovery_kwargs()
        kwargs["station_codes_fn"] = broken_station_codes_fn
        assert run_recovery(**kwargs) == EXIT_FAILED
        assert calls == []
        assert client.calls == []

    def test_unexpected_exception_is_not_a_recovery_error(self):
        """Sanity check backing the previous test: KeyError is not a
        RecoveryError, so it can only reach EXIT_FAILED via the bare
        `except Exception` handler, not the RecoveryError handler."""
        assert not issubclass(KeyError, RecoveryError)
