"""Tests for the dated long-term recovery path (lt_recovery).

Contracts under test:

- the no-overwrite guard refuses when ANY member row exists for the target
  (horizon_type, horizon_value, effective_date), and passes when none does;
- guard and read-back both fail closed on a query error;
- a run that writes only flag=2 (missing/all-NaN) rows FAILS;
- only the current and previous calendar month are recoverable, the date must
  be the configured issue date, and a future date is refused;
- an empty station list is refused before any query is issued;
- the operational flag assignment is unchanged when no recovery flag is given.

Station codes are synthetic (19999 / 19998).
"""

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
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        skip = kwargs.get("skip", 0)
        limit = kwargs.get("limit", 100)
        matching = [
            row
            for row in self.rows
            if row.get("model_type") == model
            and (start_date is None or row.get("date") >= start_date)
            and (end_date is None or row.get("date") <= end_date)
        ]
        page = matching[skip : skip + limit]
        return pd.DataFrame(page)


def make_row(model_type="LR_Base", code=STATION, date="2026-08-01", flag=0, q=12.5):
    return {
        "horizon_type": "month",
        "horizon_value": 1,
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

    def test_mode_with_only_aggregates_is_refused(self):
        config = FakeConfig(models=["EM", "Skilled Mean"])
        kwargs, calls, client = build_run_recovery_kwargs(config=config)
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []


# ─────────────────────────────────────────────────────────────────
# Date validation
# ─────────────────────────────────────────────────────────────────


class TestIssueDateParsing:
    def test_parses_iso(self):
        assert parse_issue_date("2026-08-01") == pd.Timestamp("2026-08-01")

    @pytest.mark.parametrize(
        "value", ["", None, "  ", "01.08.2026", "2026/08/01", "August", "2026-13-01"]
    )
    def test_rejects_non_iso(self, value):
        with pytest.raises(RecoveryRefused):
            parse_issue_date(value)

    def test_unpadded_components_parse_to_the_same_date(self):
        """strptime accepts 2026-8-1; it denotes the same day, so allow it."""
        assert parse_issue_date("2026-8-1") == pd.Timestamp("2026-08-01")


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
    def test_empty_refused(self, codes):
        with pytest.raises(RecoveryRefused, match="Station list is empty"):
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

    def test_guard_query_error_refuses_without_running(self):
        client = FakeClient(error=RuntimeError("connection reset"))
        kwargs, calls, _ = build_run_recovery_kwargs(client=client)
        assert run_recovery(**kwargs) == EXIT_REFUSED
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

    def test_empty_station_list_refused_before_any_query(self):
        kwargs, calls, client = build_run_recovery_kwargs(station_codes=())
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == [], "no query may be issued without org scoping"

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

    def test_config_load_failure_refuses(self):
        def broken_factory(_mode):
            raise FileNotFoundError("month_9.json missing")

        kwargs, calls, client = build_run_recovery_kwargs()
        kwargs["config_factory"] = broken_factory
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
        assert client.calls == []

    def test_client_construction_failure_refuses(self):
        def broken_client():
            raise RecoveryQueryError("API not ready")

        kwargs, calls, _ = build_run_recovery_kwargs()
        kwargs["client_factory"] = broken_client
        assert run_recovery(**kwargs) == EXIT_REFUSED
        assert calls == []
