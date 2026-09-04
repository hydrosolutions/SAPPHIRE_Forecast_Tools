"""P1-P3 tests for PREPQ-020 — short-horizon (pentad/decad) hydrograph row
existence decoupled from the iEH-HF norm lookup.

Covers contract items C1 (classify the norm response; never drop rows over
it), C2 (a previously stored norm is preserved via a read-merge, over the
correct — NOT calendar-year — window), C2a (a failed read-merge is an
anti-clobber no-write, not a fallback to an all-``None`` write), C3 (a
terminal status per ``(code, horizon)`` pair plus the counts-only
``SHORT-HORIZON RUN SUMMARY`` block and its log levels), C3a (each
``(code, horizon)`` pair gets its own exception boundary, so a failure in one
horizon cannot suppress the other), and C5 (the standalone CLI's ``main()``
diagnosis is driven off the real ``API_FAILED`` tally, not attempted/completed
list lengths).

The C4 exit-code invariant guard (test 6) lives in
``test_short_horizon_write_failure_visibility.py`` instead, next to the other
``_write_short_horizon_hydrograph_records`` tests it protects.

See ``doc/plans/issues/high_prio_gi_draft_prepq_short_horizon_norm_drops_rows.md``.

Fake station codes '19999' / '19998' only; no real station codes or
discharge values.
"""

import calendar
import datetime as dt
import logging
import os
import sys

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import sync_short_horizon_hydrograph as shh  # noqa: E402

CODE = "19999"
CODE2 = "19998"
TARGET_YEAR = 2026
PREVIOUS_YEAR = 2025
TODAY = dt.date(2027, 1, 1)  # every 2026 period is closed

PENTAD_NORMS = [float(p) for p in range(1, 73)]
DECAD_NORMS = [float(d) for d in range(1, 37)]

# Literal issue-date boundaries for pentad target_year=2026, per
# forecast_library.get_issue_date_from_pentad's own docstring examples.
# Deliberately hardcoded rather than computed via that helper — deriving the
# expectation from the function under test is exactly what would let a
# boundary bug through.
PERIOD_1_DATE = "2025-12-31"
PERIOD_2_DATE = "2026-01-05"
PERIOD_3_DATE = "2026-01-10"
LAST_PERIOD_DATE = "2026-12-25"


def _full_year_daily(year, value):
    """Every calendar day of ``year`` carries ``value`` (constant climatology)."""
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            rows.append({"date": dt.date(year, month, day).isoformat(), "discharge": value})
    return rows


def _daily_fixture():
    """Only the PREVIOUS year has daily data — enough for a non-trivial,
    deterministic envelope and 'previous' actual; 'current' stays None in
    both the baseline and failure runs, which is still a valid field to
    compare for equality.
    """
    return {PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0)}


class FakeSDK:
    """Fake iEH-HF SDK: norm payloads are consumed one per
    ``get_norm_for_site`` call, in call order; ``get_data_values_for_site``
    always returns an empty page so ``_fetch_sdk_period_actuals`` falls back
    to the local daily WDDA computation deterministically.
    """

    def __init__(self, norm_payloads):
        self._norm_payloads = list(norm_payloads)

    def get_norm_for_site(self, code, value_field, norm_period):
        assert self._norm_payloads, "get_norm_for_site called more times than payloads provided"
        payload = self._norm_payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return payload

    def get_data_values_for_site(self, filters=None, **kwargs):
        return {"count": 0, "next": None, "previous": None, "results": []}


class FakeShortHorizonClient:
    """Fake SAPPHIRE preprocessing client. Records every ``read_hydrograph``
    call (args) and every ``write_hydrograph`` call (records) for assertion.
    """

    def __init__(
        self,
        daily_by_year=None,
        existing_hydrograph=None,
        read_hydrograph_error=None,
        write_hydrograph_error_for_horizon_type=None,
        write_hydrograph_error_for_code=None,
        write_hydrograph_error=None,
        read_runoff_error=None,
        read_runoff_should_fail=None,
    ):
        self.daily_by_year = daily_by_year or {}
        self.existing_hydrograph = list(existing_hydrograph or [])
        self.read_hydrograph_error = read_hydrograph_error
        # When set, `write_hydrograph` raises `write_hydrograph_error` only
        # for a batch whose records carry this `horizon_type` (test 7 —
        # C3a: an API write failure must be scoped to that one horizon).
        self.write_hydrograph_error_for_horizon_type = write_hydrograph_error_for_horizon_type
        # When set, `write_hydrograph` raises `write_hydrograph_error` only
        # for a batch whose records carry this `code` (test 9 — both of one
        # station's horizons fail, so that station's records stay empty).
        self.write_hydrograph_error_for_code = write_hydrograph_error_for_code
        self.write_hydrograph_error = write_hydrograph_error
        # When set, `read_runoff` raises `read_runoff_error` for daily-runoff
        # reads whenever `read_runoff_should_fail(call_index, year)` is
        # truthy (PREPQ-020 `_ShortHorizonDailyReadError` tests). `call_index`
        # is a 1-based, per-client-instance counter over EVERY `read_runoff`
        # call (across both horizons and all stations sharing this client),
        # so a test can fail exactly one `_read_daily_by_year` invocation
        # (e.g. "the first `years_back + 1` calls") while leaving later
        # invocations - the same station's other horizon, or the next
        # station - genuinely unaffected. `year`-based predicates express the
        # "some years fail, some succeed" partial-gap case instead.
        self.read_runoff_error = read_runoff_error
        self.read_runoff_should_fail = read_runoff_should_fail
        self._read_runoff_call_index = 0
        self.write_calls = []
        self.read_hydrograph_calls = []

    def read_runoff(self, horizon, code, start_date, end_date, limit):
        self._read_runoff_call_index += 1
        year = int(start_date[:4])
        if (
            self.read_runoff_error is not None
            and self.read_runoff_should_fail is not None
            and self.read_runoff_should_fail(self._read_runoff_call_index, year)
        ):
            raise self.read_runoff_error
        return list(self.daily_by_year.get(year, []))

    def read_hydrograph(self, horizon, code, start_date, end_date, limit):
        self.read_hydrograph_calls.append(
            {
                "horizon": horizon,
                "code": code,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }
        )
        if self.read_hydrograph_error is not None:
            raise self.read_hydrograph_error
        return [
            dict(row)
            for row in self.existing_hydrograph
            if row["horizon_type"] == horizon
            and row["code"] == str(code)
            and start_date <= row["date"] <= end_date
        ]

    def write_hydrograph(self, records):
        records = [dict(record) for record in records]
        raise_for_horizon = (
            self.write_hydrograph_error_for_horizon_type is not None
            and records
            and records[0]["horizon_type"] == self.write_hydrograph_error_for_horizon_type
        )
        raise_for_code = (
            self.write_hydrograph_error_for_code is not None
            and records
            and records[0]["code"] == self.write_hydrograph_error_for_code
        )
        if raise_for_horizon or raise_for_code:
            raise self.write_hydrograph_error
        self.write_calls.append(records)
        return len(records)


_COMPARABLE_FIELDS = ("current", "previous", "mean", "min", "max", "q05", "q25", "q75", "q95")


def _write_pentad(sdk, client, code=CODE):
    return shh.write_station_short_horizon(
        code=code,
        horizon_type="pentad",
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )


# ---------------------------------------------------------------------------
# Test 1 — SDK norm call raises: rows still written, norm None, everything
# else matches the no-failure baseline.
# ---------------------------------------------------------------------------
def test_sdk_raise_writes_full_batch_with_none_norm_matching_baseline(caplog):
    baseline_client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    baseline_records = _write_pentad(FakeSDK([PENTAD_NORMS]), baseline_client)
    assert len(baseline_records) == 72
    baseline_by_period = {r["horizon_in_year"]: r for r in baseline_records}

    failure_client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    with caplog.at_level(logging.INFO):
        failure_records = _write_pentad(FakeSDK([RuntimeError("SDK unreachable")]), failure_client)

    assert len(failure_records) == 72
    assert len(failure_client.write_calls) == 1
    assert len(failure_client.write_calls[0]) == 72

    for record in failure_records:
        assert record["norm"] is None
        base = baseline_by_period[record["horizon_in_year"]]
        for field in _COMPARABLE_FIELDS:
            assert record[field] == base[field], (record["horizon_in_year"], field)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("SDK norm call failed" in r.message for r in warnings), (
        "the SDK-raise case must be reserved for WARNING, per the owner decision"
    )


# ---------------------------------------------------------------------------
# Test 2 — SDK norm call returns the wrong length ([]): rows still written,
# same assertions, and this case must log at INFO, not WARNING.
# ---------------------------------------------------------------------------
def test_norm_wrong_length_writes_full_batch_with_none_norm_matching_baseline(caplog):
    baseline_client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    baseline_records = _write_pentad(FakeSDK([PENTAD_NORMS]), baseline_client)
    baseline_by_period = {r["horizon_in_year"]: r for r in baseline_records}

    failure_client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    with caplog.at_level(logging.INFO):
        failure_records = _write_pentad(FakeSDK([[]]), failure_client)

    assert len(failure_records) == 72
    assert len(failure_client.write_calls) == 1
    assert len(failure_client.write_calls[0]) == 72

    for record in failure_records:
        assert record["norm"] is None
        base = baseline_by_period[record["horizon_in_year"]]
        for field in _COMPARABLE_FIELDS:
            assert record[field] == base[field], (record["horizon_in_year"], field)

    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("norm unavailable" in r.message for r in infos)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings, (
        "a missing norm is not our failure — it must log at INFO, never WARNING "
        f"(got: {[r.message for r in warnings]})"
    )


# ---------------------------------------------------------------------------
# Test 2b — norm unsized (None) and norm invalid (72 non-numeric strings):
# both classified NORM_ABSENT, rows still written, and no exception escapes
# (guards the third, unhandled `len(norms)` branch).
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "invalid_norms",
    [None, ["x"] * 72],
    ids=["unsized_none", "seventy_two_non_numeric_strings"],
)
def test_norm_unsized_or_non_numeric_is_absent_not_valid_and_does_not_raise(invalid_norms):
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_pentad(FakeSDK([invalid_norms]), client)

    assert len(records) == 72
    assert all(record["norm"] is None for record in records), (
        "a 72-element list of strings (or None) must be NORM_ABSENT, not VALID"
    )
    assert len(client.write_calls) == 1


# ---------------------------------------------------------------------------
# Test 3 — read-merge preserves a stored norm, over the CORRECT window (not
# the calendar year), matching each row on the exact (date, horizon_in_year)
# pair rather than on horizon_in_year alone.
# ---------------------------------------------------------------------------
def test_read_merge_preserves_stored_norms_over_the_correct_window_including_period_1():
    existing_hydrograph = [
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": PERIOD_1_DATE,
            "horizon_in_year": 1,
            "norm": 11.1,
        },
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": PERIOD_2_DATE,
            "horizon_in_year": 2,
            "norm": 22.2,
        },
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": PERIOD_3_DATE,
            "horizon_in_year": 3,
            "norm": 33.3,
        },
        # Decoy: the right horizon_in_year but the WRONG date (as if a stale
        # or cross-year row leaked in) — must be rejected by the exact
        # (date, horizon_in_year) match, not accepted on horizon_in_year alone.
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2026-01-01",
            "horizon_in_year": 1,
            "norm": 999.0,
        },
    ]
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(), existing_hydrograph=existing_hydrograph
    )

    records = _write_pentad(FakeSDK([[]]), client)  # norm-absent -> triggers the read-merge

    assert len(client.read_hydrograph_calls) == 1
    call = client.read_hydrograph_calls[0]
    assert call["start_date"] == PERIOD_1_DATE
    assert call["end_date"] == LAST_PERIOD_DATE
    assert call["horizon"] == "pentad"

    by_period = {r["horizon_in_year"]: r for r in records}
    assert by_period[1]["norm"] == 11.1  # period 1 IS among the preserved norms
    assert by_period[2]["norm"] == 22.2
    assert by_period[3]["norm"] == 33.3
    for period in range(4, 73):
        assert by_period[period]["norm"] is None


# ---------------------------------------------------------------------------
# Test 4 (P2 full form) — a failed read-merge on ONE horizon does not clobber,
# is API_FAILED for that (code, horizon) only, and does not abort the loop:
# neither the same station's OTHER horizon nor the next station is skipped.
#
# This is the assertion that pins C3a (the exception boundary moved inside
# the horizon loop): CODE's pentad norm-lookup is absent, so its read-merge
# runs and raises; CODE's decade norm-lookup is VALID, so it never touches
# the read-merge at all and must still be attempted, built, and written. If
# the boundary were left at station level (the pre-P2 defect), CODE's decade
# would never even call get_norm_for_site, and this test's decade-write
# assertion below would fail.
# ---------------------------------------------------------------------------
def test_failed_read_merge_is_api_failed_for_that_horizon_only_and_does_not_abort_the_loop(caplog):
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        read_hydrograph_error=requests.exceptions.ConnectionError("read failed"),
    )
    # CODE: pentad norm-absent -> read-merge raises -> API_FAILED.
    #       decade valid norms -> never reaches the read-merge -> WRITTEN.
    # CODE2: valid norms both horizons -> unaffected.
    sdk = FakeSDK([[], DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    code_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE]
    code_pentad_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "pentad"]
    code_decade_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "decade"]

    # Anti-clobber: the failed read-merge made ZERO write_hydrograph calls
    # for CODE's pentad horizon.
    assert code_pentad_calls == []

    # C3a: CODE's decade horizon is INDEPENDENTLY attempted and still
    # completes even though CODE's pentad horizon failed.
    assert len(code_decade_calls) == 1
    assert len(code_decade_calls[0]) == 36

    # The loop did not abort: the next station was still attempted, written,
    # and completed on both horizons.
    code2_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE2]
    assert len(code2_write_calls) == 2  # pentad (72 rows) + decade (36 rows)
    assert {len(call) for call in code2_write_calls} == {72, 36}
    assert CODE2 in result.completed_station_codes

    # CODE itself is "failed" at the station level (it had an API_FAILED
    # horizon), even though its decade horizon produced records.
    assert CODE in result.failed_station_codes
    assert CODE not in result.completed_station_codes

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "pentad" in r.message and CODE in r.message and "API read/write failed" in r.message
        for r in warnings
    )
    assert not any(
        "decade" in r.message and CODE in r.message and "API read/write failed" in r.message
        for r in warnings
    ), "CODE's decade horizon must not be reported as an API failure"

    # Per-horizon counts in the run summary reflect the independent outcome:
    # pentad has 1 written (CODE2) + 1 api_failed (CODE); decade has 2
    # written (CODE, CODE2) + 0 api_failed. Both sum to total_attempted=2.
    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "total_attempted=2" in summary_text
    assert "pentad_written=1 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=1" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 4b (F1 review fix) — a read-merge failure whose exception type is NOT
# a member of ``_API_READ_WRITE_ERRORS`` (e.g. ``ValueError`` from a malformed
# response) must still be classified API_FAILED for that horizon only, not
# escape ``write_short_horizon_hydrograph`` entirely and suppress every
# remaining station.
#
# Before the fix, ``_read_existing_period_norms`` let ``client.read_hydrograph``
# raise whatever it raised, and the per-horizon boundary caught only the fixed
# ``_API_READ_WRITE_ERRORS`` tuple - so a ``ValueError`` here would propagate
# out of the horizon loop and abort the whole run (CODE2 never attempted).
# ---------------------------------------------------------------------------
def test_read_merge_valueerror_is_api_failed_for_that_horizon_only_and_does_not_abort_the_loop(
    caplog,
):
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        read_hydrograph_error=ValueError("malformed response body"),
    )
    # CODE: pentad norm-absent -> read-merge raises ValueError -> API_FAILED.
    #       decade valid norms -> never reaches the read-merge -> WRITTEN.
    # CODE2: valid norms both horizons -> unaffected.
    sdk = FakeSDK([[], DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    code_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE]
    code_pentad_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "pentad"]
    code_decade_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "decade"]

    # Anti-clobber: the failed read-merge made ZERO write_hydrograph calls
    # for CODE's pentad horizon.
    assert code_pentad_calls == []

    # CODE's decade horizon is INDEPENDENTLY attempted and still completes.
    assert len(code_decade_calls) == 1
    assert len(code_decade_calls[0]) == 36

    # The loop did not abort: the next station was still attempted, written,
    # and completed on both horizons - a ValueError must not have escaped
    # write_short_horizon_hydrograph and suppressed CODE2.
    code2_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE2]
    assert len(code2_write_calls) == 2  # pentad (72 rows) + decade (36 rows)
    assert {len(call) for call in code2_write_calls} == {72, 36}
    assert CODE2 in result.completed_station_codes

    assert CODE in result.failed_station_codes
    assert CODE not in result.completed_station_codes

    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "pentad_written=1 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=1" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 5 — run summary counts and log levels (C3).
#
# Three attempts (one valid, one norm-absent, one SDK-raise) produce exact
# per-horizon counts that each sum to total_attempted, and the per-horizon
# degraded line's log level depends on which statuses contributed.
#
# Only '19999'/'19998' are permitted as station codes, so CODE is reused for
# the 1st and 3rd attempt; FakeSDK's per-call (not per-code) payload queue
# gives each attempt an independent, fully-controlled outcome regardless of
# code identity, so this still exercises three distinct status outcomes.
# ---------------------------------------------------------------------------
def test_run_summary_counts_and_log_levels_for_mixed_outcomes(caplog):
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    sdk = FakeSDK(
        [
            PENTAD_NORMS,  # attempt 1 (CODE): pentad valid
            DECAD_NORMS,  # attempt 1 (CODE): decade valid
            [],  # attempt 2 (CODE2): pentad norm-absent
            [],  # attempt 2 (CODE2): decade norm-absent
            RuntimeError("SDK unreachable"),  # attempt 3 (CODE): pentad raises
            RuntimeError("SDK unreachable"),  # attempt 3 (CODE): decade raises
        ]
    )

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2, CODE],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    assert result.attempted_station_codes == [CODE, CODE2, CODE]

    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "total_attempted=3" in summary_text
    assert "pentad_written=1 pentad_norm_absent=1 pentad_sdk_failed=1 pentad_api_failed=0" in (
        summary_text
    )
    assert "decade_written=1 decade_norm_absent=1 decade_sdk_failed=1 decade_api_failed=0" in (
        summary_text
    )
    # Per-horizon counts sum to total_attempted.
    assert 1 + 1 + 1 + 0 == 3

    # Both horizons are degraded (norm_absent=1 and sdk_failed=1 each), so
    # both lines appear, and both must be WARNING because each has a
    # sdk_failed contributor.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("pentad discharge norms unavailable for 2/3 stations" in r.message for r in warnings)
    assert any("decade discharge norms unavailable for 2/3 stations" in r.message for r in warnings)
    assert not any("discharge norms unavailable" in r.message for r in infos), (
        "a horizon with any sdk_failed contributor must log its degraded line at WARNING, "
        "never INFO"
    )


def test_run_summary_norm_absent_only_horizon_logs_info_not_warning(caplog):
    """A horizon degraded ONLY by norm-absence (no sdk_failed) logs its
    one-line note at INFO, per the 2026-09-04 owner decision that a missing
    norm is not our failure.
    """
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    # Both stations: pentad norm-absent (INFO-only degradation); decade
    # always valid (clean, no line at all).
    sdk = FakeSDK([[], DECAD_NORMS, [], DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("pentad discharge norms unavailable for 2/2 stations" in r.message for r in infos)
    assert not any("discharge norms unavailable" in r.message for r in warnings), (
        "a norm-absent-only horizon must log its degraded line at INFO, never WARNING"
    )
    # decade is clean (no norm_absent, no sdk_failed) -> no line at all.
    assert not any("decade discharge norms unavailable" in r.message for r in infos + warnings)


def test_run_summary_has_no_per_horizon_line_when_all_stations_valid(caplog):
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    sdk = FakeSDK([PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    all_messages = [r.message for r in caplog.records]
    assert not any("discharge norms unavailable" in m for m in all_messages)
    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "pentad_written=2 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=0" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 7 — an API write failure is scoped to one horizon (C3a): the OTHER
# horizon for the same station is still attempted and classified, and the
# loop continues to the next station.
# ---------------------------------------------------------------------------
def test_write_failure_on_one_horizon_does_not_block_the_other_horizon(caplog):
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        write_hydrograph_error_for_horizon_type="pentad",
        write_hydrograph_error=requests.exceptions.ConnectionError("write failed"),
    )
    sdk = FakeSDK([PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    # Pentad write never lands for either station (the fake raises for every
    # pentad batch); decade writes land for both.
    pentad_calls = [
        call for call in client.write_calls if call and call[0]["horizon_type"] == "pentad"
    ]
    decade_calls = [
        call for call in client.write_calls if call and call[0]["horizon_type"] == "decade"
    ]
    assert pentad_calls == []
    assert len(decade_calls) == 2
    assert {len(call) for call in decade_calls} == {36}

    # Both stations are "failed" (their pentad horizon hit an API failure),
    # but the loop did not abort: both were attempted and both had their
    # decade horizon classified and written.
    assert result.attempted_station_codes == [CODE, CODE2]
    assert result.failed_station_codes == [CODE, CODE2]
    assert result.completed_station_codes == []

    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "total_attempted=2" in summary_text
    assert "pentad_written=0 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=2" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 9 — a station whose EVERY horizon is API_FAILED produces zero records
# for that station, but it is still ATTEMPTED (not silently popped back off,
# per P2) and marked FAILED rather than dropped; the next station is still
# attempted, written, and completed. Regression guard: confirms P2's
# attempted-list fix is not disturbed by the P3/C5 change to main().
# ---------------------------------------------------------------------------
def test_station_with_no_records_is_failed_not_dropped_and_next_station_still_attempted(caplog):
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        write_hydrograph_error_for_code=CODE,
        write_hydrograph_error=requests.exceptions.ConnectionError("write failed"),
    )
    sdk = FakeSDK([PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    # CODE: both horizons API_FAILED -> zero records for CODE, but it is
    # still ATTEMPTED (not silently popped) and marked FAILED, not dropped.
    assert result.attempted_station_codes == [CODE, CODE2]
    assert CODE in result.failed_station_codes
    assert CODE not in result.completed_station_codes
    assert [r for r in result if r["code"] == CODE] == []

    # The next station is still attempted, written, and completed on both
    # horizons - the loop did not abort on CODE's empty result.
    code2_write_calls = [c for c in client.write_calls if c and c[0]["code"] == CODE2]
    assert len(code2_write_calls) == 2
    assert {len(c) for c in code2_write_calls} == {72, 36}
    assert CODE2 in result.completed_station_codes

    assert result.api_failed_count == 2  # CODE's pentad + decade, both API_FAILED

    assert any(
        "No short-horizon hydrograph records produced for station" in r.message
        and CODE in r.message
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Test 10 — a total daily-runoff read failure (EVERY year in the climatology
# window fails) must not build/write a full null batch. `_read_daily_by_year`
# raises `_ShortHorizonDailyReadError`, which the per-horizon boundary
# classifies as API_FAILED for that (code, horizon) pair only:
# `write_hydrograph` is never called for it, the same station's OTHER horizon
# still completes, and the next station still completes.
#
# `read_runoff_should_fail` fails only the first `HISTORY_YEARS_BACK + 1`
# calls (CODE's pentad invocation of `_read_daily_by_year`, which reads one
# year at a time); every later call - CODE's decade invocation, then CODE2's
# pentad and decade - succeeds from `daily_by_year`. This isolates the
# failure to exactly one (code, horizon) pair while going through the real
# `_read_daily_by_year` loop (not a stand-in for it).
# ---------------------------------------------------------------------------
def test_all_years_failing_daily_read_is_api_failed_for_that_horizon_only_and_does_not_write(
    caplog,
):
    first_invocation_calls = shh.HISTORY_YEARS_BACK + 1
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        read_runoff_error=requests.exceptions.ConnectionError("daily runoff read failed"),
        read_runoff_should_fail=lambda call_index, year: call_index <= first_invocation_calls,
    )
    sdk = FakeSDK([PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    code_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE]
    code_pentad_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "pentad"]
    code_decade_calls = [call for call in code_write_calls if call[0]["horizon_type"] == "decade"]

    # Anti-clobber: CODE's pentad horizon never reaches write_hydrograph.
    assert code_pentad_calls == []

    # CODE's decade horizon is INDEPENDENTLY attempted and still completes.
    assert len(code_decade_calls) == 1
    assert len(code_decade_calls[0]) == 36

    # The next station is unaffected: both its horizons complete and write.
    code2_write_calls = [call for call in client.write_calls if call and call[0]["code"] == CODE2]
    assert len(code2_write_calls) == 2
    assert {len(c) for c in code2_write_calls} == {72, 36}
    assert CODE2 in result.completed_station_codes

    # CODE is "failed" at the station level (its pentad horizon is
    # API_FAILED), even though its decade horizon produced records.
    assert result.attempted_station_codes == [CODE, CODE2]
    assert CODE in result.failed_station_codes
    assert CODE not in result.completed_station_codes
    assert result.api_failed_count == 1  # only CODE's pentad

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "pentad" in r.message and CODE in r.message and "API read/write failed" in r.message
        for r in warnings
    )
    assert not any(
        "decade" in r.message and CODE in r.message and "API read/write failed" in r.message
        for r in warnings
    ), "CODE's decade horizon must not be reported as an API failure"

    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "total_attempted=2" in summary_text
    assert "pentad_written=1 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=1" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 11 — some years fail, some succeed: the common legitimate multi-year
# gap. Regression guard for the emptiness test: this must NOT trip
# `_ShortHorizonDailyReadError` and must still write the full batch, exactly
# like the no-failure baseline.
# ---------------------------------------------------------------------------
def test_some_years_failing_daily_read_still_writes_full_batch_normally():
    baseline_client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    baseline_records = _write_pentad(FakeSDK([PENTAD_NORMS]), baseline_client)
    assert len(baseline_records) == 72
    baseline_by_period = {r["horizon_in_year"]: r for r in baseline_records}

    # Every year EXCEPT PREVIOUS_YEAR raises; PREVIOUS_YEAR (the only year
    # with data in `_daily_fixture()`) reads normally - a genuine multi-year
    # gap, not a total failure.
    partial_client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        read_runoff_error=requests.exceptions.ConnectionError("daily runoff read failed"),
        read_runoff_should_fail=lambda call_index, year: year != PREVIOUS_YEAR,
    )

    records = _write_pentad(FakeSDK([PENTAD_NORMS]), partial_client)

    assert len(records) == 72
    assert len(partial_client.write_calls) == 1
    assert len(partial_client.write_calls[0]) == 72
    for record in records:
        base = baseline_by_period[record["horizon_in_year"]]
        for field in _COMPARABLE_FIELDS:
            assert record[field] == base[field], (record["horizon_in_year"], field)


# ---------------------------------------------------------------------------
# Test 12 — no years fail at all: full batch written, no failure recorded
# (the plain baseline, asserted at the `write_short_horizon_hydrograph` level
# so a regression here would also fail the run-summary counts).
# ---------------------------------------------------------------------------
def test_no_years_failing_daily_read_is_unchanged(caplog):
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())
    sdk = FakeSDK([PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS])

    with caplog.at_level(logging.INFO):
        result = shh.write_short_horizon_hydrograph(
            codes=[CODE, CODE2],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    assert result.attempted_station_codes == [CODE, CODE2]
    assert result.completed_station_codes == [CODE, CODE2]
    assert result.failed_station_codes == []
    assert result.api_failed_count == 0
    code_write_calls = [c for c in client.write_calls if c]
    assert len(code_write_calls) == 4
    assert {len(c) for c in code_write_calls} == {72, 36}

    summary_text = "\n".join(r.message for r in caplog.records if r.levelno == logging.INFO)
    assert "pentad_written=2 pentad_norm_absent=0 pentad_sdk_failed=0 pentad_api_failed=0" in (
        summary_text
    )
    assert "decade_written=2 decade_norm_absent=0 decade_sdk_failed=0 decade_api_failed=0" in (
        summary_text
    )


# ---------------------------------------------------------------------------
# Test 8 / 8b (C5) — main()'s CLI diagnosis is driven by the real API_FAILED
# tally (C3's status counts), not by attempted/completed list lengths.
#
# Both states are constructed directly rather than via a norm-absent run:
# after C1, a norm-absent run still produces records and completed stations,
# so it would pass this branch whether or not C5 was implemented (see the
# plan's "Corrections applied after the confirm-fixes pass").
# ---------------------------------------------------------------------------
class _FakeMainResult(list):
    """Minimal stand-in for `_ShortHorizonWriteResult`, carrying only the
    three attributes `main()`'s C5 branch reads.
    """

    def __init__(self, records, attempted, completed, api_failed_count):
        super().__init__(records)
        self.attempted_station_codes = attempted
        self.completed_station_codes = completed
        self.api_failed_count = api_failed_count


def _run_main_with_fake_result(monkeypatch, fake_result):
    """Drive `shh.main()` end-to-end with every external dependency faked out
    except the C5 branch under test. Returns the `SystemExit` code.
    """
    monkeypatch.setattr(shh.sl, "load_environment", lambda: None)
    monkeypatch.setattr(shh, "IEasyHydroHFSDK", lambda: object())
    monkeypatch.setattr(shh, "resolve_sdk_station_codes", lambda sdk: [CODE, CODE2])
    monkeypatch.setattr(shh, "_get_preprocessing_client", lambda: object())
    monkeypatch.setattr(shh, "write_short_horizon_hydrograph", lambda **kwargs: fake_result)
    monkeypatch.setattr(sys, "argv", ["sync_short_horizon_hydrograph.py"])

    with pytest.raises(SystemExit) as excinfo:
        shh.main()
    return excinfo.value.code


def test_cli_does_not_report_all_failed_when_api_failed_count_is_zero(monkeypatch, caplog):
    """attempted > 0, completed == 0, API_FAILED == 0. The pre-C5 code inferred
    'All N attempted station(s) had ... API read/write failures' from the list
    lengths alone, which this state would trigger even though nothing actually
    hit an API failure. Must NOT emit that message (or exit 2 for that reason).
    """
    fake_result = _FakeMainResult(
        records=[{"some": "record"}],
        attempted=[CODE, CODE2],
        completed=[],
        api_failed_count=0,
    )

    with caplog.at_level(logging.ERROR):
        exit_code = _run_main_with_fake_result(monkeypatch, fake_result)

    assert exit_code == 0
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert not any("API read/write failures" in r.message for r in errors), (
        "a completed==0 result with zero API_FAILED must not be misreported as a total API "
        f"outage (got: {[r.message for r in errors]})"
    )


def test_cli_reports_all_failed_when_every_pair_is_api_failed(monkeypatch, caplog):
    """The genuine case: every station's every horizon is API_FAILED. Still
    produces the error and the non-zero exit it produces today.
    """
    fake_result = _FakeMainResult(
        records=[],
        attempted=[CODE, CODE2],
        completed=[],
        api_failed_count=4,  # 2 stations x 2 horizons, all API_FAILED
    )

    with caplog.at_level(logging.ERROR):
        exit_code = _run_main_with_fake_result(monkeypatch, fake_result)

    assert exit_code == 2
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "API read/write failure" in r.message and "4" in r.message and "2 attempted" in r.message
        for r in errors
    )
