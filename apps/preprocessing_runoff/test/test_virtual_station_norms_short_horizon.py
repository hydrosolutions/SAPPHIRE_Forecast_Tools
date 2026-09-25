"""PREPQ-022: virtual-station discharge norms, short-horizon (pentad/decad) writer.

Covers the ``get_virtual_station_codes`` import and the ``virtual_codes``
retry addition to ``sync_short_horizon_hydrograph._lookup_short_horizon_norms``
(iEH HF SDK commit ``1907a30`` adds ``get_norm_for_site(..., virtual=...)``
and ``get_virtual_sites()``). D-A option (b), decided by the owner: the
default (non-virtual) norm call is always tried first; a virtual-station
retry (``virtual=True``) is attempted ONLY when that default call raises AND
the code is a known virtual station. Short-horizon grades every retry
exception SDK_FAILED (no 404-vs-other distinction, unlike long-horizon).

See ``doc/plans/issues/high_prio_gi_draft_prepq_virtual_station_norms.md`` §P2
for the full test list this file (together with
``test_virtual_station_norms_long_horizon.py``) implements.

Fake station codes '19999'/'19998' only; no real station codes or discharge
values.
"""

import calendar
import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import sync_short_horizon_hydrograph as shh  # noqa: E402

CODE = "19999"
CODE2 = "19998"
TARGET_YEAR = 2026
PREVIOUS_YEAR = 2025
TODAY = dt.date(2027, 1, 1)  # every 2026 period is closed

PENTAD_NORMS = [float(p) for p in range(1, 73)]
DECAD_NORMS = [float(d) for d in range(1, 37)]

# Literal issue-date boundaries for pentad target_year=2026 (see
# test_short_horizon_norm_decoupling.py, from which this is copied verbatim):
# period 1 of 2026 is stamped with the PRECEDING 31 December.
PERIOD_1_DATE = "2025-12-31"


def _full_year_daily(year, value):
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            rows.append({"date": dt.date(year, month, day).isoformat(), "discharge": value})
    return rows


def _daily_fixture():
    return {PREVIOUS_YEAR: _full_year_daily(PREVIOUS_YEAR, 8.0)}


def _sdk_generic_failure(code=CODE):
    return ValueError(f"Could not retrieve discharge norm for site {code}, got status code 500")


class VirtualAwareFakeSDK:
    """Fake iEH HF SDK with SEPARATE payload queues for the default
    (``virtual=False``) call and the virtual retry (``virtual=True``); see
    the long-horizon test file's identically-named fake for the full
    rationale. ``get_data_values_for_site`` always returns an empty page so
    ``_fetch_sdk_period_actuals`` falls back to the local daily WDDA
    computation deterministically, same as ``FakeSDK`` in
    ``test_short_horizon_norm_decoupling.py``.
    """

    def __init__(
        self,
        default_payloads=(),
        virtual_payloads=(),
        virtual_sites=(),
        get_virtual_sites_error=None,
    ):
        self._default_payloads = list(default_payloads)
        self._virtual_payloads = list(virtual_payloads)
        self._virtual_sites = list(virtual_sites)
        self._get_virtual_sites_error = get_virtual_sites_error
        self.get_virtual_sites_calls = 0
        self.default_calls: list[tuple] = []
        self.virtual_calls: list[tuple] = []

    def get_virtual_sites(self):
        self.get_virtual_sites_calls += 1
        if self._get_virtual_sites_error is not None:
            raise self._get_virtual_sites_error
        return [dict(site) for site in self._virtual_sites]

    def get_norm_for_site(self, code, value_field, norm_period, virtual=False):
        if virtual:
            self.virtual_calls.append((code, value_field, norm_period))
            assert self._virtual_payloads, (
                "get_norm_for_site(virtual=True) called more times than virtual payloads provided"
            )
            payload = self._virtual_payloads.pop(0)
        else:
            self.default_calls.append((code, value_field, norm_period))
            assert self._default_payloads, (
                "get_norm_for_site(virtual=False) called more times than default payloads provided"
            )
            payload = self._default_payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return payload

    def get_data_values_for_site(self, filters=None, **kwargs):
        return {"count": 0, "next": None, "previous": None, "results": []}


class FakeShortHorizonClient:
    """Minimal SAPPHIRE preprocessing client stand-in (copied from
    test_short_horizon_norm_decoupling.py's FakeShortHorizonClient so this
    file stays self-contained)."""

    def __init__(self, daily_by_year=None, existing_hydrograph=None, read_hydrograph_error=None):
        self.daily_by_year = daily_by_year or {}
        self.existing_hydrograph = list(existing_hydrograph or [])
        self.read_hydrograph_error = read_hydrograph_error
        self.write_calls = []
        self.read_hydrograph_calls = []

    def read_runoff(self, horizon, code, start_date, end_date, limit):
        year = int(start_date[:4])
        return list(self.daily_by_year.get(year, []))

    def read_hydrograph(self, horizon, code, start_date, end_date, limit):
        self.read_hydrograph_calls.append(
            {"horizon": horizon, "code": code, "start_date": start_date, "end_date": end_date}
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
        self.write_calls.append(records)
        return len(records)


def _write_pentad(sdk, client, code=CODE, virtual_codes=None):
    return shh.write_station_short_horizon(
        code=code,
        horizon_type="pentad",
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
        virtual_codes=virtual_codes,
    )


def _write_decad(sdk, client, code=CODE, virtual_codes=None):
    return shh.write_station_short_horizon(
        code=code,
        horizon_type="decade",
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
        virtual_codes=virtual_codes,
    )


def _existing_pentad_norms(code, norm_for_period_1=None):
    """One stored pentad row for period 1, stamped with the preceding 31
    December (PERIOD_1_DATE) -- the read-merge window is NOT calendar-year.
    """
    return [
        {
            "horizon_type": "pentad",
            "code": code,
            "date": PERIOD_1_DATE,
            "horizon_in_year": 1,
            "norm": norm_for_period_1,
            "current": None,
            "previous": None,
        }
    ]


# ---------------------------------------------------------------------------
# 1. Regular code: default call has no `virtual` kwarg; output unchanged.
# ---------------------------------------------------------------------------
def test_regular_code_default_call_has_no_virtual_kwarg_and_output_unchanged():
    sdk = VirtualAwareFakeSDK(default_payloads=[PENTAD_NORMS], virtual_sites=[])
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_pentad(sdk, client)

    norms = [record["norm"] for record in records]
    assert norms == PENTAD_NORMS
    assert records.status is shh._ShortHorizonWriteStatus.WRITTEN
    assert sdk.virtual_calls == []
    assert len(sdk.default_calls) == 1


# ---------------------------------------------------------------------------
# 2. Virtual code, valid values -> VALID, norm written (72/36).
# ---------------------------------------------------------------------------
def test_virtual_code_pentad_default_raises_retry_succeeds():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[PENTAD_NORMS],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_pentad(sdk, client, virtual_codes=frozenset({CODE}))

    norms = [record["norm"] for record in records]
    assert norms == PENTAD_NORMS
    assert records.status is shh._ShortHorizonWriteStatus.WRITTEN
    assert len(sdk.virtual_calls) == 1
    assert sdk.virtual_calls[0] == (CODE, "discharge", "p")


def test_virtual_code_decad_default_raises_retry_succeeds():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[DECAD_NORMS],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_decad(sdk, client, virtual_codes=frozenset({CODE}))

    norms = [record["norm"] for record in records]
    assert norms == DECAD_NORMS
    assert records.status is shh._ShortHorizonWriteStatus.WRITTEN
    assert len(sdk.virtual_calls) == 1
    assert sdk.virtual_calls[0] == (CODE, "discharge", "d")


# ---------------------------------------------------------------------------
# 3. Virtual `[]` -> NORM_ABSENT; preservation (period 1 stamped 31 Dec Y-1
#    kept, never-normed stays normless, a failed preservation read writes no
#    nulls).
# ---------------------------------------------------------------------------
def test_virtual_code_empty_list_norm_absent_preserves_stored_period_1_norm():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        existing_hydrograph=_existing_pentad_norms(CODE, norm_for_period_1=42.0),
    )

    records = _write_pentad(sdk, client, virtual_codes=frozenset({CODE}))

    period_1 = next(r for r in records if r["horizon_in_year"] == 1)
    assert period_1["norm"] == 42.0
    assert period_1["date"] == PERIOD_1_DATE
    assert records.status is shh._ShortHorizonWriteStatus.NORM_ABSENT


def test_virtual_code_empty_list_never_normed_period_stays_normless():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_pentad(sdk, client, virtual_codes=frozenset({CODE}))

    period_1 = next(r for r in records if r["horizon_in_year"] == 1)
    assert period_1["norm"] is None


def test_virtual_code_empty_list_failed_preservation_read_writes_no_nulls():
    # _read_existing_period_norms wraps any client.read_hydrograph failure in
    # _ShortHorizonNormReadError; write_short_horizon_hydrograph's per-horizon
    # boundary catches that and does not write the horizon at all.
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure(), PENTAD_NORMS],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(
        daily_by_year=_daily_fixture(),
        read_hydrograph_error=ValueError("malformed response body"),
    )

    records = shh.write_short_horizon_hydrograph(
        codes=[CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    pentad_records = [r for r in records if r["horizon_type"] == "pentad"]
    assert pentad_records == []
    assert client.write_calls == [] or all(
        rec[0]["horizon_type"] != "pentad" for rec in client.write_calls if rec
    )
    assert CODE in records.failed_station_codes


# ---------------------------------------------------------------------------
# 4. get_virtual_sites() raises -> WARNING, codes graded exactly as today.
# ---------------------------------------------------------------------------
def test_get_virtual_sites_raises_degrades_to_empty_set_and_logs_warning(caplog):
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure(), _sdk_generic_failure()],
        get_virtual_sites_error=RuntimeError("virtual listing endpoint down"),
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    with caplog.at_level("WARNING", logger="sync_long_horizon_hydrograph"):
        records = shh.write_short_horizon_hydrograph(
            codes=[CODE],
            iehhf_sdk=sdk,
            client=client,
            target_year=TARGET_YEAR,
            today=TODAY,
        )

    assert sdk.virtual_calls == []
    pentad_norms = [r["norm"] for r in records if r["horizon_type"] == "pentad"]
    assert all(norm is None for norm in pentad_norms)
    assert any("get_virtual_sites" in message for message in caplog.messages)
    assert CODE in records.completed_station_codes


def test_get_virtual_sites_raises_regular_station_unaffected():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[PENTAD_NORMS],
        get_virtual_sites_error=RuntimeError("virtual listing endpoint down"),
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = _write_pentad(sdk, client)

    norms = [record["norm"] for record in records]
    assert norms == PENTAD_NORMS
    assert records.status is shh._ShortHorizonWriteStatus.WRITTEN


# ---------------------------------------------------------------------------
# 5. Virtual retry raises -> SDK_FAILED (short-horizon: every exception is
#    SDK_FAILED, no 404-vs-other distinction, unlike long-horizon).
# ---------------------------------------------------------------------------
def test_virtual_retry_raises_classifies_sdk_failed_pentad():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[_sdk_generic_failure()],
        virtual_sites=[{"site_code": CODE}],
    )

    result = shh._lookup_short_horizon_norms(CODE, "pentad", sdk, virtual_codes=frozenset({CODE}))

    assert result.classification is shh._NormClassification.SDK_FAILED


def test_virtual_retry_404_shaped_valueerror_still_classifies_sdk_failed_short_horizon():
    # Long-horizon grades an SDK-shaped 404 ValueError as NORM_ABSENT; the
    # asymmetry is intentional -- short-horizon grades EVERY raised exception
    # as SDK_FAILED, 404 included.
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[
            ValueError(f"Could not retrieve discharge norm for site {CODE}, got status code 404")
        ],
        virtual_sites=[{"site_code": CODE}],
    )

    result = shh._lookup_short_horizon_norms(CODE, "pentad", sdk, virtual_codes=frozenset({CODE}))

    assert result.classification is shh._NormClassification.SDK_FAILED


# ---------------------------------------------------------------------------
# 6. get_virtual_sites() called once per writer invocation, not per station
#    or per horizon.
# ---------------------------------------------------------------------------
def test_get_virtual_sites_called_once_per_writer_invocation_not_per_station_or_horizon():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[PENTAD_NORMS, DECAD_NORMS, PENTAD_NORMS, DECAD_NORMS],
        virtual_sites=[],
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    shh.write_short_horizon_hydrograph(
        codes=[CODE, CODE2],
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    assert sdk.get_virtual_sites_calls == 1
    assert len(sdk.default_calls) == 4


# ---------------------------------------------------------------------------
# 7. Mixed coverage for one virtual code: pentad `[]`, decad valid.
# ---------------------------------------------------------------------------
def test_mixed_coverage_pentad_empty_decad_valid_for_same_virtual_code():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure(), _sdk_generic_failure()],
        virtual_payloads=[[], DECAD_NORMS],
        virtual_sites=[{"site_code": CODE}],
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = shh.write_short_horizon_hydrograph(
        codes=[CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    pentad_norms = [r["norm"] for r in records if r["horizon_type"] == "pentad"]
    decad_norms = [r["norm"] for r in records if r["horizon_type"] == "decade"]
    assert all(norm is None for norm in pentad_norms)
    assert decad_norms == DECAD_NORMS
    assert CODE in records.completed_station_codes


# ---------------------------------------------------------------------------
# 8. Code-type robustness: int work-list code vs str virtual-set code, and
#    the reverse.
# ---------------------------------------------------------------------------
def test_int_worklist_code_matches_str_virtual_site_code():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[PENTAD_NORMS],
        virtual_sites=[{"site_code": CODE}],  # str
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = shh.write_short_horizon_hydrograph(
        codes=[int(CODE)],  # int work-list code
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    pentad_norms = [r["norm"] for r in records if r["horizon_type"] == "pentad"]
    assert len(sdk.virtual_calls) >= 1
    assert pentad_norms == PENTAD_NORMS


def test_str_worklist_code_matches_int_virtual_site_code():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[PENTAD_NORMS],
        virtual_sites=[{"site_code": int(CODE)}],  # int, as the real SDK may return
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = shh.write_short_horizon_hydrograph(
        codes=[CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    pentad_norms = [r["norm"] for r in records if r["horizon_type"] == "pentad"]
    assert len(sdk.virtual_calls) >= 1
    assert pentad_norms == PENTAD_NORMS


# ---------------------------------------------------------------------------
# 9. D-A collision fixtures: code in both registries.
# ---------------------------------------------------------------------------
def test_da_collision_default_succeeds_uses_regular_norm_no_retry():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[PENTAD_NORMS],
        virtual_payloads=[[999.0] * 72],  # would be wrong if ever used
        virtual_sites=[{"site_code": CODE}],
    )

    result = shh._lookup_short_horizon_norms(CODE, "pentad", sdk, virtual_codes=frozenset({CODE}))

    assert result.classification is shh._NormClassification.VALID
    assert result.norms == PENTAD_NORMS
    assert sdk.virtual_calls == []


def test_da_collision_default_raises_uses_virtual_retry():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[PENTAD_NORMS],
        virtual_sites=[{"site_code": CODE}],
    )

    result = shh._lookup_short_horizon_norms(CODE, "pentad", sdk, virtual_codes=frozenset({CODE}))

    assert result.classification is shh._NormClassification.VALID
    assert result.norms == PENTAD_NORMS
    assert len(sdk.virtual_calls) == 1


# ---------------------------------------------------------------------------
# 11. Operational cache-hit path (writer-level): discovery succeeds -> virtual
#     norm fetched; discovery fails -> today's behaviour.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("discovery_raises", [False, True])
def test_writer_discovery_outcome_controls_virtual_lookup(discovery_raises):
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_generic_failure()],
        virtual_payloads=[PENTAD_NORMS] if not discovery_raises else [],
        virtual_sites=[{"site_code": CODE}],
        get_virtual_sites_error=RuntimeError("outage") if discovery_raises else None,
    )
    client = FakeShortHorizonClient(daily_by_year=_daily_fixture())

    records = shh.write_short_horizon_hydrograph(
        codes=[CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=TARGET_YEAR,
        today=TODAY,
    )

    pentad_records = [r for r in records if r["horizon_type"] == "pentad"]
    if discovery_raises:
        assert sdk.virtual_calls == []
        assert all(r["norm"] is None for r in pentad_records)
    else:
        assert len(sdk.virtual_calls) >= 1
        assert [r["norm"] for r in pentad_records] == PENTAD_NORMS
