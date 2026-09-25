"""PREPQ-022: virtual-station discharge norms, long-horizon (monthly) writer.

Covers the ``get_virtual_station_codes`` / ``_lookup_monthly_norms_virtual_retry``
addition to ``sync_long_horizon_hydrograph.py`` (iEH HF SDK commit ``1907a30``
adds ``get_norm_for_site(..., virtual=...)`` and ``get_virtual_sites()``).
D-A option (b), decided by the owner: the default (non-virtual) norm call is
always tried first; a virtual-station retry (``virtual=True``) is attempted
ONLY when that default call raises AND the code is a known virtual station.
The retry's own result/exception is graded alone -- the default call's
exception is discarded in that case.

See ``doc/plans/issues/high_prio_gi_draft_prepq_virtual_station_norms.md`` §P2
for the full test list this file (together with
``test_virtual_station_norms_short_horizon.py``) implements.

Fake station codes '19999'/'19998' only; no real station codes or discharge
values.
"""

import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import sync_long_horizon_hydrograph as sync_lhh  # noqa: E402

TEST_CODE = "19999"
OTHER_CODE = "19998"

_EMPTY_SDK_PAGE = {"count": 0, "next": None, "previous": None, "results": []}


def _norms():
    return [float(month) for month in range(1, 13)]


def _sdk_404(code=TEST_CODE):
    return ValueError(f"Could not retrieve discharge norm for site {code}, got status code 404")


def _sdk_500(code=TEST_CODE):
    return ValueError(f"Could not retrieve discharge norm for site {code}, got status code 500")


class VirtualAwareFakeSDK:
    """Fake iEH HF SDK with SEPARATE payload queues for the default
    (``virtual=False``) call and the virtual retry (``virtual=True``), so a
    test can assert exactly which queue was drawn from, how many times, and
    with what arguments.

    ``get_virtual_sites()`` returns ``virtual_sites`` (a list of dicts, each
    needing only ``site_code`` for these tests) or raises
    ``get_virtual_sites_error`` if set, mirroring the real SDK's
    ``ValueError`` on a non-200 listing response.
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
        # No SDK period actuals: force the local daily-aggregation fallback in
        # sync_short_horizon_hydrograph._fetch_sdk_period_actuals, same as the
        # strict FakeSDK in test_sync_long_horizon_hydrograph.py.
        return dict(_EMPTY_SDK_PAGE)


class FakeHydrographClient:
    """Minimal in-memory SAPPHIRE preprocessing client stand-in (copied from
    test_sync_long_horizon_hydrograph.py's FakeHydrographClient so this file
    stays self-contained)."""

    def __init__(self, runoff_by_year=None, existing_hydrograph=None, read_hydrograph_error=None):
        self.runoff_by_year = runoff_by_year or {}
        self.records_by_key = {}
        self.write_calls = []
        self.read_hydrograph_error = read_hydrograph_error
        self.read_hydrograph_calls = []
        for record in existing_hydrograph or []:
            self.records_by_key[self._key(record)] = dict(record)

    @staticmethod
    def _key(record):
        return (record["horizon_type"], record["code"], record["date"])

    def read_runoff(self, horizon, code, start_date, end_date, limit):
        year = int(start_date[:4])
        return self.runoff_by_year.get(year, [])

    def read_hydrograph(self, horizon, code, start_date, end_date, limit):
        self.read_hydrograph_calls.append((horizon, code, start_date, end_date))
        if self.read_hydrograph_error is not None:
            raise self.read_hydrograph_error
        rows = [
            record
            for record in self.records_by_key.values()
            if record["horizon_type"] == horizon
            and record["code"] == str(code)
            and start_date <= record["date"] <= end_date
        ]
        return sync_lhh.pd.DataFrame(rows)

    def write_hydrograph(self, records):
        records = [dict(record) for record in records]
        self.write_calls.append(records)
        for record in records:
            self.records_by_key[self._key(record)] = record

    def written_records(self):
        return list(self.records_by_key.values())


def _records_by_horizon(records, horizon_type):
    return [record for record in records if record["horizon_type"] == horizon_type]


def _record_for_month(records, month):
    return next(record for record in records if record["horizon_value"] == month)


def _status_for(records, code):
    return dict(records.station_statuses)[code]


def _existing_month_norms(code, norms_by_month):
    return [
        {
            "horizon_type": "month",
            "code": code,
            "date": f"2026-{month:02d}-01",
            "day_of_year": sync_lhh.MID_MONTH_DOY[month - 1],
            "horizon_value": month,
            "horizon_in_year": month,
            "norm": norms_by_month.get(month),
            "previous": None,
            "current": None,
        }
        for month in range(1, 13)
    ]


# ---------------------------------------------------------------------------
# 1. Regular code: default call has no `virtual` kwarg; output unchanged.
# ---------------------------------------------------------------------------
def test_regular_code_default_call_has_no_virtual_kwarg_and_output_unchanged():
    sdk = VirtualAwareFakeSDK(default_payloads=[_norms()], virtual_sites=[])
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == _norms()
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN
    assert sdk.virtual_calls == []
    assert len(sdk.default_calls) == 1
    assert sdk.get_virtual_sites_calls == 1


# ---------------------------------------------------------------------------
# 2. Virtual code, valid values -> VALID, norm written (12 values).
# ---------------------------------------------------------------------------
def test_virtual_code_default_raises_retry_succeeds_norm_written():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_norms()],
        virtual_sites=[{"site_code": TEST_CODE}],
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == _norms()
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN
    assert len(sdk.virtual_calls) == 1
    assert sdk.virtual_calls[0] == (TEST_CODE, "discharge", "m")


# ---------------------------------------------------------------------------
# 3. Virtual `[]` -> NORM_ABSENT; preservation (stored norm kept, never-normed
#    stays normless, a failed preservation read writes no nulls).
# ---------------------------------------------------------------------------
def test_virtual_code_empty_list_norm_absent_preserves_existing_stored_norm():
    existing = _existing_month_norms(TEST_CODE, {month: float(month) for month in range(1, 13)})
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": TEST_CODE}],
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []}, existing_hydrograph=existing)

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == [float(month) for month in range(1, 13)]
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT


def test_virtual_code_empty_list_never_normed_station_stays_normless():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": TEST_CODE}],
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == [None] * 12


def test_virtual_code_empty_list_failed_preservation_read_writes_no_nulls():
    # A read_hydrograph failure during the NORM_ABSENT read-merge is not
    # caught inside write_station_monthly_hydrograph; it propagates to
    # write_long_horizon_hydrograph's API_FAILED boundary, so nothing is
    # written for this station at all -- never an all-None batch.
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[[]],
        virtual_sites=[{"site_code": TEST_CODE}],
    )
    client = FakeHydrographClient(
        runoff_by_year={2025: [], 2026: []},
        read_hydrograph_error=sync_lhh.requests.exceptions.ConnectionError("tunnel down"),
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert list(_records_by_horizon(records, "month")) == []
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.API_FAILED
    assert client.write_calls == []


# ---------------------------------------------------------------------------
# 4. get_virtual_sites() raises -> WARNING, codes graded exactly as today.
# ---------------------------------------------------------------------------
def test_get_virtual_sites_raises_degrades_to_empty_set_and_logs_warning(caplog):
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        get_virtual_sites_error=RuntimeError("virtual listing endpoint down"),
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    with caplog.at_level("WARNING", logger="sync_long_horizon_hydrograph"):
        records = sync_lhh.write_long_horizon_hydrograph(
            codes=[TEST_CODE],
            iehhf_sdk=sdk,
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )

    # No virtual retry was ever attempted -- discovery failed, so the code is
    # never treated as virtual, and the default call's own SDK_FAILED
    # exception (a 404-shaped ValueError) is graded exactly as today.
    assert sdk.virtual_calls == []
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT
    assert any("get_virtual_sites" in message for message in caplog.messages)


def test_get_virtual_sites_raises_regular_station_unaffected():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_norms()],
        get_virtual_sites_error=RuntimeError("virtual listing endpoint down"),
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == _norms()
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN


# ---------------------------------------------------------------------------
# 5. Virtual retry raises -> SDK_FAILED; 404-shaped ValueError -> NORM_ABSENT.
# ---------------------------------------------------------------------------
def test_virtual_retry_404_shaped_valueerror_classifies_norm_absent():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_sdk_404()],
        virtual_sites=[{"site_code": TEST_CODE}],
    )

    result = sync_lhh._lookup_monthly_norms(TEST_CODE, sdk, virtual_codes=frozenset({TEST_CODE}))

    assert result.classification is sync_lhh._NormClassification.NORM_ABSENT
    assert isinstance(result.exception, ValueError)


def test_virtual_retry_non_404_status_code_classifies_sdk_failed():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_sdk_500()],
        virtual_sites=[{"site_code": TEST_CODE}],
    )

    result = sync_lhh._lookup_monthly_norms(TEST_CODE, sdk, virtual_codes=frozenset({TEST_CODE}))

    assert result.classification is sync_lhh._NormClassification.SDK_FAILED
    assert isinstance(result.exception, ValueError)


def test_virtual_retry_unparseable_exception_classifies_sdk_failed():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[ConnectionError("tunnel down")],
        virtual_sites=[{"site_code": TEST_CODE}],
    )

    result = sync_lhh._lookup_monthly_norms(TEST_CODE, sdk, virtual_codes=frozenset({TEST_CODE}))

    assert result.classification is sync_lhh._NormClassification.SDK_FAILED
    assert isinstance(result.exception, ConnectionError)


# ---------------------------------------------------------------------------
# 6. get_virtual_sites() called once per writer invocation, not per station.
# ---------------------------------------------------------------------------
def test_get_virtual_sites_called_once_per_writer_invocation_not_per_station():
    codes = [TEST_CODE, OTHER_CODE, "19997"]
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_norms(), _norms(), _norms()],
        virtual_sites=[],
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    sync_lhh.write_long_horizon_hydrograph(
        codes=codes,
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert sdk.get_virtual_sites_calls == 1
    assert len(sdk.default_calls) == 3


# ---------------------------------------------------------------------------
# 8. Code-type robustness: int work-list code vs str virtual-set code, and
#    the reverse; documents (without changing) resolve_sdk_station_codes'
#    compare-before-stringify quirk.
# ---------------------------------------------------------------------------
def test_int_worklist_code_matches_str_virtual_site_code():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_norms()],
        virtual_sites=[{"site_code": TEST_CODE}],  # str
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[int(TEST_CODE)],  # int work-list code
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert len(sdk.virtual_calls) == 1
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN


def test_str_worklist_code_matches_int_virtual_site_code():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_norms()],
        virtual_sites=[{"site_code": int(TEST_CODE)}],  # int, as the real SDK may return
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],  # str work-list code (str(code) is applied by the writer anyway)
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert len(sdk.virtual_calls) == 1
    assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN


def test_resolve_sdk_station_codes_int_code_evades_str_manual_set(monkeypatch):
    """Documents an existing quirk in resolve_sdk_station_codes (line ~968-975,
    not touched by PREPQ-022): the manual-code filter is `code not in
    manual_set` BEFORE `str(code)` is applied, so an int-typed SDK site code
    that happens to also be a manual code (stored in config as a string)
    evades the filter and is NOT excluded. This is pre-existing behaviour --
    documented here, not changed.
    """
    monkeypatch.setattr(
        sync_lhh,
        "get_all_forecast_sites_from_HF_SDK",
        lambda sdk: ([], [int(TEST_CODE), OTHER_CODE], []),
    )
    monkeypatch.setattr(sync_lhh, "_get_manual_site_codes", lambda: [TEST_CODE, OTHER_CODE])

    codes = sync_lhh.resolve_sdk_station_codes(sdk=object())

    # OTHER_CODE is a str in both the site list and the manual set -> excluded.
    assert OTHER_CODE not in codes
    # TEST_CODE is an int in the site list but a str in the manual set -> the
    # `in` check never matches, so it survives filtering despite being
    # "manual". Quirk preserved as-is.
    assert TEST_CODE in codes


# ---------------------------------------------------------------------------
# 9. D-A collision fixtures: code in both registries.
# ---------------------------------------------------------------------------
def test_da_collision_default_succeeds_uses_regular_norm_no_retry():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_norms()],
        virtual_payloads=[[999.0] * 12],  # would be wrong if ever used
        virtual_sites=[{"site_code": TEST_CODE}],
    )

    result = sync_lhh._lookup_monthly_norms(TEST_CODE, sdk, virtual_codes=frozenset({TEST_CODE}))

    assert result.classification is sync_lhh._NormClassification.VALID
    assert result.norms == _norms()
    assert sdk.virtual_calls == []


def test_da_collision_default_raises_uses_virtual_retry():
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_norms()],
        virtual_sites=[{"site_code": TEST_CODE}],
    )

    result = sync_lhh._lookup_monthly_norms(TEST_CODE, sdk, virtual_codes=frozenset({TEST_CODE}))

    assert result.classification is sync_lhh._NormClassification.VALID
    assert result.norms == _norms()
    assert len(sdk.virtual_calls) == 1


# ---------------------------------------------------------------------------
# 11. Operational cache-hit path (writer-level): discovery succeeds -> virtual
#     norm fetched; discovery fails -> today's behaviour. Both already
#     exercised above at the write_long_horizon_hydrograph writer level
#     (test 2 and test_get_virtual_sites_raises_* above); this test pins the
#     contrast side by side for one station.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("discovery_raises", [False, True])
def test_writer_discovery_outcome_controls_virtual_lookup(discovery_raises):
    sdk = VirtualAwareFakeSDK(
        default_payloads=[_sdk_404()],
        virtual_payloads=[_norms()] if not discovery_raises else [],
        virtual_sites=[{"site_code": TEST_CODE}],
        get_virtual_sites_error=RuntimeError("outage") if discovery_raises else None,
    )
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    if discovery_raises:
        assert sdk.virtual_calls == []
        assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT
    else:
        assert len(sdk.virtual_calls) == 1
        assert _status_for(records, TEST_CODE) is sync_lhh.LongHorizonStationWriteStatus.WRITTEN
