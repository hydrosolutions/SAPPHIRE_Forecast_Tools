"""PREPQ-022: virtual-station discharge norms through the backfill orchestrator.

Covers plan item 10 (§P2): a captured-record backfill test showing virtual
norms written for all horizons, and that ``backfill(dry_run=True)`` remains
unchanged (never touches the real client's store) once the writers can also
resolve virtual-station norms.

``backfill_discharge_aggregation.py`` calls
``sync_short_horizon_hydrograph.write_short_horizon_hydrograph`` and
``sync_long_horizon_hydrograph.write_long_horizon_hydrograph`` UNMODIFIED and
positionally (no ``virtual_codes`` argument) -- both writers resolve their own
virtual-station set internally via ``get_virtual_station_codes``, once per
writer per year, exactly like the operational entry points.

See ``doc/plans/issues/high_prio_gi_draft_prepq_virtual_station_norms.md`` §P2.

Fake station code '19999' only; no real station codes or discharge values.
"""

import calendar
import datetime as dt
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import backfill_discharge_aggregation as bda  # noqa: E402

CODE = "19999"


def _full_year_daily_rows(year, value=8.0):
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            rows.append({"date": dt.date(year, month, day).isoformat(), "discharge": value})
    return rows


class FakeClient:
    """In-memory stand-in for the SAPPHIRE preprocessing client's hydrograph
    contract (copied from test_backfill_discharge_aggregation_m4.py's
    FakeClient, but with a non-empty ``read_runoff`` -- a full synthetic
    daily-discharge year for every year requested -- so the short-horizon
    pentad/decade writer has usable climatology data instead of raising
    ``_ShortHorizonDailyReadError`` for lack of any daily rows at all).
    """

    def __init__(self):
        self.store: dict[tuple[str, str, str], dict] = {}

    def write_hydrograph(self, records):
        for record in records:
            key = (str(record["horizon_type"]), str(record["code"]), str(record["date"])[:10])
            self.store[key] = dict(record)
        return len(records)

    def read_hydrograph(self, horizon, code, start_date=None, end_date=None, skip=None, limit=None):
        rows = []
        for (horizon_type, stored_code, date_str), record in self.store.items():
            if horizon_type != horizon or stored_code != str(code):
                continue
            if start_date is not None and date_str < str(start_date)[:10]:
                continue
            if end_date is not None and date_str > str(end_date)[:10]:
                continue
            rows.append(dict(record))
        if not rows:
            return pd.DataFrame(columns=["horizon_type", "code", "date"])
        return pd.DataFrame(rows)

    def read_runoff(self, horizon, code, start_date, end_date, limit=None):
        year = int(str(start_date)[:4])
        return _full_year_daily_rows(year)

    def readiness_check(self):
        return True


class _StubSDKWithVirtual:
    """Minimal iEH-HF SDK stand-in (like backfill's own ``_StubSDK`` in
    test_backfill_discharge_aggregation_m4.py, not modified here) whose
    default (``virtual=False``) call RAISES for ``virtual_code`` -- only the
    ``virtual=True`` retry succeeds, returning a norm value (2.0)
    distinguishable from the regular-station stub value (1.0) so assertions
    can tell which path produced a given record.
    """

    _NORM_LENGTHS = {"p": 72, "d": 36, "m": 12}

    def __init__(self, virtual_code):
        self._virtual_code = str(virtual_code)
        self.get_virtual_sites_calls = 0

    def get_virtual_sites(self):
        self.get_virtual_sites_calls += 1
        return [{"site_code": self._virtual_code}]

    def get_norm_for_site(self, code, variable, norm_period=None, virtual=False):
        length = self._NORM_LENGTHS[norm_period]
        if str(code) == self._virtual_code:
            if not virtual:
                raise ValueError(
                    f"Could not retrieve discharge norm for site {code}, got status code 404"
                )
            return [2.0] * length
        return [1.0] * length

    def get_data_values_for_site(self, filters=None):
        return []


def test_compute_backfill_records_virtual_station_norms_present_in_all_horizons():
    real = FakeClient()
    sdk = _StubSDKWithVirtual(virtual_code=CODE)

    records = bda.compute_backfill_records(
        codes=[CODE],
        iehhf_sdk=sdk,
        real_client=real,
        target_year=2025,
        today=dt.date(2026, 7, 3),
    )

    horizon_types = {record["horizon_type"] for record in records}
    assert horizon_types <= {"pentad", "decade", "month", "quarter", "season"}
    assert len(records) > 0

    pentad_norms = {r["norm"] for r in records if r["horizon_type"] == "pentad"}
    decade_norms = {r["norm"] for r in records if r["horizon_type"] == "decade"}
    month_norms = {r["norm"] for r in records if r["horizon_type"] == "month"}
    assert pentad_norms == {2.0}
    assert decade_norms == {2.0}
    assert month_norms == {2.0}
    # quarter/season derive their norm as a mean of the monthly norms, so the
    # constant 2.0 monthly norm rolls up unchanged (no other value possible).
    assert {r["norm"] for r in records if r["horizon_type"] == "quarter"} <= {2.0}
    assert {r["norm"] for r in records if r["horizon_type"] == "season"} <= {2.0}

    # Called once per writer (short-horizon, long-horizon) per year -- not
    # once per station, and not zero (i.e. not skipped).
    assert sdk.get_virtual_sites_calls == 2

    # The real client's store was never touched: only the capturing client
    # (internal to compute_backfill_records) saw writes.
    assert real.store == {}


def test_backfill_dry_run_with_virtual_station_norms_never_writes(tmp_path):
    real = FakeClient()
    sdk = _StubSDKWithVirtual(virtual_code=CODE)

    summary = bda.backfill(
        codes=[CODE],
        iehhf_sdk=sdk,
        real_client=real,
        target_years=[2025],
        today=dt.date(2026, 7, 3),
        dry_run=True,
        snapshot_dir=str(tmp_path),
    )

    # dry-run: real_client.write_hydrograph is never called, virtual-station
    # handling or not -- same invariant as the non-virtual dry-run test in
    # test_backfill_discharge_aggregation_m4.py.
    assert real.store == {}
    assert summary["dry_run"] is True
    year_entry = summary["years"][0]
    assert year_entry["year"] == 2025
    assert year_entry["dry_run"] is True
    assert year_entry["verified"] is None
    assert year_entry["record_count"] > 0
    # Every record is "added" against an empty store, virtual-station rows
    # included.
    assert year_entry["diff"]["added"] == year_entry["record_count"]
    assert year_entry["diff"]["changed"] == 0
    assert year_entry["diff"]["unchanged"] == 0
