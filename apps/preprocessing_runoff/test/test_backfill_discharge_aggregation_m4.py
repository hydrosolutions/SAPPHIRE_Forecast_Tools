"""Milestone M4 safety-rail tests for ``backfill_discharge_aggregation``.

Covers the write-capturing client, the diff/snapshot/verify safety rails, dry-run vs.
live behaviour of ``backfill()``, the fail-loudly guarantee on verification mismatch, and
one end-to-end pass through the real M2/M3 writers via ``compute_backfill_records``.

Fake station code '19999' only; no real station codes or discharge values.
"""

import datetime as dt
import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import backfill_discharge_aggregation as bda

CODE = "19999"


class FakeClient:
    """In-memory stand-in for the SAPPHIRE preprocessing client's hydrograph contract."""

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

    def read_runoff(self, *args, **kwargs):
        return []

    def readiness_check(self):
        return True


class _LossyClient(FakeClient):
    """A client whose ``write_hydrograph`` silently drops the first record it receives."""

    def write_hydrograph(self, records):
        return super().write_hydrograph(records[1:])


class _StubSDK:
    """Minimal iEH-HF SDK stand-in satisfying what the M2/M3 writers call."""

    _NORM_LENGTHS = {"p": 72, "d": 36, "m": 12}

    def get_norm_for_site(self, code, variable, norm_period=None):
        return [1.0] * self._NORM_LENGTHS[norm_period]

    def get_data_values_for_site(self, filters=None):
        return []


def _synthetic_records(year):
    return [
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": f"{year}-01-05",
            "current": 10.0,
            "norm": 1.0,
        },
        {
            "horizon_type": "decade",
            "code": CODE,
            "date": f"{year}-01-10",
            "current": 20.0,
            "norm": 2.0,
        },
    ]


# ---------------------------------------------------------------------------
# 1. _CapturingClient
# ---------------------------------------------------------------------------


def test_capturing_client_intercepts_writes_without_touching_real_client():
    real = FakeClient()
    capturing = bda._CapturingClient(real)
    records = [
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 1.0},
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-10", "current": 2.0},
    ]

    result = capturing.write_hydrograph(records)

    assert result == 2
    assert real.store == {}
    assert capturing.captured == records
    assert capturing.captured[0] is not records[0]


def test_capturing_client_reads_pass_through_to_real_client():
    real = FakeClient()
    real.write_hydrograph(
        [{"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 1.0}]
    )
    capturing = bda._CapturingClient(real)

    df = capturing.read_hydrograph(
        horizon="pentad", code=CODE, start_date="2025-01-01", end_date="2025-01-31"
    )

    assert len(df) == 1
    assert capturing.readiness_check() is True
    assert capturing.read_runoff(horizon="day", code=CODE) == []


# ---------------------------------------------------------------------------
# 2. diff_records
# ---------------------------------------------------------------------------


def test_diff_records_classifies_added_unchanged_changed_and_treats_none_nan_as_equal():
    existing = [
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-05",
            "current": 10.0,
            "previous": None,
        },
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-10",
            "current": 20.0,
            "previous": 5.0,
        },
    ]
    new = [
        # unchanged: current identical, previous None vs. NaN must count as equal
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-05",
            "current": 10.0,
            "previous": float("nan"),
        },
        # changed: current differs
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-10",
            "current": 25.0,
            "previous": 5.0,
        },
        # added: brand-new key
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-15",
            "current": 30.0,
            "previous": None,
        },
    ]

    diff = bda.diff_records(existing, new)

    assert diff["added"] == 1
    assert diff["unchanged"] == 1
    assert diff["changed"] == 1
    assert diff["examples_omitted"] == 0

    kinds = {example["kind"] for example in diff["examples"]}
    assert kinds == {"added", "changed"}
    changed_example = next(e for e in diff["examples"] if e["kind"] == "changed")
    assert changed_example["fields"]["current"] == {"old": 20.0, "new": 25.0}
    assert "previous" not in changed_example["fields"]


# ---------------------------------------------------------------------------
# 3. snapshot_existing
# ---------------------------------------------------------------------------


def test_snapshot_existing_returns_only_overlapping_stored_rows():
    real = FakeClient()
    real.write_hydrograph(
        [
            {"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 1.0},
            {"horizon_type": "pentad", "code": CODE, "date": "2025-06-05", "current": 2.0},
        ]
    )
    new_records = [
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 99.0},
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-10", "current": 99.0},
    ]

    existing = bda.snapshot_existing(real, new_records)

    dates = {row["date"][:10] for row in existing}
    assert dates == {"2025-01-05"}


def test_snapshot_existing_returns_empty_when_nothing_stored():
    real = FakeClient()
    new_records = [{"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 1.0}]

    assert bda.snapshot_existing(real, new_records) == []


# ---------------------------------------------------------------------------
# 4. verify_written
# ---------------------------------------------------------------------------


def test_verify_written_returns_empty_when_store_matches():
    real = FakeClient()
    written = [
        {"horizon_type": "decade", "code": CODE, "date": "2025-02-10", "current": 3.0, "norm": 1.0}
    ]
    real.write_hydrograph(written)

    assert bda.verify_written(real, written) == []


def test_verify_written_reports_missing_key():
    real = FakeClient()
    written = [{"horizon_type": "decade", "code": CODE, "date": "2025-02-10", "current": 3.0}]

    discrepancies = bda.verify_written(real, written)

    assert len(discrepancies) == 1
    assert "missing after write" in discrepancies[0]


def test_verify_written_reports_field_mismatch():
    real = FakeClient()
    written = [{"horizon_type": "decade", "code": CODE, "date": "2025-02-10", "current": 3.0}]
    real.write_hydrograph(
        [{"horizon_type": "decade", "code": CODE, "date": "2025-02-10", "current": 999.0}]
    )

    discrepancies = bda.verify_written(real, written)

    assert len(discrepancies) == 1
    assert "field=current" in discrepancies[0]


# ---------------------------------------------------------------------------
# 5. _resolve_target_years
# ---------------------------------------------------------------------------


def test_resolve_target_years_default_window():
    assert bda._resolve_target_years(dt.date(2026, 7, 3)) == [2023, 2024, 2025]


def test_resolve_target_years_explicit_target_year_ignores_years():
    assert bda._resolve_target_years(dt.date(2026, 7, 3), years=3, target_year=2020) == [2020]


def test_resolve_target_years_custom_years():
    assert bda._resolve_target_years(dt.date(2026, 7, 3), years=1) == [2025]


# ---------------------------------------------------------------------------
# 6. backfill(dry_run=True)
# ---------------------------------------------------------------------------


def test_backfill_dry_run_never_writes_and_reports_diff(tmp_path, monkeypatch):
    real = FakeClient()
    pre_existing = {
        "horizon_type": "pentad",
        "code": CODE,
        "date": "2025-01-05",
        "current": 1.0,
        "norm": 1.0,
    }
    real.write_hydrograph([pre_existing])
    store_before = dict(real.store)

    monkeypatch.setattr(bda, "compute_backfill_records", lambda *a, **k: _synthetic_records(2025))

    summary = bda.backfill(
        codes=[CODE],
        iehhf_sdk=SimpleNamespace(),
        real_client=real,
        target_years=[2025],
        today=dt.date(2026, 7, 3),
        dry_run=True,
        snapshot_dir=str(tmp_path),
    )

    # Store is completely unchanged by a dry run.
    assert real.store == store_before

    assert summary["dry_run"] is True
    assert len(summary["years"]) == 1
    year_entry = summary["years"][0]
    assert year_entry["year"] == 2025
    assert year_entry["dry_run"] is True
    assert year_entry["verified"] is None
    # pentad 2025-01-05 pre-exists with a different `current` -> "changed";
    # decade 2025-01-10 is brand new -> "added".
    assert year_entry["diff"]["changed"] == 1
    assert year_entry["diff"]["added"] == 1
    assert year_entry["diff"]["unchanged"] == 0

    diff_files = list(tmp_path.glob("backfill_dryrun_diff_2025_*.json"))
    assert len(diff_files) == 1
    with open(diff_files[0]) as handle:
        payload = json.load(handle)
    assert payload["year"] == 2025
    assert payload["diff"]["added"] == 1
    assert payload["diff"]["changed"] == 1


# ---------------------------------------------------------------------------
# 7. backfill(dry_run=False)
# ---------------------------------------------------------------------------


def test_backfill_live_writes_snapshots_pre_write_state_and_verifies(tmp_path, monkeypatch):
    real = FakeClient()
    old_record = {
        "horizon_type": "pentad",
        "code": CODE,
        "date": "2025-01-05",
        "current": 111.0,
        "norm": 1.0,
    }
    real.write_hydrograph([old_record])

    new_records = [
        {
            "horizon_type": "pentad",
            "code": CODE,
            "date": "2025-01-05",
            "current": 222.0,
            "norm": 1.0,
        },
        {
            "horizon_type": "decade",
            "code": CODE,
            "date": "2025-01-10",
            "current": 20.0,
            "norm": 2.0,
        },
    ]
    monkeypatch.setattr(bda, "compute_backfill_records", lambda *a, **k: new_records)

    summary = bda.backfill(
        codes=[CODE],
        iehhf_sdk=SimpleNamespace(),
        real_client=real,
        target_years=[2025],
        today=dt.date(2026, 7, 3),
        dry_run=False,
        snapshot_dir=str(tmp_path),
    )

    # (a) the store now holds the newly written values.
    pentad_key = ("pentad", CODE, "2025-01-05")
    decade_key = ("decade", CODE, "2025-01-10")
    assert real.store[pentad_key]["current"] == 222.0
    assert real.store[decade_key]["current"] == 20.0

    # (b) the snapshot file captures the OLD (pre-write) state, not the new one.
    snapshot_files = list(tmp_path.glob("backfill_snapshot_2025_*.json"))
    assert len(snapshot_files) == 1
    with open(snapshot_files[0]) as handle:
        payload = json.load(handle)
    snapshotted_currents = {row["current"] for row in payload["existing"]}
    assert snapshotted_currents == {111.0}

    # (c) backfill() returned normally: verification succeeded.
    year_entry = summary["years"][0]
    assert year_entry["verified"] is True
    assert year_entry["dry_run"] is False


# ---------------------------------------------------------------------------
# 8. Fail-loudly on a lossy write
# ---------------------------------------------------------------------------


def test_backfill_live_raises_when_write_silently_drops_a_record(tmp_path, monkeypatch):
    lossy = _LossyClient()
    new_records = [
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-05", "current": 10.0},
        {"horizon_type": "pentad", "code": CODE, "date": "2025-01-10", "current": 20.0},
    ]
    monkeypatch.setattr(bda, "compute_backfill_records", lambda *a, **k: new_records)

    with pytest.raises(RuntimeError, match="verification failed"):
        bda.backfill(
            codes=[CODE],
            iehhf_sdk=SimpleNamespace(),
            real_client=lossy,
            target_years=[2025],
            today=dt.date(2026, 7, 3),
            dry_run=False,
            snapshot_dir=str(tmp_path),
        )


# ---------------------------------------------------------------------------
# 9. End-to-end through the real M2/M3 writers
# ---------------------------------------------------------------------------


def test_compute_backfill_records_end_to_end_through_real_writers():
    real = FakeClient()
    sdk = _StubSDK()

    records = bda.compute_backfill_records(
        codes=[CODE],
        iehhf_sdk=sdk,
        real_client=real,
        target_year=2025,
        today=dt.date(2026, 7, 3),
    )

    horizon_types = {record["horizon_type"] for record in records}
    assert horizon_types  # something non-trivial was captured
    assert horizon_types <= {"pentad", "decade", "month", "quarter", "season"}
    assert len(records) > 0
    # The real client's store was never touched: only the capturing client saw writes.
    assert real.store == {}
    assert all(record["code"] == CODE for record in records)
