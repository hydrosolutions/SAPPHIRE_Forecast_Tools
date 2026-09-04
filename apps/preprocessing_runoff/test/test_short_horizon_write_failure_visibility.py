"""Tests for the (Fix 3) operational-visibility behaviour of the pentad/decad
short-horizon hydrograph write step in ``preprocessing_runoff.py``.

The short-horizon (pentad/decad) hydrograph write is a display/skill-metric
artifact, not a forecast-run blocker: a failure in
``sync_short_horizon_hydrograph.write_short_horizon_hydrograph`` must NEVER
abort the operational run. But a silently-swallowed ``logger.warning`` made
real failures easy to miss operationally. This must now be:

  (a) logged at ERROR level with a clear, greppable message, while still
      NOT propagating out of the write step (non-fatal), and
  (b) in maintenance mode, folded into the existing post-write validation
      log output so it surfaces alongside that summary.

Driving ``main()`` end-to-end is impractical (it needs a live SDK, site
lists, CSV I/O, etc.), so these tests exercise the narrowest units that carry
this behaviour: ``_write_short_horizon_hydrograph_records`` (a) and
``_maintenance_post_write_note`` (b).

Fake station code '19999'; no real discharge values.
"""

import calendar
import datetime as dt
import logging
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Sibling test modules use the convention `sys.path.insert(0, ".../src"); import
# src`, which binds the top-level name "src" to the flat src/src.py module. If
# one of those has already run during this pytest session, `sys.modules["src"]`
# is cached as that flat module, so `preprocessing_runoff`'s own
# `from src import src` (expecting the src/ PACKAGE) would silently reuse the
# wrong cached object and raise ImportError. Import with that cache
# temporarily cleared, then restore it so sibling test modules collected
# later in the same session are unaffected.
_saved_src_modules = {
    _name: _mod for _name, _mod in sys.modules.items() if _name == "src" or _name.startswith("src.")
}
for _name in list(_saved_src_modules):
    del sys.modules[_name]
try:
    import preprocessing_runoff as pr  # noqa: E402
    import sync_short_horizon_hydrograph as shh  # noqa: E402
finally:
    for _name in list(sys.modules):
        if _name == "src" or _name.startswith("src."):
            del sys.modules[_name]
    sys.modules.update(_saved_src_modules)

CODE = "19999"


# --------------------------------------------------------------------------
# Test 6 (C4 regression guard) — a fully norm-absent run must not newly raise
# or propagate a non-zero status through `_write_short_horizon_hydrograph_records`.
#
# `_write_short_horizon_hydrograph_records` documents that this write "must
# NEVER abort the operational run" (`preprocessing_runoff.py`). PREPQ-020's P1
# decoupled row existence from the norm lookup so NORM_ABSENT no longer drops
# rows or raises — but that guarantee lives in
# `write_short_horizon_hydrograph`, one hop away from this function. This test
# calls the REAL `write_short_horizon_hydrograph` (only `_get_preprocessing_client`
# is faked out) so a future change that reintroduces an early-return/raise on
# NORM_ABSENT is caught here, at the boundary C4 protects, not only in the
# norm-decoupling test module.
# --------------------------------------------------------------------------
class _FakeSDKAllNormsAbsent:
    """Every `get_norm_for_site` call returns the wrong length -> NORM_ABSENT
    for every station and every horizon.
    """

    def get_norm_for_site(self, code, value_field, norm_period):
        return []

    def get_data_values_for_site(self, filters=None, **kwargs):
        return {"count": 0, "next": None, "previous": None, "results": []}


def _varied_daily_rows(year: int, offset: float) -> list[dict]:
    """One full calendar year of daily discharge with a seasonal (within-year)
    and an ``offset`` (across-year) component, so a climatology built from
    several of these years has a genuinely non-degenerate envelope - mean,
    min, max, and quantiles that differ from each other - rather than the
    single repeated constant a flatter fixture would produce.
    """
    rows = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            doy = dt.date(year, month, day).timetuple().tm_yday
            value = 10.0 + 4.0 * math.sin(2 * math.pi * doy / 365.0) + offset
            rows.append(
                {"date": dt.date(year, month, day).isoformat(), "discharge": round(value, 2)}
            )
    return rows


class _FakeClientNormAbsentWithDailyData:
    """Real, varied daily runoff across three prior years (so the built
    climatology envelope is non-degenerate) but no stored hydrograph rows
    for any station - the norm-absent path with nothing to preserve via the
    read-merge.

    Renamed from ``_FakeClientNoDailyData``: that name described what made
    this test pass for the wrong reason. This test is about the
    NORM-ABSENT path (see the SDK fake and the test name), not about total
    daily-data absence - that is a separate, now-guarded scenario
    (``_ShortHorizonDailyReadError``, PREPQ-020) whose whole point is that it
    must NOT be allowed to flow into a written batch, because a batch with
    every envelope field None would silently clobber previously stored
    values via the API's field-by-field upsert. Returning ``[]`` from
    ``read_runoff`` for every year (the old behaviour of this fixture)
    constructed exactly that scenario by accident, so the 72/36-row counts
    below were passing because the rows were all-null, not because the
    norm-absent path was doing anything meaningful.

    Records every ``write_hydrograph`` call so the guard test below can pin
    that rows are actually written (C1) with real envelope values, not
    merely that nothing raises.
    """

    def __init__(self):
        self.write_calls: list[list[dict]] = []
        self._daily_by_year = {
            2021: _varied_daily_rows(2021, 0.0),
            2022: _varied_daily_rows(2022, 1.5),
            2023: _varied_daily_rows(2023, -1.0),
        }

    def read_runoff(self, horizon, code, start_date, end_date, limit):
        year = int(start_date[:4])
        return list(self._daily_by_year.get(year, []))

    def read_hydrograph(self, horizon, code, start_date, end_date, limit):
        return []

    def write_hydrograph(self, records):
        records = [dict(record) for record in records]
        self.write_calls.append(records)
        return len(records)


def test_write_step_returns_none_and_does_not_raise_when_every_station_is_norm_absent(
    monkeypatch, caplog
):
    client = _FakeClientNormAbsentWithDailyData()
    monkeypatch.setattr(shh, "_get_preprocessing_client", lambda: client)

    with caplog.at_level(logging.ERROR):
        result = pr._write_short_horizon_hydrograph_records(
            ieh_hf_sdk=_FakeSDKAllNormsAbsent(),
            site_codes=[CODE],
            target_year=2024,
            today=dt.date(2024, 1, 10),
        )

    assert result is None
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]

    # The C4 guarantee this test exists to pin: rows are actually WRITTEN for
    # the norm-absent station, not merely absent-of-error. The pre-fix
    # `return []` on norm-absence satisfied both assertions above without
    # writing anything, so this is the assertion that makes the test real -
    # it fails if the norm-absent early return is reintroduced.
    pentad_calls = [c for c in client.write_calls if c and c[0]["horizon_type"] == "pentad"]
    decade_calls = [c for c in client.write_calls if c and c[0]["horizon_type"] == "decade"]
    assert len(pentad_calls) == 1
    assert len(pentad_calls[0]) == 72
    assert all(record["code"] == CODE for record in pentad_calls[0])
    assert len(decade_calls) == 1
    assert len(decade_calls[0]) == 36
    assert all(record["code"] == CODE for record in decade_calls[0])

    # The assertion that would have caught the fixture bug directly: with a
    # real daily-runoff climatology behind it, the written batch must
    # actually carry envelope values, not the all-None batch produced when
    # `_read_daily_by_year` finds no usable daily data anywhere for this
    # station-horizon (exactly the scenario `_ShortHorizonDailyReadError`
    # exists to stop from being written at all). `norm` must stay `None`:
    # that IS the norm-absent condition this test is for - there is no
    # stored norm for the read-merge to preserve.
    envelope_fields = ("mean", "min", "max", "q05", "q25", "q75", "q95")
    for horizon_records, expected_len in ((pentad_calls[0], 72), (decade_calls[0], 36)):
        assert len(horizon_records) == expected_len
        assert all(record["norm"] is None for record in horizon_records)
        for field in envelope_fields:
            assert any(record[field] is not None for record in horizon_records), (
                f"{field} is None across the entire batch - the daily-runoff fixture is not "
                "producing a usable climatology, so this would pass on an all-null batch again"
            )


# --------------------------------------------------------------------------
# Part (a) — write failure is caught, logged at ERROR, and does not propagate
# --------------------------------------------------------------------------
def test_write_step_logs_error_and_does_not_propagate_on_writer_failure(monkeypatch, caplog):
    def _raise_boom(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(shh, "_get_preprocessing_client", lambda: object())
    monkeypatch.setattr(shh, "write_short_horizon_hydrograph", _raise_boom)

    with caplog.at_level(logging.ERROR):
        # Must NOT raise - the caller (the operational run) continues.
        result = pr._write_short_horizon_hydrograph_records(
            ieh_hf_sdk=object(),
            site_codes=[CODE],
            target_year=2024,
            today=dt.date(2024, 1, 10),
        )

    assert result is not None  # failure is reported back, not swallowed silently
    assert "boom" in result
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("boom" in r.message for r in error_records), (
        "writer failure must be logged at ERROR level with the underlying error visible"
    )
    # Greppable marker so operators/log-scrapers can find this failure class.
    assert any("SHORT_HORIZON_HYDROGRAPH_WRITE_FAILED" in r.message for r in error_records)


def test_write_step_returns_none_and_logs_no_error_on_success(monkeypatch, caplog):
    monkeypatch.setattr(shh, "_get_preprocessing_client", lambda: object())
    monkeypatch.setattr(shh, "write_short_horizon_hydrograph", lambda **kwargs: {"ok": True})

    with caplog.at_level(logging.ERROR):
        result = pr._write_short_horizon_hydrograph_records(
            ieh_hf_sdk=object(),
            site_codes=[CODE],
            target_year=2024,
            today=dt.date(2024, 1, 10),
        )

    assert result is None
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


# --------------------------------------------------------------------------
# Part (b) — maintenance-mode fold into the post-write validation log output
# --------------------------------------------------------------------------
def test_maintenance_note_logs_error_when_write_failed(caplog):
    with caplog.at_level(logging.ERROR):
        pr._maintenance_post_write_note("SHORT_HORIZON_HYDROGRAPH_WRITE_FAILED: boom")

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert "boom" in error_records[0].message


def test_maintenance_note_is_a_no_op_when_no_failure(caplog):
    with caplog.at_level(logging.ERROR):
        pr._maintenance_post_write_note(None)

    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


@pytest.mark.parametrize("error_message", ["x failed", "y failed: RuntimeError: boom"])
def test_maintenance_note_message_is_greppable_and_preserves_original_error(caplog, error_message):
    with caplog.at_level(logging.ERROR):
        pr._maintenance_post_write_note(error_message)

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert error_message in error_records[0].message
    assert "[DATA]" in error_records[0].message  # matches the module's validation log prefix
