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

import datetime as dt
import logging
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
