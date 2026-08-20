"""Tests for the mode/model ValueError messages (INFRA-037 / ML-016).

Several `raise ValueError(...)` call sites in the ML pipeline used a literal
"%s" placeholder with no %-formatting, so the raised message printed the
literal two characters "%s" instead of the offending env-var value. This
made an unset env var (None) and an empty string ("") indistinguishable —
and unhelpful — in logs and tracebacks.

Fixed sites (now interpolated with an f-string using `{value!r}` so None
and "" render as visibly different values):
- recalculate_nan_forecasts.py: prediction-mode guard (PREDICTION_MODE)
- make_forecast.py: model guard (MODEL_TO_USE) and prediction-mode guard
  (PREDICTION_MODE)
- fill_ml_gaps.py: prediction-mode guard (PREDICTION_MODE)
- hindcast_ML_models.py: hindcast-mode guard (HINDCAST_MODE, read from
  env var SAPPHIRE_HINDCAST_MODE)

This file exercises the guards through their REAL code paths:
- recalculate_nan_forecasts.recalculate_nan_forecasts() — the mode guard
  runs before sl.load_environment(), so calling the real function is cheap.
- make_forecast.get_predictor_class() — the smallest real entry point that
  reaches the model-guard raise, without needing to run the rest of
  make_ml_forecast() (which requires sl.load_environment() plus predictor
  setup that isn't relevant to this guard).
- make_forecast.make_ml_forecast() — its own, separate PREDICTION_MODE
  guard (further down the same function, after get_predictor_class()).
  Reachable cheaply too: setup_library is mocked as a bare MagicMock, so
  sl.load_environment() is a no-op, and get_predictor_class() just needs a
  valid SAPPHIRE_MODEL_TO_USE to pass through to the PREDICTION_MODE check.
- fill_ml_gaps.fill_ml_gaps() — same structure as recalculate_nan_forecasts:
  the mode guard runs before sl.load_environment(), so it is included here
  too even though the task only required the first two.

hindcast_ML_models.py's HINDCAST_MODE guard is NOT exercised via a real
call here. The existing test_hindcast.py in this directory documents that
hindcast_ML_models.py is deliberately tested via source-text assertions
"without executing main() or importing heavy ML dependencies (torch,
darts, pytorch_lightning)" — importing it pulls in darts.models
(TFTModel/TiDEModel/TSMixerModel), likelihood models, and several
torch/torchmetrics submodules beyond what make_forecast.py needs. Adding a
full-import test here would diverge from that established local
convention for a guard the task did not require covering. The fix at
hindcast_ML_models.py:158 is covered by direct inspection instead (see
test_hindcast_mode_error_uses_fstring_interpolation below), which is cheap
(no import) and still regresses the exact defect: a literal "%s" with no
formatting.
"""

import os
import sys
import tempfile
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Mock heavy dependencies before importing the modules under test (same
# pattern as test_recalculate_nan_api_write.py / test_write_forecast.py).
# ---------------------------------------------------------------------------
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.utils.likelihood_models"] = MagicMock()
sys.modules["darts.utils.likelihood_models.base"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["torch.optim"] = MagicMock()
sys.modules["torch.optim.lr_scheduler"] = MagicMock()
sys.modules["torch.nn"] = MagicMock()
sys.modules["torch.nn.modules"] = MagicMock()
sys.modules["torch.nn.modules.loss"] = MagicMock()
sys.modules["torch.serialization"] = MagicMock()
sys.modules["torchmetrics"] = MagicMock()
sys.modules["torchmetrics.collections"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()
sys.modules["setup_library"] = MagicMock()

# Use absolute paths: the import below temporarily changes the process cwd
# (see comment there), and relative sys.path entries are re-resolved
# against the *current* cwd at import time, not at insert time.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scr")))
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast")),
)

# fill_ml_gaps.py, make_forecast.py and recalculate_nan_forecasts.py all set up
# module-level logging at IMPORT time using a path relative to the process cwd
# ("logs/log", via TimedRotatingFileHandler). The mandated test runner
# (run_tests.sh) invokes pytest from apps/, so an unguarded import here would
# append to apps/logs/log — the LIVE operator log directory used by
# run_locally.sh. Redirect the cwd to a throwaway temp directory for the
# duration of the import only, so "logs/log" is created (and immediately
# discarded) there instead of in the repo working tree.
_prior_cwd = os.getcwd()
with tempfile.TemporaryDirectory(prefix="sapphire_ml_test_mode_error_messages_") as _tmp_import_dir:
    os.chdir(_tmp_import_dir)
    try:
        import fill_ml_gaps  # noqa: E402
        import make_forecast  # noqa: E402
        import recalculate_nan_forecasts  # noqa: E402
    finally:
        # Restore the cwd before the TemporaryDirectory context manager
        # tries to remove _tmp_import_dir (can't rmdir the cwd on some
        # platforms) and before any other test code runs.
        os.chdir(_prior_cwd)

# ---------------------------------------------------------------------------
# Regression guard for the cwd-redirect trick above (INFRA-037 defect 3a).
# Without this test, the whole `_prior_cwd = os.getcwd(); with
# tempfile.TemporaryDirectory() ...` block above is unenforced: deleting it
# does not fail any test in this file.
# ---------------------------------------------------------------------------

_REAL_APPS_LOGS_LOG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "logs", "log")
)


def test_cwd_redirect_keeps_module_logging_out_of_real_apps_logs():
    """make_forecast.py and recalculate_nan_forecasts.py both call
    `TimedRotatingFileHandler("logs/log", ...)` at *import* time, which
    resolves against the process cwd at that exact moment and freezes into
    the handler's `.baseFilename`. This test file is the first file (under
    pytest's default alphabetical collection order in this test directory)
    to import either module, so if the tempdir cwd redirect above were ever
    removed, their `file_handler` would bind to the LIVE apps/logs/log --
    since run_tests.sh (the mandated runner) invokes pytest with cwd=apps/.

    fill_ml_gaps.py is deliberately not checked here: test_fill_ml_gaps.py
    and test_fill_ml_gaps_null_loop.py both import it earlier (alphabetically,
    ahead of this file) without any cwd guard of their own, so
    fill_ml_gaps's file_handler is already bound to the real apps/logs/log
    by the time this file's own import runs, regardless of whether this
    file's redirect is present. This file's redirect has no effect on
    fill_ml_gaps either way, so asserting on it here would not actually
    exercise this file's own guard.
    """
    for module in (make_forecast, recalculate_nan_forecasts):
        bound_path = os.path.abspath(module.file_handler.baseFilename)
        assert bound_path != _REAL_APPS_LOGS_LOG, (
            f"{module.__name__}.file_handler is bound to the live "
            "apps/logs/log -- the tempdir cwd redirect around this file's "
            "module-level imports appears to have been removed or broken."
        )


# ---------------------------------------------------------------------------
# Real defence against writing to apps/logs/log (INFRA-037 review round 3).
#
# The cwd redirect above only protects whichever import runs FIRST. Under
# the mandated runner (run_tests.sh), pytest collects test_fill_ml_gaps.py
# (and other unguarded siblings, tracked separately -- see module
# docstring) alphabetically BEFORE this file. Those siblings import
# fill_ml_gaps.py with no cwd guard, which binds fill_ml_gaps.logger's
# handler to the real apps/logs/log at that earlier import. Python then
# caches the module: this file's later `import fill_ml_gaps` inside the
# tempdir redirect just returns the already-bound module object, so the
# redirect has no effect on it -- and the same risk applies to
# make_forecast/recalculate_nan_forecasts if some future sibling ever
# imports them unguarded too.
#
# This autouse fixture is the real guard: for every test in this file, it
# detaches -- from each module's `logger` -- any handler whose resolved
# `baseFilename` points at the real apps/logs/log, regardless of which
# file imported the module first or in what order, and reattaches it
# afterwards so logging state is left exactly as found.
# ---------------------------------------------------------------------------

_MODULES_UNDER_TEST = (fill_ml_gaps, make_forecast, recalculate_nan_forecasts)


@pytest.fixture(autouse=True)
def _detach_live_apps_logs_handlers():
    """Make every test in this file incapable of writing to apps/logs/log.

    Belt-and-suspenders with the tempdir cwd redirect above: that redirect
    only protects the import that happens to run first, which under the
    real runner is a sibling file, not this one. This fixture instead acts
    on whichever handler each module's `logger` ends up holding, so it
    works regardless of import order or which sibling test file ran first.
    """
    detached = []  # (logger, handler) pairs removed for the duration
    for module in _MODULES_UNDER_TEST:
        logger = getattr(module, "logger", None)
        if logger is None:
            continue
        for handler in list(logger.handlers):
            base_filename = getattr(handler, "baseFilename", None)
            if base_filename is None:
                continue
            if os.path.abspath(base_filename) == _REAL_APPS_LOGS_LOG:
                logger.removeHandler(handler)
                detached.append((logger, handler))
    try:
        yield
    finally:
        for logger, handler in detached:
            logger.addHandler(handler)


_UNSET = object()  # sentinel: "do not set the env var at all"


def _set_or_clear(monkeypatch, name, value):
    """Set env var *name* to *value*, or delete it if value is _UNSET."""
    if value is _UNSET:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, value)


# ---------------------------------------------------------------------------
# recalculate_nan_forecasts.py — prediction-mode guard (PREDICTION_MODE)
# ---------------------------------------------------------------------------


class TestRecalculateNanForecastsPredictionModeMessage:
    """The PREDICTION_MODE guard at recalculate_nan_forecasts.py:~163-165.

    Invoked through the real recalculate_nan_forecasts() function. The
    guard runs before sl.load_environment(), so no environment/API mocking
    is needed to reach it — SAPPHIRE_MODEL_TO_USE just needs to be a valid
    value so execution reaches the PREDICTION_MODE check.
    """

    @pytest.mark.parametrize(
        ("prediction_mode", "expected_repr"),
        [
            (_UNSET, "None"),
            ("", "''"),
            ("WEEKLY", "'WEEKLY'"),
        ],
        ids=["unset", "empty_string", "junk_value"],
    )
    def test_exact_message(self, monkeypatch, prediction_mode, expected_repr):
        monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
        _set_or_clear(monkeypatch, "SAPPHIRE_PREDICTION_MODE", prediction_mode)

        with pytest.raises(ValueError) as exc_info:
            recalculate_nan_forecasts.recalculate_nan_forecasts()

        expected = (
            f"Prediction mode {expected_repr} is not supported.\n"
            "Please choose one of the following prediction modes: PENTAD, DECAD"
        )
        assert str(exc_info.value) == expected


# ---------------------------------------------------------------------------
# make_forecast.py — model guard (MODEL_TO_USE) in get_predictor_class()
# ---------------------------------------------------------------------------


class TestMakeForecastModelGuardMessage:
    """The MODEL_TO_USE guard at make_forecast.py:~458-461, inside
    get_predictor_class().

    get_predictor_class() is a standalone module-level function; calling it
    directly (rather than through make_ml_forecast()) reaches the exact
    same raise without requiring sl.load_environment() or predictor setup.
    The value passed in mirrors make_ml_forecast()'s own
    `MODEL_TO_USE = os.getenv("SAPPHIRE_MODEL_TO_USE")` line, using the env
    var directly so the test still reflects "env var unset/empty/junk".
    """

    @pytest.mark.parametrize(
        ("model_to_use", "expected_repr"),
        [
            (_UNSET, "None"),
            ("", "''"),
            ("WEEKLY", "'WEEKLY'"),
        ],
        ids=["unset", "empty_string", "junk_value"],
    )
    def test_exact_message(self, monkeypatch, model_to_use, expected_repr):
        monkeypatch.setenv("ieasyhydroforecast_available_ML_models", "TFT,TIDE,TSMIXER,ARIMA")
        _set_or_clear(monkeypatch, "SAPPHIRE_MODEL_TO_USE", model_to_use)

        MODEL_TO_USE = os.getenv("SAPPHIRE_MODEL_TO_USE")

        with pytest.raises(ValueError) as exc_info:
            make_forecast.get_predictor_class(MODEL_TO_USE)

        expected = (
            f"Model {expected_repr} is not supported.\n"
            "Please choose one of the following models: TFT, TIDE, TSMIXER, ARIMA"
        )
        assert str(exc_info.value) == expected


# ---------------------------------------------------------------------------
# make_forecast.py — prediction-mode guard (PREDICTION_MODE) in
# make_ml_forecast() itself (INFRA-037 defect 3b) -- a separate guard from
# get_predictor_class()'s MODEL_TO_USE guard above, further down the same
# function, using the identical f-string-interpolated message shape.
# ---------------------------------------------------------------------------


class TestMakeMlForecastPredictionModeMessage:
    """The PREDICTION_MODE guard at make_forecast.py:~526-529, inside
    make_ml_forecast() -- unlike recalculate_nan_forecasts.py/fill_ml_gaps.py,
    this guard runs AFTER sl.load_environment() and get_predictor_class().
    Both are cheap here: `setup_library` is mocked as a bare MagicMock (see
    the module-level sys.modules stubbing above), so sl.load_environment()
    is a no-op MagicMock call with no real environment/API access, and
    get_predictor_class() only needs SAPPHIRE_MODEL_TO_USE to be one of the
    values in ieasyhydroforecast_available_ML_models to pass through
    without raising, exactly as in TestMakeForecastModelGuardMessage above.
    """

    @pytest.mark.parametrize(
        ("prediction_mode", "expected_repr"),
        [
            (_UNSET, "None"),
            ("", "''"),
            ("WEEKLY", "'WEEKLY'"),
        ],
        ids=["unset", "empty_string", "junk_value"],
    )
    def test_exact_message(self, monkeypatch, prediction_mode, expected_repr):
        monkeypatch.setenv("ieasyhydroforecast_available_ML_models", "TFT,TIDE,TSMIXER,ARIMA")
        monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
        _set_or_clear(monkeypatch, "SAPPHIRE_PREDICTION_MODE", prediction_mode)

        with pytest.raises(ValueError) as exc_info:
            make_forecast.make_ml_forecast()

        expected = (
            f"Prediction mode {expected_repr} is not supported.\n"
            "Please choose one of the following prediction modes: PENTAD, DECAD"
        )
        assert str(exc_info.value) == expected


# ---------------------------------------------------------------------------
# fill_ml_gaps.py — prediction-mode guard (PREDICTION_MODE)
# ---------------------------------------------------------------------------


class TestFillMlGapsPredictionModeMessage:
    """The PREDICTION_MODE guard at fill_ml_gaps.py:~166-169.

    Same structure as recalculate_nan_forecasts.py: the guard runs before
    sl.load_environment(), so invoking the real fill_ml_gaps() function is
    cheap. Not required by the task, but included since the setup cost is
    identical to the required recalculate_nan_forecasts.py case.
    """

    @pytest.mark.parametrize(
        ("prediction_mode", "expected_repr"),
        [
            (_UNSET, "None"),
            ("", "''"),
            ("WEEKLY", "'WEEKLY'"),
        ],
        ids=["unset", "empty_string", "junk_value"],
    )
    def test_exact_message(self, monkeypatch, prediction_mode, expected_repr):
        monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "TFT")
        _set_or_clear(monkeypatch, "SAPPHIRE_PREDICTION_MODE", prediction_mode)

        with pytest.raises(ValueError) as exc_info:
            fill_ml_gaps.fill_ml_gaps()

        expected = (
            f"Prediction mode {expected_repr} is not supported.\n"
            "Please choose one of the following prediction modes: PENTAD, DECAD"
        )
        assert str(exc_info.value) == expected


# ---------------------------------------------------------------------------
# hindcast_ML_models.py — hindcast-mode guard (HINDCAST_MODE), by source
# inspection only (see module docstring for why the real call path is not
# exercised here).
# ---------------------------------------------------------------------------

_HINDCAST_SOURCE_PATH = os.path.join(os.path.dirname(__file__), "..", "hindcast_ML_models.py")


def test_hindcast_mode_error_uses_fstring_interpolation():
    """hindcast_ML_models.py's HINDCAST_MODE guard must interpolate the
    actual value (not a literal, un-formatted "%s").

    This is a source-text regression guard rather than a real-call test
    (see module docstring): it fails if the raise ever reverts to
    `"Prediction mode %s is not supported..."` with no %-formatting or
    f-string interpolation.
    """
    with open(_HINDCAST_SOURCE_PATH) as fh:
        source = fh.read()

    assert 'f"Prediction mode {HINDCAST_MODE!r} is not supported.' in source, (
        "Expected the HINDCAST_MODE guard to f-string-interpolate HINDCAST_MODE; "
        "got a source that no longer matches the expected fixed message."
    )
    # Regression guard: the old, broken literal must not be present.
    assert '"Prediction mode %s is not supported' not in source
