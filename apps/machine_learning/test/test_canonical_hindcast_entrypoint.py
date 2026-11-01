"""Behavioral tests for hindcast_ML_models.main() — canonical 11-target contracts.

WHAT IS TESTED (the REAL main() entrypoint, not a copy of its logic):
  1. n=11 is passed to predictor.hindcast() for TFT/TIDE/TSMIXER with any
     SAPPHIRE_HINDCAST_MODE value (PENTAD or DECAD).
  2. The CSV artifact filename embeds "DECAD" (not "PENTAD" and not the raw
     SAPPHIRE_HINDCAST_MODE value).
  3. _write_ml_forecast_to_api is invoked with horizon="decade".
  4. The output CSV contains exactly 11 rows per issue date × all issue dates
     in [start_iso, end_iso] (one station, no virtual twin).
  5. Date ranges crossing year boundaries and spanning Feb 29 (leap year) work.

WHAT WOULD MAKE THESE TESTS FAIL (regression detection):
  - main() uses HINDCAST_MODE directly instead of calling canonical_ml_mode().
  - gen_mode="PENTAD" branch reached → forecast_horizon=6 passed to hindcast().
  - Scaler path does not get "_Decad" suffix → FileNotFoundError because only
    the *_Decad directory is created in the tempdir.
  - API writer called with horizon="pentad" instead of "decade".
  - FakePredictor emits only the last issue → truncated start is undetectable.

EXTERNAL BOUNDARIES (all replaced by synthetic fakes):
  - ML model inference:   FakePredictor, no checkpoint loaded
  - ERA5 meteo API/CSV:   in-memory synthetic DataFrame
  - Discharge API/CSV:    in-memory synthetic DataFrame
  - SAPPHIRE API write:   captured fake callable
  - Network / DB:         SAPPHIRE_API_AVAILABLE patched; client never constructed

ISOLATION STRATEGY:
  ALL heavy-dependency mocking and the production hindcast_ML_models import are
  performed inside the module-scoped _isolate_heavy_deps fixture, NOT at module
  collection time.  Reason: pytest collects ALL test files before any fixture
  setup runs; collection-time mocks would still be active when later files are
  collected, corrupting their module-level imports.  By deferring to the fixture,
  mocks are injected only during this file's test execution and are fully removed
  on teardown.

  The fixture force-replaces ALL heavy deps (even pre-installed ones) with
  MagicMock, evicts production modules from sys.modules to force a fresh import,
  and wraps ALL setup+yield in try/finally so that a setup failure still restores
  the full pre-entry state.
"""

import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ============================================================================
# Constants computed at collection time — pure path arithmetic, no imports.
# ============================================================================
_ML_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_IEASY_DIR = os.path.abspath(os.path.join(_ML_DIR, "..", "iEasyHydroForecast"))

_HEAVY_DEPS = [
    "darts",
    "darts.TimeSeries",
    "darts.concatenate",
    "darts.utils",
    "darts.utils.timeseries_generation",
    "darts.utils.likelihood_models",
    "darts.utils.likelihood_models.base",
    "darts.models",
    "pytorch_lightning",
    "pytorch_lightning.callbacks",
    "torch",
    "torch.optim",
    "torch.optim.lr_scheduler",
    "torch.nn",
    "torch.nn.modules",
    "torch.nn.modules.loss",
    "torch.serialization",
    "torchmetrics",
    "torchmetrics.collections",
    "pe_oudin",
    "pe_oudin.PE_Oudin",
    "suntime",
    "matplotlib",
    "matplotlib.pyplot",
    "setup_library",
]

# Production module prefixes to evict before re-importing hindcast_ML_models.
# These must be freshly imported against our mocked heavy deps each run.
_PRODUCTION_PREFIXES = (
    "hindcast_ML_models",
    "scr",
    "forecast_library",
    "setup_library",
)

# Module-level reference: None at collection time, assigned inside the
# module-scoped fixture below.  All test bodies and _run_main() resolve this
# name via module globals at call time (after fixture setup has run).
hindcast_ML_models = None  # noqa: N816


# ============================================================================
# Module-scoped isolation fixture: ALL mocking + production import live here.
#
# Why: pytest collects every test file before running any fixture setup.
# Collection-time mocks injected at module level would remain active while
# other files are collected, causing their imports to see our fakes.  Moving
# the work here means mocks are only active during this file's test run and
# are fully cleaned up before any other module needs to import real packages.
#
# Snapshot timing: sys.modules and sys.path are snapshotted at fixture ENTRY
# (before any mocking), not at collection time, so the before/after comparison
# is exact.  try/finally wraps ALL setup+yield so a setup failure (e.g. import
# error) still restores every replaced or evicted entry.
# ============================================================================


@pytest.fixture(scope="module", autouse=True)
def _isolate_heavy_deps():
    """Inject heavy-dep mocks, import production module, restore on teardown."""
    global hindcast_ML_models

    # ---- SNAPSHOT at fixture entry (before any mutations) ----
    pre_entry_modules = dict(sys.modules)  # full shallow copy — includes pre-existing real deps
    pre_entry_path = list(sys.path)
    pre_global = hindcast_ML_models  # None at collection time

    try:
        # FORCE replace ALL heavy deps with MagicMock — even pre-existing real ones.
        # The old `if dep not in sys.modules` guard skipped installed packages and
        # left real darts/torch bound in the production import's namespace.
        for dep in _HEAVY_DEPS:
            sys.modules[dep] = MagicMock()

        # Evict production modules so the re-import below runs against our fresh
        # mocked deps rather than a cached real-model-class binding.
        for key in list(sys.modules.keys()):
            if any(
                key == prefix or key.startswith(prefix + ".") for prefix in _PRODUCTION_PREFIXES
            ):
                del sys.modules[key]

        # Ensure source paths are present before the production import.
        for p in (_ML_DIR, _IEASY_DIR):
            if p not in sys.path:
                sys.path.insert(0, p)

        # Import the production module inside a temp dir so its module-level
        # TimedRotatingFileHandler("logs/log", ...) writes there, not into the repo.
        prior_cwd = os.getcwd()
        import_tmp = tempfile.mkdtemp(prefix="sap_hindcast_ep_import_")
        try:
            os.chdir(import_tmp)
            import hindcast_ML_models as _hm
        finally:
            os.chdir(prior_cwd)

        # Assign to the module-level name so _run_main() can reference it.
        hindcast_ML_models = _hm

        yield  # ← tests run here

    finally:
        # ---- TEARDOWN (runs even on setup failure) ----
        # Remove every module that did not exist at fixture entry.
        for key in list(sys.modules.keys()):
            if key not in pre_entry_modules:
                del sys.modules[key]
        # Restore every module that was present at fixture entry
        # (including any we force-replaced or evicted above).
        for key, val in pre_entry_modules.items():
            sys.modules[key] = val
        # Restore sys.path to exact pre-entry state.
        sys.path[:] = pre_entry_path
        # Restore module-level global.
        hindcast_ML_models = pre_global

        # Verify: no keys we added should survive.
        leaked = [k for k in sys.modules if k not in pre_entry_modules]
        assert not leaked, f"sys.modules cleanup incomplete after teardown: {leaked!r}"


# ============================================================================
# Test-only context manager: used by TestIsolationFixtureRestoration to verify
# that the isolation+restore logic works under various injection scenarios,
# without depending on the autouse fixture above.
# ============================================================================


@contextmanager
def _isolation_test_context(modules_to_fake=None, modules_to_evict=None):
    """
    Minimal isolation context for testing the fixture's restore invariants.

    Snapshots sys.modules and sys.path at entry; unconditionally restores them
    on exit (even on exception) so callers can assert exact pre/post equality.
    """
    if modules_to_fake is None:
        modules_to_fake = []
    if modules_to_evict is None:
        modules_to_evict = []

    pre_modules = dict(sys.modules)
    pre_path = list(sys.path)

    try:
        for dep in modules_to_fake:
            sys.modules[dep] = MagicMock()
        for dep in modules_to_evict:
            sys.modules.pop(dep, None)
        yield
    finally:
        for key in list(sys.modules.keys()):
            if key not in pre_modules:
                del sys.modules[key]
        for key, val in pre_modules.items():
            sys.modules[key] = val
        sys.path[:] = pre_path


# ============================================================================
# Isolation contract: explicit tests that the fixture did its job correctly.
# These run BEFORE teardown so they assert the live setup state, not the loop.
# ============================================================================


class TestIsolationContract:
    """Spot-checks that _isolate_heavy_deps configured the module correctly."""

    def test_module_assigned_by_fixture(self):
        """hindcast_ML_models must be a real module object, not None."""
        assert hindcast_ML_models is not None, (
            "hindcast_ML_models is None at test execution time. "
            "The module-scoped _isolate_heavy_deps autouse fixture must have "
            "imported and assigned it before any test in this file runs."
        )

    def test_heavy_dep_darts_is_mocked(self):
        """darts must be a MagicMock — the fixture force-replaces all heavy deps."""
        assert isinstance(sys.modules.get("darts"), MagicMock), (
            "darts is not a MagicMock. The isolation fixture force-replaces all "
            "heavy deps (including pre-installed packages) with MagicMock before "
            "importing the production module. If this fails, the fixture did not "
            "run, or the teardown of a prior test session restored a real darts."
        )


# ============================================================================
# Isolation fixture restoration: tests that _isolation_test_context (and by
# extension the autouse fixture's same logic) restores state correctly under
# preloaded-inert, fresh-module, and deliberate-failure cases.
# ============================================================================


class TestIsolationFixtureRestoration:
    """
    Verifies isolation+restore invariants using _isolation_test_context so
    no real ML/torch/darts imports are needed and no state leaks across tests.
    """

    def test_preloaded_inert_module_is_force_replaced_and_restored(self):
        """Pre-existing inert MagicMock is replaced inside context and restored after."""
        sentinel = MagicMock(name="sentinel_inert")
        # Inject a pre-existing entry (simulates cached real or inert module).
        sys.modules["_test_isol_preloaded"] = sentinel
        try:
            with _isolation_test_context(modules_to_fake=["_test_isol_preloaded"]):
                replaced = sys.modules["_test_isol_preloaded"]
                assert replaced is not sentinel, (
                    "Force-replace did not overwrite the pre-existing entry"
                )
                assert isinstance(replaced, MagicMock)
            # After context: original sentinel must be restored.
            assert sys.modules.get("_test_isol_preloaded") is sentinel, (
                "Pre-existing module identity not restored after context exit"
            )
        finally:
            sys.modules.pop("_test_isol_preloaded", None)

    def test_fresh_module_is_injected_then_removed(self):
        """A dep absent before context is present inside and absent after."""
        assert "_test_isol_fresh" not in sys.modules
        with _isolation_test_context(modules_to_fake=["_test_isol_fresh"]):
            assert isinstance(sys.modules.get("_test_isol_fresh"), MagicMock)
        assert "_test_isol_fresh" not in sys.modules, (
            "Newly-injected module leaked after context exit"
        )

    def test_deliberate_import_setup_failure_restores_modules_and_path(self):
        """On exception inside the context, sys.modules and sys.path are restored."""
        pre_path = list(sys.path)
        fake_path = "/tmp/_test_isol_path_sentinel_must_not_persist"
        assert fake_path not in sys.path

        try:
            with _isolation_test_context(modules_to_fake=["_test_isol_fail_dep"]):
                # Mutate sys.path inside the context to simulate a mid-setup failure.
                sys.path.insert(0, fake_path)
                raise RuntimeError("simulated import failure")
        except RuntimeError:
            pass

        assert fake_path not in sys.path, "sys.path not restored after simulated setup failure"
        assert "_test_isol_fail_dep" not in sys.modules, (
            "Injected mock module leaked after simulated setup failure"
        )
        assert sys.path == pre_path, "sys.path differs from pre-context state after failure"

    def test_no_real_ml_imports_during_isolation_context(self):
        """_isolation_test_context never imports real torch/darts modules."""
        ml_before = {k for k in sys.modules if "torch" in k or "darts" in k}
        with _isolation_test_context():
            ml_during = {k for k in sys.modules if "torch" in k or "darts" in k}
            assert ml_during == ml_before, (
                f"Real ML modules appeared inside _isolation_test_context: {ml_during - ml_before}"
            )
        ml_after = {k for k in sys.modules if "torch" in k or "darts" in k}
        assert ml_after == ml_before, (
            f"Real ML modules leaked after _isolation_test_context exit: {ml_after - ml_before}"
        )


# ============================================================================
# FakePredictor — captures hindcast(n=…) arg, returns rows for ALL issue dates
# ============================================================================


class FakePredictor:
    """
    Drop-in for TFTPredictor / TiDEPredictor / TSMIXERPredictor.

    Accepts the same **kwargs as BaseDartsDLPredictor (model, scalers,
    static_features, dl_config_params, unique_id_col) without using them.
    All per-call observables are recorded in the class-level `calls` list,
    which _run_main() clears before each invocation.

    hindcast() derives the COMPLETE issue date range from the discharge window
    that main() passes in:
      - main() reindexes discharge to [start_iso - (icl+1), end_iso + n]
      - So: start_issue = min(df_dates) + (icl+1) = start_iso
            end_issue   = max(df_dates) - n        = end_iso
      - One 11-row block (issue+1 .. issue+n) is emitted per issue date.

    This means a regression that truncates start_iso (keeping end_iso the same)
    changes min(df_dates) and therefore changes start_issue, making the missing
    early-issue rows detectable in the complete-set assertions.
    """

    calls: list = []

    def __init__(self, **kwargs):
        pass

    def get_input_chunk_length(self) -> int:
        return 30

    def hindcast(self, df_rivers_org, df_covariates, code, n):
        icl = self.get_input_chunk_length()
        # Recover issue date range: main() shifted discharge min by -(icl+1),
        # so reversing recovers start_iso; max was set to end_iso + n.
        start_issue = pd.Timestamp(df_rivers_org["date"].min()) + pd.Timedelta(days=icl + 1)
        end_issue = pd.Timestamp(df_rivers_org["date"].max()) - pd.Timedelta(days=n)
        issue_dates = pd.date_range(start_issue, end_issue, freq="D")
        # One entry per hindcast() invocation (not per issue) so len(calls)==1
        # for a single-station run regardless of how many issue dates are covered.
        FakePredictor.calls.append({"n": n, "code": code, "num_issues": len(issue_dates)})
        rows = []
        for issue in issue_dates:
            for i in range(n):
                rows.append(
                    {
                        "date": issue + pd.Timedelta(days=i + 1),
                        "forecast_date": issue,
                        "Q5": 5.0,
                        "Q10": 10.0,
                        "Q15": 15.0,
                        "Q20": 20.0,
                        "Q25": 25.0,
                        "Q30": 30.0,
                        "Q35": 35.0,
                        "Q40": 40.0,
                        "Q45": 45.0,
                        "Q50": 50.0,
                        "Q55": 55.0,
                        "Q60": 60.0,
                        "Q65": 65.0,
                        "Q70": 70.0,
                        "Q75": 75.0,
                        "Q80": 80.0,
                        "Q85": 85.0,
                        "Q90": 90.0,
                        "Q95": 95.0,
                    }
                )
        return pd.DataFrame(rows)


# ============================================================================
# Synthetic data helpers
# ============================================================================


def _fake_hydroposts():
    """Single synthetic station eligible for all three canonical models."""
    df = pd.DataFrame(
        {
            "code": ["19991"],
            "TFT": [True],
            "TIDE": [True],
            "TSMIXER": [True],
            "ARIMA": [False],
        }
    )
    # Use object dtype so the value stays Python False, not numpy.bool_.
    # main() checks `test_value is not False`; numpy.bool_(False) is not False
    # evaluates to True and would append a spurious twin row.  With object dtype
    # the identity check is False and no twin copy is made.
    df["virtual_station_name_twin"] = pd.array([False], dtype=object)
    return (["19991"], ["19991"], df)


def _fake_era5(start_iso: str, end_iso: str) -> pd.DataFrame:
    """
    ERA5 meteo for station 19991.

    Window starts 70 days before start_iso (satisfies the 60-day look-back
    guard in main()) and ends 20 days after end_iso (satisfies the
    end_iso + forecast_horizon=11 guard).  All calendar edge cases — year
    boundaries, leap Feb 29 — that fall in the derived window are included.
    """
    era5_start = pd.Timestamp(start_iso) - pd.Timedelta(days=70)
    era5_end = pd.Timestamp(end_iso) + pd.Timedelta(days=20)
    dates = pd.date_range(era5_start, era5_end, freq="D")
    return pd.DataFrame(
        {
            "code": "19991",
            "date": dates,
            "T": [5.0 + 0.05 * d.day_of_year for d in dates],
            "P": [1.0 + 0.01 * d.day_of_year for d in dates],
        }
    )


def _fake_discharge(start_iso: str, end_iso: str) -> pd.DataFrame:
    """Daily discharge for station 19991, same span as ERA5."""
    d_start = pd.Timestamp(start_iso) - pd.Timedelta(days=70)
    d_end = pd.Timestamp(end_iso) + pd.Timedelta(days=20)
    dates = pd.date_range(d_start, d_end, freq="D")
    return pd.DataFrame({"code": "19991", "date": dates, "discharge": 10.0})


def _setup_tempdir(tmp_path: Path, model: str) -> dict:
    """
    Create the on-disk artefacts required by main() and return an env-var dict.

    Filesystem layout:
      tmp_path/
        models/
          static_features.csv          ← int code 19991, LAT, LON
          {model}_Decad/               ← canonical scaler dir (ONLY this exists)
            scaler_stats_discharge.csv
            scaler_stats_era5.csv
            scaler_stats_static.csv
            stub.pt                    ← glob("*.pt")[0] stub
        intermediate/
          discharge/                   ← main() creates hindcast/{model}/ under here

    Regression guard: ONLY the *_Decad directory is created.  If main()
    reverts to using HINDCAST_MODE directly (gen_mode → "PENTAD"), the scaler
    path becomes models/{model} (no suffix) which does not exist → FileNotFoundError,
    which is a clear, early test failure signal.
    """
    models_dir = tmp_path / "models"
    intermediate_dir = tmp_path / "intermediate"
    scaler_dir = models_dir / f"{model}_Decad"
    scaler_dir.mkdir(parents=True)
    (intermediate_dir / "discharge").mkdir(parents=True)

    # static_features.csv — code as integer so static_features.loc[int_code] works
    pd.DataFrame({"code": [19991], "LAT": [42.0], "LON": [75.0]}).to_csv(
        models_dir / "static_features.csv", index=False
    )
    # Scaler CSVs — content arbitrary; FakePredictor discards them
    pd.DataFrame({"Unnamed: 0": [19991], "mean": [10.0], "std": [2.0]}).to_csv(
        scaler_dir / "scaler_stats_discharge.csv", index=False
    )
    pd.DataFrame({"Unnamed: 0": ["T", "P"], "mean": [5.0, 1.0], "std": [2.0, 0.5]}).to_csv(
        scaler_dir / "scaler_stats_era5.csv", index=False
    )
    pd.DataFrame({"Unnamed: 0": ["LAT"], "mean": [42.0], "std": [1.0]}).to_csv(
        scaler_dir / "scaler_stats_static.csv", index=False
    )
    # Empty .pt stub so glob.glob("*.pt")[0] succeeds without a real checkpoint
    (scaler_dir / "stub.pt").touch()

    return {
        "SAPPHIRE_MODEL_TO_USE": model,
        "SAPPHIRE_HINDCAST_MODE": "PENTAD",  # overridden per-test via _run_main
        "SAPPHIRE_API_ENABLED": "true",
        "ieasyforecast_intermediate_data_path": str(intermediate_dir),
        "ieasyhydroforecast_models_and_scalers_path": str(models_dir),
        "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static_features.csv",
        f"ieasyhydroforecast_PATH_TO_SCALER_{model}": model,
        "ieasyhydroforecast_HRU_CONTROL_MEMBER": "hru_test",
        "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "reanalysis",
        f"ieasyhydroforecast_THRESHOLD_MISSING_DAYS_{model}": "5",
        "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "3",
        "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "discharge",
        "ieasyhydroforecast_NEW_STATIONS": "None",
    }


# ============================================================================
# Main test runner
# ============================================================================


def _run_main(
    model: str,
    mode: str,
    start_iso: str,
    end_iso: str,
    tmp_path: Path,
    monkeypatch,
) -> dict:
    """
    Call the real hindcast_ML_models.main() with all external I/O replaced by
    synthetic fakes.  Returns a dict of observables for assertions.

    Mocking strategy:
      - predictor classes patched on their actual source modules via monkeypatch
        (automatically restored after each test).
      - SAPPHIRE_API_AVAILABLE patched to True so the api-write branch in main()
        is exercised and the captured fake callable is invoked.
      - _write_ml_forecast_to_api replaced with a capturing closure.
      - read_meteo_data_combined patched on hindcast_ML_models's own namespace
        (it is imported there as a name via `from scr.utils_ml_forecast import`).
      - fl.read_daily_discharge_data patched on the fl module object bound in
        hindcast_ML_models.
      - calculate_pet_oudin / calculate_daylight_hours patched on
        hindcast_ML_models.utils_ml_forecast to prevent a MagicMock PE_Oudin
        return value from corrupting the ERA5 DataFrame used by pd.concat.
    """
    FakePredictor.calls.clear()
    api_calls: list = []

    era5_df = _fake_era5(start_iso, end_iso)
    discharge_df = _fake_discharge(start_iso, end_iso)
    pentad_list, decad_list, hydroposts_df = _fake_hydroposts()

    env = _setup_tempdir(tmp_path, model)
    env["SAPPHIRE_HINDCAST_MODE"] = mode
    env["ieasyhydroforecast_START_DATE"] = start_iso
    env["ieasyhydroforecast_END_DATE"] = end_iso

    # Inject FakePredictor for all three canonical predictor class attributes.
    # monkeypatch restores the originals after the test.
    monkeypatch.setattr(hindcast_ML_models.TFTPredictor, "TFTPredictor", FakePredictor)
    monkeypatch.setattr(hindcast_ML_models.TiDEPredictor, "TiDEPredictor", FakePredictor)
    monkeypatch.setattr(hindcast_ML_models.TSMixerPredictor, "TSMIXERPredictor", FakePredictor)

    # Hydroposts config reader (avoids JSON file dependency)
    monkeypatch.setattr(
        hindcast_ML_models.utils_ml_forecast,
        "get_hydroposts_for_pentadal_and_decadal_forecasts",
        lambda: (pentad_list, decad_list, hydroposts_df),
    )

    # Meteo reader (avoids real CSV / API call)
    def _fake_meteo(
        site_codes=None,
        start_date=None,
        end_date=None,
        csv_path_t=None,
        csv_path_p=None,
    ):
        return era5_df.copy()

    monkeypatch.setattr(hindcast_ML_models, "read_meteo_data_combined", _fake_meteo)

    # Discharge reader
    monkeypatch.setattr(
        hindcast_ML_models.fl,
        "read_daily_discharge_data",
        lambda: discharge_df.copy(),
    )

    # PET / daylight: return numeric Series so ERA5 DataFrame stays well-formed.
    # Without this patch, MagicMock PE_Oudin propagates into the column and
    # makes subsequent pd.concat fail.
    def _fake_pet(df_era5, lat):
        return pd.Series(0.5, index=df_era5.index, dtype=float)

    def _fake_daylight(lat, lon, df_era5):
        return pd.Series(12.0, index=df_era5.index, dtype=float)

    monkeypatch.setattr(hindcast_ML_models.utils_ml_forecast, "calculate_pet_oudin", _fake_pet)
    monkeypatch.setattr(
        hindcast_ML_models.utils_ml_forecast, "calculate_daylight_hours", _fake_daylight
    )

    # API write capture: enable the code path then intercept the call.
    # Capture the full data DataFrame (before normalize_ml_csv_columns runs)
    # so date-assertion tests can verify both the API payload and the CSV.
    def _fake_api_write(data, horizon, model_type):
        api_calls.append(
            {
                "horizon": horizon,
                "model_type": model_type,
                "n_rows": len(data),
                "data": data.copy(),
            }
        )
        return True

    monkeypatch.setattr(hindcast_ML_models, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(hindcast_ML_models, "_write_ml_forecast_to_api", _fake_api_write)

    with patch.dict(os.environ, env, clear=False):
        hindcast_ML_models.main()

    out_dir = Path(env["ieasyforecast_intermediate_data_path"]) / "discharge" / "hindcast" / model
    csv_files = sorted(out_dir.glob("*.csv")) if out_dir.exists() else []

    csv_df = pd.read_csv(csv_files[0]) if csv_files else pd.DataFrame()
    return {
        "predictor_calls": list(FakePredictor.calls),
        "api_calls": api_calls,
        "csv_files": csv_files,
        "out_dir": out_dir,
        "csv_df": csv_df,
    }


# ============================================================================
# 1. Forecast horizon: n=11 passed to predictor.hindcast() for all combinations
# ============================================================================


class TestHindcastMainForecastHorizon:
    """
    For all three canonical models × both HINDCAST_MODE values, main() must
    pass n=11 to predictor.hindcast().

    These tests would fail if main() computed forecast_horizon from
    HINDCAST_MODE directly (giving n=6 for PENTAD) instead of from gen_mode
    (which canonical_ml_mode() locks to DECAD → n=11).
    """

    @pytest.mark.parametrize(
        "model,mode",
        [
            ("TFT", "PENTAD"),
            ("TFT", "DECAD"),
            ("TIDE", "PENTAD"),
            ("TIDE", "DECAD"),
            ("TSMIXER", "PENTAD"),
            ("TSMIXER", "DECAD"),
        ],
    )
    def test_n_11_passed_to_predictor(self, model, mode, tmp_path, monkeypatch):
        result = _run_main(model, mode, "2024-03-01", "2024-03-03", tmp_path, monkeypatch)
        calls = result["predictor_calls"]
        assert len(calls) == 1, (
            f"{model}/{mode}: expected 1 predictor.hindcast() call, got {len(calls)}"
        )
        actual_n = calls[0]["n"]
        assert actual_n == 11, (
            f"{model}/{mode}: predictor.hindcast(n=...) received n={actual_n!r}, "
            f"expected 11 (DECAD canonical horizon). "
            f"n=6 indicates main() is using HINDCAST_MODE={mode!r} directly "
            f"instead of gen_mode=canonical_ml_mode({model!r}, {mode!r})='DECAD'."
        )


# ============================================================================
# 2. DECAD artifact filename: CSV embeds "DECAD" for canonical models
# ============================================================================


class TestHindcastMainDecadArtifacts:
    """
    The output CSV artifact must be named *_DECAD_* for canonical models
    regardless of SAPPHIRE_HINDCAST_MODE.
    """

    @pytest.mark.parametrize("model", ["TFT", "TIDE", "TSMIXER"])
    def test_csv_filename_contains_decad_when_hindcast_mode_is_pentad(
        self, model, tmp_path, monkeypatch
    ):
        result = _run_main(model, "PENTAD", "2024-03-01", "2024-03-03", tmp_path, monkeypatch)
        csv_files = result["csv_files"]
        assert len(csv_files) == 1, (
            f"{model}/PENTAD: expected 1 CSV in output dir, got {len(csv_files)}: {csv_files}"
        )
        filename = csv_files[0].name
        assert "DECAD" in filename, (
            f"{model}/PENTAD: CSV filename {filename!r} does not contain 'DECAD'. "
            f"Expected pattern: {model}_DECAD_hindcast_daily_*. "
            f"If 'PENTAD' appears instead, gen_mode='DECAD' was not applied to "
            f"the artifact filename (main() used HINDCAST_MODE directly)."
        )
        assert "PENTAD" not in filename, (
            f"{model}/PENTAD: CSV filename {filename!r} still contains 'PENTAD'."
        )

    @pytest.mark.parametrize("model", ["TFT", "TIDE", "TSMIXER"])
    def test_csv_filename_contains_decad_when_hindcast_mode_is_decad(
        self, model, tmp_path, monkeypatch
    ):
        result = _run_main(model, "DECAD", "2024-03-01", "2024-03-03", tmp_path, monkeypatch)
        csv_files = result["csv_files"]
        assert len(csv_files) == 1, f"{model}/DECAD: expected 1 CSV, got {len(csv_files)}"
        filename = csv_files[0].name
        assert "DECAD" in filename, (
            f"{model}/DECAD: CSV filename {filename!r} does not contain 'DECAD'."
        )


# ============================================================================
# 3. API writer capture: horizon="decade" for all canonical models
# ============================================================================


class TestHindcastMainApiWriterCapture:
    """
    _write_ml_forecast_to_api must receive horizon='decade' for canonical models.

    main() selects: horizon = "pentad" if gen_mode == "PENTAD" else "decade".
    If gen_mode reverts to "PENTAD" for HINDCAST_MODE=PENTAD, horizon becomes
    "pentad" and the assertion fails.
    """

    @pytest.mark.parametrize(
        "model,mode",
        [
            ("TFT", "PENTAD"),
            ("TFT", "DECAD"),
            ("TIDE", "PENTAD"),
            ("TIDE", "DECAD"),
            ("TSMIXER", "PENTAD"),
            ("TSMIXER", "DECAD"),
        ],
    )
    def test_api_writer_receives_decade_horizon(self, model, mode, tmp_path, monkeypatch):
        result = _run_main(model, mode, "2024-03-01", "2024-03-03", tmp_path, monkeypatch)
        api_calls = result["api_calls"]
        assert len(api_calls) == 1, (
            f"{model}/{mode}: expected 1 _write_ml_forecast_to_api call, got {len(api_calls)}"
        )
        horizon = api_calls[0]["horizon"]
        assert horizon == "decade", (
            f"{model}/{mode}: _write_ml_forecast_to_api(horizon=...) received "
            f"{horizon!r}, expected 'decade'. "
            f"horizon='pentad' means gen_mode='DECAD' was not applied to the "
            f"horizon selector in main()."
        )


# ============================================================================
# 4. Complete output set: CSV and API payload contain exactly 11×num_issues rows
#    with the exact (code, forecast_date, date) triples derived from requested
#    start/end — independent of FakePredictor's derivation logic.
# ============================================================================


class TestHindcastMain11TargetsOutput:
    """
    With one station and FakePredictor.hindcast() returning 11 rows per issue
    date, the output must have exactly 11 × (end_iso - start_iso + 1) rows in
    total (no virtual-twin duplication because _fake_hydroposts uses object-dtype
    False which preserves the Python identity check `test_value is not False`
    as False).

    Assertions derive the expected (forecast_date, date) set directly from
    start_iso and end_iso — not from FakePredictor's own derivation — so a bug
    in main() that shifts start_iso or end_iso is independently detected.
    """

    @pytest.mark.parametrize(
        "model,mode",
        [
            ("TFT", "PENTAD"),
            ("TFT", "DECAD"),
            ("TIDE", "PENTAD"),
            ("TSMIXER", "PENTAD"),
        ],
    )
    def test_output_has_11_rows_per_issue_no_extras(self, model, mode, tmp_path, monkeypatch):
        """
        Total CSV rows = 11 × num_issues.  Any unexpected twin rows inflate the
        count; a missing issue deflates it.

        start_iso="2024-03-01", end_iso="2024-03-03" → 3 issues × 11 = 33 rows.
        n=6 → HINDCAST_MODE used directly instead of gen_mode (18 rows).
        n=22 → spurious twin row (numpy.bool_ identity leak, 66 rows).
        Fewer than 33 rows → early-issue truncation or start_iso shift in main().
        """
        start_iso, end_iso = "2024-03-01", "2024-03-03"
        expected_issues = pd.date_range(start_iso, end_iso, freq="D")
        expected_count = 11 * len(expected_issues)

        result = _run_main(model, mode, start_iso, end_iso, tmp_path, monkeypatch)
        csv_files = result["csv_files"]
        assert len(csv_files) == 1, f"{model}/{mode}: expected exactly 1 CSV output file"
        df = result["csv_df"]
        assert len(df) == expected_count, (
            f"{model}/{mode}: output CSV has {len(df)} rows, expected {expected_count} "
            f"(= 11 targets × {len(expected_issues)} issues). "
            f"{len(df) // 11} issues found; expected {len(expected_issues)}."
        )

    def test_march_multi_issue_exact_triples(self, tmp_path, monkeypatch):
        """
        start=2024-03-01, end=2024-03-03 → 3 issues (Mar 1, 2, 3).
        CSV reload AND captured API payload must contain exactly the 33
        (forecast_date, date) pairs derived from these dates — no more, no fewer.
        This assertion is computed from start_iso/end_iso, not from FakePredictor.
        """
        start_iso, end_iso = "2024-03-01", "2024-03-03"
        result = _run_main("TFT", "PENTAD", start_iso, end_iso, tmp_path, monkeypatch)

        expected_issues = pd.date_range(start_iso, end_iso, freq="D")
        expected_pairs = {
            (issue, issue + pd.Timedelta(days=i + 1))
            for issue in expected_issues
            for i in range(11)
        }
        expected_count = len(expected_pairs)  # 33

        # Assert from reloaded CSV
        csv_df = result["csv_df"]
        assert len(csv_df) == expected_count, (
            f"CSV row count: {len(csv_df)}, expected {expected_count}"
        )
        csv_df["date"] = pd.to_datetime(csv_df["date"])
        csv_df["forecast_date"] = pd.to_datetime(csv_df["forecast_date"])
        actual_pairs_csv = set(zip(csv_df["forecast_date"], csv_df["date"], strict=False))
        assert actual_pairs_csv == expected_pairs, (
            f"CSV (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_csv}\n"
            f"  Extra:   {actual_pairs_csv - expected_pairs}"
        )

        # Assert from captured API writer frame (before normalize_ml_csv_columns)
        api_data = result["api_calls"][0]["data"]
        api_data["date"] = pd.to_datetime(api_data["date"])
        api_data["forecast_date"] = pd.to_datetime(api_data["forecast_date"])
        assert len(api_data) == expected_count, (
            f"API frame row count: {len(api_data)}, expected {expected_count}"
        )
        actual_pairs_api = set(zip(api_data["forecast_date"], api_data["date"], strict=False))
        assert actual_pairs_api == expected_pairs, (
            f"API (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_api}\n"
            f"  Extra:   {actual_pairs_api - expected_pairs}"
        )

    def test_truncated_start_regression_is_detectable(self, tmp_path, monkeypatch):
        """
        start_iso is included in the expected issue set.  If main() truncated
        start_iso (e.g. due to a date-shift bug), the early issue dates disappear
        from the output even though end_iso=2024-03-03 stays the same.

        With start_iso=2024-03-01, the issue set is [Mar 1, Mar 2, Mar 3] → 33 rows,
        and Mar 1 must appear as a forecast_date.  A truncation to start_iso=2024-03-02
        would drop Mar 1 issues entirely and reduce the count to 22 — both assertions
        below would fail, catching the regression despite the unchanged end date.
        """
        start_iso, end_iso = "2024-03-01", "2024-03-03"
        result = _run_main("TFT", "PENTAD", start_iso, end_iso, tmp_path, monkeypatch)
        df = result["csv_df"]
        assert len(df) == 33, (
            f"Expected 33 rows (3 issues × 11), got {len(df)}. "
            f"A truncated start_iso would give 22 rows (2 issues) even with "
            f"end_iso={end_iso} unchanged."
        )
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        assert pd.Timestamp("2024-03-01") in set(df["forecast_date"]), (
            "March 1 issue date missing from output — a start_iso truncation "
            "causes this even when end_iso=2024-03-03 is unchanged."
        )


# ============================================================================
# 5. Cross-year calendar: date range 2024-12-28 → 2024-12-29 (crosses Jan 1)
# ============================================================================


class TestHindcastMainCrossYearCalendar:
    """
    A hindcast window ending in late December whose 11-day tail extends into
    the following year must run without error and produce 11 rows per issue.

    The ERA5 window (70 days before start, 20 days after end) spans
    2024-10-19 to 2025-01-18, crossing the year boundary.
    """

    @pytest.mark.parametrize("model", ["TFT", "TIDE", "TSMIXER"])
    def test_cross_year_boundary_produces_n_11(self, model, tmp_path, monkeypatch):
        result = _run_main(model, "PENTAD", "2024-12-28", "2024-12-29", tmp_path, monkeypatch)
        calls = result["predictor_calls"]
        assert len(calls) == 1, (
            f"{model}/cross-year: expected 1 predictor.hindcast() call, got {len(calls)}"
        )
        assert calls[0]["n"] == 11, f"{model}/cross-year: n={calls[0]['n']}, expected 11"
        csv_files = result["csv_files"]
        assert len(csv_files) == 1, f"{model}/cross-year: expected 1 output CSV"
        assert "DECAD" in csv_files[0].name, (
            f"{model}/cross-year: CSV filename does not contain 'DECAD'"
        )

    def test_cross_year_exact_triples(self, tmp_path, monkeypatch):
        """
        start=2024-12-28, end=2024-12-29 → 2 issues; targets cross into Jan 2025.
        CSV and API payload must contain the exact 22 (forecast_date, date) pairs.
        Expected set is derived from start_iso/end_iso, not from FakePredictor.
        """
        start_iso, end_iso = "2024-12-28", "2024-12-29"
        result = _run_main("TFT", "PENTAD", start_iso, end_iso, tmp_path, monkeypatch)

        expected_issues = pd.date_range(start_iso, end_iso, freq="D")
        expected_pairs = {
            (issue, issue + pd.Timedelta(days=i + 1))
            for issue in expected_issues
            for i in range(11)
        }
        expected_count = len(expected_pairs)  # 22

        # Sanity: Jan 1 must be in at least one target window.
        jan1 = pd.Timestamp("2025-01-01")
        assert any(
            jan1 == issue + pd.Timedelta(days=i + 1) for issue in expected_issues for i in range(11)
        ), "sanity: Jan 1 2025 must appear in at least one target window"

        # Assert from reloaded CSV
        csv_df = result["csv_df"]
        assert len(csv_df) == expected_count, (
            f"CSV row count: {len(csv_df)}, expected {expected_count}"
        )
        csv_df["date"] = pd.to_datetime(csv_df["date"])
        csv_df["forecast_date"] = pd.to_datetime(csv_df["forecast_date"])
        actual_pairs_csv = set(zip(csv_df["forecast_date"], csv_df["date"], strict=False))
        assert actual_pairs_csv == expected_pairs, (
            f"CSV (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_csv}\n"
            f"  Extra:   {actual_pairs_csv - expected_pairs}"
        )

        # Assert from captured API writer frame
        api_data = result["api_calls"][0]["data"]
        api_data["date"] = pd.to_datetime(api_data["date"])
        api_data["forecast_date"] = pd.to_datetime(api_data["forecast_date"])
        assert len(api_data) == expected_count
        actual_pairs_api = set(zip(api_data["forecast_date"], api_data["date"], strict=False))
        assert actual_pairs_api == expected_pairs, (
            f"API (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_api}\n"
            f"  Extra:   {actual_pairs_api - expected_pairs}"
        )


# ============================================================================
# 6. Leap-year calendar: Feb 29, 2024 in the lookback window or target window
# ============================================================================


class TestHindcastMainLeapCalendar:
    """
    Two leap-year scenarios:
      1. Feb 29 in the 70-day lookback ERA5 window (start_date after Feb 29).
      2. Feb 29 in the target window (end_iso before Feb 29 by < 11 days).
    """

    @pytest.mark.parametrize("model", ["TFT", "TIDE", "TSMIXER"])
    def test_leap_year_feb29_in_window_produces_n_11(self, model, tmp_path, monkeypatch):
        result = _run_main(model, "PENTAD", "2024-03-05", "2024-03-07", tmp_path, monkeypatch)
        calls = result["predictor_calls"]
        assert len(calls) == 1, (
            f"{model}/leap: expected 1 predictor.hindcast() call, got {len(calls)}"
        )
        assert calls[0]["n"] == 11, f"{model}/leap: n={calls[0]['n']}, expected 11"

    def test_leap_feb29_in_targets_exact_triples(self, tmp_path, monkeypatch):
        """
        start=2024-02-23, end=2024-02-24 → 2 issues; Feb 29 falls in target window.
        CSV and API payload must contain the exact 22 (forecast_date, date) pairs.
        Expected set is derived from start_iso/end_iso, not from FakePredictor.
        """
        start_iso, end_iso = "2024-02-23", "2024-02-24"
        result = _run_main("TFT", "PENTAD", start_iso, end_iso, tmp_path, monkeypatch)

        expected_issues = pd.date_range(start_iso, end_iso, freq="D")
        expected_pairs = {
            (issue, issue + pd.Timedelta(days=i + 1))
            for issue in expected_issues
            for i in range(11)
        }
        expected_count = len(expected_pairs)  # 22

        # Sanity: Feb 29 must appear in at least one target window.
        feb29 = pd.Timestamp("2024-02-29")
        assert any(
            feb29 == issue + pd.Timedelta(days=i + 1)
            for issue in expected_issues
            for i in range(11)
        ), "sanity: Feb 29 2024 must appear in at least one target window"

        # Assert from reloaded CSV
        csv_df = result["csv_df"]
        assert len(csv_df) == expected_count, (
            f"CSV row count: {len(csv_df)}, expected {expected_count}"
        )
        csv_df["date"] = pd.to_datetime(csv_df["date"])
        csv_df["forecast_date"] = pd.to_datetime(csv_df["forecast_date"])
        actual_pairs_csv = set(zip(csv_df["forecast_date"], csv_df["date"], strict=False))
        assert actual_pairs_csv == expected_pairs, (
            f"CSV (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_csv}\n"
            f"  Extra:   {actual_pairs_csv - expected_pairs}"
        )

        # Assert from captured API writer frame
        api_data = result["api_calls"][0]["data"]
        api_data["date"] = pd.to_datetime(api_data["date"])
        api_data["forecast_date"] = pd.to_datetime(api_data["forecast_date"])
        assert len(api_data) == expected_count
        actual_pairs_api = set(zip(api_data["forecast_date"], api_data["date"], strict=False))
        assert actual_pairs_api == expected_pairs, (
            f"API (forecast_date, date) pairs mismatch.\n"
            f"  Missing: {expected_pairs - actual_pairs_api}\n"
            f"  Extra:   {actual_pairs_api - expected_pairs}"
        )
