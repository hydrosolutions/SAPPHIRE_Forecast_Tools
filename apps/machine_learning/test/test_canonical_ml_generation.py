"""Tests for ML-020: canonical DECAD generation for TFT/TiDE/TSMixer.

Covers:
- canonical_ml_mode() unit tests
- make_forecast.make_ml_forecast() entrypoint integration tests: prove
  that for PENTAD+TFT/TiDE/TSMixer the function routes through gen_mode=DECAD,
  passes n=11 to the predictor, selects the _Decad scaler asset, and persists
  a 11-row target fan in the output CSV.
- Functional tests for fill_ml_gaps.call_hindcast_script and
  recalculate_nan_forecasts.call_hindcast_script using canonical (DECAD) path
- Functional tests for add_new_station.call_hindcast_script (TFT/ARIMA)
- add_new_station.main() orchestration: overlapping codes → ONE hindcast call
- Behavior test for recalculate_nan_forecasts flag 1/2 selection
"""

import datetime
import importlib
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# No sys.path mutations at collection time.  Path insertions and all
# sys.modules mutations are managed by _ml_env_ctx (used by _ml_module_env
# and isolation tests), which snapshots and restores everything in a
# try/finally so setup failures leave no residue.
# ---------------------------------------------------------------------------

# Module-level placeholders; populated by _ml_module_env before any test executes.
add_new_station = None
fill_ml_gaps = None
make_forecast = None
recalculate_nan_forecasts = None
canonical_ml_mode = None

# ---------------------------------------------------------------------------
# Heavy-dependency stub list — everything needed to import the production
# modules without requiring torch/darts/etc to be installed.
# ---------------------------------------------------------------------------
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

_PROD_MODULE_PREFIXES = (
    "add_new_station",
    "fill_ml_gaps",
    "make_forecast",
    "recalculate_nan_forecasts",
    "scr",
)

_SENTINEL = object()


@contextmanager
def _ml_env_ctx():
    """Setup/teardown context for ML module imports.

    Snapshots sys.path, sys.modules, and module-global bindings before any
    mutation so that ANY failure during setup, yield, or teardown leaves no
    residue.  Used by both _ml_module_env (module fixture) and isolation
    tests, so the cleanup path is exercised by real tests, not copied code.
    """
    _this = sys.modules[__name__]

    # 1. Snapshot sys.path before any insertion.
    _path_snap = list(sys.path)

    # 2. Snapshot sys.modules entries this context will touch (pop them so
    #    the imports start clean; _SENTINEL marks keys that were absent).
    _mod_snap: dict = {}
    for key in list(sys.modules):
        if key in _HEAVY_DEPS or any(
            key == p or key.startswith(p + ".") for p in _PROD_MODULE_PREFIXES
        ):
            _mod_snap[key] = sys.modules.pop(key)
    for key in _HEAVY_DEPS:
        if key not in _mod_snap:
            _mod_snap[key] = _SENTINEL  # was absent; must stay absent after restore

    # 3. Snapshot module-global bindings injected by this context.
    _GLOBAL_ATTRS = (
        "add_new_station",
        "fill_ml_gaps",
        "make_forecast",
        "recalculate_nan_forecasts",
        "canonical_ml_mode",
    )
    _global_snap = {attr: getattr(_this, attr, _SENTINEL) for attr in _GLOBAL_ATTRS}

    try:
        # 4. Add sys.path entries required to import the production modules.
        sys.path.insert(
            0,
            os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast")
            ),
        )
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scr")))
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

        # 5. Install heavy-dependency stubs.
        for key in _HEAVY_DEPS:
            sys.modules[key] = MagicMock()

        # 6. Import production modules (cwd → tempdir to suppress log files).
        _prior_cwd = os.getcwd()
        with tempfile.TemporaryDirectory(prefix="sapphire_ml_test_iso_") as _tmpdir:
            os.chdir(_tmpdir)
            try:
                _ans = importlib.import_module("add_new_station")
                _fmg = importlib.import_module("fill_ml_gaps")
                _mf = importlib.import_module("make_forecast")
                _rnf = importlib.import_module("recalculate_nan_forecasts")
            finally:
                os.chdir(_prior_cwd)

        _utils = sys.modules["scr.utils_ml_forecast"]
        _cmm = _utils.canonical_ml_mode

        # 7. Inject as module globals so test bodies find them by name.
        _this.add_new_station = _ans
        _this.fill_ml_gaps = _fmg
        _this.make_forecast = _mf
        _this.recalculate_nan_forecasts = _rnf
        _this.canonical_ml_mode = _cmm

        yield

    finally:
        # 8. Restore sys.path exactly (removes entries added in step 4).
        sys.path[:] = _path_snap

        # 9. Remove stubs / production modules installed during setup.
        for key in list(sys.modules):
            if key in _HEAVY_DEPS or any(
                key == p or key.startswith(p + ".") for p in _PROD_MODULE_PREFIXES
            ):
                sys.modules.pop(key, None)

        # 10. Restore prior module identities (not just pop — prior objects back).
        for key, saved in _mod_snap.items():
            if saved is _SENTINEL:
                sys.modules.pop(key, None)  # was absent; keep absent
            else:
                sys.modules[key] = saved  # restore prior object identity

        # 11. Restore module-global bindings (prior objects, not just None).
        for attr, saved in _global_snap.items():
            if saved is _SENTINEL:
                setattr(_this, attr, None)  # restore to placeholder value
            else:
                setattr(_this, attr, saved)


@pytest.fixture(scope="module", autouse=True)
def _ml_module_env():
    """Install ML stubs, import production modules, restore everything on teardown.

    All snapshot/restore logic lives in _ml_env_ctx so isolation tests can
    exercise the same cleanup path without duplicating cleanup code.
    """
    with _ml_env_ctx():
        yield


# ===========================================================================
# 1. canonical_ml_mode() unit tests
# ===========================================================================


class TestCanonicalMlMode:
    """Unit tests for the canonical_ml_mode() helper."""

    def test_tft_pentad_returns_decad(self):
        assert canonical_ml_mode("TFT", "PENTAD") == "DECAD"

    def test_tide_pentad_returns_decad(self):
        assert canonical_ml_mode("TIDE", "PENTAD") == "DECAD"

    def test_tsmixer_pentad_returns_decad(self):
        assert canonical_ml_mode("TSMIXER", "PENTAD") == "DECAD"

    def test_tft_decad_returns_decad(self):
        assert canonical_ml_mode("TFT", "DECAD") == "DECAD"

    def test_tide_decad_returns_decad(self):
        assert canonical_ml_mode("TIDE", "DECAD") == "DECAD"

    def test_tsmixer_decad_returns_decad(self):
        assert canonical_ml_mode("TSMIXER", "DECAD") == "DECAD"

    def test_arima_pentad_returns_pentad(self):
        assert canonical_ml_mode("ARIMA", "PENTAD") == "PENTAD"

    def test_arima_decad_returns_decad(self):
        assert canonical_ml_mode("ARIMA", "DECAD") == "DECAD"

    def test_empty_model_passthrough(self):
        assert canonical_ml_mode("", "PENTAD") == "PENTAD"

    def test_none_model_passthrough(self):
        assert canonical_ml_mode(None, "PENTAD") == "PENTAD"

    def test_empty_mode_passthrough(self):
        assert canonical_ml_mode("TFT", "") == ""

    def test_none_mode_passthrough(self):
        assert canonical_ml_mode("TFT", None) is None

    def test_lowercase_tft_normalised(self):
        """Model name matching is case-insensitive."""
        assert canonical_ml_mode("tft", "PENTAD") == "DECAD"

    def test_lowercase_tide_normalised(self):
        assert canonical_ml_mode("tide", "PENTAD") == "DECAD"


# ===========================================================================
# 2. make_forecast.make_ml_forecast() entrypoint integration tests
#
#    These tests call the REAL make_ml_forecast() entry point with fake
#    data/filesystem/predictor boundaries.  They verify:
#      (a) forecast_horizon=11 (n=11 passed to predictor.predict())
#      (b) _Decad scaler asset selected (only _Decad dir exists; base dir
#          absent → FileNotFoundError if gen_mode reverts to PENTAD)
#      (c) output CSV has 11 unique target rows (issue_date+1..issue_date+11)
#
#    A test in this class CANNOT pass by copying the branch logic from
#    make_forecast.py — it must call make_ml_forecast() and observe output.
#    If make_forecast.py reverts gen_mode to PENTAD for TFT+PENTAD, these
#    tests fail because (b) raises FileNotFoundError and (a) captures n=6.
# ===========================================================================

_SCALER_BASE = "scaler_base"  # env-var value; production code appends _Decad


def _make_fake_predictor_class(captured_n: list):
    """Return a fake predictor class that records n and returns n target rows."""

    class FakePredictor:
        def __init__(self, model, scalers, static_features, dl_config_params, unique_id_col):
            pass

        def get_input_chunk_length(self):
            return 10

        def predict(self, df_rivers_org, df_covariates, code, n):
            captured_n.append(n)
            today = pd.Timestamp.today().normalize()
            rows = [
                {
                    "date": today + pd.Timedelta(days=i + 1),
                    "Q5": 1.0,
                    "Q25": 2.0,
                    "Q50": 3.0,
                    "Q75": 4.0,
                    "Q95": 5.0,
                }
                for i in range(n)
            ]
            return pd.DataFrame(rows)

    return FakePredictor


def _make_fake_predictor_class_fixed_date(captured_n: list, issue_date: datetime.date):
    """Fake predictor that returns rows relative to a caller-supplied issue_date."""

    class FakePredictor:
        def __init__(self, model, scalers, static_features, dl_config_params, unique_id_col):
            pass

        def get_input_chunk_length(self):
            return 10

        def predict(self, df_rivers_org, df_covariates, code, n):
            captured_n.append(n)
            base = pd.Timestamp(issue_date)
            rows = [
                {
                    "date": base + pd.Timedelta(days=i + 1),
                    "Q5": 1.0,
                    "Q25": 2.0,
                    "Q50": 3.0,
                    "Q75": 4.0,
                    "Q95": 5.0,
                }
                for i in range(n)
            ]
            return pd.DataFrame(rows)

    return FakePredictor


def _setup_scaler_fs(tmp_path, model):
    """Create the _Decad scaler directory with required files.

    Intentionally does NOT create the base directory (without _Decad suffix).
    If make_ml_forecast() reverts gen_mode to PENTAD for TFT/TIDE/TSMIXER,
    it will try to open {SCALER_BASE}_{model} (base, no suffix) which does
    not exist → FileNotFoundError → test fails with a clear signal.
    """
    models_dir = tmp_path / "models"
    # Only _Decad variant exists — base dir is absent on purpose.
    scaler_dir = models_dir / f"{_SCALER_BASE}_{model}_Decad"
    scaler_dir.mkdir(parents=True)

    (scaler_dir / "fake_model.pt").write_bytes(b"fake-checkpoint")

    (scaler_dir / "model_config.json").write_text(
        json.dumps({"input_chunk_length": 10, "output_chunk_length": 11, "num_samples": 1})
    )

    pd.DataFrame({"Unnamed: 0": [19999], "mean": [10.0], "std": [1.0]}).to_csv(
        scaler_dir / "scaler_stats_discharge.csv", index=False
    )
    pd.DataFrame({"Unnamed: 0": ["P", "T"], "mean": [0.0, 5.0], "std": [1.0, 1.0]}).to_csv(
        scaler_dir / "scaler_stats_era5.csv", index=False
    )
    pd.DataFrame({"Unnamed: 0": ["LAT", "LON"], "mean": [42.0, 74.0], "std": [1.0, 1.0]}).to_csv(
        scaler_dir / "scaler_stats_static.csv", index=False
    )

    return models_dir, scaler_dir


def _env_for_make_forecast(tmp_path, model, mode):
    """Return environment variable dict for make_ml_forecast()."""
    return {
        "SAPPHIRE_MODEL_TO_USE": model,
        "SAPPHIRE_PREDICTION_MODE": mode,
        "ieasyforecast_intermediate_data_path": str(tmp_path),
        "ieasyhydroforecast_models_and_scalers_path": str(tmp_path / "models"),
        f"ieasyhydroforecast_PATH_TO_SCALER_{model}": f"{_SCALER_BASE}_{model}",
        "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static.csv",
        "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        "ieasyhydroforecast_PATH_TO_QMAPPED_ERA5": "era5",
        "ieasyhydroforecast_HRU_CONTROL_MEMBER": "hru001",
        "ieasyhydroforecast_available_ML_models": "TFT,TIDE,TSMIXER,ARIMA",
        f"ieasyhydroforecast_THRESHOLD_MISSING_DAYS_{model}": "3",
        "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "2",
        "ieasyhydroforecast_forcing_gap_fill_recent_day_threshold": "7",
        "ieasyhydroforecast_forcing_gap_limit_recent": "1",
        "ieasyhydroforecast_forcing_gap_limit_past": "3",
    }


def _fake_hydroposts_df():
    """Minimal hydroposts DataFrame with one test station.

    virtual_station_name_twin is stored as dtype=object so that .iloc[0]
    returns Python's False singleton. make_forecast checks `is not False`
    (identity), so numpy.bool_(False) would incorrectly trigger the twin
    branch; object dtype preserves the Python literal.
    """
    df = pd.DataFrame(
        {
            "code": ["19999"],
            "TFT": [True],
            "TIDE": [True],
            "TSMIXER": [True],
            "ARIMA": [True],
        }
    )
    df["virtual_station_name_twin"] = pd.array([False], dtype=object)
    return df


def _fake_discharge(n_days=120):
    """Clean daily discharge DataFrame for station 19999."""
    today = pd.Timestamp.today().normalize()
    dates = pd.date_range(today - pd.Timedelta(days=n_days), today, freq="D")
    return pd.DataFrame({"code": "19999", "date": dates, "discharge": 10.0})


def _fake_era5(n_days=120):
    """Clean daily ERA5 DataFrame for station 19999 with forecast window."""
    today = pd.Timestamp.today().normalize()
    dates = pd.date_range(
        today - pd.Timedelta(days=n_days), today + pd.Timedelta(days=20), freq="D"
    )
    return pd.DataFrame({"code": 19999, "date": dates, "T": 5.0, "P": 1.0})


def _fake_static_features():
    """Minimal static features DataFrame."""
    return pd.DataFrame({"code": [19999], "LAT": [42.0], "LON": [74.0], "area": [1000.0]})


def _apply_make_forecast_patches(monkeypatch, model, mode, captured_n, tmp_path):
    """Apply all boundary patches needed to run make_ml_forecast() in isolation."""
    monkeypatch.setattr(make_forecast.sl, "load_environment", lambda: None)

    fake_predictor_cls = _make_fake_predictor_class(captured_n)
    monkeypatch.setattr(make_forecast, "get_predictor_class", lambda m: fake_predictor_cls)

    hydroposts = _fake_hydroposts_df()
    monkeypatch.setattr(
        make_forecast,
        "get_rivers_to_predict",
        lambda m: ([19999], hydroposts),
    )

    monkeypatch.setattr(make_forecast.fl, "read_daily_discharge_data", lambda: _fake_discharge())
    monkeypatch.setattr(make_forecast, "prepare_forcing_data", lambda **kwargs: _fake_era5())
    monkeypatch.setattr(make_forecast, "prepare_static_data", lambda path: _fake_static_features())
    monkeypatch.setattr(
        make_forecast.utils_ml_forecast,
        "get_codes_to_use",
        lambda discharge, era5, static: [19999],
    )
    monkeypatch.setattr(
        make_forecast.utils_ml_forecast,
        "fill_forcing_gaps",
        lambda df, *args, **kwargs: df,
    )

    import numpy as np

    monkeypatch.setattr(
        make_forecast.utils_ml_forecast,
        "calculate_pet_oudin",
        lambda df_era5, lat: np.ones(len(df_era5)),
    )
    monkeypatch.setattr(
        make_forecast.utils_ml_forecast,
        "calculate_daylight_hours",
        lambda lat, lon, df_era5: pd.Series(12.0, index=df_era5.index),
    )
    monkeypatch.setattr(
        make_forecast,
        "_read_ml_forecasts_from_api",
        lambda **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(make_forecast, "SAPPHIRE_API_AVAILABLE", False)

    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "True")
    for key, val in _env_for_make_forecast(tmp_path, model, mode).items():
        monkeypatch.setenv(key, val)


class TestMakeMlForecastEntrypoint:
    """Integration tests: call make_forecast.make_ml_forecast() directly.

    All data I/O and filesystem boundaries are faked via monkeypatch.
    The canonical_ml_mode routing, forecast_horizon selection, scaler-path
    construction, and CSV write inside make_ml_forecast() run real.

    Fail-proof design: only the _Decad scaler directory is created. If
    make_ml_forecast() reverts gen_mode to PENTAD (the pre-patch behavior),
    it looks for the base scaler directory which does not exist and raises
    FileNotFoundError before any predictor call — test fails unambiguously.
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
    def test_predictor_receives_n_11(self, model, mode, tmp_path, monkeypatch):
        """For TFT/TIDE/TSMIXER both PENTAD and DECAD must pass n=11 to predict()."""
        _setup_scaler_fs(tmp_path, model)
        captured_n = []
        _apply_make_forecast_patches(monkeypatch, model, mode, captured_n, tmp_path)

        make_forecast.make_ml_forecast()

        assert len(captured_n) == 1, (
            f"{model}+{mode}: expected exactly 1 predictor.predict() call, got {len(captured_n)}"
        )
        assert captured_n[0] == 11, (
            f"{model}+{mode}: predictor.predict() received n={captured_n[0]}, expected 11. "
            "If n=6, gen_mode reverted to PENTAD (canonical_ml_mode not applied in make_forecast)."
        )

    @pytest.mark.parametrize(
        "model,mode",
        [
            ("TFT", "PENTAD"),
            ("TIDE", "PENTAD"),
            ("TSMIXER", "PENTAD"),
        ],
    )
    def test_output_csv_has_11_unique_targets(self, model, mode, tmp_path, monkeypatch):
        """Output forecast_latest CSV must contain exactly 11 unique target dates."""
        _setup_scaler_fs(tmp_path, model)
        captured_n = []
        _apply_make_forecast_patches(monkeypatch, model, mode, captured_n, tmp_path)

        make_forecast.make_ml_forecast()

        prefix = "pentad" if mode == "PENTAD" else "decad"
        csv_path = tmp_path / "output" / model / f"{prefix}_{model}_forecast_latest.csv"
        assert csv_path.exists(), f"forecast_latest CSV not written at {csv_path}"

        df = pd.read_csv(csv_path)
        assert len(df) == 11, f"{model}+{mode}: forecast CSV has {len(df)} rows, expected 11."
        unique_dates = pd.to_datetime(df["date"]).nunique()
        assert unique_dates == 11, (
            f"{model}+{mode}: CSV has {unique_dates} unique target dates, expected 11."
        )

        today = pd.Timestamp.today().normalize()
        expected = {today + pd.Timedelta(days=i + 1) for i in range(11)}
        got = set(pd.to_datetime(df["date"]))
        assert got == expected, (
            f"{model}+{mode}: target dates mismatch.\n  expected: {sorted(expected)}\n  got: {sorted(got)}"
        )

    @pytest.mark.parametrize(
        "model,fake_issue_date",
        [
            ("TFT", datetime.date(2024, 12, 27)),  # targets span Dec 28 – Jan 7
            ("TSMIXER", datetime.date(2024, 2, 25)),  # targets span Feb 26 – Mar 7 (leap year)
        ],
    )
    def test_cross_year_pentad_produces_11_unique_targets(
        self, model, fake_issue_date, tmp_path, monkeypatch
    ):
        """With a forced cross-year/leap issue date, output still has 11 unique targets."""
        _setup_scaler_fs(tmp_path, model)
        captured_n = []

        # Inject a fixed issue date so targets span the year/month boundary.
        fake_dt = datetime.datetime(
            fake_issue_date.year, fake_issue_date.month, fake_issue_date.day
        )
        fake_predictor_cls = _make_fake_predictor_class_fixed_date(captured_n, fake_issue_date)
        _apply_make_forecast_patches(monkeypatch, model, "PENTAD", captured_n, tmp_path)
        # Override predictor class set by _apply_make_forecast_patches with date-aware variant.
        monkeypatch.setattr(make_forecast, "get_predictor_class", lambda m: fake_predictor_cls)
        # Override discharge and ERA5 data to centre on the fake date.
        base = pd.Timestamp(fake_issue_date)
        fake_discharge_df = pd.DataFrame(
            {
                "code": "19999",
                "date": pd.date_range(base - pd.Timedelta(days=120), base, freq="D"),
                "discharge": 10.0,
            }
        )
        fake_era5_df = pd.DataFrame(
            {
                "code": 19999,
                "date": pd.date_range(
                    base - pd.Timedelta(days=120), base + pd.Timedelta(days=20), freq="D"
                ),
                "T": 5.0,
                "P": 1.0,
            }
        )
        monkeypatch.setattr(
            make_forecast.fl, "read_daily_discharge_data", lambda: fake_discharge_df
        )
        monkeypatch.setattr(make_forecast, "prepare_forcing_data", lambda **kwargs: fake_era5_df)

        # Patch datetime.datetime.now() so make_forecast uses fake_issue_date as "today".
        with patch("make_forecast.datetime") as mock_dt_module:
            mock_dt_module.datetime.now.return_value = fake_dt
            make_forecast.make_ml_forecast()

        assert len(captured_n) == 1
        assert captured_n[0] == 11, (
            f"{model} cross-year {fake_issue_date}: predictor received n={captured_n[0]}, expected 11."
        )

        csv_path = tmp_path / "output" / model / f"pentad_{model}_forecast_latest.csv"
        assert csv_path.exists()
        df = pd.read_csv(csv_path)
        assert len(df) == 11
        assert pd.to_datetime(df["date"]).nunique() == 11, (
            f"{model} cross-year {fake_issue_date}: expected 11 unique target dates."
        )


# ===========================================================================
# 3. Functional test: fill_ml_gaps.call_hindcast_script uses canonical path
# ===========================================================================


def _make_hindcast_csv(path: str, model: str = "TFT", n_rows: int = 3):
    """Write a minimal hindcast CSV at the given path."""
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "code": "19999",
                "forecast_date": f"2024-01-0{i + 1}",
                "date": f"2024-01-0{i + 6}",
                "flag": 0,
                "Q5": 45.0,
                "Q25": 75.0,
                "Q50": 95.0,
                "Q75": 115.0,
                "Q95": 145.0,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


class TestFillMlGapsCallHindcastScriptCanonicalPath:
    """fill_ml_gaps.call_hindcast_script must read from the DECAD filename
    when called with TFT+PENTAD.
    """

    def test_tft_pentad_reads_decad_filename(self, tmp_path):
        """When MODEL_TO_USE=TFT and PREDICTION_MODE=PENTAD, the function
        must read a file named TFT_DECAD_hindcast_daily_... (not PENTAD).
        """
        model = "TFT"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        # Create the CANONICAL (DECAD) path that the function should look for
        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        canonical_file = hindcast_dir / f"{model}_DECAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(canonical_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = fill_ml_gaps.call_hindcast_script(
                    min_missing_date=start,
                    max_missing_date=end,
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from canonical DECAD path"

    def test_arima_pentad_reads_pentad_filename(self, tmp_path):
        """When MODEL_TO_USE=ARIMA and PREDICTION_MODE=PENTAD, the function
        must read a file named ARIMA_PENTAD_hindcast_daily_... (unchanged).
        """
        model = "ARIMA"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        arima_file = hindcast_dir / f"{model}_PENTAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(arima_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = fill_ml_gaps.call_hindcast_script(
                    min_missing_date=start,
                    max_missing_date=end,
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from ARIMA PENTAD path"

    def test_nonzero_exit_with_existing_artifact_raises(self, tmp_path):
        """Nonzero returncode must raise RuntimeError before the artifact is read,
        even when a canonical DECAD file from a prior run exists on disk."""
        model = "TFT"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        # File exists — must NOT be consumed on subprocess failure.
        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        canonical_file = hindcast_dir / f"{model}_DECAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(canonical_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="child failed")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                with pytest.raises(RuntimeError, match="return code"):
                    fill_ml_gaps.call_hindcast_script(
                        min_missing_date=start,
                        max_missing_date=end,
                        MODEL_TO_USE=model,
                        intermediate_data_path=str(tmp_path),
                        PREDICTION_MODE=mode,
                    )

    def test_nonzero_exit_without_artifact_raises_runtime_not_file(self, tmp_path):
        """Nonzero returncode with no artifact must raise RuntimeError, not
        FileNotFoundError — proving the read is never reached."""
        model = "TFT"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        # No CSV written — absence must be irrelevant.

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=2, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                with pytest.raises(RuntimeError):
                    fill_ml_gaps.call_hindcast_script(
                        min_missing_date=start,
                        max_missing_date=end,
                        MODEL_TO_USE=model,
                        intermediate_data_path=str(tmp_path),
                        PREDICTION_MODE=mode,
                    )


# ===========================================================================
# 4. Functional test: recalculate_nan_forecasts.call_hindcast_script uses
#    canonical path
# ===========================================================================


class TestRecalculateNanCallHindcastScriptCanonicalPath:
    """recalculate_nan_forecasts.call_hindcast_script must read from the
    DECAD filename when called with TFT+PENTAD.
    """

    def test_tft_pentad_reads_decad_filename(self, tmp_path):
        model = "TFT"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        canonical_file = hindcast_dir / f"{model}_DECAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(canonical_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = recalculate_nan_forecasts.call_hindcast_script(
                    min_missing_date=start,
                    max_missing_date=end,
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    codes_with_nan=["19999"],
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from canonical DECAD path"

    def test_arima_pentad_reads_pentad_filename(self, tmp_path):
        """ARIMA with PENTAD must remain at PENTAD filename (not normalized)."""
        model = "ARIMA"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        arima_file = hindcast_dir / f"{model}_PENTAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(arima_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "SAPPHIRE_PREDICTION_MODE": mode,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = recalculate_nan_forecasts.call_hindcast_script(
                    min_missing_date=start,
                    max_missing_date=end,
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    codes_with_nan=["19999"],
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from ARIMA PENTAD path"


# ===========================================================================
# 5. Functional test: add_new_station.call_hindcast_script uses canonical path
#    (missing from previous draft)
# ===========================================================================


class TestAddNewStationCallHindcastScriptCanonicalPath:
    """add_new_station.call_hindcast_script must read from the DECAD filename
    when called with TFT+PENTAD, and from PENTAD when ARIMA+PENTAD.
    """

    def test_tft_pentad_reads_decad_filename(self, tmp_path):
        model = "TFT"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        canonical_file = hindcast_dir / f"{model}_DECAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(canonical_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = add_new_station.call_hindcast_script(
                    start_date=start,
                    end_date=end,
                    codes_hindcast=["19999"],
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from canonical DECAD path"

    def test_arima_pentad_reads_pentad_filename(self, tmp_path):
        """ARIMA+PENTAD must stay at PENTAD filename."""
        model = "ARIMA"
        mode = "PENTAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        arima_file = hindcast_dir / f"{model}_PENTAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(arima_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                result = add_new_station.call_hindcast_script(
                    start_date=start,
                    end_date=end,
                    codes_hindcast=["19999"],
                    MODEL_TO_USE=model,
                    intermediate_data_path=str(tmp_path),
                    PREDICTION_MODE=mode,
                )

        assert result is not None
        assert len(result) > 0, "Expected non-empty DataFrame from ARIMA PENTAD path"

    def test_nonzero_exit_with_existing_artifact_raises(self, tmp_path):
        """Nonzero returncode must raise RuntimeError before the artifact is read,
        even when the canonical DECAD file exists from a prior run."""
        model = "TFT"
        mode = "DECAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)
        canonical_file = hindcast_dir / f"{model}_DECAD_hindcast_daily_{start}_{end}.csv"
        _make_hindcast_csv(str(canonical_file), model=model)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                with pytest.raises(RuntimeError, match="return code"):
                    add_new_station.call_hindcast_script(
                        start_date=start,
                        end_date=end,
                        codes_hindcast=["19999"],
                        MODEL_TO_USE=model,
                        intermediate_data_path=str(tmp_path),
                        PREDICTION_MODE=mode,
                    )

    def test_nonzero_exit_without_artifact_raises_runtime_not_file(self, tmp_path):
        """Nonzero returncode with no artifact on disk must raise RuntimeError,
        not FileNotFoundError — proving the read is never reached."""
        model = "TFT"
        mode = "DECAD"
        start = "2024-01-01"
        end = "2024-01-03"

        hindcast_dir = tmp_path / "output" / "hindcast" / model
        hindcast_dir.mkdir(parents=True)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=2, stdout="", stderr="")
            with patch.dict(
                os.environ,
                {
                    "SAPPHIRE_MODEL_TO_USE": model,
                    "ieasyforecast_intermediate_data_path": str(tmp_path),
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                },
            ):
                with pytest.raises(RuntimeError):
                    add_new_station.call_hindcast_script(
                        start_date=start,
                        end_date=end,
                        codes_hindcast=["19999"],
                        MODEL_TO_USE=model,
                        intermediate_data_path=str(tmp_path),
                        PREDICTION_MODE=mode,
                    )


# ===========================================================================
# 6. add_new_station.main() orchestration test
#    Overlapping new codes → exactly ONE hindcast call for canonical model,
#    correct union, correct per-horizon distribution, no healthy overwrite.
# ===========================================================================


def _read_forecast_csv(path) -> pd.DataFrame:
    """Read a forecast CSV with date columns parsed, code column as str."""
    df = pd.read_csv(str(path), parse_dates=["date", "forecast_date"])
    df["code"] = df["code"].astype(str)
    return df


def _canonical_sort(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by canonical key (code, forecast_date, date) and reset index."""
    df = df.copy()
    df["code"] = df["code"].astype(str)
    return df.sort_values(["code", "forecast_date", "date"]).reset_index(drop=True)


class TestAddNewStationOrchestration:
    """add_new_station.main() with asymmetric archive keys must call
    call_hindcast_script exactly ONCE for canonical models (union of all
    genuinely missing codes), distribute correctly to each horizon output,
    and preserve existing healthy values without overwriting them.

    Archive layout:
      A = 19991: present (healthy, Q50=10.0) in pentad only
      B = 19992: present (healthy, Q50=10.0) in decad only
      C = 19993: absent from both

    Expected new_codes_pentad = {B, C}; new_codes_decad = {A, C}.
    Union = {A, B, C} → ONE DECAD hindcast call for canonical models.
    Hindcast rows use date=2024-01-05, archive rows date=2024-01-01 —
    different keys so dedup keeps both if a filter leaks wrongly.
    """

    def _run_main_with_overlapping_codes(self, tmp_path, monkeypatch, model):
        """Shared setup for asymmetric-archive orchestration tests.

        Returns (hindcast_calls, output_dir).

        Archive layout (actual CSV files on disk — drives the CSV fallback path
        when the API fake returns empty for every per-code call):
          A = 19991: present (all 5 quantiles) in pentad CSV only
          B = 19992: present (all 5 quantiles) in decad CSV only
          C = 19993: absent from both CSVs

        API fake returns empty → _read_scoped returns empty for both horizons
        → production fallback reads the archive CSVs.

        Expected: new_codes_pentad = {B, C}; new_codes_decad = {A, C}.
        Union = {A, B, C} → ONE DECAD hindcast call for canonical models.
        Archive date (2024-01-01) ≠ hindcast date (2024-01-05), so dedup
        never collapses an archive row with a hindcast row.
        """
        era5_dir = tmp_path / "era5"
        era5_dir.mkdir()
        (era5_dir / "dummy.nc").touch()
        output_dir = tmp_path / "output" / model
        output_dir.mkdir(parents=True)

        # ── Pre-create asymmetric CSV archives ───────────────────────────────
        # Full required columns: code, forecast_date, date, flag, Q5–Q95.
        # A in pentad only; B in decad only; C absent from both.
        _ARCH_Q5, _ARCH_Q25, _ARCH_Q50 = 5.0, 7.5, 10.0
        _ARCH_Q75, _ARCH_Q95 = 12.5, 15.0

        def _archive_csv(code):
            return pd.DataFrame(
                [
                    {
                        "code": str(code),
                        "forecast_date": "2024-01-01",
                        "date": "2024-01-01",
                        "flag": 4,
                        "Q5": _ARCH_Q5,
                        "Q25": _ARCH_Q25,
                        "Q50": _ARCH_Q50,
                        "Q75": _ARCH_Q75,
                        "Q95": _ARCH_Q95,
                    }
                ]
            )

        _archive_csv("19991").to_csv(output_dir / f"pentad_{model}_forecast.csv", index=False)
        _archive_csv("19992").to_csv(output_dir / f"decad_{model}_forecast.csv", index=False)

        monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", model)
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
        monkeypatch.setenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE", "output")
        monkeypatch.setenv("ieasyhydroforecast_OUTPUT_PATH_REANALYSIS", "era5")

        # All three codes scoped
        monkeypatch.setattr(
            add_new_station,
            "get_permitted_station_codes",
            lambda: ["19991", "19992", "19993"],
        )

        # API returns empty for ALL per-code calls → drives existing CSV fallback.
        # (Both _read_scoped calls return empty DataFrame → condition
        # `not decad_api.empty and not pentad_api.empty` is False → CSV path.)
        monkeypatch.setattr(
            add_new_station,
            "_read_ml_forecasts_from_api",
            lambda **kwargs: pd.DataFrame(),
        )

        hydroposts = pd.DataFrame(
            {
                "code": ["19991", "19992", "19993"],
                "TFT": [True, True, True],
                "TIDE": [True, True, True],
                "TSMIXER": [True, True, True],
                "ARIMA": [True, True, True],
            }
        )
        monkeypatch.setattr(
            add_new_station.utils_ml_forecast,
            "get_hydroposts_for_pentadal_and_decadal_forecasts",
            lambda: (
                ["19991", "19992", "19993"],
                ["19991", "19992", "19993"],
                hydroposts,
            ),
        )

        hindcast_calls = []
        # Hindcast values are clearly distinct from archive values (Q50 20 vs 10)
        # and use a different date (2024-01-05 vs 2024-01-01) so dedup never
        # collapses an archive row with a hindcast row.
        _HIND_Q5, _HIND_Q25, _HIND_Q50 = 10.0, 15.0, 20.0
        _HIND_Q75, _HIND_Q95 = 25.0, 30.0

        def fake_call_hindcast(
            start_date,
            end_date,
            codes_hindcast,
            MODEL_TO_USE,
            intermediate_data_path,
            PREDICTION_MODE,
        ):
            hindcast_calls.append(
                {
                    "codes": sorted(str(c) for c in codes_hindcast),
                    "mode": PREDICTION_MODE,
                }
            )
            rows = [
                {
                    "code": str(c),
                    "forecast_date": "2024-01-01",
                    "date": "2024-01-05",
                    "flag": 4,
                    "Q5": _HIND_Q5,
                    "Q25": _HIND_Q25,
                    "Q50": _HIND_Q50,
                    "Q75": _HIND_Q75,
                    "Q95": _HIND_Q95,
                }
                for c in codes_hindcast
            ]
            return pd.DataFrame(rows)

        monkeypatch.setattr(add_new_station, "call_hindcast_script", fake_call_hindcast)
        monkeypatch.setattr(add_new_station, "SAPPHIRE_API_AVAILABLE", False)

        add_new_station.main()
        return hindcast_calls, output_dir

    def test_canonical_model_calls_hindcast_once_for_union(self, tmp_path, monkeypatch):
        """TFT: ONE hindcast call whose code set equals the union {A, B, C}."""
        calls, _ = self._run_main_with_overlapping_codes(tmp_path, monkeypatch, "TFT")

        assert len(calls) == 1, f"Expected 1 hindcast call for TFT, got {len(calls)}: {calls}"
        assert calls[0]["mode"] == "DECAD", (
            f"Canonical TFT hindcast must use mode=DECAD, got {calls[0]['mode']!r}"
        )
        assert set(calls[0]["codes"]) == {"19991", "19992", "19993"}, (
            f"Expected union {{19991, 19992, 19993}}, got {set(calls[0]['codes'])}"
        )

    def test_canonical_per_horizon_distribution(self, tmp_path, monkeypatch):
        """Per-horizon filter distributes correctly.

        Pentad: A preserved (full archive payload), B and C added (full hindcast payload).
        Decad:  B preserved (full archive payload), A and C added (full hindcast payload).

        Row-count == 3 assertion is mutation-sensitive: removing the per-horizon
        filter leaks all union codes into each output (4 rows) and the full-payload
        comparison fails for the wrongly-placed code.  The exact assert_frame_equal
        additionally catches corruption of any quantile (Q5/Q25/Q75/Q95), changed
        dates, or unexpected extra rows.
        """
        _, output_dir = self._run_main_with_overlapping_codes(tmp_path, monkeypatch, "TFT")

        # Archive values (A in pentad, B in decad)
        _AQ5, _AQ25, _AQ50, _AQ75, _AQ95 = 5.0, 7.5, 10.0, 12.5, 15.0
        # Hindcast values (new codes, full 5-quantile payload)
        _HQ5, _HQ25, _HQ50, _HQ75, _HQ95 = 10.0, 15.0, 20.0, 25.0, 30.0
        _AD = pd.Timestamp("2024-01-01")  # archive date
        _HD = pd.Timestamp("2024-01-05")  # hindcast date
        _FD = pd.Timestamp("2024-01-01")  # forecast_date (same for all rows)

        # ── Expected pentad: A (archive) + B (hindcast) + C (hindcast) ──────
        expected_pentad = _canonical_sort(
            pd.DataFrame(
                [
                    # A: archive row — all 5 quantiles must be exact
                    {
                        "code": "19991",
                        "forecast_date": _FD,
                        "date": _AD,
                        "flag": 4,
                        "Q5": _AQ5,
                        "Q25": _AQ25,
                        "Q50": _AQ50,
                        "Q75": _AQ75,
                        "Q95": _AQ95,
                    },
                    # B: new to pentad — full hindcast payload
                    {
                        "code": "19992",
                        "forecast_date": _FD,
                        "date": _HD,
                        "flag": 4,
                        "Q5": _HQ5,
                        "Q25": _HQ25,
                        "Q50": _HQ50,
                        "Q75": _HQ75,
                        "Q95": _HQ95,
                    },
                    # C: new to both — full hindcast payload
                    {
                        "code": "19993",
                        "forecast_date": _FD,
                        "date": _HD,
                        "flag": 4,
                        "Q5": _HQ5,
                        "Q25": _HQ25,
                        "Q50": _HQ50,
                        "Q75": _HQ75,
                        "Q95": _HQ95,
                    },
                ]
            )
        )

        # ── Expected decad: B (archive) + A (hindcast) + C (hindcast) ───────
        expected_decad = _canonical_sort(
            pd.DataFrame(
                [
                    # A: new to decad — full hindcast payload
                    {
                        "code": "19991",
                        "forecast_date": _FD,
                        "date": _HD,
                        "flag": 4,
                        "Q5": _HQ5,
                        "Q25": _HQ25,
                        "Q50": _HQ50,
                        "Q75": _HQ75,
                        "Q95": _HQ95,
                    },
                    # B: archive row — all 5 quantiles must be exact
                    {
                        "code": "19992",
                        "forecast_date": _FD,
                        "date": _AD,
                        "flag": 4,
                        "Q5": _AQ5,
                        "Q25": _AQ25,
                        "Q50": _AQ50,
                        "Q75": _AQ75,
                        "Q95": _AQ95,
                    },
                    # C: new to both — full hindcast payload
                    {
                        "code": "19993",
                        "forecast_date": _FD,
                        "date": _HD,
                        "flag": 4,
                        "Q5": _HQ5,
                        "Q25": _HQ25,
                        "Q50": _HQ50,
                        "Q75": _HQ75,
                        "Q95": _HQ95,
                    },
                ]
            )
        )

        actual_pentad = _canonical_sort(_read_forecast_csv(output_dir / "pentad_TFT_forecast.csv"))
        actual_decad = _canonical_sort(_read_forecast_csv(output_dir / "decad_TFT_forecast.csv"))

        assert len(actual_pentad) == 3, (
            f"Pentad must have exactly 3 rows (A-archive + B-hindcast + C-hindcast), "
            f"got {len(actual_pentad)}.  Filter leak → extra rows."
        )
        assert len(actual_decad) == 3, (
            f"Decad must have exactly 3 rows (B-archive + A-hindcast + C-hindcast), "
            f"got {len(actual_decad)}.  Filter leak → extra rows."
        )

        pd.testing.assert_frame_equal(
            actual_pentad,
            expected_pentad,
            check_dtype=False,
            check_like=True,
            obj="pentad output",
        )
        pd.testing.assert_frame_equal(
            actual_decad,
            expected_decad,
            check_dtype=False,
            check_like=True,
            obj="decad output",
        )

    def test_existing_healthy_code_preserved(self, tmp_path, monkeypatch):
        """A (19991) is the existing healthy pentad code; B (19992) is the
        existing healthy decad code.  Both must be preserved with their
        COMPLETE archive payload (all 5 quantiles), not just Q50.

        assert_frame_equal on the filtered rows catches corruption of any
        quantile column, a changed date, or a duplicate row that happens to
        preserve Q50 while doubling the entry.
        """
        _, output_dir = self._run_main_with_overlapping_codes(tmp_path, monkeypatch, "TFT")

        _AQ5, _AQ25, _AQ50, _AQ75, _AQ95 = 5.0, 7.5, 10.0, 12.5, 15.0
        _AD = pd.Timestamp("2024-01-01")
        _FD = pd.Timestamp("2024-01-01")

        # A (19991) must survive in pentad with the exact archive payload
        actual_pentad_a = (
            _read_forecast_csv(output_dir / "pentad_TFT_forecast.csv")
            .pipe(lambda df: df[df["code"] == "19991"])
            .reset_index(drop=True)
        )
        expected_a = pd.DataFrame(
            [
                {
                    "code": "19991",
                    "forecast_date": _FD,
                    "date": _AD,
                    "flag": 4,
                    "Q5": _AQ5,
                    "Q25": _AQ25,
                    "Q50": _AQ50,
                    "Q75": _AQ75,
                    "Q95": _AQ95,
                }
            ]
        )
        assert len(actual_pentad_a) == 1, (
            "A must be preserved in pentad with exactly 1 row "
            "(2 rows → hindcast leaked into archive slot)"
        )
        pd.testing.assert_frame_equal(
            actual_pentad_a,
            expected_a,
            check_dtype=False,
            check_like=True,
            obj="A pentad archive row",
        )

        # B (19992) must survive in decad with the exact archive payload
        actual_decad_b = (
            _read_forecast_csv(output_dir / "decad_TFT_forecast.csv")
            .pipe(lambda df: df[df["code"] == "19992"])
            .reset_index(drop=True)
        )
        expected_b = pd.DataFrame(
            [
                {
                    "code": "19992",
                    "forecast_date": _FD,
                    "date": _AD,
                    "flag": 4,
                    "Q5": _AQ5,
                    "Q25": _AQ25,
                    "Q50": _AQ50,
                    "Q75": _AQ75,
                    "Q95": _AQ95,
                }
            ]
        )
        assert len(actual_decad_b) == 1, (
            "B must be preserved in decad with exactly 1 row "
            "(2 rows → hindcast leaked into archive slot)"
        )
        pd.testing.assert_frame_equal(
            actual_decad_b,
            expected_b,
            check_dtype=False,
            check_like=True,
            obj="B decad archive row",
        )

    def test_arima_uses_legacy_two_call_flow(self, tmp_path, monkeypatch):
        """ARIMA: two hindcast calls (one PENTAD, one DECAD); codes asymmetric."""
        calls, _ = self._run_main_with_overlapping_codes(tmp_path, monkeypatch, "ARIMA")

        assert len(calls) == 2, f"Expected 2 hindcast calls for ARIMA, got {len(calls)}: {calls}"
        modes = {c["mode"] for c in calls}
        assert "PENTAD" in modes, "ARIMA legacy flow must include a PENTAD call"
        assert "DECAD" in modes, "ARIMA legacy flow must include a DECAD call"


# ===========================================================================
# 7. Behavior test: recalculate_nan_forecasts only selects flag 1/2 codes
#    (replaces source-text assertion)
# ===========================================================================


class TestRecalculateNanFlagBehavior:
    """recalculate_nan_forecasts.recalculate_nan_forecasts() must only pass
    flag-1 and flag-2 codes to call_hindcast_script. Flag-3 and flag-4 rows
    must be excluded. This test would fail on old code using isin([1, 2, 3]).
    """

    def test_only_flag1_and_flag2_codes_trigger_hindcast(self, tmp_path, monkeypatch):
        model = "TFT"
        mode = "PENTAD"

        # Output directory needed for CSV write at end of function
        output_dir = tmp_path / "output" / model
        output_dir.mkdir(parents=True)

        monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", model)
        monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", mode)
        monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
        monkeypatch.setenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE", "output")

        # Four codes with different flags:
        #   19991 → flag=1 → should trigger hindcast
        #   19992 → flag=2 → should trigger hindcast
        #   19993 → flag=3 → must NOT trigger hindcast (P1 opt-in retry, not default)
        #   19994 → flag=4 → must NOT trigger hindcast (healthy)
        base = pd.Timestamp("2024-01-01")

        def make_rows(code, flag_val):
            return [
                {
                    "code": str(code),
                    "forecast_date": (base + pd.Timedelta(days=i)).isoformat(),
                    "date": (base + pd.Timedelta(days=i)).isoformat(),
                    "flag": flag_val,
                    "Q50": float("nan") if flag_val <= 2 else 10.0,
                }
                for i in range(3)
            ]

        code_data = {
            "19991": pd.DataFrame(make_rows("19991", 1)),
            "19992": pd.DataFrame(make_rows("19992", 2)),
            "19993": pd.DataFrame(make_rows("19993", 3)),
            "19994": pd.DataFrame(make_rows("19994", 4)),
        }

        monkeypatch.setattr(
            recalculate_nan_forecasts,
            "get_permitted_station_codes",
            lambda: list(code_data.keys()),
        )

        def fake_read_api(model_type, horizon_type, code=None, **kwargs):
            if code is not None and str(code) in code_data:
                return code_data[str(code)].copy()
            return pd.DataFrame()

        monkeypatch.setattr(recalculate_nan_forecasts, "_read_ml_forecasts_from_api", fake_read_api)

        captured_codes = []

        def fake_call_hindcast(
            min_missing_date,
            max_missing_date,
            MODEL_TO_USE,
            intermediate_data_path,
            codes_with_nan,
            PREDICTION_MODE,
        ):
            captured_codes.extend([str(c) for c in codes_with_nan])
            rows = [
                {
                    "code": str(c),
                    "forecast_date": min_missing_date,
                    "date": min_missing_date,
                    "flag": 4,
                    "Q50": 20.0,
                }
                for c in codes_with_nan
            ]
            return pd.DataFrame(rows)

        monkeypatch.setattr(recalculate_nan_forecasts, "call_hindcast_script", fake_call_hindcast)
        monkeypatch.setattr(recalculate_nan_forecasts, "SAPPHIRE_API_AVAILABLE", False)
        monkeypatch.setattr(
            recalculate_nan_forecasts,
            "normalize_ml_csv_columns",
            lambda df: df,
        )

        recalculate_nan_forecasts.recalculate_nan_forecasts()

        assert "19991" in captured_codes, "Flag-1 code must trigger hindcast"
        assert "19992" in captured_codes, "Flag-2 code must trigger hindcast"
        assert "19993" not in captured_codes, (
            "Flag-3 code must NOT trigger hindcast (P1 opt-in only)"
        )
        assert "19994" not in captured_codes, "Flag-4 (healthy) code must NOT trigger hindcast"


# ===========================================================================
# 8. _ml_env_ctx isolation tests
#    Verify that _ml_env_ctx restores sys.path and sys.modules identities on
#    BOTH normal completion and setup failure — using the actual context
#    manager, not copied cleanup code.
# ===========================================================================


class TestMlEnvCtxIsolation:
    """_ml_env_ctx must leave sys.path and sys.modules unchanged after any exit.

    These tests call _ml_env_ctx directly (nested inside the active module-scope
    _ml_module_env) so the same cleanup code is exercised.  The setup failure
    is injected by monkeypatching importlib.import_module to raise at a safe
    boundary (the 'make_forecast' import).
    """

    def test_setup_failure_restores_path_and_modules(self, monkeypatch):
        """ImportError during setup must not leave sys.path extended or
        sys.modules polluted.

        Tests two cases in one call:
        - Preloaded inert sentinel: 'darts' is replaced with a test-specific
          object before entry; context must restore it to that object after
          failure (not leave the newly-installed stub in place).
        - Absent module: 'torch' is removed before entry; context must keep
          it absent after failure (must not leave a stub behind).

        Neither 'darts' nor 'torch' is the real library — the stubs from
        _ml_module_env are object()s or MagicMocks, never the actual packages.
        """
        # Snapshot what _ml_module_env currently holds for these keys so we
        # can restore for the other tests after this test manipulates them.
        _orig_darts = sys.modules.get("darts", _SENTINEL)
        _orig_torch = sys.modules.get("torch", _SENTINEL)

        # Preloaded inert sentinel: a fresh object stands in for 'darts'.
        _preloaded_sentinel = object()
        monkeypatch.setitem(sys.modules, "darts", _preloaded_sentinel)
        # Absent module: remove 'torch' so _ml_env_ctx snapshot records _SENTINEL.
        monkeypatch.delitem(sys.modules, "torch", raising=False)

        path_before = list(sys.path)

        # Capture the real import_module before patching it.
        _real_import = importlib.import_module

        def _fail_at_make_forecast(name, *args, **kwargs):
            if name == "make_forecast":
                raise ImportError("test-only-failure-19999")
            return _real_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", _fail_at_make_forecast)

        with pytest.raises(ImportError, match="test-only-failure-19999"):
            with _ml_env_ctx():
                pytest.fail("should not reach yield body")

        # ── Assert state immediately after context exit ──────────────────
        # sys.path must be restored to exactly the pre-entry list.
        assert list(sys.path) == path_before, (
            "_ml_env_ctx must restore sys.path after ImportError during setup"
        )

        # Preloaded sentinel must be back (not replaced by the new stub that
        # was installed during step 5 and should now be cleaned up).
        assert sys.modules.get("darts") is _preloaded_sentinel, (
            "_ml_env_ctx must restore the preloaded 'darts' sentinel after failure, "
            "not leave the newly-installed MagicMock stub"
        )

        # Absent module must remain absent (context must not leave a stub behind).
        assert "torch" not in sys.modules, (
            "_ml_env_ctx must keep 'torch' absent after failure — "
            "it was absent at entry and context must not leave a stub"
        )
