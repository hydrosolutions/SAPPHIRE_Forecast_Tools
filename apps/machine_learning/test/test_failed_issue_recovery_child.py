"""Tests for the P1b recovery compute-only child boundary (ML-020).

WHAT IS TESTED (the REAL hindcast_ML_models.main() and the REAL
compute_recovery_unit() wrapper, not copies of their logic):

1. Recovery compute-only mode routes through predict() exactly once; no
   historical_forecasts() call.
2. Observations passed to predict() end exactly at issue_date; no future data.
3. _write_ml_forecast_to_api and to_csv are not called in compute-only mode.
4. JSON request/response lifecycle: malformed/stale/mismatched/no-output/
   nonzero-exit/timeout all return None from compute_recovery_unit().
5. Input scope: missing one history day / forcing only through 11 when
   output_chunk_length > 11 blocks before predict().
6. output_chunk_length < 11 blocks before inference.
7. Missing Q10 / inf Q50 / extra issue date / duplicate targets / missing
   target all block validation.
8. API/ML/model/station opt-out: compute is refused, predict() never called.
9. Existing default entrypoint (no compute-only marker) is byte-equivalent —
   hindcast() is still called for the ordinary path.

ISOLATION STRATEGY:
All heavy-dependency mocking and production module imports live inside the
module-scoped _ml_child_env_ctx context manager, which snapshots and restores
sys.modules, sys.path, and module-global bindings in try/finally.  No
collection-time mutations.

FAKE BOUNDARY PROCESS ADAPTER:
Tests that exercise the full compute path call _run_main_in_process(), which
imports hindcast_ML_models.main() under isolated config and a FakePredictor
that records calls.  This is NOT a mock of main(); it IS main() running with
controlled inputs.  Writer spies (api_write, to_csv) raise AssertionError if
called in compute-only mode.

All station codes are synthetic (19xxx range per repo convention).
No real API, no model inference, no database, no real CSV reads.
"""

import datetime
import importlib
import json
import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module-level placeholders; populated by the module-scoped fixture.
# ---------------------------------------------------------------------------
hindcast_ML_models = None  # noqa: N816
recalculate_nan_forecasts = None
compute_recovery_unit = None

# ---------------------------------------------------------------------------
# Heavy deps to stub; mirrors test_canonical_hindcast_entrypoint.py.
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
    "hindcast_ML_models",
    "recalculate_nan_forecasts",
    "scr",
    "forecast_library",
    "setup_library",
    "add_new_station",
    "fill_ml_gaps",
    "make_forecast",
)

_SENTINEL = object()
_GLOBAL_ATTRS = ("hindcast_ML_models", "recalculate_nan_forecasts", "compute_recovery_unit")

_ML_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_IEASY_DIR = os.path.abspath(os.path.join(_ML_DIR, "..", "iEasyHydroForecast"))

# ---------------------------------------------------------------------------
# Synthetic codes (repo convention: 19xxx range, no real station codes)
# ---------------------------------------------------------------------------
CODE_A = "19011"

# Issue date for all tests
ISSUE_DATE = "2026-09-20"
ISSUE_DT = datetime.date.fromisoformat(ISSUE_DATE)

# ---------------------------------------------------------------------------
# Input chunk lengths for the three DL models (per spec: TFT=30, TiDE=20, TSMixer=20)
# ---------------------------------------------------------------------------
_MODEL_ICL = {"TFT": 30, "TIDE": 20, "TSMIXER": 20}
_OUTPUT_CHUNK_LENGTH = 11


# ---------------------------------------------------------------------------
# Module isolation context (shared between module fixture and isolation tests)
# ---------------------------------------------------------------------------


@contextmanager
def _ml_child_env_ctx():
    """Snapshot+restore context for ML module imports.

    Manages sys.path, sys.modules, and module-global bindings.
    All setup/teardown wrapped in try/finally so setup failures leave no residue.
    """
    _this = sys.modules[__name__]
    _path_snap = list(sys.path)
    _mod_snap: dict = {}
    for key in list(sys.modules):
        if key in _HEAVY_DEPS or any(
            key == p or key.startswith(p + ".") for p in _PROD_MODULE_PREFIXES
        ):
            _mod_snap[key] = sys.modules.pop(key)
    for key in _HEAVY_DEPS:
        if key not in _mod_snap:
            _mod_snap[key] = _SENTINEL
    _global_snap = {attr: getattr(_this, attr, _SENTINEL) for attr in _GLOBAL_ATTRS}

    try:
        sys.path.insert(
            0,
            os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast")
            ),
        )
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scr")))
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

        for key in _HEAVY_DEPS:
            sys.modules[key] = MagicMock()

        prior_cwd = os.getcwd()
        with tempfile.TemporaryDirectory(prefix="sap_ml_child_test_") as _td:
            os.chdir(_td)
            try:
                _hm = importlib.import_module("hindcast_ML_models")
                _rnf = importlib.import_module("recalculate_nan_forecasts")
            finally:
                os.chdir(prior_cwd)

        _this.hindcast_ML_models = _hm
        _this.recalculate_nan_forecasts = _rnf
        _this.compute_recovery_unit = _rnf.compute_recovery_unit
        yield

    finally:
        sys.path[:] = _path_snap
        for key in list(sys.modules):
            if key in _HEAVY_DEPS or any(
                key == p or key.startswith(p + ".") for p in _PROD_MODULE_PREFIXES
            ):
                sys.modules.pop(key, None)
        for key, saved in _mod_snap.items():
            if saved is _SENTINEL:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = saved
        for attr, saved in _global_snap.items():
            if saved is _SENTINEL:
                setattr(_this, attr, None)
            else:
                setattr(_this, attr, saved)


@pytest.fixture(scope="module", autouse=True)
def _ml_child_module_env():
    with _ml_child_env_ctx():
        yield


# ---------------------------------------------------------------------------
# FakePredictor — records predict() and hindcast() calls; never calls
# historical_forecasts (that would be the BaseDartsDLPredictor.hindcast path).
# ---------------------------------------------------------------------------


class FakePredictor:
    """Records all predict() and hindcast() calls without running inference.

    predict() returns a valid 11-row DataFrame with 19 quantile columns.
    hindcast() returns a valid hindcast DataFrame for the ordinary path tests.
    """

    def __init__(
        self,
        model=None,
        scalers=None,
        static_features=None,
        dl_config_params=None,
        unique_id_col="code",
        input_chunk_length=30,
        output_chunk_length=11,
    ):
        self._icl = input_chunk_length
        self._ocl = output_chunk_length
        self.window_sizes = [3, 5, 10]
        self.future_covariates_cols = ["P", "T", "PET", "daylight_hours"]
        self.predict_calls: list = []
        self.hindcast_calls: list = []

    def get_input_chunk_length(self) -> int:
        return self._icl

    def get_max_forecast_horizon(self) -> int:
        return self._ocl

    def predict(self, df_rivers_org, df_covariates, code, n):
        self.predict_calls.append(
            {
                "df_rivers_org": df_rivers_org.copy(),
                "df_covariates": df_covariates.copy(),
                "code": code,
                "n": n,
            }
        )
        # Return 11 rows: dates issue+1..issue+11, all 19 quantiles = 1.0
        last_obs = pd.to_datetime(df_rivers_org["date"]).max()
        q_cols = [
            f"Q{int(q * 100)}"
            for q in [
                0.05,
                0.10,
                0.15,
                0.20,
                0.25,
                0.30,
                0.35,
                0.40,
                0.45,
                0.50,
                0.55,
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.85,
                0.90,
                0.95,
            ]
        ]
        rows = []
        for i in range(1, n + 1):
            row = {"date": last_obs + pd.Timedelta(days=i)}
            for q in q_cols:
                row[q] = 1.0
            rows.append(row)
        return pd.DataFrame(rows)

    def hindcast(self, df_rivers_org, df_covariates, code, n):
        self.hindcast_calls.append(
            {
                "df_rivers_org": df_rivers_org,
                "df_covariates": df_covariates,
                "code": code,
                "n": n,
            }
        )
        last_obs = pd.to_datetime(df_rivers_org["date"]).max()
        q_cols = [
            f"Q{int(q * 100)}"
            for q in [
                0.05,
                0.10,
                0.15,
                0.20,
                0.25,
                0.30,
                0.35,
                0.40,
                0.45,
                0.50,
                0.55,
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.85,
                0.90,
                0.95,
            ]
        ]
        rows = []
        forecast_dt = last_obs
        for i in range(1, n + 1):
            row = {"date": forecast_dt + pd.Timedelta(days=i), "forecast_date": forecast_dt}
            for q in q_cols:
                row[q] = 1.0
            rows.append(row)
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Synthetic data builders
# ---------------------------------------------------------------------------


def _build_discharge(code_int: int, end_date: str, n_days: int) -> pd.DataFrame:
    """Return n_days consecutive daily discharge rows ending at end_date."""
    end = pd.Timestamp(end_date)
    dates = [end - pd.Timedelta(days=n_days - 1 - i) for i in range(n_days)]
    return pd.DataFrame(
        {
            "code": code_int,
            "date": dates,
            "discharge": [10.0] * n_days,
        }
    )


def _build_era5(code_int: int, start_date: str, end_date: str) -> pd.DataFrame:
    """Return daily ERA5 rows from start_date to end_date (inclusive)."""
    dates = pd.date_range(start_date, end_date, freq="D")
    return pd.DataFrame(
        {
            "code": code_int,
            "date": dates,
            "T": 15.0,
            "P": 2.0,
            "PET": 3.0,
            "daylight_hours": 12.0,
        }
    )


# ---------------------------------------------------------------------------
# Fake-boundary main() runner
#
# Calls hindcast_ML_models.main() in-process with:
#   - FakePredictor replacing the real predictor_class (captures calls)
#   - Synthetic discharge + ERA5 data replacing real reads
#   - _write_ml_forecast_to_api spy (raises if called in compute-only mode)
#   - DataFrame.to_csv spy (raises if called in compute-only mode)
#   - sl.load_environment() no-op
#   - All env vars set to synthetic values pointing at the temp tree
# ---------------------------------------------------------------------------


def _run_main_in_process(
    tmpdir: str,
    model: str,
    issue_date: str,
    code: str,
    request_path: str | None,
    response_path: str | None,
    fake_predictor: FakePredictor,
    icl: int = 30,
    ocl: int = 11,
    extra_discharge_days: int = 0,
    covariate_end_offset: int | None = None,
    api_write_raises: bool = False,
    csv_write_raises: bool = False,
    api_enabled: bool = True,
    ml_enabled: bool = True,
    model_available: bool = True,
    station_permitted: bool = True,
    era5_lookback_override: int | None = None,
) -> None:
    """Run hindcast_ML_models.main() in-process under controlled config.

    All boundaries (model loading, data reading, API writing) are replaced
    with synthetic fakes so no real inference, API, or file I/O occurs.

    The era5 data is built to span 70 days before issue_date so it passes
    hindcast_ML_models.py's 60-day minimum lookback check, unless
    era5_lookback_override is set to test exact-minimum ERA5 coverage.

    Args:
        tmpdir: Temporary directory for all file artifacts.
        model: Model name ("TFT", "TIDE", "TSMIXER").
        issue_date: ISO date string for START_DATE=END_DATE.
        code: Station code string.
        request_path: Path to write the compute-only request JSON (None = no request).
        response_path: Path where the child should write its response (None = no response).
        fake_predictor: FakePredictor instance whose calls will be verified.
        icl: input_chunk_length to set on fake_predictor.
        ocl: output_chunk_length to set on fake_predictor.
        extra_discharge_days: Extra days of history beyond icl+max_ws (default: 0).
            Pass a negative value to simulate insufficient history.
        covariate_end_offset: Override cov_end offset (days past issue_date). Default = max(11,ocl).
        api_write_raises: If True, _write_ml_forecast_to_api raises AssertionError.
        csv_write_raises: If True, DataFrame.to_csv raises AssertionError.
        api_enabled: SAPPHIRE_API_ENABLED value.
        ml_enabled: ieasyhydroforecast_run_ML_models value.
        model_available: Whether model appears in available_ML_models.
        station_permitted: Whether code appears in the station config.
        era5_lookback_override: When set, use this exact number of days before
            issue_date as the ERA5 start (skips the default 70-day margin).
            Use to test exact-minimum or under-minimum ERA5 coverage in recovery mode.
    """
    fake_predictor._icl = icl
    fake_predictor._ocl = ocl

    code_int = int(code)
    max_ws = max(fake_predictor.window_sizes)
    required_history = icl + max_ws + extra_discharge_days
    cov_end_offset = covariate_end_offset if covariate_end_offset is not None else max(11, ocl)

    obs = _build_discharge(code_int, issue_date, max(1, required_history))

    if era5_lookback_override is not None:
        era5_lookback = era5_lookback_override
    else:
        # ERA5 must start at least 60 days before start_date to pass the built-in check:
        #   if start_date < era5_min + 60 days → raise ValueError
        # Build it 70 days before issue_date to give a comfortable margin.
        era5_lookback = max(70, required_history + 5)
    era5_start = (pd.Timestamp(issue_date) - pd.Timedelta(days=era5_lookback)).strftime("%Y-%m-%d")
    era5_end = (pd.Timestamp(issue_date) + pd.Timedelta(days=cov_end_offset)).strftime("%Y-%m-%d")
    era5 = _build_era5(code_int, era5_start, era5_end)

    # Create fake asset tree; static_features.csv must live inside the scalers dir
    # because hindcast_ML_models.py builds its path as:
    #   os.path.join(MODELS_AND_SCALERS_PATH, PATH_TO_STATIC_FEATURES)
    scalers_root = os.path.join(tmpdir, "scalers")
    scalers_dir = os.path.join(scalers_root, model + "_Decad")
    os.makedirs(scalers_dir, exist_ok=True)
    open(os.path.join(scalers_dir, "model.pt"), "w").close()
    for name in ("scaler_stats_discharge", "scaler_stats_era5", "scaler_stats_static"):
        pd.DataFrame(
            {
                "Unnamed: 0": [code_int],
                "mean": [0.0],
                "std": [1.0],
                "min": [0.0],
                "max": [10.0],
            }
        ).to_csv(os.path.join(scalers_dir, f"{name}.csv"), index=False)
    # Static features: path is joined with MODELS_AND_SCALERS_PATH (scalers_root)
    pd.DataFrame({"code": [code_int], "LAT": [42.0], "LON": [74.0]}).to_csv(
        os.path.join(scalers_root, "static_features.csv"), index=False
    )

    # Station config
    config_dir = os.path.join(tmpdir, "config")
    os.makedirs(config_dir, exist_ok=True)
    station_ids = [code] if station_permitted else []
    for fname in ("station_selection.json", "station_selection_decad.json"):
        with open(os.path.join(config_dir, fname), "w") as f:
            json.dump({"stationsID": station_ids}, f)
    with open(os.path.join(config_dir, "hydroposts_ml.json"), "w") as f:
        json.dump({"stationsID": {code: {model: True, "virtual_station_name_twin": False}}}, f)

    env_patch = {
        "SAPPHIRE_MODEL_TO_USE": model,
        "SAPPHIRE_HINDCAST_MODE": "DECAD",
        "ieasyhydroforecast_START_DATE": issue_date,
        "ieasyhydroforecast_END_DATE": issue_date,
        "ieasyhydroforecast_NEW_STATIONS": code,
        "ieasyforecast_intermediate_data_path": tmpdir,
        "ieasyhydroforecast_models_and_scalers_path": scalers_root,
        "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static_features.csv",
        f"ieasyhydroforecast_PATH_TO_SCALER_{model}": model,
        "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
        "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "era5",
        "ieasyhydroforecast_HRU_CONTROL_MEMBER": "kghm",
        f"ieasyhydroforecast_THRESHOLD_MISSING_DAYS_{model}": "5",
        "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "3",
        "ieasyhydroforecast_forcing_gap_fill_recent_day_threshold": "7",
        "ieasyhydroforecast_forcing_gap_limit_past": "3",
        "SAPPHIRE_API_ENABLED": "true" if api_enabled else "false",
        "ieasyhydroforecast_run_ML_models": "true" if ml_enabled else "false",
        "ieasyhydroforecast_available_ML_models": model if model_available else "OTHER",
        "ieasyforecast_configuration_path": config_dir,
        "ieasyforecast_config_file_station_selection": "station_selection.json",
        "ieasyforecast_config_file_station_selection_decad": "station_selection_decad.json",
        "ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts": "hydroposts_ml.json",
        "SAPPHIRE_API_URL": "http://localhost:9999",
    }
    # Set recovery markers only when paths are provided; remove pre-existing keys
    # when operating in ordinary mode so no marker is visible to the module.
    if request_path is not None:
        env_patch["_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"] = request_path
    if response_path is not None:
        env_patch["_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"] = response_path

    import hindcast_ML_models as hm

    def fake_predictor_factory(*args, **kwargs):
        return fake_predictor

    def fake_read_discharge():
        return obs.copy()

    def fake_read_meteo(*args, **kwargs):
        return era5.copy()

    def fake_load_env():
        pass

    def fake_pet(df, lat):
        return pd.Series([3.0] * len(df), index=df.index)

    def fake_daylight(lat, lon, df):
        return pd.Series([12.0] * len(df), index=df.index)

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("Writer called in compute-only mode — forbidden")

    api_write_spy = _raise_if_called if api_write_raises else MagicMock(return_value=True)
    to_csv_spy = _raise_if_called if csv_write_raises else MagicMock()

    _MARKER_REQ = "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"
    _MARKER_RESP = "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"

    with (
        patch.dict(os.environ, env_patch, clear=False),
        patch.object(hm.TFTPredictor, "TFTPredictor", fake_predictor_factory),
        patch.object(hm.TiDEPredictor, "TiDEPredictor", fake_predictor_factory),
        patch.object(hm.TSMixerPredictor, "TSMIXERPredictor", fake_predictor_factory),
        patch.object(hm.sl, "load_environment", fake_load_env),
        patch.object(hm.fl, "read_daily_discharge_data", fake_read_discharge),
        patch.object(hm, "read_meteo_data_combined", fake_read_meteo),
        patch.object(hm, "_write_ml_forecast_to_api", api_write_spy),
        patch.object(hm.utils_ml_forecast, "calculate_pet_oudin", fake_pet),
        patch.object(hm.utils_ml_forecast, "calculate_daylight_hours", fake_daylight),
        patch("pandas.DataFrame.to_csv", to_csv_spy),
    ):
        # In ordinary mode, ensure no stale marker keys are visible
        if request_path is None:
            os.environ.pop(_MARKER_REQ, None)
        if response_path is None:
            os.environ.pop(_MARKER_RESP, None)
        try:
            hm.main()
        except SystemExit as exc:
            # compute-only mode calls sys.exit(0) on success; re-raise non-zero failures
            if exc.code != 0:
                raise


# ---------------------------------------------------------------------------
# Helper: build and write a compute-only request JSON, return (inv_id, paths)
# ---------------------------------------------------------------------------


def _write_request(
    tmpdir: str, model: str, code: str, issue_date: str, timeout_s: float = 120.0
) -> tuple[str, str, str]:
    """Write a request JSON; return (invocation_id, request_path, response_path)."""
    inv_id = str(uuid.uuid4())
    deadline = (datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_s)).isoformat()
    req = {
        "invocation_id": inv_id,
        "model": model,
        "source_code": code,
        "issue_date": issue_date,
        "deadline_utc": deadline,
    }
    req_path = os.path.join(tmpdir, "request.json")
    resp_path = os.path.join(tmpdir, "response.json")
    with open(req_path, "w") as f:
        json.dump(req, f)
    return inv_id, req_path, resp_path


# ===========================================================================
# Tests: isolation contract
# ===========================================================================


class TestIsolationContract:
    def test_modules_assigned(self):
        assert hindcast_ML_models is not None
        assert recalculate_nan_forecasts is not None
        assert compute_recovery_unit is not None

    def test_heavy_dep_darts_mocked(self):
        assert isinstance(sys.modules.get("darts"), MagicMock)


# ===========================================================================
# Tests: compute-only routing — predict called once, no writes
# ===========================================================================


class TestComputeOnlyRouting:
    """Main contract: predict() called once, hindcast() never, no writers."""

    def test_predict_called_once_for_tft(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_tft_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1, (
            f"Expected predict() called once, got {len(predictor.predict_calls)}"
        )
        assert len(predictor.hindcast_calls) == 0, (
            "hindcast() must not be called in compute-only mode"
        )

    def test_predict_called_once_for_tide(self):
        predictor = FakePredictor(input_chunk_length=20, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_tide_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TIDE", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TIDE",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=20,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1
        assert len(predictor.hindcast_calls) == 0

    def test_observations_end_at_issue_date(self):
        """The discharge slice passed to predict() must not extend past issue_date."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_obsend_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1
        discharge_passed = predictor.predict_calls[0]["df_rivers_org"]
        max_obs_date = pd.to_datetime(discharge_passed["date"]).max()
        assert max_obs_date == pd.Timestamp(ISSUE_DATE), (
            f"Observations extend past issue_date: {max_obs_date} > {ISSUE_DATE}"
        )

    def test_api_write_not_called_in_compute_mode(self):
        """_write_ml_forecast_to_api must not be called; spy raises if called."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_noapi_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            # If api_write_raises=True, calling it would raise AssertionError
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
        # If we get here, the writer was not called (would have raised).

    def test_response_json_written_and_valid(self):
        """Child writes a valid response JSON with invocation_id and 11 rows."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_resp_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
            with open(resp_path) as f:
                resp = json.load(f)

        assert resp["invocation_id"] == inv_id
        assert resp["model"] == "TFT"
        assert str(resp["source_code"]) == CODE_A
        assert resp["issue_date"] == ISSUE_DATE
        assert resp["flag"] == 4
        assert isinstance(resp["rows"], list)
        assert len(resp["rows"]) == 11

    def test_response_targets_are_issue_plus_1_through_11(self):
        """Response rows must cover issue+1..issue+11, not issue itself."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_tgt_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
            with open(resp_path) as f:
                resp = json.load(f)

        dates_in_resp = sorted(r["date"] for r in resp["rows"])
        expected = sorted((ISSUE_DT + datetime.timedelta(days=i)).isoformat() for i in range(1, 12))
        assert dates_in_resp == expected

    def test_response_all_19_quantiles_finite(self):
        """All 19 quantile columns must be present and finite in each row."""
        q_cols = [
            f"Q{int(q * 100)}"
            for q in [
                0.05,
                0.10,
                0.15,
                0.20,
                0.25,
                0.30,
                0.35,
                0.40,
                0.45,
                0.50,
                0.55,
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.85,
                0.90,
                0.95,
            ]
        ]
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_q19_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
            with open(resp_path) as f:
                resp = json.load(f)

        for row in resp["rows"]:
            for q in q_cols:
                assert q in row, f"Row missing quantile {q}"
                assert np.isfinite(float(row[q])), f"Non-finite {q} in row {row['date']}"


# ===========================================================================
# Tests: output_chunk_length constraints
# ===========================================================================


class TestOutputChunkLengthConstraints:
    """output_chunk_length < 11 blocks before inference; > 11 allowed."""

    def test_ocl_below_11_blocks_before_predict(self):
        """If output_chunk_length=10 < 11, predict() must not be called."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=10)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_child_ocl_lo_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=10,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0, "predict() must not be called when ocl < 11"

    def test_ocl_equal_11_allowed(self):
        """output_chunk_length=11 is the minimum valid value."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_child_ocl_eq_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1

    def test_ocl_above_11_valid_with_full_covariate_coverage(self):
        """output_chunk_length=15 > 11 is allowed; covariates extend through issue+15."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=15)
        with tempfile.TemporaryDirectory(prefix="sap_child_ocl_hi_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=15,
                covariate_end_offset=15,  # must extend through issue+15
                api_write_raises=True,
                csv_write_raises=True,
            )
            assert len(predictor.predict_calls) == 1
            # Only 11 targets must be in the response even when ocl=15
            with open(resp_path) as f:
                resp = json.load(f)
            assert len(resp["rows"]) == 11


# ===========================================================================
# Tests: source history validation
# ===========================================================================


class TestSourceHistoryValidation:
    """Insufficient or non-consecutive history blocks before predict()."""

    def test_missing_one_history_day_blocks(self):
        """Providing icl+max(0,max_ws-1)-1 rows (one short of exact minimum) must block."""
        # required_history = icl + max(0, max_ws-1) = 30 + 9 = 39.
        # extra_discharge_days=-2 gives _run_main_in_process icl+max_ws+(-2)=38 rows — one short.
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_child_hist_short_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    extra_discharge_days=-2,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_covariate_coverage_only_through_11_when_ocl_above_11_blocks(self):
        """If ocl=15 but covariates only reach issue+11, compute must be blocked."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=15)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_child_cov_short_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=15,
                    covariate_end_offset=11,  # only through 11, but ocl=15 needs 15
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0


# ===========================================================================
# Tests: response validation in compute_recovery_unit()
# ===========================================================================


class TestResponseValidation:
    """compute_recovery_unit() rejects invalid JSON responses."""

    def _good_response(self, inv_id: str, model: str, code: str, issue_date: str) -> dict:
        issue_dt = datetime.date.fromisoformat(issue_date)
        q_cols = [
            f"Q{int(q * 100)}"
            for q in [
                0.05,
                0.10,
                0.15,
                0.20,
                0.25,
                0.30,
                0.35,
                0.40,
                0.45,
                0.50,
                0.55,
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.85,
                0.90,
                0.95,
            ]
        ]
        rows = []
        for i in range(1, 12):
            row = {"date": (issue_dt + datetime.timedelta(days=i)).isoformat()}
            for q in q_cols:
                row[q] = 1.0
            rows.append(row)
        return {
            "invocation_id": inv_id,
            "model": model,
            "source_code": code,
            "issue_date": issue_date,
            "flag": 4,
            "rows": rows,
        }

    def _run_with_response(self, resp_dict: dict) -> "pd.DataFrame | None":
        """Write a fake response JSON and call compute_recovery_unit via a mock subprocess."""
        # We patch subprocess.run to: write resp_dict to response_path, return exit 0
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            if resp_path:
                with open(resp_path, "w") as f:
                    json.dump(resp_dict, f, allow_nan=False)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            return rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)

    def test_valid_response_returns_dataframe(self):
        inv_id = str(uuid.uuid4())
        resp = self._good_response(inv_id, "TFT", CODE_A, ISSUE_DATE)
        # We need to match the invocation_id that compute_recovery_unit generates,
        # so we patch uuid.uuid4 to return a fixed value.
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            # Patch the invocation_id in the response to match what was generated
            actual_inv = env.get("_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH")
            if actual_inv:
                with open(actual_inv) as f:
                    req = json.load(f)
                # Write response matching the actual invocation_id
                resp_copy = dict(resp)
                resp_copy["invocation_id"] = req["invocation_id"]
                if resp_path:
                    with open(resp_path, "w") as f:
                        json.dump(resp_copy, f, allow_nan=False)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 11

    def test_missing_invocation_id_rejected(self):
        """Response with wrong invocation_id is rejected."""
        resp = self._good_response("wrong-id", "TFT", CODE_A, ISSUE_DATE)
        result = self._run_with_response(resp)
        assert result is None

    def test_nonzero_child_exit_rejected(self):
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            return MagicMock(returncode=2)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)
        assert result is None

    def test_no_output_file_rejected(self):
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            # Write nothing to response_path
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)
        assert result is None

    def test_timeout_rejected(self):
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            raise rnf.subprocess.TimeoutExpired(cmd, timeout)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=0.001)
        assert result is None

    def test_wrong_model_in_response_rejected(self):
        resp = self._good_response("inv", "TIDE", CODE_A, ISSUE_DATE)
        result = self._run_with_response(resp)
        assert result is None

    def test_wrong_code_in_response_rejected(self):
        resp = self._good_response("inv", "TFT", "19099", ISSUE_DATE)
        result = self._run_with_response(resp)
        assert result is None

    def test_wrong_issue_date_in_response_rejected(self):
        resp = self._good_response("inv", "TFT", CODE_A, "2026-09-21")
        result = self._run_with_response(resp)
        assert result is None

    def test_missing_q10_row_rejected(self):
        """A response row missing Q10 must be rejected."""
        inv_id = "inv"
        resp = self._good_response(inv_id, "TFT", CODE_A, ISSUE_DATE)
        del resp["rows"][0]["Q10"]
        result = self._run_with_response(resp)
        assert result is None

    def test_inf_q50_row_rejected(self):
        """A row with inf in Q50 must be rejected."""
        # json.dump with allow_nan=False would reject inf, so we simulate
        # the response being read with inf already deserialized (shouldn't happen
        # in strict mode, but we test the parent-side validation path directly).
        inv_id = "inv"
        resp = self._good_response(inv_id, "TFT", CODE_A, ISSUE_DATE)
        resp["rows"][3]["Q50"] = float("inf")
        # json.dump would raise with allow_nan=False; patch json.load to inject it
        import json as _json

        original_load = _json.load

        def patched_load(f, **kwargs):
            data = original_load(f, **kwargs)
            data["rows"][3]["Q50"] = float("inf")
            return data

        import recalculate_nan_forecasts as rnf_mod

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            if resp_path:
                # Write a valid JSON (inf cannot be serialized, so use a placeholder)
                safe_resp = self._good_response("inv", "TFT", CODE_A, ISSUE_DATE)
                with open(resp_path, "w") as f:
                    json.dump(safe_resp, f)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf_mod.subprocess, "run", fake_run), patch("json.load", patched_load):
            result = rnf_mod.compute_recovery_unit(candidate, timeout_seconds=30.0)
        assert result is None

    def test_extra_issue_date_in_rows_rejected(self):
        """Rows containing the issue_date itself (not a target) must be rejected."""
        resp = self._good_response("inv", "TFT", CODE_A, ISSUE_DATE)
        # Replace one target with the issue_date itself
        resp["rows"][0]["date"] = ISSUE_DATE
        result = self._run_with_response(resp)
        assert result is None

    def test_duplicate_targets_rejected(self):
        """Duplicate target dates in rows must be rejected."""
        resp = self._good_response("inv", "TFT", CODE_A, ISSUE_DATE)
        resp["rows"][1]["date"] = resp["rows"][0]["date"]
        result = self._run_with_response(resp)
        assert result is None

    def test_fewer_than_11_rows_rejected(self):
        resp = self._good_response("inv", "TFT", CODE_A, ISSUE_DATE)
        resp["rows"] = resp["rows"][:10]
        result = self._run_with_response(resp)
        assert result is None

    def test_flag_not_4_rejected(self):
        resp = self._good_response("inv", "TFT", CODE_A, ISSUE_DATE)
        resp["flag"] = 3
        result = self._run_with_response(resp)
        assert result is None


# ===========================================================================
# Tests: gate checks block compute (predict() never called)
# ===========================================================================


class TestGateChecks:
    """When a gate is off, child exits non-zero; predict() not called."""

    def test_api_disabled_blocks(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_gate_api_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_enabled=False,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_ml_disabled_blocks(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_gate_ml_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    ml_enabled=False,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_model_not_available_blocks(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_gate_modl_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    model_available=False,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_station_not_permitted_blocks(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_gate_stn_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    station_permitted=False,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0


# ===========================================================================
# Tests: ordinary default entrypoint is byte-equivalent (no compute marker)
# ===========================================================================


class TestOrdinaryPathUnchanged:
    """Without compute marker, hindcast() is still used; predict() not called."""

    def test_ordinary_path_calls_hindcast_not_predict(self):
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_ord_") as td:
            # No request_path/response_path → ordinary mode
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                request_path=None,
                response_path=None,
                fake_predictor=predictor,
                icl=30,
                ocl=11,
                api_write_raises=False,
                csv_write_raises=False,
            )
        assert len(predictor.hindcast_calls) == 1, "Ordinary mode must call hindcast() exactly once"
        assert len(predictor.predict_calls) == 0, "Ordinary mode must not call predict()"


# ===========================================================================
# Tests: compute_recovery_unit() input validation
# ===========================================================================


class TestComputeRecoveryUnitValidation:
    """Validates that compute_recovery_unit() itself refuses bad inputs."""

    def test_arima_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": ISSUE_DATE, "model": "ARIMA", "code": CODE_A},
            timeout_seconds=30.0,
        )
        assert result is None

    def test_zero_timeout_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": ISSUE_DATE, "model": "TFT", "code": CODE_A},
            timeout_seconds=0.0,
        )
        assert result is None

    def test_negative_timeout_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": ISSUE_DATE, "model": "TFT", "code": CODE_A},
            timeout_seconds=-5.0,
        )
        assert result is None

    def test_invalid_issue_date_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": "not-a-date", "model": "TFT", "code": CODE_A},
            timeout_seconds=30.0,
        )
        assert result is None

    def test_missing_candidate_keys_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"model": "TFT", "code": CODE_A},  # missing issue_date
            timeout_seconds=30.0,
        )
        assert result is None

    def test_nan_timeout_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": ISSUE_DATE, "model": "TFT", "code": CODE_A},
            timeout_seconds=float("nan"),
        )
        assert result is None

    def test_inf_timeout_rejected(self):
        import recalculate_nan_forecasts as rnf

        result = rnf.compute_recovery_unit(
            {"issue_date": ISSUE_DATE, "model": "TFT", "code": CODE_A},
            timeout_seconds=float("inf"),
        )
        assert result is None


# ===========================================================================
# Tests: recovery marker contract — partial/empty markers fail-closed
# ===========================================================================


class TestRecoveryMarkerContract:
    """Any presence of either marker (even empty) must select recovery and fail if incomplete."""

    def _run_with_env(self, req_path_val, resp_path_val, model="TFT"):
        """Call main() with custom marker env vars; return the SystemExit code."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        env_override = {}
        if req_path_val is not None:
            env_override["_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"] = req_path_val
        if resp_path_val is not None:
            env_override["_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"] = resp_path_val

        with tempfile.TemporaryDirectory(prefix="sap_marker_") as td:
            with pytest.raises(SystemExit) as exc_info:
                with tempfile.TemporaryDirectory(prefix="sap_req_") as req_td:
                    # Write a minimal valid request so file-open doesn't fail first
                    req_path = os.path.join(req_td, "request.json")
                    resp_path = os.path.join(req_td, "response.json")
                    inv_id = str(uuid.uuid4())
                    with open(req_path, "w") as f:
                        json.dump(
                            {
                                "invocation_id": inv_id,
                                "model": model,
                                "source_code": CODE_A,
                                "issue_date": ISSUE_DATE,
                                "deadline_utc": (
                                    datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
                                ).isoformat(),
                            },
                            f,
                        )
                    # Substitute empty string for "empty" sentinel
                    actual_req = (
                        ""
                        if req_path_val == ""
                        else (req_path if req_path_val == "<valid>" else (req_path_val or ""))
                    )
                    actual_resp = (
                        ""
                        if resp_path_val == ""
                        else (resp_path if resp_path_val == "<valid>" else (resp_path_val or ""))
                    )
                    _run_main_in_process(
                        td,
                        model,
                        ISSUE_DATE,
                        CODE_A,
                        request_path=actual_req or None,
                        response_path=actual_resp or None,
                        fake_predictor=predictor,
                        api_write_raises=True,
                        csv_write_raises=True,
                    )
        return exc_info.value.code

    def test_request_only_marker_fails_closed(self):
        """REQUEST set, RESPONSE absent → recovery validation triggered → sys.exit(2)."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_reqonly_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            # Only set REQUEST; leave RESPONSE unset by passing None to env patch
            with pytest.raises(SystemExit) as exc_info:
                import hindcast_ML_models as hm

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                }
                # Ensure RESPONSE key is absent (not just empty)
                env_clean = {
                    k: v
                    for k, v in os.environ.items()
                    if k != "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH"
                }
                env_clean.update(env_patch)
                with patch.dict(os.environ, env_clean, clear=True):
                    hm.main()
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_response_only_marker_fails_closed(self):
        """RESPONSE set, REQUEST absent → recovery validation triggered → sys.exit(2)."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_responly_") as td:
            resp_path = os.path.join(td, "response.json")
            with pytest.raises(SystemExit) as exc_info:
                import hindcast_ML_models as hm

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
                }
                env_clean = {
                    k: v
                    for k, v in os.environ.items()
                    if k != "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH"
                }
                env_clean.update(env_patch)
                with patch.dict(os.environ, env_clean, clear=True):
                    hm.main()
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_empty_request_path_fails_closed(self):
        """REQUEST set to empty string → fail-closed; no ordinary mode fallthrough."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_emptyreq_") as td:
            _, _, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            with pytest.raises(SystemExit) as exc_info:
                import hindcast_ML_models as hm

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": "",
                    "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
                }
                with patch.dict(os.environ, env_patch, clear=False):
                    hm.main()
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0

    def test_empty_response_path_fails_closed(self):
        """RESPONSE set to empty string → fail-closed; no ordinary mode fallthrough."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_emptyresp_") as td:
            inv_id, req_path, _ = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            with pytest.raises(SystemExit) as exc_info:
                import hindcast_ML_models as hm

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                    "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": "",
                }
                with patch.dict(os.environ, env_patch, clear=False):
                    hm.main()
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0


# ===========================================================================
# Tests: post-budget rejection — child exits 0 but elapsed >= timeout
# ===========================================================================


class TestPostBudgetRejection:
    """compute_recovery_unit() discards a valid result if elapsed >= timeout."""

    def test_post_budget_exit0_rejected(self):
        """Subprocess exits 0 with valid output but elapsed time >= budget → None."""
        import recalculate_nan_forecasts as rnf

        timeout = 0.5  # 500 ms budget

        def slow_but_valid_run(cmd, env, timeout_arg):
            # Write a valid response
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            req_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH")
            if req_path:
                with open(req_path) as f:
                    req = json.load(f)
            issue_dt = datetime.date.fromisoformat(ISSUE_DATE)
            q_cols = [
                "Q5",
                "Q10",
                "Q15",
                "Q20",
                "Q25",
                "Q30",
                "Q35",
                "Q40",
                "Q45",
                "Q50",
                "Q55",
                "Q60",
                "Q65",
                "Q70",
                "Q75",
                "Q80",
                "Q85",
                "Q90",
                "Q95",
            ]
            rows = []
            for i in range(1, 12):
                row = {"date": (issue_dt + datetime.timedelta(days=i)).isoformat()}
                for q in q_cols:
                    row[q] = 1.0
                rows.append(row)
            resp = {
                "invocation_id": req["invocation_id"],
                "model": "TFT",
                "source_code": CODE_A,
                "issue_date": ISSUE_DATE,
                "flag": 4,
                "rows": rows,
            }
            if resp_path:
                with open(resp_path, "w") as f:
                    json.dump(resp, f)
            # Simulate taking longer than budget by patching monotonic elapsed
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }

        # Patch time.monotonic so elapsed appears >= timeout
        _call_count = [0]
        _start_val = [0.0]

        original_monotonic = time.monotonic

        def fake_monotonic():
            _call_count[0] += 1
            if _call_count[0] == 1:
                _start_val[0] = original_monotonic()
                return _start_val[0]
            # Second call returns start + timeout + epsilon
            return _start_val[0] + timeout + 0.1

        with (
            patch.object(rnf.subprocess, "run", slow_but_valid_run),
            patch("time.monotonic", fake_monotonic),
        ):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=timeout)

        assert result is None, "Result after budget exhaustion must be None"


# ===========================================================================
# Tests: predictor identity violation in _run_recovery_compute
# ===========================================================================


class TestPredictorIdentityViolation:
    """If predictor stamps wrong code in df_pred, _run_recovery_compute rejects it."""

    class _WrongCodePredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            df["code"] = 99999  # wrong code
            return df

    def test_wrong_code_in_df_pred_blocks(self):
        """predict() returning wrong code column must exit non-zero."""
        predictor = self._WrongCodePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_id_viol_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0


# ===========================================================================
# Tests: noncanonical timestamp rejection
# ===========================================================================


class TestNoncanonicalTimestampRejection:
    """Timezone-aware or non-midnight timestamps in df_pred must be rejected."""

    class _TimezonePredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            # Replace date with timezone-aware timestamps using stdlib (no pytz dependency)
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(datetime.UTC)
            return df

    class _NonMidnightPredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            # Replace date with timestamps at noon
            df["date"] = pd.to_datetime(df["date"]) + pd.Timedelta(hours=12)
            return df

    def test_non_midnight_timestamp_blocked(self):
        """Non-midnight naive timestamps (e.g. noon) must be rejected."""
        predictor = self._NonMidnightPredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_nonmid_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0


# ===========================================================================
# Tests: non-finite discharge in source history
# ===========================================================================


class TestNonFiniteDischarge:
    """Non-finite (NaN/inf) discharge values in source history must block predict()."""

    class _NaNDischargePredictor(FakePredictor):
        pass  # predict() itself is fine; we inject NaN at the discharge-build level

    def test_nan_discharge_blocks_predict(self):
        """Source history with a NaN discharge value must exit non-zero."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_nandisch_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)

                import hindcast_ML_models as hm

                code_int = int(CODE_A)
                icl = 30
                max_ws = 10
                n_days = icl + max_ws
                # Build discharge with a NaN in the last row
                end = pd.Timestamp(ISSUE_DATE)
                dates = [end - pd.Timedelta(days=n_days - 1 - i) for i in range(n_days)]
                disc_vals = [10.0] * n_days
                disc_vals[-1] = float("nan")
                obs = pd.DataFrame({"code": code_int, "date": dates, "discharge": disc_vals})

                era5_start = (pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=75)).strftime("%Y-%m-%d")
                era5_end = (pd.Timestamp(ISSUE_DATE) + pd.Timedelta(days=11)).strftime("%Y-%m-%d")
                era5 = _build_era5(code_int, era5_start, era5_end)

                scalers_root = os.path.join(td, "scalers")
                scalers_dir = os.path.join(scalers_root, "TFT_Decad")
                os.makedirs(scalers_dir, exist_ok=True)
                open(os.path.join(scalers_dir, "model.pt"), "w").close()
                for name in ("scaler_stats_discharge", "scaler_stats_era5", "scaler_stats_static"):
                    pd.DataFrame(
                        {
                            "Unnamed: 0": [code_int],
                            "mean": [0.0],
                            "std": [1.0],
                            "min": [0.0],
                            "max": [10.0],
                        }
                    ).to_csv(os.path.join(scalers_dir, f"{name}.csv"), index=False)
                pd.DataFrame({"code": [code_int], "LAT": [42.0], "LON": [74.0]}).to_csv(
                    os.path.join(scalers_root, "static_features.csv"), index=False
                )

                config_dir = os.path.join(td, "config")
                os.makedirs(config_dir, exist_ok=True)
                for fname in ("station_selection.json", "station_selection_decad.json"):
                    with open(os.path.join(config_dir, fname), "w") as f:
                        json.dump({"stationsID": [CODE_A]}, f)
                with open(os.path.join(config_dir, "hydroposts_ml.json"), "w") as f:
                    json.dump(
                        {"stationsID": {CODE_A: {"TFT": True, "virtual_station_name_twin": False}}},
                        f,
                    )

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "ieasyforecast_intermediate_data_path": td,
                    "ieasyhydroforecast_models_and_scalers_path": scalers_root,
                    "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static_features.csv",
                    "ieasyhydroforecast_PATH_TO_SCALER_TFT": "TFT",
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                    "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "era5",
                    "ieasyhydroforecast_HRU_CONTROL_MEMBER": "kghm",
                    "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT": "5",
                    "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "3",
                    "ieasyhydroforecast_forcing_gap_fill_recent_day_threshold": "7",
                    "ieasyhydroforecast_forcing_gap_limit_past": "3",
                    "SAPPHIRE_API_ENABLED": "true",
                    "ieasyhydroforecast_run_ML_models": "true",
                    "ieasyhydroforecast_available_ML_models": "TFT",
                    "ieasyforecast_configuration_path": config_dir,
                    "ieasyforecast_config_file_station_selection": "station_selection.json",
                    "ieasyforecast_config_file_station_selection_decad": "station_selection_decad.json",
                    "ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts": "hydroposts_ml.json",
                    "SAPPHIRE_API_URL": "http://localhost:9999",
                    "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                    "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
                }

                def fake_predictor_factory(*args, **kwargs):
                    return predictor

                with (
                    patch.dict(os.environ, env_patch, clear=False),
                    patch.object(hm.TFTPredictor, "TFTPredictor", fake_predictor_factory),
                    patch.object(hm.TiDEPredictor, "TiDEPredictor", fake_predictor_factory),
                    patch.object(hm.TSMixerPredictor, "TSMIXERPredictor", fake_predictor_factory),
                    patch.object(hm.sl, "load_environment", lambda: None),
                    patch.object(hm.fl, "read_daily_discharge_data", lambda: obs.copy()),
                    patch.object(hm, "read_meteo_data_combined", lambda **kw: era5.copy()),
                    patch.object(
                        hm.utils_ml_forecast,
                        "calculate_pet_oudin",
                        lambda df, lat: pd.Series([3.0] * len(df), index=df.index),
                    ),
                    patch.object(
                        hm.utils_ml_forecast,
                        "calculate_daylight_hours",
                        lambda lat, lon, df: pd.Series([12.0] * len(df), index=df.index),
                    ),
                ):
                    try:
                        hm.main()
                    except SystemExit as exc:
                        if exc.code != 0:
                            raise

        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0, "predict() must not be called with NaN discharge"


# ===========================================================================
# Tests: timezone-aware timestamp using stdlib datetime.timezone.utc (Gap D)
# ===========================================================================


class TestTimezoneAwareStdlib:
    """Timezone-aware timestamps from predict() must be rejected; test uses only stdlib."""

    class _StdlibTZPredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(datetime.UTC)
            return df

    def test_stdlib_utc_timestamp_blocked(self):
        """Timezone-aware timestamps (stdlib UTC) must exit non-zero; no pytz dependency."""
        predictor = self._StdlibTZPredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_tz_stdlib_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0


# ===========================================================================
# Tests: malformed request / mandatory deadline (Gap A)
# ===========================================================================


class TestMalformedRequestAndDeadline:
    """Non-dict request JSON or missing deadline_utc must exit(2) before any env load."""

    def _run_with_bad_request_content(self, bad_content: str, model: str = "TFT") -> int:
        """Write bad_content directly as request file; return SystemExit code."""
        import hindcast_ML_models as hm

        with tempfile.TemporaryDirectory(prefix="sap_badreq_") as td:
            req_path = os.path.join(td, "request.json")
            resp_path = os.path.join(td, "response.json")
            with open(req_path, "w") as f:
                f.write(bad_content)
            env_patch = {
                "SAPPHIRE_MODEL_TO_USE": model,
                "SAPPHIRE_HINDCAST_MODE": "DECAD",
                "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
            }
            with pytest.raises(SystemExit) as exc_info:
                with (
                    patch.dict(os.environ, env_patch, clear=False),
                    patch.object(hm.sl, "load_environment", lambda: None),
                ):
                    hm.main()
            return exc_info.value.code

    def test_list_request_json_rejected(self):
        """Request JSON that is a list (not a dict) must exit(2)."""
        code = self._run_with_bad_request_content(
            '[{"model":"TFT","invocation_id":"x","source_code":"19011",'
            '"issue_date":"2026-09-20","deadline_utc":"2099-01-01T00:00:00"}]'
        )
        assert code != 0

    def test_null_request_json_rejected(self):
        """Request JSON that is null must exit(2)."""
        code = self._run_with_bad_request_content("null")
        assert code != 0

    def test_missing_deadline_utc_rejected(self):
        """Request JSON without deadline_utc must exit(2)."""
        import hindcast_ML_models as hm

        with tempfile.TemporaryDirectory(prefix="sap_nodel_") as td:
            req_path = os.path.join(td, "request.json")
            resp_path = os.path.join(td, "response.json")
            inv_id = str(uuid.uuid4())
            with open(req_path, "w") as f:
                json.dump(
                    {
                        "invocation_id": inv_id,
                        "model": "TFT",
                        "source_code": CODE_A,
                        "issue_date": ISSUE_DATE,
                        # deadline_utc deliberately absent
                    },
                    f,
                )
            predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
            env_patch = {
                "SAPPHIRE_MODEL_TO_USE": "TFT",
                "SAPPHIRE_HINDCAST_MODE": "DECAD",
                "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
            }
            with pytest.raises(SystemExit) as exc_info:
                with (
                    patch.dict(os.environ, env_patch, clear=False),
                    patch.object(hm.sl, "load_environment", lambda: None),
                ):
                    hm.main()
            assert exc_info.value.code != 0
            assert len(predictor.predict_calls) == 0

    def test_empty_deadline_utc_rejected(self):
        """Request JSON with empty deadline_utc string must exit(2)."""
        import hindcast_ML_models as hm

        with tempfile.TemporaryDirectory(prefix="sap_empdl_") as td:
            req_path = os.path.join(td, "request.json")
            resp_path = os.path.join(td, "response.json")
            inv_id = str(uuid.uuid4())
            with open(req_path, "w") as f:
                json.dump(
                    {
                        "invocation_id": inv_id,
                        "model": "TFT",
                        "source_code": CODE_A,
                        "issue_date": ISSUE_DATE,
                        "deadline_utc": "",
                    },
                    f,
                )
            env_patch = {
                "SAPPHIRE_MODEL_TO_USE": "TFT",
                "SAPPHIRE_HINDCAST_MODE": "DECAD",
                "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
            }
            with pytest.raises(SystemExit) as exc_info:
                with (
                    patch.dict(os.environ, env_patch, clear=False),
                    patch.object(hm.sl, "load_environment", lambda: None),
                ):
                    hm.main()
            assert exc_info.value.code != 0


# ===========================================================================
# Tests: malformed parent response JSON (Gap A)
# ===========================================================================


class TestMalformedParentResponse:
    """compute_recovery_unit() must return None for list/null/non-mapping response JSON."""

    def _run_with_raw_response(self, raw_json_str: str) -> "pd.DataFrame | None":
        import recalculate_nan_forecasts as rnf

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            if resp_path:
                with open(resp_path, "w") as f:
                    f.write(raw_json_str)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            return rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)

    def test_list_response_returns_none(self):
        result = self._run_with_raw_response('[{"invocation_id":"x"}]')
        assert result is None

    def test_null_response_returns_none(self):
        result = self._run_with_raw_response("null")
        assert result is None

    def test_integer_response_returns_none(self):
        result = self._run_with_raw_response("42")
        assert result is None


# ===========================================================================
# Tests: model column=False in hydroposts config (Gap B)
# ===========================================================================


class TestModelColumnFalseBlocks:
    """When hydroposts_ml.json has model=False for the station, recovery must block."""

    def test_model_column_false_blocks_compute(self):
        """If the station has model=False in the config, compute must exit non-zero."""
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_modfalse_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)

                import hindcast_ML_models as hm

                code_int = int(CODE_A)
                icl = 30
                max_ws = 10
                n_days = icl + max_ws
                obs = _build_discharge(code_int, ISSUE_DATE, n_days)
                era5_start = (pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=75)).strftime("%Y-%m-%d")
                era5_end = (pd.Timestamp(ISSUE_DATE) + pd.Timedelta(days=11)).strftime("%Y-%m-%d")
                era5 = _build_era5(code_int, era5_start, era5_end)

                scalers_root = os.path.join(td, "scalers")
                scalers_dir = os.path.join(scalers_root, "TFT_Decad")
                os.makedirs(scalers_dir, exist_ok=True)
                open(os.path.join(scalers_dir, "model.pt"), "w").close()
                for name in ("scaler_stats_discharge", "scaler_stats_era5", "scaler_stats_static"):
                    pd.DataFrame(
                        {
                            "Unnamed: 0": [code_int],
                            "mean": [0.0],
                            "std": [1.0],
                            "min": [0.0],
                            "max": [10.0],
                        }
                    ).to_csv(os.path.join(scalers_dir, f"{name}.csv"), index=False)
                pd.DataFrame({"code": [code_int], "LAT": [42.0], "LON": [74.0]}).to_csv(
                    os.path.join(scalers_root, "static_features.csv"), index=False
                )

                config_dir = os.path.join(td, "config")
                os.makedirs(config_dir, exist_ok=True)
                for fname in ("station_selection.json", "station_selection_decad.json"):
                    with open(os.path.join(config_dir, fname), "w") as f:
                        json.dump({"stationsID": [CODE_A]}, f)
                # model column is False → station excluded from codes_model_can_predict
                with open(os.path.join(config_dir, "hydroposts_ml.json"), "w") as f:
                    json.dump(
                        {
                            "stationsID": {
                                CODE_A: {"TFT": False, "virtual_station_name_twin": False}
                            }
                        },
                        f,
                    )

                env_patch = {
                    "SAPPHIRE_MODEL_TO_USE": "TFT",
                    "SAPPHIRE_HINDCAST_MODE": "DECAD",
                    "ieasyhydroforecast_START_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_END_DATE": ISSUE_DATE,
                    "ieasyhydroforecast_NEW_STATIONS": CODE_A,
                    "ieasyforecast_intermediate_data_path": td,
                    "ieasyhydroforecast_models_and_scalers_path": scalers_root,
                    "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static_features.csv",
                    "ieasyhydroforecast_PATH_TO_SCALER_TFT": "TFT",
                    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
                    "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "era5",
                    "ieasyhydroforecast_HRU_CONTROL_MEMBER": "kghm",
                    "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT": "5",
                    "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "3",
                    "ieasyhydroforecast_forcing_gap_fill_recent_day_threshold": "7",
                    "ieasyhydroforecast_forcing_gap_limit_past": "3",
                    "SAPPHIRE_API_ENABLED": "true",
                    "ieasyhydroforecast_run_ML_models": "true",
                    "ieasyhydroforecast_available_ML_models": "TFT",
                    "ieasyforecast_configuration_path": config_dir,
                    "ieasyforecast_config_file_station_selection": "station_selection.json",
                    "ieasyforecast_config_file_station_selection_decad": "station_selection_decad.json",
                    "ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts": "hydroposts_ml.json",
                    "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
                    "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
                }

                def fake_predictor_factory(*args, **kwargs):
                    return predictor

                with (
                    patch.dict(os.environ, env_patch, clear=False),
                    patch.object(hm.TFTPredictor, "TFTPredictor", fake_predictor_factory),
                    patch.object(hm.TiDEPredictor, "TiDEPredictor", fake_predictor_factory),
                    patch.object(hm.TSMixerPredictor, "TSMIXERPredictor", fake_predictor_factory),
                    patch.object(hm.sl, "load_environment", lambda: None),
                    patch.object(hm.fl, "read_daily_discharge_data", lambda: obs.copy()),
                    patch.object(hm, "read_meteo_data_combined", lambda **kw: era5.copy()),
                    patch.object(
                        hm.utils_ml_forecast,
                        "calculate_pet_oudin",
                        lambda df, lat: pd.Series([3.0] * len(df), index=df.index),
                    ),
                    patch.object(
                        hm.utils_ml_forecast,
                        "calculate_daylight_hours",
                        lambda lat, lon, df: pd.Series([12.0] * len(df), index=df.index),
                    ),
                ):
                    try:
                        hm.main()
                    except SystemExit as exc:
                        if exc.code != 0:
                            raise

        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0


# ===========================================================================
# Tests: exact minimum history passes (Gap C)
# ===========================================================================


class TestExactMinimumHistory:
    """Exactly icl + max_ws - 1 rows is the minimum; passing it must succeed."""

    def test_exact_minimum_history_passes(self):
        """Providing exactly required_history = icl + max(0, max_ws-1) rows must succeed."""
        icl = 30
        # max_ws from FakePredictor.window_sizes = [3,5,10], so max_ws=10
        max_ws = 10
        required = icl + max(0, max_ws - 1)  # = 39
        # extra_discharge_days=0 means _build_discharge provides icl+max_ws=40 rows.
        # We need to provide exactly 39, so extra_discharge_days=-1.
        predictor = FakePredictor(input_chunk_length=icl, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_exact_hist_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=icl,
                ocl=11,
                extra_discharge_days=-1,  # 40 - 1 = 39 = required_history
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1, (
            f"Exact minimum history ({required} rows) must pass; predict() must be called once"
        )


# ===========================================================================
# Tests: forcing grid validation (Gap C)
# ===========================================================================


class TestForcingGridValidation:
    """Missing interior forcing, duplicate rows, or start gap must block compute."""

    class _MissingForcingInteriorPredictor(FakePredictor):
        """predict() is never reached; we validate forcing before the call."""

        pass

    def _run_with_custom_era5(self, td, era5_override, predictor=None):
        """Run main() with a custom era5 DataFrame injected via patch."""
        import hindcast_ML_models as hm

        if predictor is None:
            predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
        code_int = int(CODE_A)
        obs = _build_discharge(code_int, ISSUE_DATE, 40)

        scalers_root = os.path.join(td, "scalers")
        scalers_dir = os.path.join(scalers_root, "TFT_Decad")
        os.makedirs(scalers_dir, exist_ok=True)
        open(os.path.join(scalers_dir, "model.pt"), "w").close()
        for name in ("scaler_stats_discharge", "scaler_stats_era5", "scaler_stats_static"):
            pd.DataFrame(
                {
                    "Unnamed: 0": [code_int],
                    "mean": [0.0],
                    "std": [1.0],
                    "min": [0.0],
                    "max": [10.0],
                }
            ).to_csv(os.path.join(scalers_dir, f"{name}.csv"), index=False)
        pd.DataFrame({"code": [code_int], "LAT": [42.0], "LON": [74.0]}).to_csv(
            os.path.join(scalers_root, "static_features.csv"), index=False
        )
        config_dir = os.path.join(td, "config")
        os.makedirs(config_dir, exist_ok=True)
        for fname in ("station_selection.json", "station_selection_decad.json"):
            with open(os.path.join(config_dir, fname), "w") as f:
                json.dump({"stationsID": [CODE_A]}, f)
        with open(os.path.join(config_dir, "hydroposts_ml.json"), "w") as f:
            json.dump(
                {"stationsID": {CODE_A: {"TFT": True, "virtual_station_name_twin": False}}}, f
            )

        env_patch = {
            "SAPPHIRE_MODEL_TO_USE": "TFT",
            "SAPPHIRE_HINDCAST_MODE": "DECAD",
            "ieasyhydroforecast_START_DATE": ISSUE_DATE,
            "ieasyhydroforecast_END_DATE": ISSUE_DATE,
            "ieasyhydroforecast_NEW_STATIONS": CODE_A,
            "ieasyforecast_intermediate_data_path": td,
            "ieasyhydroforecast_models_and_scalers_path": scalers_root,
            "ieasyhydroforecast_PATH_TO_STATIC_FEATURES": "static_features.csv",
            "ieasyhydroforecast_PATH_TO_SCALER_TFT": "TFT",
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
            "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "era5",
            "ieasyhydroforecast_HRU_CONTROL_MEMBER": "kghm",
            "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT": "5",
            "ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END": "3",
            "ieasyhydroforecast_forcing_gap_fill_recent_day_threshold": "7",
            "ieasyhydroforecast_forcing_gap_limit_past": "3",
            "SAPPHIRE_API_ENABLED": "true",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
            "ieasyforecast_configuration_path": config_dir,
            "ieasyforecast_config_file_station_selection": "station_selection.json",
            "ieasyforecast_config_file_station_selection_decad": "station_selection_decad.json",
            "ieasyhydroforecast_config_hydroposts_available_for_ml_forecasts": "hydroposts_ml.json",
            "_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH": req_path,
            "_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH": resp_path,
        }

        def fake_predictor_factory(*args, **kwargs):
            return predictor

        with (
            patch.dict(os.environ, env_patch, clear=False),
            patch.object(hm.TFTPredictor, "TFTPredictor", fake_predictor_factory),
            patch.object(hm.TiDEPredictor, "TiDEPredictor", fake_predictor_factory),
            patch.object(hm.TSMixerPredictor, "TSMIXERPredictor", fake_predictor_factory),
            patch.object(hm.sl, "load_environment", lambda: None),
            patch.object(hm.fl, "read_daily_discharge_data", lambda: obs.copy()),
            patch.object(hm, "read_meteo_data_combined", lambda **kw: era5_override.copy()),
            patch.object(
                hm.utils_ml_forecast,
                "calculate_pet_oudin",
                lambda df, lat: pd.Series([3.0] * len(df), index=df.index),
            ),
            patch.object(
                hm.utils_ml_forecast,
                "calculate_daylight_hours",
                lambda lat, lon, df: pd.Series([12.0] * len(df), index=df.index),
            ),
        ):
            try:
                hm.main()
            except SystemExit as exc:
                if exc.code != 0:
                    raise
        return predictor

    def _direct_recovery_compute(self, era5_override, predictor=None):
        """Call _run_recovery_compute directly, bypassing main() and fill_forcing_gaps."""
        import hindcast_ML_models as hm

        if predictor is None:
            predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        code_int = int(CODE_A)
        n_days = 40
        obs = _build_discharge(code_int, ISSUE_DATE, n_days)
        request = {
            "invocation_id": str(uuid.uuid4()),
            "model": "TFT",
            "source_code": CODE_A,
            "issue_date": ISSUE_DATE,
        }
        with tempfile.TemporaryDirectory(prefix="sap_direct_rc_") as td:
            response_path = os.path.join(td, "response.json")
            deadline = datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
            return hm._run_recovery_compute(
                request=request,
                predictor=predictor,
                era5_data=era5_override,
                observed_discharge=obs,
                rivers_to_predict=[code_int],
                response_path=response_path,
                deadline=deadline,
            )

    def test_missing_interior_forcing_blocks(self):
        """A gap in the interior of the ERA5 grid must block before predict().

        Calls _run_recovery_compute directly to bypass fill_forcing_gaps in main().
        """
        code_int = int(CODE_A)
        era5_start = (pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=75)).strftime("%Y-%m-%d")
        era5_end = (pd.Timestamp(ISSUE_DATE) + pd.Timedelta(days=11)).strftime("%Y-%m-%d")
        era5 = _build_era5(code_int, era5_start, era5_end)
        # Drop one interior row within obs window to create a date gap
        drop_date = pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=5)
        era5 = era5[era5["date"] != drop_date].reset_index(drop=True)

        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        rc = self._direct_recovery_compute(era5, predictor)
        assert rc != 0, "Interior forcing gap must return non-zero exit code"
        assert len(predictor.predict_calls) == 0

    def test_duplicate_forcing_rows_block(self):
        """Duplicate dates in the ERA5 grid within the recovery window must block.

        Calls _run_recovery_compute directly to bypass fill_forcing_gaps in main().
        """
        code_int = int(CODE_A)
        era5_start = (pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=75)).strftime("%Y-%m-%d")
        era5_end = (pd.Timestamp(ISSUE_DATE) + pd.Timedelta(days=11)).strftime("%Y-%m-%d")
        era5 = _build_era5(code_int, era5_start, era5_end)
        # Duplicate a row within [obs_start, cov_end_ts] (obs_start = issue - 38)
        dup_date = pd.Timestamp(ISSUE_DATE) - pd.Timedelta(days=10)
        dup = era5[era5["date"] == dup_date].copy()
        era5 = pd.concat([era5, dup], ignore_index=True)

        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        rc = self._direct_recovery_compute(era5, predictor)
        assert rc != 0, "Duplicate forcing row must return non-zero exit code"
        assert len(predictor.predict_calls) == 0


# ===========================================================================
# Tests: forecast_date / model column identity check (Gap D)
# ===========================================================================


class TestPredictorOutputIdentity:
    """predict() returning wrong forecast_date or model column must exit non-zero."""

    class _WrongForecastDatePredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            # Stamp forecast_date as the wrong date
            df["forecast_date"] = pd.Timestamp("2020-01-01")
            return df

    class _WrongModelPredictor(FakePredictor):
        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            df["model"] = "WRONGMODEL"
            return df

    def test_wrong_forecast_date_blocks(self):
        predictor = self._WrongForecastDatePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_wrongfd_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0

    def test_wrong_model_column_blocks(self):
        predictor = self._WrongModelPredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_wrongmod_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0


# ===========================================================================
# Tests: boolean quantile values rejected (Gap D)
# ===========================================================================


class TestBooleanQuantileRejection:
    """Boolean quantile values must be rejected in both child and parent."""

    class _BoolQuantilePredictor(FakePredictor):
        """Returns True in Q50 for all rows — must be rejected before serialization."""

        def predict(self, df_rivers_org, df_covariates, code, n):
            df = super().predict(df_rivers_org, df_covariates, code, n)
            df["Q50"] = True
            return df

    def test_bool_quantile_in_child_blocks(self):
        """Child must exit non-zero when predict() returns boolean in a quantile column."""
        predictor = self._BoolQuantilePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_boolq_child_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    api_write_raises=True,
                    csv_write_raises=True,
                )
        assert exc_info.value.code != 0

    def test_bool_quantile_in_parent_returns_none(self):
        """compute_recovery_unit() must return None when a response row has boolean quantile."""
        import recalculate_nan_forecasts as rnf

        issue_dt = datetime.date.fromisoformat(ISSUE_DATE)
        q_cols = [
            "Q5",
            "Q10",
            "Q15",
            "Q20",
            "Q25",
            "Q30",
            "Q35",
            "Q40",
            "Q45",
            "Q50",
            "Q55",
            "Q60",
            "Q65",
            "Q70",
            "Q75",
            "Q80",
            "Q85",
            "Q90",
            "Q95",
        ]
        rows = []
        for i in range(1, 12):
            row = {"date": (issue_dt + datetime.timedelta(days=i)).isoformat()}
            for q in q_cols:
                row[q] = True if q == "Q50" else 1.0  # boolean in Q50
            rows.append(row)

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            req_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH")
            if req_path:
                with open(req_path) as f:
                    req = json.load(f)
            resp = {
                "invocation_id": req["invocation_id"] if req_path else "x",
                "model": "TFT",
                "source_code": CODE_A,
                "issue_date": ISSUE_DATE,
                "flag": 4,
                "rows": rows,
            }
            if resp_path:
                with open(resp_path, "w") as f:
                    json.dump(resp, f)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)
        assert result is None


# ===========================================================================
# Tests: malformed row type in otherwise valid eleven-row response
# ===========================================================================


class TestMalformedResponseRows:
    """compute_recovery_unit() must return None when a row is not a dict.

    A valid 11-element rows list containing one null/int/list element must not
    raise AttributeError from row.get("date") — the isinstance(row, dict) guard
    must catch it and return None.
    """

    def _make_rows(self) -> list:
        """Return 11 valid dict rows for ISSUE_DATE."""
        issue_dt = datetime.date.fromisoformat(ISSUE_DATE)
        q_cols = [
            "Q5",
            "Q10",
            "Q15",
            "Q20",
            "Q25",
            "Q30",
            "Q35",
            "Q40",
            "Q45",
            "Q50",
            "Q55",
            "Q60",
            "Q65",
            "Q70",
            "Q75",
            "Q80",
            "Q85",
            "Q90",
            "Q95",
        ]
        rows = []
        for i in range(1, 12):
            row = {"date": (issue_dt + datetime.timedelta(days=i)).isoformat()}
            for q in q_cols:
                row[q] = 1.0
            rows.append(row)
        return rows

    @pytest.mark.parametrize(
        "bad_row,label",
        [
            (None, "null"),
            (42, "int"),
            (["date", "Q5"], "list"),
        ],
    )
    def test_non_dict_row_returns_none(self, bad_row, label):
        """Eleven-row response where one element is not a dict must return None."""
        import recalculate_nan_forecasts as rnf

        rows = self._make_rows()
        rows[0] = bad_row  # inject malformed element into otherwise valid list

        def fake_run(cmd, env, timeout):
            resp_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_RESPONSE_PATH")
            req_path = env.get("_SAPPHIRE_RECOVERY_COMPUTE_REQUEST_PATH")
            req = {}
            if req_path:
                with open(req_path) as f:
                    req = json.load(f)
            resp = {
                "invocation_id": req.get("invocation_id", "x"),
                "model": "TFT",
                "source_code": CODE_A,
                "issue_date": ISSUE_DATE,
                "flag": 4,
                "rows": rows,
            }
            if resp_path:
                with open(resp_path, "w") as f:
                    json.dump(resp, f)
            return MagicMock(returncode=0)

        candidate = {
            "issue_date": ISSUE_DATE,
            "model": "TFT",
            "code": CODE_A,
            "replace_keys": [],
            "insert_keys": [],
        }
        with patch.object(rnf.subprocess, "run", fake_run):
            result = rnf.compute_recovery_unit(candidate, timeout_seconds=30.0)
        assert result is None, (
            f"Expected None for {label} row element, got {result!r}; "
            "isinstance(row, dict) guard is missing"
        )


# ===========================================================================
# Tests: exact-minimum forcing history reaches predict(); short variants rejected
# ===========================================================================


class TestExactMinimumForcingHistory:
    """Recovery with exactly icl+max(0,max_ws-1) obs rows and ERA5 from obs_start
    through issue+max(11,ocl) must reach predict() exactly once.

    These tests exercise main() with the smallest valid ERA5 lookback
    (required_history-1 days before issue_date), which is below the ordinary
    60-day minimum that was previously applied unconditionally.  Before the fix
    the 60-day check raised ValueError here; after the fix it is skipped in
    recovery mode and _run_recovery_compute performs the model-specific check.
    """

    def test_exact_minimum_obs_and_forcing_tft_reaches_predict(self):
        """icl=30, max_ws=10 → required_history=39.
        ERA5 from issue-38 through issue+11, obs=39 rows: predict() called once."""
        # required_history = icl + max(0, max_ws-1) = 30+9 = 39
        # obs = 39 rows (extra_discharge_days=-1 → icl+max_ws-1 = 39)
        # era5_lookback=38 → era5 starts at issue-38 (39 days incl. issue_date)
        # Ordinary 60-day check: issue < (issue-38)+60 = issue+22 → would raise.
        # With fix: skipped in recovery mode; _run_recovery_compute validates instead.
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_exact_tft_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TFT",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=30,
                ocl=11,
                extra_discharge_days=-1,
                era5_lookback_override=38,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1, (
            f"Expected predict() called once, got {len(predictor.predict_calls)}"
        )
        assert len(predictor.hindcast_calls) == 0

    def test_one_day_short_obs_rejected_before_predict_tft(self):
        """icl=30, obs=38 (one short of 39) → _run_recovery_compute rejects before predict()."""
        # extra_discharge_days=-2 → obs=38; era5_lookback=38 → coverage fine
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_short_obs_tft_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    extra_discharge_days=-2,
                    era5_lookback_override=38,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0, (
            "predict() must not be called when obs is one day short of required_history"
        )

    def test_one_day_short_forcing_end_rejected_before_predict_tft(self):
        """ERA5 ends at issue+10 instead of issue+11 → covariate gap rejects before predict()."""
        # covariate_end_offset=10 → era5 ends at issue+10; _run_recovery_compute needs issue+11
        predictor = FakePredictor(input_chunk_length=30, output_chunk_length=11)
        with pytest.raises(SystemExit) as exc_info:
            with tempfile.TemporaryDirectory(prefix="sap_short_cov_tft_") as td:
                inv_id, req_path, resp_path = _write_request(td, "TFT", CODE_A, ISSUE_DATE)
                _run_main_in_process(
                    td,
                    "TFT",
                    ISSUE_DATE,
                    CODE_A,
                    req_path,
                    resp_path,
                    predictor,
                    icl=30,
                    ocl=11,
                    extra_discharge_days=-1,
                    era5_lookback_override=38,
                    covariate_end_offset=10,
                )
        assert exc_info.value.code != 0
        assert len(predictor.predict_calls) == 0, (
            "predict() must not be called when ERA5 ends one day short of issue+max(11,ocl)"
        )

    def test_exact_minimum_obs_and_forcing_tide_reaches_predict(self):
        """icl=20, max_ws=10 → required_history=29.
        ERA5 from issue-28 through issue+11, obs=29 rows: predict() called once."""
        # required_history = 20+9 = 29; obs=29 rows (extra_discharge_days=-1 → 20+10-1=29)
        # era5_lookback=28 → era5 starts at issue-28 (29 days incl. issue_date)
        predictor = FakePredictor(input_chunk_length=20, output_chunk_length=11)
        with tempfile.TemporaryDirectory(prefix="sap_exact_tide_") as td:
            inv_id, req_path, resp_path = _write_request(td, "TIDE", CODE_A, ISSUE_DATE)
            _run_main_in_process(
                td,
                "TIDE",
                ISSUE_DATE,
                CODE_A,
                req_path,
                resp_path,
                predictor,
                icl=20,
                ocl=11,
                extra_discharge_days=-1,
                era5_lookback_override=28,
                api_write_raises=True,
                csv_write_raises=True,
            )
        assert len(predictor.predict_calls) == 1, (
            f"Expected predict() called once for TiDE, got {len(predictor.predict_calls)}"
        )
        assert len(predictor.hindcast_calls) == 0
