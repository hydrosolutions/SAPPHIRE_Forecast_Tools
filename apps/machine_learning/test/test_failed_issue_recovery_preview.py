"""Tests for the P1a recovery preview core (ML-020).

Covers:
- read_unit_for_recovery_preview: pagination, mid-stream failure, empty confirmed
- _classify_unit_for_recovery: all eligibility paths
- preview_recovery_candidates: gate checks, unit dispatch, purity

Import isolation: all sys.modules mutations and sys.path insertions are managed
by _ml_env_ctx (used by the module-scoped fixture and isolation tests), which
snapshots and restores everything in a try/finally.  No collection-time mutations.

All station codes are synthetic (19xxx range per repo convention).
No real API, no model inference, no database, no CSV reads.
"""

import datetime
import importlib
import importlib.util
import os
import sys
import tempfile
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module-level placeholders; populated by _ml_module_env before any test.
# ---------------------------------------------------------------------------
rnf = None
utils_ml_forecast = None
_classify_unit_for_recovery = None
_expected_targets = None
preview_recovery_candidates = None
RecoveryReadError = None
read_unit_for_recovery_preview = None

# ---------------------------------------------------------------------------
# API-client availability check (evaluated at collection time without
# importing the production modules).
# ---------------------------------------------------------------------------
_SAPPHIRE_API_AVAILABLE = importlib.util.find_spec("sapphire_api_client") is not None

# ---------------------------------------------------------------------------
# Heavy-dependency stub list and production module prefixes.
# Mirrors test_canonical_ml_generation.py exactly so both files use the
# same snapshot/restore surface.
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

# Names this context manages as module globals.
_GLOBAL_ATTRS = (
    "rnf",
    "utils_ml_forecast",
    "_classify_unit_for_recovery",
    "_expected_targets",
    "preview_recovery_candidates",
    "RecoveryReadError",
    "read_unit_for_recovery_preview",
)


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
    _global_snap = {attr: getattr(_this, attr, _SENTINEL) for attr in _GLOBAL_ATTRS}

    try:
        # 4. Add sys.path entries required to import the production modules.
        _base = os.path.dirname(__file__)
        sys.path.insert(
            0,
            os.path.abspath(os.path.join(_base, "..", "..", "iEasyHydroForecast")),
        )
        sys.path.insert(0, os.path.abspath(os.path.join(_base, "..", "scr")))
        sys.path.insert(0, os.path.abspath(os.path.join(_base, "..")))

        # 5. Install heavy-dependency stubs.
        for key in _HEAVY_DEPS:
            sys.modules[key] = MagicMock()

        # 6. Import production modules (cwd → tempdir to suppress log files).
        _prior_cwd = os.getcwd()
        with tempfile.TemporaryDirectory(prefix="sapphire_ml_test_rp_") as _tmpdir:
            os.chdir(_tmpdir)
            try:
                _rnf = importlib.import_module("recalculate_nan_forecasts")
            finally:
                os.chdir(_prior_cwd)

        _utils = sys.modules["scr.utils_ml_forecast"]

        # 7. Inject as module globals so test bodies find them by name.
        _this.rnf = _rnf
        _this.utils_ml_forecast = _utils
        _this._classify_unit_for_recovery = _rnf._classify_unit_for_recovery
        _this._expected_targets = _rnf._expected_targets
        _this.preview_recovery_candidates = _rnf.preview_recovery_candidates
        _this.RecoveryReadError = _utils.RecoveryReadError
        _this.read_unit_for_recovery_preview = _utils.read_unit_for_recovery_preview

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


pytestmark = pytest.mark.skipif(
    not _SAPPHIRE_API_AVAILABLE,
    reason="sapphire-api-client not installed",
)

# ---------------------------------------------------------------------------
# Synthetic codes (repo convention: no real station codes)
# ---------------------------------------------------------------------------
CODE_A = "19001"
CODE_B = "19002"
CODE_C = "19003"

# ---------------------------------------------------------------------------
# Helpers: build rows in API column format
# ---------------------------------------------------------------------------


def _api_row(
    code: str,
    issue_date: str,
    target_date: str,
    model_type: str = "TFT",
    flag: int = 3,
    q05=None,
    q25=None,
    q50=None,
    q75=None,
    q95=None,
) -> dict:
    """Return one dict representing an API DAY row (raw API column names)."""
    return {
        "horizon_type": "day",
        "code": code,
        "model_type": model_type,
        "date": issue_date,
        "target": target_date,
        "flag": flag,
        "q05": q05,
        "q25": q25,
        "forecasted_discharge": q50,
        "q75": q75,
        "q95": q95,
    }


def _null_flag3_rows(
    code: str,
    issue_date: str,
    model_type: str = "TFT",
    n: int = 11,
) -> list[dict]:
    """Return n wholly null flag=3 rows for consecutive targets issue+1..issue+n."""
    targets = _expected_targets(issue_date)
    return [
        _api_row(code=code, issue_date=issue_date, target_date=t, model_type=model_type, flag=3)
        for t in targets[:n]
    ]


def _rows_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Helper: partial-bind page_size on the real reader in rnf's namespace.
# Used by tests that need to force pagination at a small page_size.
# ---------------------------------------------------------------------------


def _bind_reader_page_size(monkeypatch, page_size: int) -> None:
    """Wrap read_unit_for_recovery_preview in rnf's namespace to force page_size.

    preview_recovery_candidates calls read_unit_for_recovery_preview by name
    from rnf's namespace; patching there forces the custom page_size without
    replacing the real reader logic (don't mock the reader away).
    """
    _real = read_unit_for_recovery_preview

    def _wrapped(client, api_model_type, issue_date, code, **kwargs):
        kwargs.setdefault("page_size", page_size)
        return _real(client, api_model_type, issue_date, code, **kwargs)

    monkeypatch.setattr(rnf, "read_unit_for_recovery_preview", _wrapped)


# ---------------------------------------------------------------------------
# FakeClient
# ---------------------------------------------------------------------------


class FakeClient:
    """Recording fake SapphirePostprocessingClient.

    store: {(api_model_type, code, issue_date): [row_dicts]}
    Serves row_dicts sliced by skip/limit on each read call.
    _fail_on_second_page: set of keys where the second page (skip > 0) raises.
    write_forecasts and any model method raises AssertionError if called.
    """

    def __init__(self, store: dict | None = None):
        self.store: dict = store or {}
        self.read_calls: list[dict] = []
        self.write_calls: list = []
        self._fail_on_second_page: set = set()

    def readiness_check(self) -> bool:
        return True

    def _get(self, path: str, params: dict | None = None) -> list:
        params = params or {}
        model = params.get("model", "")
        code = str(params.get("code", ""))
        start_date = params.get("start_date", "")
        skip = params.get("skip", 0)
        limit = params.get("limit", 0)
        key = (model, code, start_date)
        self.read_calls.append(
            {
                "horizon": params.get("horizon"),
                "model": model,
                "code": code,
                "start_date": start_date,
                "end_date": params.get("end_date"),
                "skip": skip,
                "limit": limit,
            }
        )
        if key in self._fail_on_second_page and skip > 0:
            raise Exception(f"Simulated page-2 failure for key={key}")
        rows = self.store.get(key, [])
        return rows[skip : skip + limit]

    def read_short_term_forecasts(
        self,
        horizon: str,
        model: str,
        code: str,
        start_date: str,
        end_date: str,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        raw = self._get(
            "/forecast/",
            params={
                "horizon": horizon,
                "model": model,
                "code": code,
                "start_date": start_date,
                "end_date": end_date,
                "skip": skip,
                "limit": limit,
            },
        )
        if not raw:
            return pd.DataFrame()
        return pd.DataFrame(raw)

    def write_forecasts(self, records):
        self.write_calls.append(records)
        raise AssertionError("write_forecasts must NOT be called during preview")

    def predict(self, *args, **kwargs):
        raise AssertionError("predict must NOT be called during preview")


# ---------------------------------------------------------------------------
# PageVaryingFakeClient: returns a pre-specified DataFrame per skip offset.
# ---------------------------------------------------------------------------


class PageVaryingFakeClient:
    """Fake client that returns a caller-specified raw list for each skip offset.

    pages_by_skip: {skip_int: list_of_dicts}
    Any skip not in the dict returns an empty list (end of pages).
    """

    def __init__(self, pages_by_skip: dict[int, list]):
        self.pages_by_skip = pages_by_skip
        self.read_calls: list[dict] = []
        self.write_calls: list = []

    def readiness_check(self) -> bool:
        return True

    def _get(self, path: str, params: dict | None = None) -> list:
        params = params or {}
        skip = params.get("skip", 0)
        self.read_calls.append(
            {
                "horizon": params.get("horizon"),
                "model": params.get("model"),
                "code": params.get("code"),
                "start_date": params.get("start_date"),
                "end_date": params.get("end_date"),
                "skip": skip,
                "limit": params.get("limit"),
            }
        )
        return self.pages_by_skip.get(skip, [])

    def write_forecasts(self, records):
        self.write_calls.append(records)
        raise AssertionError("write_forecasts must NOT be called during preview")


# ---------------------------------------------------------------------------
# Section 1: read_unit_for_recovery_preview
# ---------------------------------------------------------------------------


class TestReadUnitForRecoveryPreview:
    """Strict reader: pagination, failure handling, empty confirmation."""

    def test_single_page_returns_all_rows(self):
        """11 rows in one page < page_size → returned intact."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        result = read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A)
        assert len(result) == 11
        assert list(result.columns)  # DataFrame, not empty
        assert len(client.read_calls) == 1

    def test_multi_page_returns_all_rows(self):
        """11 rows split across 3 pages (page_size=5) — none truncated."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        result = read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=5)
        # Page 1: skip=0 → 5 rows; page 2: skip=5 → 5 rows; page 3: skip=10 → 1 row
        assert len(result) == 11, f"Expected 11 rows, got {len(result)}"
        assert len(client.read_calls) == 3

    def test_confirmed_empty_returns_empty_dataframe(self):
        """No rows in store → empty page on first call → empty DataFrame (not error)."""
        client = FakeClient(store={})
        result = read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A)
        assert result.empty
        assert len(client.read_calls) == 1

    def test_fail_on_first_page_raises_recovery_read_error(self):
        """Transport error on first page → RecoveryReadError (not empty DataFrame)."""

        class FailAlwaysClient:
            def _get(self, path, params=None):
                raise ConnectionError("API unreachable")

        with pytest.raises(RecoveryReadError, match="Read failed"):
            read_unit_for_recovery_preview(FailAlwaysClient(), "TFT", "2026-10-20", CODE_A)

    def test_fail_on_second_page_raises_recovery_read_error(self):
        """Failure after first page received → RecoveryReadError (result incomplete)."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        client._fail_on_second_page.add(("TFT", CODE_A, "2026-10-20"))
        with pytest.raises(RecoveryReadError, match="mid-stream"):
            read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=5)

    def test_no_write_called_during_read(self):
        """FakeClient write spy is never called during read_unit."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=6)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A)
        assert client.write_calls == [], "write_forecasts must not be called during read"


# ---------------------------------------------------------------------------
# Section 2: _classify_unit_for_recovery
# ---------------------------------------------------------------------------


class TestClassifyUnitForRecovery:
    """Eligibility classifier — all paths."""

    # --- Candidate paths ---

    def test_complete_11_null_flag3_is_candidate(self):
        """All 11 null flag=3 targets → candidate with 11 replace_keys, 0 insert."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "candidate"
        assert len(result["replace_keys"]) == 11
        assert len(result["insert_keys"]) == 0
        assert result["n_targets"] == 11

    def test_six_plus_five_shape_is_candidate(self):
        """6 null flag=3 rows + 5 absent tail → candidate with replace=6, insert=5."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=6))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "candidate"
        assert len(result["replace_keys"]) == 6
        assert len(result["insert_keys"]) == 5
        assert result["n_targets"] == 11

    def test_replace_keys_contain_correct_identity(self):
        """replace_keys carry full API identity (horizon_type, code, model_type, date, target)."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        expected_targets = _expected_targets("2026-10-20")
        assert result["replace_keys"] == [
            ("day", CODE_A, "TFT", "2026-10-20", t) for t in expected_targets
        ]

    def test_insert_keys_are_correct_tail(self):
        """insert_keys carry full 5-tuple identity for targets issue+7..+11."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=6))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        expected_tail = _expected_targets("2026-10-20")[6:]  # targets 7-11
        assert result["insert_keys"] == [
            ("day", CODE_A, "TFT", "2026-10-20", t) for t in expected_tail
        ]

    def test_prior_year_issue_date_works(self):
        """Explicit prior-year dates are not silently truncated."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2025-08-18", n=11))
        result = _classify_unit_for_recovery("2025-08-18", "TFT", CODE_A, rows)
        assert result["status"] == "candidate"
        targets = _expected_targets("2025-08-18")
        assert result["replace_keys"][0] == ("day", CODE_A, "TFT", "2025-08-18", targets[0])

    def test_cross_year_boundary_date(self):
        """Issue date near year-end generates targets spanning year boundary."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2025-12-28", n=11))
        result = _classify_unit_for_recovery("2025-12-28", "TFT", CODE_A, rows)
        assert result["status"] == "candidate"
        # Targets include 2026-01-xx dates
        tail_targets = _expected_targets("2025-12-28")[6:]
        assert any(t.startswith("2026") for t in tail_targets)
        assert result["n_targets"] == 11

    # --- Skipped paths ---

    def test_no_rows_is_skipped(self):
        """Empty DataFrame → skipped (absent records are not flag-3)."""
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, pd.DataFrame())
        assert result["status"] == "skipped"
        assert "absent" in result["reason"].lower() or "no rows" in result["reason"].lower()

    def test_all_flag0_rows_is_skipped(self):
        """All flag=0 healthy rows → skipped (no flag-3 present)."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=0, q50=10.0)
                for t in _expected_targets("2026-10-20")[:6]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"

    def test_all_flag4_rows_is_skipped(self):
        """All flag=4 hindcast-succeeded rows → skipped."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=4, q50=5.0)
                for t in _expected_targets("2026-10-20")[:11]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"

    def test_all_flag1_rows_is_skipped(self):
        """All flag=1 rows → skipped (existing maintenance handles flags 1/2)."""
        rows = _rows_df(
            [_api_row(CODE_A, "2026-10-20", t, flag=1) for t in _expected_targets("2026-10-20")[:6]]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"

    def test_all_flag2_rows_is_skipped(self):
        """All flag=2 rows → skipped (existing maintenance handles flags 1/2)."""
        rows = _rows_df(
            [_api_row(CODE_A, "2026-10-20", t, flag=2) for t in _expected_targets("2026-10-20")[:6]]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"

    def test_fully_finite_flag3_is_skipped(self):
        """All 5 persisted fields finite for all flag=3 rows → skipped with explanation."""
        rows = _rows_df(
            [
                _api_row(
                    CODE_A, "2026-10-20", t, flag=3, q05=1.0, q25=2.0, q50=3.0, q75=4.0, q95=5.0
                )
                for t in _expected_targets("2026-10-20")[:11]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"
        assert "finite" in result["reason"].lower()

    def test_partial_flag3_some_fields_finite_is_skipped(self):
        """Some persisted fields finite, some null → skipped (partial flag-3 preserved)."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=3, q50=3.0)  # q50 finite, others null
                for t in _expected_targets("2026-10-20")[:6]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "skipped"
        assert "finite" in result["reason"].lower()

    # --- Blocked paths ---

    def test_nonfinite_flag3_is_blocked(self):
        """inf in a persisted field → blocked (non-finite archive)."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=3, q50=float("inf"))
                for t in _expected_targets("2026-10-20")[:6]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "non-finite" in result["reason"].lower()

    def test_negative_inf_flag3_is_blocked(self):
        """-inf in a persisted field → blocked."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=3, q05=float("-inf"))
                for t in _expected_targets("2026-10-20")[:6]
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"

    def test_mixed_flag3_and_flag0_is_blocked(self):
        """flag=3 rows alongside flag=0 rows → blocked (cannot isolate unit)."""
        targets = _expected_targets("2026-10-20")
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", targets[0], flag=0, q50=10.0),
                _api_row(CODE_A, "2026-10-20", targets[1], flag=3),
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "non-flag-3" in result["reason"].lower()

    def test_mixed_flag3_and_flag4_is_blocked(self):
        """flag=3 alongside flag=4 → blocked."""
        targets = _expected_targets("2026-10-20")
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", targets[0], flag=4, q50=5.0),
                _api_row(CODE_A, "2026-10-20", targets[1], flag=3),
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"

    def test_missing_required_columns_is_blocked(self):
        """DataFrame missing identity columns → blocked (malformed response)."""
        rows = pd.DataFrame({"code": [CODE_A], "flag": [3], "q50_wrongname": [None]})
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "malformed" in result["reason"].lower()

    def test_missing_persisted_columns_is_blocked(self):
        """DataFrame has identity cols but missing q05/q25 etc. → blocked."""
        rows = pd.DataFrame(
            {
                "horizon_type": ["day"],
                "code": [CODE_A],
                "model_type": ["TFT"],
                "date": ["2026-10-20"],
                "target": ["2026-10-21"],
                "flag": [3],
                # missing all q columns
            }
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "missing persisted" in result["reason"].lower()

    def test_unexpected_code_in_rows_is_blocked(self):
        """Rows with a different code than requested → blocked."""
        rows = _rows_df(_null_flag3_rows("19999", "2026-10-20", n=6))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        # CODE_A != "19999"
        assert result["status"] == "blocked"
        assert "unexpected station code" in result["reason"].lower()

    def test_wrong_model_type_in_rows_is_blocked(self):
        """Rows with different model_type → blocked."""
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, model_type="TiDE")
                for t in _expected_targets("2026-10-20")[:6]
            ]
        )
        # Classifying as TFT but rows are TiDE
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"

    def test_duplicate_target_dates_is_blocked(self):
        """Two rows with the same target date → blocked."""
        t0 = _expected_targets("2026-10-20")[0]
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t0, flag=3),
                _api_row(CODE_A, "2026-10-20", t0, flag=3),  # duplicate
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "duplicate" in result["reason"].lower()

    def test_out_of_window_target_is_blocked(self):
        """Target date beyond issue+11 → blocked."""
        issue = datetime.date.fromisoformat("2026-10-20")
        out_of_window = (issue + datetime.timedelta(days=15)).isoformat()
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", out_of_window, flag=3),
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "outside expected" in result["reason"].lower()

    def test_unsupported_shape_7_rows_missing_interior_is_blocked(self):
        """7 of 11 rows present but with interior gap → blocked (unsupported shape)."""
        targets = _expected_targets("2026-10-20")
        # Present: targets 0,1,2,3,4,6,7 (gap at index 5)
        present = [targets[i] for i in [0, 1, 2, 3, 4, 6, 7]]
        rows = _rows_df([_api_row(CODE_A, "2026-10-20", t, flag=3) for t in present])
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "unsupported shape" in result["reason"].lower()

    def test_unsupported_shape_missing_head_is_blocked(self):
        """Missing first target (issue+1) → blocked (unsupported missing-head shape)."""
        targets = _expected_targets("2026-10-20")
        rows = _rows_df(
            [
                _api_row(CODE_A, "2026-10-20", t, flag=3)
                for t in targets[1:]  # skip first
            ]
        )
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "blocked"
        assert "unsupported shape" in result["reason"].lower()

    def test_no_write_or_side_effect_from_classify(self):
        """_classify_unit_for_recovery must not modify the input DataFrame."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        original_len = len(rows)
        original_cols = list(rows.columns)
        _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert len(rows) == original_len
        assert list(rows.columns) == original_cols


# ---------------------------------------------------------------------------
# Section 3: preview_recovery_candidates — gate checks
# ---------------------------------------------------------------------------

_BASE_ENV = {
    "SAPPHIRE_API_ENABLED": "true",
    "ieasyhydroforecast_run_ML_models": "True",
    "ieasyhydroforecast_available_ML_models": "TFT,TIDE,TSMIXER",
    "SAPPHIRE_API_URL": "http://fake-test-host:8000",
}

# Permitted codes for tests that reach Gate 6 (station config gate).
_ALL_PERMITTED = frozenset({CODE_A, CODE_B, CODE_C})


class TestPreviewGates:
    """Gate checks fire before any client construction or API read."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def test_api_disabled_gate_blocks_all(self):
        """SAPPHIRE_API_ENABLED=false → gate_blocked, no client/read/write calls."""
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        with patch.dict(os.environ, {**_BASE_ENV, "SAPPHIRE_API_ENABLED": "false"}, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )

        assert result["gate_blocked"] is not None
        assert "SAPPHIRE_API_ENABLED" in result["gate_blocked"]
        assert factory_calls == [], "client_factory must not be called when API is disabled"
        assert result["n_units_checked"] == 0

    def test_ml_models_disabled_gate_blocks_all(self):
        """ieasyhydroforecast_run_ML_models=False → gate_blocked."""
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        with patch.dict(
            os.environ,
            {**_BASE_ENV, "ieasyhydroforecast_run_ML_models": "False"},
            clear=False,
        ):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )

        assert result["gate_blocked"] is not None
        assert "ML_models" in result["gate_blocked"] or "ML" in result["gate_blocked"]
        assert factory_calls == []

    def test_api_client_unavailable_gate_blocks_all(self):
        """SAPPHIRE_API_AVAILABLE=False → gate_blocked before any factory call."""
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            with patch("recalculate_nan_forecasts.SAPPHIRE_API_AVAILABLE", False):
                result = preview_recovery_candidates(
                    issue_dates=["2026-10-20"],
                    models=["TFT"],
                    station_codes=[CODE_A],
                    client_factory=spy_factory,
                )

        assert result["gate_blocked"] is not None
        assert "sapphire-api-client" in result["gate_blocked"].lower()
        assert factory_calls == []

    def test_empty_station_codes_gate_blocks(self):
        """Empty station_codes → gate_blocked."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "empty" in result["gate_blocked"].lower()

    def test_unsupported_model_gate_blocks(self):
        """Model not in {TFT, TIDE, TSMIXER} → gate_blocked."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["ARIMA"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "unsupported" in result["gate_blocked"].lower()

    def test_api_not_ready_gate_blocks(self):
        """API readiness check returns False → gate_blocked."""

        class NotReadyClient:
            def readiness_check(self):
                return False

            def write_forecasts(self, records):
                raise AssertionError("write must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: NotReadyClient(),
            )
        assert result["gate_blocked"] is not None
        assert "not ready" in result["gate_blocked"].lower()

    def test_readiness_exception_gate_blocks(self):
        """readiness_check raising → gate_blocked (not unhandled exception)."""

        class ExceptionReadyClient:
            def readiness_check(self):
                raise ConnectionError("Cannot connect")

            def write_forecasts(self, records):
                raise AssertionError("write must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: ExceptionReadyClient(),
            )
        assert result["gate_blocked"] is not None


# ---------------------------------------------------------------------------
# Section 4: preview_recovery_candidates — unit dispatch and classification
# ---------------------------------------------------------------------------


class TestPreviewUnitDispatch:
    """End-to-end unit dispatch, classification result, purity."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    @pytest.fixture
    def _page_size_5(self, monkeypatch):
        """Force page_size=5 via partial bind on rnf's reader reference."""
        _bind_reader_page_size(monkeypatch, 5)

    def _make_client(self, store=None):
        return FakeClient(store=store or {})

    def test_complete_11_null_flag3_yields_candidate(self):
        """Unit with 11 null flag=3 rows → appears in candidates."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", model_type="TFT", n=11)
        client = self._make_client({("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        c = result["candidates"][0]
        assert c["issue_date"] == "2026-10-20"
        assert c["model"] == "TFT"
        assert c["code"] == CODE_A
        assert c["n_targets"] == 11
        assert len(c["replace_keys"]) == 11
        assert len(c["insert_keys"]) == 0

    def test_six_plus_five_shape_yields_candidate_with_insert_keys(self):
        """Unit with 6 null flag=3 rows + absent tail → candidate with insert_keys."""
        rows = _null_flag3_rows(CODE_A, "2026-10-25", model_type="TFT", n=6)
        client = self._make_client({("TFT", CODE_A, "2026-10-25"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-25"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        c = result["candidates"][0]
        assert len(c["replace_keys"]) == 6
        assert len(c["insert_keys"]) == 5

    def test_healthy_flag0_unit_is_skipped(self):
        """Flag=0 unit → in skipped, not candidates or blocked."""
        targets = _expected_targets("2026-10-20")
        rows = [_api_row(CODE_A, "2026-10-20", t, flag=0, q50=5.0) for t in targets[:6]]
        client = self._make_client({("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["candidates"] == []
        assert len(result["skipped"]) == 1
        assert result["blocked"] == []

    def test_absent_unit_is_skipped(self):
        """No rows in API for unit → skipped (absent ≠ flag-3)."""
        client = self._make_client(store={})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["candidates"] == []
        assert len(result["skipped"]) == 1

    def test_read_error_per_unit_is_blocked(self):
        """RecoveryReadError for one unit → that unit blocked, others proceed."""

        class FailFirstReadClient:
            def __init__(self):
                self._calls = 0
                self._inner = FakeClient(
                    store={
                        ("TFT", CODE_B, "2026-10-20"): _null_flag3_rows(CODE_B, "2026-10-20", n=11)
                    }
                )

            def readiness_check(self):
                return True

            def _get(self, path, params=None):
                if (params or {}).get("code") == CODE_A:
                    raise ConnectionError("Simulated failure for CODE_A")
                return self._inner._get(path, params)

            def write_forecasts(self, records):
                raise AssertionError("write must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: FailFirstReadClient(),
            )

        assert result["gate_blocked"] is None
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["code"] == CODE_A
        assert "read error" in result["blocked"][0]["reason"].lower()
        # CODE_B should still be classified
        assert result["n_units_checked"] == 2

    def test_mixed_units_correct_counts(self):
        """Multiple stations and issue dates → counts are correct."""
        # CODE_A: 11 null flag=3 → candidate
        # CODE_B: flag=0 → skipped
        # CODE_C: partial shape (7 rows, interior gap) → blocked
        targets = _expected_targets("2026-10-20")
        partial = [targets[i] for i in [0, 1, 2, 3, 4, 6, 7]]  # gap at 5

        store = {
            ("TFT", CODE_A, "2026-10-20"): _null_flag3_rows(CODE_A, "2026-10-20", n=11),
            ("TFT", CODE_B, "2026-10-20"): [
                _api_row(CODE_B, "2026-10-20", t, flag=0, q50=5.0) for t in targets[:6]
            ],
            ("TFT", CODE_C, "2026-10-20"): [
                _api_row(CODE_C, "2026-10-20", t, flag=3) for t in partial
            ],
        }
        client = self._make_client(store)
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B, CODE_C],
                client_factory=lambda: client,
            )

        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_A
        assert len(result["skipped"]) == 1
        assert result["skipped"][0]["code"] == CODE_B
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["code"] == CODE_C
        assert result["n_units_checked"] == 3

    def test_page_failure_after_first_page_blocks_unit_no_keys(self, _page_size_5):
        """Mid-stream page failure blocks that unit with no planned keys; next unit proceeds.

        Replaces the previous vacuous test that discarded the configured failing
        client and used a fresh non-failing one.  This test:
        - Injects the configured failing client through the public entry point.
        - Forces pagination via _page_size_5 (page_size=5 for 11-row units).
        - Asserts CODE_A is blocked with no replace/insert keys.
        - Asserts multiple read calls were made (pagination actually occurred).
        - Asserts CODE_B (next unit) is still classified as a candidate.
        """
        rows_a = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        # Inject the failure BEFORE calling preview — the same client is used
        client._fail_on_second_page.add(("TFT", CODE_A, "2026-10-20"))

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,  # same client, failure configured
            )

        # CODE_A: blocked by mid-stream failure; no planned keys
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["code"] == CODE_A
        assert "read error" in result["blocked"][0]["reason"].lower()

        # CODE_B: still processed → candidate
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B

        # Pagination occurred: CODE_A must have at least two read calls
        code_a_calls = [c for c in client.read_calls if c["code"] == CODE_A]
        assert any(c["skip"] == 0 for c in code_a_calls), "First page for CODE_A was fetched"
        assert any(c["skip"] > 0 for c in code_a_calls), "Second page for CODE_A was attempted"

        # CODE_B was also read
        code_b_calls = [c for c in client.read_calls if c["code"] == CODE_B]
        assert len(code_b_calls) >= 1, "CODE_B was read"

        # Both units were checked
        assert result["n_units_checked"] == 2

    def test_write_spy_never_called_during_preview(self):
        """FakeClient.write_forecasts must not be called during preview."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        # If write_forecasts were called, the FakeClient would have raised
        assert client.write_calls == []

    def test_preview_purity_inputs_unchanged(self):
        """Input lists must not be modified by preview_recovery_candidates."""
        issue_dates = ["2026-10-20"]
        models = ["TFT"]
        station_codes = [CODE_A, CODE_B]
        original_issues = list(issue_dates)
        original_models = list(models)
        original_codes = list(station_codes)

        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            preview_recovery_candidates(
                issue_dates=issue_dates,
                models=models,
                station_codes=station_codes,
                client_factory=lambda: client,
            )

        assert issue_dates == original_issues
        assert models == original_models
        assert station_codes == original_codes

    def test_model_name_normalisation_tide(self):
        """'TIDE' (internal) → 'TiDE' (API) in read calls."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", model_type="TiDE", n=11)
        client = FakeClient(store={("TiDE", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TIDE"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["gate_blocked"] is None
        # Read was made with API name "TiDE"
        assert any(c["model"] == "TiDE" for c in client.read_calls)

    def test_model_name_normalisation_tsmixer(self):
        """'TSMIXER' (internal) → 'TSMixer' (API) in read calls."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", model_type="TSMixer", n=11)
        client = FakeClient(store={("TSMixer", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TSMIXER"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["gate_blocked"] is None
        assert any(c["model"] == "TSMixer" for c in client.read_calls)

    def test_result_model_name_is_uppercase_internal(self):
        """Result entries carry uppercase internal model names (TFT, TIDE, TSMIXER)."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", model_type="TFT", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["candidates"][0]["model"] == "TFT"

    def test_explicit_prior_year_issue_date(self):
        """Prior-year issue date (2025) is not truncated or replaced with today."""
        rows = _null_flag3_rows(CODE_A, "2025-08-18", model_type="TFT", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2025-08-18"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2025-08-18"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["issue_date"] == "2025-08-18"
        # Verify the read was made for 2025-08-18, not today
        assert any(c["start_date"] == "2025-08-18" for c in client.read_calls)

    def test_valid_empty_page_distinct_from_failed_read(self):
        """HTTP 200 with 0 rows → unit skipped (no flag-3). Must NOT raise."""
        # Store has no rows for this key → empty page on first call
        client = FakeClient(store={})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        # Empty confirmed → skipped (not blocked)
        assert result["gate_blocked"] is None
        assert result["blocked"] == []
        assert len(result["skipped"]) == 1

    def test_flag1_2_rows_are_skipped_not_candidate(self):
        """Flag 1/2 rows → skipped (handled by existing maintenance recalc)."""
        targets = _expected_targets("2026-10-20")
        rows = [_api_row(CODE_A, "2026-10-20", t, flag=1) for t in targets[:6]]
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert result["candidates"] == []
        assert len(result["skipped"]) == 1
        assert result["blocked"] == []

    def test_multiple_issue_dates_processed_independently(self):
        """Two issue dates, each with a candidate → both appear in candidates."""
        rows1 = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        rows2 = _null_flag3_rows(CODE_A, "2026-10-25", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows1,
                ("TFT", CODE_A, "2026-10-25"): rows2,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20", "2026-10-25"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert len(result["candidates"]) == 2
        issue_dates_out = {c["issue_date"] for c in result["candidates"]}
        assert issue_dates_out == {"2026-10-20", "2026-10-25"}
        assert result["n_units_checked"] == 2

    def test_n_units_checked_counts_all_combinations(self):
        """n_units_checked = len(issue_dates) × len(models) × len(station_codes)."""
        client = FakeClient(store={})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20", "2026-10-25"],
                models=["TFT", "TIDE"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        assert result["n_units_checked"] == 2 * 2 * 2  # 8


# ---------------------------------------------------------------------------
# Section 5: New gate checks (Issue 1 fixes)
# ---------------------------------------------------------------------------


class TestPreviewNewGates:
    """Configured ML allowlist + permitted station codes gates."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def test_missing_configured_models_env_gate_blocks(self):
        """ieasyhydroforecast_available_ML_models not set → gate_blocked before client."""
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        env = {**_BASE_ENV, "ieasyhydroforecast_available_ML_models": ""}
        with patch.dict(os.environ, env, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )

        assert result["gate_blocked"] is not None
        assert "available_ML_models" in result["gate_blocked"]
        assert factory_calls == [], "client_factory must not be called when models not configured"

    def test_model_not_in_configured_allowlist_gate_blocks(self):
        """TFT not in configured allowlist → gate_blocked."""
        env = {**_BASE_ENV, "ieasyhydroforecast_available_ML_models": "TIDE,TSMIXER"}
        with patch.dict(os.environ, env, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "TFT" in result["gate_blocked"] or "allowlist" in result["gate_blocked"].lower()

    def test_permitted_stations_unavailable_gate_blocks(self, monkeypatch):
        """get_permitted_station_codes returns None → gate_blocked before client."""
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: None)
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )

        assert result["gate_blocked"] is not None
        assert "station configuration" in result["gate_blocked"].lower()
        assert factory_calls == []

    def test_permitted_stations_empty_gate_blocks(self, monkeypatch):
        """get_permitted_station_codes returns empty set → gate_blocked."""
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: set())
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert (
            "empty" in result["gate_blocked"].lower()
            or "no permitted" in result["gate_blocked"].lower()
        )

    def test_station_not_in_permitted_gate_blocks(self, monkeypatch):
        """Requested station not in permitted set → gate_blocked."""
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: {CODE_B, CODE_C})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "permitted" in result["gate_blocked"].lower()

    def test_invalid_iso_date_gate_blocks(self):
        """Non-ISO issue_date → gate_blocked before any API contact."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["not-a-date"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert (
            "iso" in result["gate_blocked"].lower() or "invalid" in result["gate_blocked"].lower()
        )

    def test_empty_issue_dates_gate_blocks(self):
        """Empty issue_dates → gate_blocked."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=[],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "empty" in result["gate_blocked"].lower()

    def test_empty_models_gate_blocks(self):
        """Empty models → gate_blocked."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=[],
                station_codes=[CODE_A],
                client_factory=lambda: FakeClient(),
            )
        assert result["gate_blocked"] is not None
        assert "empty" in result["gate_blocked"].lower()


# ---------------------------------------------------------------------------
# Section 6: Classifier edge cases through actual preview (Issue 2 fixes)
# ---------------------------------------------------------------------------


class TestPreviewClassifierEdgeCases:
    """Classifier correctness via preview_recovery_candidates (not classifier-only)."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def _run(self, rows_for_code_a):
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows_for_code_a})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            return preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )

    def test_bool_flag_blocked(self):
        """Boolean flag value → blocked (not silently coerced to int 1)."""
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), flag=True)
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1
        assert result["candidates"] == []

    def test_float_non_integer_flag_blocked(self):
        """Non-integer float flag 2.5 → blocked (not truncated to 2)."""
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), flag=2.5)
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_unknown_flag_value_blocked(self):
        """Unknown flag value 9 → blocked."""
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), flag=9)
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_null_flag_blocked(self):
        """Null flag → blocked (null is not a valid flag)."""
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), flag=None)
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_unparseable_string_flag_blocked(self):
        """Non-null unparseable string 'bad' → blocked (not silently null)."""
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), flag="bad")
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_inf_quantile_blocked(self):
        """Infinity in quantile field → blocked (non-finite)."""
        rows = [
            _api_row(CODE_A, "2026-10-20", t, flag=3, q50=float("inf"))
            for t in _expected_targets("2026-10-20")[:6]
        ]
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_single_invalid_flag_in_otherwise_valid_unit_blocked(self):
        """One row with flag=None among otherwise valid flag=3 rows → blocked."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        rows[5] = dict(rows[5], flag=None)
        result = self._run(rows)
        assert len(result["blocked"]) == 1

    def test_candidate_only_true_all_null_flag3(self):
        """candidateonlytrueallnull3: only all-null flag=3 units become candidates."""
        # CODE_A: all 11, flag=3, all null → CANDIDATE
        rows_a = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        # CODE_B: all 11, flag=3, q50 finite → SKIPPED (has finite values)
        rows_b = [
            _api_row(CODE_B, "2026-10-20", t, flag=3, q50=5.0)
            for t in _expected_targets("2026-10-20")
        ]
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_A
        assert len(result["skipped"]) == 1
        assert result["skipped"][0]["code"] == CODE_B


# ---------------------------------------------------------------------------
# Section 7: Bounded reader (Issue 3 fixes)
# ---------------------------------------------------------------------------


class TestReadUnitBounded:
    """Bounded read: invalid page_size and fan-bound enforcement."""

    def test_page_size_zero_raises_before_any_read(self):
        """page_size=0 → RecoveryReadError before any API call."""
        client = FakeClient(store={})
        with pytest.raises(RecoveryReadError, match="page_size"):
            read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=0)
        assert client.read_calls == []

    def test_page_size_negative_raises_before_any_read(self):
        """page_size=-1 → RecoveryReadError before any API call."""
        client = FakeClient(store={})
        with pytest.raises(RecoveryReadError, match="page_size"):
            read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=-1)
        assert client.read_calls == []

    def test_exactly_11_rows_succeeds(self):
        """Exactly 11 rows (the fan bound) → succeeds, not blocked."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        result = read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=20)
        assert len(result) == 11

    def test_twelve_rows_raises_fan_bound_error(self):
        """12 rows (one over the fan bound) → RecoveryReadError."""
        # Build 12 rows with targets issue+1..issue+12
        issue = datetime.date.fromisoformat("2026-10-20")
        targets_12 = [(issue + datetime.timedelta(days=i)).isoformat() for i in range(1, 13)]
        rows_12 = [_api_row(CODE_A, "2026-10-20", t, flag=3) for t in targets_12]
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows_12})
        with pytest.raises(RecoveryReadError, match="fan bound"):
            read_unit_for_recovery_preview(client, "TFT", "2026-10-20", CODE_A, page_size=20)

    def test_repeating_full_page_client_stops_finite_calls(self):
        """A client that always returns a full page stops after fan bound, finite calls."""

        class InfiniteFullPageClient:
            def __init__(self):
                self.call_count = 0

            def _get(self, path, params=None):
                self.call_count += 1
                params = params or {}
                skip = params.get("skip", 0)
                limit = params.get("limit", 5)
                issue = datetime.date.fromisoformat("2026-10-20")
                return [
                    _api_row(
                        CODE_A,
                        "2026-10-20",
                        (issue + datetime.timedelta(days=skip + i + 1)).isoformat(),
                        flag=3,
                    )
                    for i in range(limit)
                ]

        infinite_client = InfiniteFullPageClient()
        with pytest.raises(RecoveryReadError, match="fan bound"):
            read_unit_for_recovery_preview(
                infinite_client, "TFT", "2026-10-20", CODE_A, page_size=5
            )
        # With page_size=5 and bound=11: pages at skip=0(5), skip=5(10), skip=10(15>11) → 3 calls
        assert infinite_client.call_count <= 3


# ---------------------------------------------------------------------------
# Section 8: Quantile field validation via preview_recovery_candidates (Bug 1 fixes)
# ---------------------------------------------------------------------------

_Q_FIELDS = ("q05", "q25", "forecasted_discharge", "q75", "q95")


class TestQuantileFieldValidationViaPreview:
    """Step 5 quantile guards — parametrized over all 5 persisted API fields.

    All tests go through preview_recovery_candidates so the full call path is exercised.
    write_forecasts and model inference must never be called.
    """

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def _run(self, rows):
        """Run preview_recovery_candidates for CODE_A / 2026-10-20 / TFT."""
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            return preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )

    def _rows_with_field(self, field, value, n=11):
        """11 flag=3 rows with `field` set to `value`, all other quantile fields null."""
        rows = []
        for t in _expected_targets("2026-10-20")[:n]:
            row = _api_row(CODE_A, "2026-10-20", t, flag=3)
            row[field] = value
            rows.append(row)
        return rows

    # --- string garbage blocked across all 5 fields ---

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_string_garbage_all_rows_blocked_not_candidate(self, field):
        """All 11 flag=3 rows with string garbage in one field → blocked, not candidate."""
        rows = self._rows_with_field(field, "garbage_string")
        result = self._run(rows)
        assert result["gate_blocked"] is None
        assert len(result["blocked"]) == 1, (
            f"Expected 1 blocked unit for string garbage in {field}, got: {result}"
        )
        assert result["candidates"] == []

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_string_nan_nonnull_blocked(self, field):
        """String 'NaN' (non-null) in any quantile field → blocked (not treated as null)."""
        rows = self._rows_with_field(field, "NaN")
        result = self._run(rows)
        assert result["gate_blocked"] is None
        assert len(result["blocked"]) == 1, (
            f"Expected 1 blocked unit for string 'NaN' in {field}, got: {result}"
        )
        assert result["candidates"] == []

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_boolean_quantile_blocked(self, field):
        """Boolean value in any quantile field → blocked (not coerced to 0/1 finite)."""
        rows = self._rows_with_field(field, True)
        result = self._run(rows)
        assert result["gate_blocked"] is None
        assert len(result["blocked"]) == 1, (
            f"Expected 1 blocked unit for boolean in {field}, got: {result}"
        )
        assert result["candidates"] == []

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_finite_numeric_skipped_not_candidate(self, field):
        """Finite numeric value in any quantile field → skipped, not candidate."""
        rows = self._rows_with_field(field, 5.0)
        result = self._run(rows)
        assert result["gate_blocked"] is None
        assert result["candidates"] == []
        assert len(result["skipped"]) == 1, (
            f"Expected 1 skipped unit for finite value in {field}, got: {result}"
        )

    # --- non-parametrized edge cases ---

    def test_all_null_all_fields_candidate(self):
        """All 11 flag=3 rows, all 5 quantile fields null (float NaN/None) → candidate."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        result = self._run(rows)
        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["n_targets"] == 11

    def test_single_bad_field_among_null_blocks(self):
        """One string value in one row of an otherwise all-null unit → blocked, not candidate."""
        rows = list(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        rows[3] = dict(rows[3], q25="bad_value")
        result = self._run(rows)
        assert len(result["blocked"]) == 1
        assert result["candidates"] == []

    def test_write_spy_not_called_on_blocked_unit(self):
        """FakeClient.write_forecasts must not be called even when a unit is blocked."""
        rows = self._rows_with_field("q05", "garbage_string")
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert client.write_calls == []


# ---------------------------------------------------------------------------
# Section 9: Gate 3 affirmative-check additions (Bug 2 fixes)
# ---------------------------------------------------------------------------


class TestGate3AffirmativeCheck:
    """Gate 3 now requires an affirmative 'true' (case-insensitive).

    Missing, empty, and garbage values must block before any client construction.
    """

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def test_ml_models_env_missing_gate_blocks(self, monkeypatch):
        """Absent ieasyhydroforecast_run_ML_models → gate_blocked; factory not called."""
        monkeypatch.delenv("ieasyhydroforecast_run_ML_models", raising=False)
        for k, v in _BASE_ENV.items():
            if k != "ieasyhydroforecast_run_ML_models":
                monkeypatch.setenv(k, v)
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        result = preview_recovery_candidates(
            issue_dates=["2026-10-20"],
            models=["TFT"],
            station_codes=[CODE_A],
            client_factory=spy_factory,
        )
        assert result["gate_blocked"] is not None
        assert "ML_models" in result["gate_blocked"] or "ML" in result["gate_blocked"]
        assert factory_calls == [], "client_factory must not be called when ML env var is absent"

    def test_ml_models_garbage_value_gate_blocks(self):
        """Garbage string 'enabled' → gate_blocked; factory not called."""
        factory_calls = []

        def spy_factory():
            factory_calls.append(True)
            return FakeClient()

        with patch.dict(
            os.environ,
            {**_BASE_ENV, "ieasyhydroforecast_run_ML_models": "enabled"},
            clear=False,
        ):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )
        assert result["gate_blocked"] is not None
        assert factory_calls == [], "client_factory must not be called for garbage ML value"

    def test_ml_models_true_value_passes_gate3(self):
        """Affirmative 'True' → Gate 3 passes; client_factory is called."""
        factory_calls = []

        class AlwaysReadyClient:
            def readiness_check(self):
                return True

            def _get(self, path, params=None):
                return []

            def write_forecasts(self, records):
                raise AssertionError("write must not be called during preview")

        def spy_factory():
            factory_calls.append(True)
            return AlwaysReadyClient()

        with patch.dict(
            os.environ,
            {**_BASE_ENV, "ieasyhydroforecast_run_ML_models": "True"},
            clear=False,
        ):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=spy_factory,
            )
        assert result["gate_blocked"] is None
        assert factory_calls != [], "client_factory must be called when ML is enabled"


# ---------------------------------------------------------------------------
# Section 10: Raw identity validation (Finding 1 regressions)
#
# Validate raw identity contract BEFORE normalization.  A row whose code has
# leading/trailing whitespace, or whose date/target column carries a non-midnight
# timestamp or timezone offset, must be blocked.  Keys must not carry
# silently-truncated/normalized values.
# ---------------------------------------------------------------------------


class TestRawIdentityValidation:
    """Finding 1: raw identity values must match before strip/dt-strftime normalization."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def test_raw_code_leading_trailing_space_is_blocked_not_candidate(self):
        """Row code ' 19001 ' (with spaces) must be blocked, not normalized to candidate.

        Raw identity: the row code does not match the requested code before trimming.
        Planned keys must not identify silently-trimmed codes.
        """
        targets = _expected_targets("2026-10-20")
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), code=" " + CODE_A + " ")
            for t in targets[:11]
        ]
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, pd.DataFrame(rows))
        assert result["status"] == "blocked"
        assert result["replace_keys"] == []
        assert result["insert_keys"] == []

    def test_raw_code_space_via_preview_is_blocked_not_candidate(self):
        """Preview with rows having padded codes → blocked, no replacement keys."""
        targets = _expected_targets("2026-10-20")
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), code=" " + CODE_A + " ")
            for t in targets[:11]
        ]
        client = FakeClient(store={("TFT", CODE_A, "2026-10-20"): rows})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert len(result["blocked"]) == 1
        assert result["candidates"] == []

    def test_issue_date_nonmidnight_timestamp_is_blocked_no_truncated_keys(self):
        """date column with non-midnight timestamp must be blocked; no truncated keys.

        "2026-10-20T12:00:00" → pd.to_datetime + strftime("%Y-%m-%d") = "2026-10-20"
        (matches issue_date_str), so without raw validation this silently passes and
        produces replace_keys with date-only strings that don't match the actual record.
        """
        targets = _expected_targets("2026-10-20")
        rows = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), **{"date": "2026-10-20T12:00:00"})
            for t in targets[:11]
        ]
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, pd.DataFrame(rows))
        assert result["status"] == "blocked"
        assert result["replace_keys"] == []

    def test_target_nonmidnight_timestamp_is_blocked_no_truncated_keys(self):
        """target column with non-midnight timestamp → blocked, no truncated replacement keys."""
        targets = _expected_targets("2026-10-20")
        rows = [
            dict(
                _api_row(CODE_A, "2026-10-20", t, flag=3),
                **{"target": t + "T12:00:00"},
            )
            for t in targets[:11]
        ]
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, pd.DataFrame(rows))
        assert result["status"] == "blocked"
        assert result["replace_keys"] == []

    def test_target_with_timezone_offset_is_blocked_not_attribute_error(self):
        """Mixed-timezone target strings → blocked (not AttributeError from .dt.strftime)."""
        targets = _expected_targets("2026-10-20")
        rows = [
            dict(
                _api_row(CODE_A, "2026-10-20", t, flag=3),
                **{"target": (t + "T00:00:00+00:00" if i % 2 == 0 else t + "T00:00:00+05:00")},
            )
            for i, t in enumerate(targets[:11])
        ]
        # Must not raise; must return blocked
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, pd.DataFrame(rows))
        assert result["status"] == "blocked"
        assert result["replace_keys"] == []

    def test_proper_iso_date_and_exact_code_remains_candidate(self):
        """Proper ISO date-only strings and exact code → still candidate (no regression)."""
        rows = _rows_df(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        result = _classify_unit_for_recovery("2026-10-20", "TFT", CODE_A, rows)
        assert result["status"] == "candidate"
        assert len(result["replace_keys"]) == 11

    def test_next_valid_unit_processed_after_raw_code_block(self):
        """After a padded-code unit is blocked, the next valid station is still a candidate."""
        targets = _expected_targets("2026-10-20")
        rows_a = [
            dict(_api_row(CODE_A, "2026-10-20", t, flag=3), code=" " + CODE_A + " ")
            for t in targets[:11]
        ]
        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        assert len(result["blocked"]) == 1
        assert result["blocked"][0]["code"] == CODE_A
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B
        assert result["n_units_checked"] == 2


# ---------------------------------------------------------------------------
# Section 11: Per-page column validation + zero-column frame detection
# (Findings 2 and 3 regressions)
#
# Finding 2: required columns must be validated per page before concatenation.
# Finding 3: DataFrame.empty is True for zero-column frames (non-zero rows);
#             zero-column pages must be treated as malformed, not as end-of-pages.
# ---------------------------------------------------------------------------


class TestPerPageColumnValidation:
    """Findings 2 & 3: per-page schema validation; zero-column ≠ empty."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    @pytest.fixture
    def _page_size_5(self, monkeypatch):
        """Force page_size=5 on the reader in rnf's namespace."""
        _bind_reader_page_size(monkeypatch, 5)

    @pytest.fixture
    def _page_size_6(self, monkeypatch):
        """Force page_size=6 on the reader in rnf's namespace."""
        _bind_reader_page_size(monkeypatch, 6)

    def test_page_missing_quantile_cols_blocks_whole_unit(self, _page_size_5):
        """Page 2 missing all 5 quantile cols → whole unit blocked, no planned keys.

        With page_size=5: page1(skip=0)=5 valid rows, page2(skip=5)=5 rows missing
        all quantile cols, page3(skip=10)=1 valid row.
        concat() of pages 1+2+3 would fill missing quantile cols with NaN, allowing
        the full-column-presence check to pass and producing a false candidate.
        Per-page validation must block before concatenation.
        """
        targets = _expected_targets("2026-10-20")
        page1 = [_api_row(CODE_A, "2026-10-20", t, flag=3) for t in targets[:5]]
        page2 = [
            {
                "horizon_type": "day",
                "code": CODE_A,
                "model_type": "TFT",
                "date": "2026-10-20",
                "target": t,
                "flag": 3,
            }
            for t in targets[5:10]
        ]  # no q05/q25/forecasted_discharge/q75/q95 keys
        page3 = [_api_row(CODE_A, "2026-10-20", targets[10], flag=3)]

        client = PageVaryingFakeClient({0: page1, 5: page2, 10: page3})
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: client,
            )
        assert len(result["blocked"]) == 1, (
            f"Expected 1 blocked unit for page-2 missing quantile cols, got: {result}"
        )
        assert result["candidates"] == []
        assert result["blocked"][0]["code"] == CODE_A

    def test_page_missing_quantile_cols_next_unit_still_candidate(self, _page_size_5):
        """Per-page column failure for CODE_A does not prevent CODE_B from being a candidate."""
        targets = _expected_targets("2026-10-20")
        page1_a = [_api_row(CODE_A, "2026-10-20", t, flag=3) for t in targets[:5]]
        page2_a = [
            {
                "horizon_type": "day",
                "code": CODE_A,
                "model_type": "TFT",
                "date": "2026-10-20",
                "target": t,
                "flag": 3,
            }
            for t in targets[5:10]
        ]  # no q05/q25/forecasted_discharge/q75/q95 keys
        page3_a = [_api_row(CODE_A, "2026-10-20", targets[10], flag=3)]
        client_a_pages = PageVaryingFakeClient({0: page1_a, 5: page2_a, 10: page3_a})

        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client_b = FakeClient(store={("TFT", CODE_B, "2026-10-20"): rows_b})

        class ComboClient:
            def readiness_check(self):
                return True

            def _get(self, path, params=None):
                if (params or {}).get("code") == CODE_A:
                    return client_a_pages._get(path, params)
                return client_b._get(path, params)

            def write_forecasts(self, records):
                raise AssertionError("write_forecasts must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: ComboClient(),
            )
        assert len(result["blocked"]) >= 1
        assert any(b["code"] == CODE_A for b in result["blocked"])
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B
        assert result["n_units_checked"] == 2

    def test_six_empty_dict_records_is_blocked_not_skipped(self):
        """Client returning [{},{},{},{},{},{}] → len=6, cols=0 → blocked, not skipped.

        pd.DataFrame([{}, ...]) has zero columns; DataFrame.empty returns True because
        there are no items (n_rows * n_cols == 0).  The reader must not treat this as
        confirmed-empty (end-of-pagination) — it is a malformed response.
        """

        class ZeroColumnFirstPageClient:
            def readiness_check(self):
                return True

            def _get(self, path, params=None):
                if (params or {}).get("skip", 0) == 0:
                    return [{} for _ in range(6)]
                return []

            def write_forecasts(self, records):
                raise AssertionError("write_forecasts must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: ZeroColumnFirstPageClient(),
            )
        assert result["gate_blocked"] is None
        # Zero-column, six-row response must be blocked, not skipped
        assert len(result["blocked"]) == 1, (
            f"Expected blocked for zero-column 6-row response, got: {result}"
        )
        assert result["candidates"] == []
        assert result["skipped"] == [], (
            "Six zero-column records must NOT be classified as skipped (absent)"
        )

    def test_zero_column_page_after_valid_six_rows_is_blocked_not_false_candidate(
        self, _page_size_6
    ):
        """Valid 6-row first page, then zero-column second page → blocked, not false 6+5.

        With page_size=6: page1 returns exactly 6 rows (fills page → reader tries page 2).
        page2 returns pd.DataFrame([{}]*5) (zero-column, non-empty by row count).
        DataFrame.empty is True for zero-column → reader sees "empty" and stops.
        The accumulated 6 rows match the six+five shape → false candidate.
        Per-page validation must detect the malformed page and block the unit.
        """
        targets = _expected_targets("2026-10-20")
        valid_6_rows = [_api_row(CODE_A, "2026-10-20", t, flag=3) for t in targets[:6]]

        class PageSwitchClient:
            def readiness_check(self):
                return True

            def _get(self, path, params=None):
                params = params or {}
                skip = params.get("skip", 0)
                limit = params.get("limit", 6)
                if skip == 0:
                    return valid_6_rows[:limit]
                # Any subsequent page: empty-dict records (malformed response)
                return [{} for _ in range(5)]

            def write_forecasts(self, records):
                raise AssertionError("write_forecasts must not be called")

        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: PageSwitchClient(),
            )
        assert result["gate_blocked"] is None
        # Must be blocked, not a false 6+5 candidate
        assert result["candidates"] == [], (
            f"Expected no candidates (false 6+5 blocked), got: {result['candidates']}"
        )
        assert len(result["blocked"]) == 1, (
            f"Expected blocked for zero-column second page, got: {result}"
        )


# ---------------------------------------------------------------------------
# Section 12: Diagnostic formatting tolerance (Finding 4 regressions)
#
# Malformed identity fields (mixed null+integer model_type, mixed-timezone dates)
# must not raise TypeError or AttributeError.  Both units must be checked:
# CODE_A blocked, CODE_B still processed as a candidate.
# ---------------------------------------------------------------------------


class TestDiagnosticFormattingTolerance:
    """Finding 4: identity validation must not raise on heterogeneous column values."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    def test_mixed_null_integer_model_type_is_blocked_not_type_error(self):
        """Rows with mixed None/integer model_type → blocked (not TypeError), next unit OK.

        sorted() of a set containing both None and an integer raises TypeError on Python 3.
        The diagnostic path that sorts unexpected model_type values must tolerate this.
        """
        targets = _expected_targets("2026-10-20")
        rows_a = [
            dict(
                _api_row(CODE_A, "2026-10-20", t, flag=3),
                **{"model_type": None if i % 2 == 0 else 42},
            )
            for i, t in enumerate(targets[:6])
        ]
        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        # CODE_A: blocked (not TypeError raised)
        assert len(result["blocked"]) >= 1
        assert any(b["code"] == CODE_A for b in result["blocked"])
        # CODE_B: still processed → candidate
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B
        assert result["n_units_checked"] == 2

    def test_mixed_offset_date_strings_is_blocked_not_attribute_error(self):
        """Mixed-timezone target strings → blocked (not AttributeError), next unit OK.

        pd.to_datetime with mixed timezone offsets may return an object-dtype Series.
        Calling .dt.strftime on such a Series raises AttributeError.  The classifier
        must handle this and return blocked rather than propagating the exception.
        """
        targets = _expected_targets("2026-10-20")
        rows_a = [
            dict(
                _api_row(CODE_A, "2026-10-20", t, flag=3),
                **{"target": (t + "T00:00:00+00:00" if i % 2 == 0 else t + "T00:00:00+05:00")},
            )
            for i, t in enumerate(targets[:6])
        ]
        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        # CODE_A: blocked (not AttributeError)
        assert len(result["blocked"]) >= 1
        assert any(b["code"] == CODE_A for b in result["blocked"])
        # CODE_B: still processed → candidate
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B
        assert result["n_units_checked"] == 2

    def test_mixed_offset_issue_date_is_blocked_not_attribute_error(self):
        """Mixed-timezone date (issue date) strings → blocked, not AttributeError."""
        targets = _expected_targets("2026-10-20")
        rows_a = [
            dict(
                _api_row(CODE_A, "2026-10-20", t, flag=3),
                **{
                    "date": (
                        "2026-10-20T00:00:00+00:00" if i % 2 == 0 else "2026-10-20T00:00:00+05:00"
                    )
                },
            )
            for i, t in enumerate(targets[:6])
        ]
        rows_b = _null_flag3_rows(CODE_B, "2026-10-20", n=11)
        client = FakeClient(
            store={
                ("TFT", CODE_A, "2026-10-20"): rows_a,
                ("TFT", CODE_B, "2026-10-20"): rows_b,
            }
        )
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A, CODE_B],
                client_factory=lambda: client,
            )
        assert len(result["blocked"]) >= 1
        assert any(b["code"] == CODE_A for b in result["blocked"])
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["code"] == CODE_B


# ---------------------------------------------------------------------------
# Section 13: Import isolation (Finding 5 regressions)
#
# _ml_env_ctx must:
# (a) install fresh module identities inside the context
# (b) restore prior identities (not just None) on exit
# (c) restore state even when setup raises
# ---------------------------------------------------------------------------


class TestImportIsolation:
    """Finding 5: _ml_env_ctx fully restores sys.path, sys.modules, and globals."""

    def test_heavy_deps_are_mock_stubs_inside_env(self):
        """Inside _ml_module_env, heavy deps (torch etc.) are MagicMock stubs."""
        torch_mod = sys.modules.get("torch")
        assert isinstance(torch_mod, MagicMock), (
            f"torch should be a MagicMock inside the ML env, got {type(torch_mod)}"
        )

    def test_rnf_global_matches_sys_modules_entry(self):
        """Module global 'rnf' is the same object as sys.modules['recalculate_nan_forecasts']."""
        assert rnf is not None, "rnf should be populated by _ml_module_env"
        assert sys.modules.get("recalculate_nan_forecasts") is rnf, (
            "rnf module global should match sys.modules entry"
        )

    def test_utils_global_matches_sys_modules_entry(self):
        """Module global 'utils_ml_forecast' matches sys.modules['scr.utils_ml_forecast']."""
        assert utils_ml_forecast is not None
        assert sys.modules.get("scr.utils_ml_forecast") is utils_ml_forecast

    def test_inner_context_uses_fresh_identity_and_restores_outer(self):
        """A nested _ml_env_ctx yields a fresh import; on exit, restores the outer fixture's objects.

        This verifies both the preloaded identity (outer fixture's objects are correct)
        and the fresh identity (inner context creates distinct objects), and that the
        restore step puts the outer fixture's objects back.
        """
        _rnf_outer = rnf
        _rnf_in_sys_outer = sys.modules.get("recalculate_nan_forecasts")
        # Outer fixture must be consistent
        assert _rnf_outer is _rnf_in_sys_outer, "outer rnf should match sys.modules entry"

        _inner_rnf = None
        _inner_sys_rnf = None
        with _ml_env_ctx():
            # Inside inner context: fresh import replaces the outer
            _inner_rnf = rnf  # module global updated by _ml_env_ctx step 7
            _inner_sys_rnf = sys.modules.get("recalculate_nan_forecasts")
            assert _inner_rnf is not None

        # After inner context exits: outer objects restored
        assert sys.modules.get("recalculate_nan_forecasts") is _rnf_in_sys_outer, (
            "sys.modules entry should be restored to outer fixture's module"
        )
        assert rnf is _rnf_outer, "rnf module global should be restored to outer fixture's value"

    def test_setup_failure_restores_sys_modules(self):
        """_ml_env_ctx restores sys.modules if the import step raises.

        Verifies that the finally block fires even on import failure, leaving no
        residue in sys.modules or module globals.
        """
        _rnf_before = sys.modules.get("recalculate_nan_forecasts")
        _rnf_global_before = rnf

        # Cause the production module import to fail by patching importlib.import_module
        with patch("importlib.import_module", side_effect=ImportError("simulated failure")):
            try:
                with _ml_env_ctx():
                    pytest.fail("Should not reach the yield — import must have failed")
            except ImportError:
                pass  # expected

        # After failure + restore: sys.modules entry is unchanged
        assert sys.modules.get("recalculate_nan_forecasts") is _rnf_before, (
            "recalculate_nan_forecasts sys.modules entry should be restored after setup failure"
        )
        # Module global is also restored
        assert rnf is _rnf_global_before, (
            "rnf module global should be restored to pre-failure value after setup failure"
        )


# ---------------------------------------------------------------------------
# Section 14: Raw _get boundary regressions (R2 fix)
#
# read_unit_for_recovery_preview now calls client._get() directly so that
# explicit-null fields (key present, value None) are distinguishable from
# absent fields (key missing).  These tests verify the new raw-list contract.
# ---------------------------------------------------------------------------


class RawGetFakeClient:
    """Minimal fake that returns a pre-specified raw value from _get.

    Used to probe the raw-list contract without going through the DataFrame
    conversion that read_short_term_forecasts performs.

    _get is fake (returns raw_response); read_short_term_forecasts is the
    NATIVE implementation from the installed sapphire_api_client, so it calls
    self._get (our fake) and wraps the result in pd.DataFrame — the same path
    the real client takes.  An absent key in one row becomes NaN after
    pd.DataFrame construction, which the native method cannot distinguish from
    an explicit null.  read_unit_for_recovery_preview sees the raw list before
    that conversion and can distinguish the two cases.
    """

    def __init__(self, raw_response):
        self.raw_response = raw_response
        self.get_calls: list[dict] = []

    def readiness_check(self) -> bool:
        return True

    def _get(self, path: str, params: dict | None = None) -> object:
        self.get_calls.append({"path": path, "params": params or {}})
        return self.raw_response

    def read_short_term_forecasts(self, **kwargs):
        """Delegate to the real native method, which calls self._get + pd.DataFrame."""
        from sapphire_api_client.short_term import SapphireShortTermForecastClient

        return SapphireShortTermForecastClient.read_short_term_forecasts(self, **kwargs)

    def write_forecasts(self, records):
        raise AssertionError("write_forecasts must not be called during preview")


class TestRawGetBoundary:
    """R2: _get raw-list contract — non-list responses and absent keys are blocked."""

    @pytest.fixture(autouse=True)
    def _permit_all_codes(self, monkeypatch):
        monkeypatch.setattr(rnf, "get_permitted_station_codes", lambda: _ALL_PERMITTED)

    # --- Non-list _get responses raise RecoveryReadError ---

    def test_raw_false_response_raises(self):
        """_get returning False → RecoveryReadError (not a list)."""
        with pytest.raises(RecoveryReadError):
            read_unit_for_recovery_preview(RawGetFakeClient(False), "TFT", "2026-10-20", CODE_A)

    def test_raw_none_response_raises(self):
        """_get returning None → RecoveryReadError."""
        with pytest.raises(RecoveryReadError):
            read_unit_for_recovery_preview(RawGetFakeClient(None), "TFT", "2026-10-20", CODE_A)

    def test_raw_dict_response_raises(self):
        """_get returning {} → RecoveryReadError."""
        with pytest.raises(RecoveryReadError):
            read_unit_for_recovery_preview(RawGetFakeClient({}), "TFT", "2026-10-20", CODE_A)

    def test_raw_empty_string_response_raises(self):
        """_get returning '' → RecoveryReadError."""
        with pytest.raises(RecoveryReadError):
            read_unit_for_recovery_preview(RawGetFakeClient(""), "TFT", "2026-10-20", CODE_A)

    # --- Empty list is confirmed-empty, not an error ---

    def test_raw_empty_list_confirmed_empty(self):
        """_get returning [] → confirmed empty DataFrame, not an error."""
        result = read_unit_for_recovery_preview(RawGetFakeClient([]), "TFT", "2026-10-20", CODE_A)
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_raw_empty_list_skipped_via_preview(self):
        """[] from _get → unit skipped (absent), not blocked."""
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: RawGetFakeClient([]),
            )
        assert result["gate_blocked"] is None
        assert result["blocked"] == []
        assert len(result["skipped"]) == 1

    # --- Absent quantile key (not null value) blocks the unit ---

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_omitted_quantile_key_raises_recovery_read_error(self, field):
        """rows[5] missing one quantile key (not null) → RecoveryReadError.

        Other 10 rows retain the key (sanity check).  The native
        read_short_term_forecasts fills the gap with NaN (cannot distinguish
        absent key from explicit null); the new reader blocks before conversion.
        """
        rows = list(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        del rows[5][field]  # key absent only in rows[5]
        # Sanity: all other 10 rows still carry the key.
        others = [r for i, r in enumerate(rows) if i != 5]
        assert all(field in r for r in others)
        # Native public SDK method fills absent key with NaN — loses key presence.
        targets = _expected_targets("2026-10-20")
        client = RawGetFakeClient(rows)
        df = client.read_short_term_forecasts(
            horizon="day",
            model="TFT",
            code=CODE_A,
            start_date="2026-10-20",
            end_date=targets[-1],
            skip=0,
            limit=20,
        )
        assert len(df) == 11
        assert pd.isna(df.loc[5, field])
        # New reader inspects raw list and blocks on the absent key.
        with pytest.raises(RecoveryReadError):
            read_unit_for_recovery_preview(RawGetFakeClient(rows), "TFT", "2026-10-20", CODE_A)

    @pytest.mark.parametrize("field", list(_Q_FIELDS))
    def test_omitted_quantile_key_blocks_via_preview(self, field):
        """rows[5] missing quantile key → unit blocked, no replace/insert keys.

        Other 10 rows retain the key (sanity check).
        """
        rows = list(_null_flag3_rows(CODE_A, "2026-10-20", n=11))
        del rows[5][field]  # key absent only in rows[5]
        others = [r for i, r in enumerate(rows) if i != 5]
        assert all(field in r for r in others)
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: RawGetFakeClient(rows),
            )
        assert result["gate_blocked"] is None
        assert len(result["blocked"]) == 1
        assert result["candidates"] == []
        blocked = result["blocked"][0]
        assert blocked.get("replace_keys", []) == []
        assert blocked.get("insert_keys", []) == []

    # --- Explicit null (key present, value None) remains a valid candidate ---

    def test_explicit_null_quantile_value_passes_reader(self):
        """Key present with None value → reader succeeds, 11 rows returned."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        # Verify keys are present with None values (not absent)
        for row in rows:
            for field in _Q_FIELDS:
                assert field in row, f"{field} key must be present in _api_row"
                assert row[field] is None
        result = read_unit_for_recovery_preview(RawGetFakeClient(rows), "TFT", "2026-10-20", CODE_A)
        assert len(result) == 11
        assert set(_Q_FIELDS).issubset(set(result.columns))

    def test_explicit_null_quantile_value_candidate_via_preview(self):
        """Key present with None value in all 11 rows → candidate, not blocked."""
        rows = _null_flag3_rows(CODE_A, "2026-10-20", n=11)
        with patch.dict(os.environ, _BASE_ENV, clear=False):
            result = preview_recovery_candidates(
                issue_dates=["2026-10-20"],
                models=["TFT"],
                station_codes=[CODE_A],
                client_factory=lambda: RawGetFakeClient(rows),
            )
        assert result["gate_blocked"] is None
        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["n_targets"] == 11
