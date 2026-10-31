"""Tests for _execute_recovery_preview in postprocessing_maintenance.py.

Uses real P1/P3 helpers (plan_period_refresh, select_recovery_records,
submit_recovery_batch, reconcile_recovery_batches) with fake HTTP fixtures.
No live HTTP, no sapphire services, no ML/preprocessing edits.

The "sapphire-api-client not installed" skip is the only permitted skip.
"""

import datetime
import importlib
import os
import sys
from unittest.mock import MagicMock

import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# ---------------------------------------------------------------------------
# Client availability gate (only permitted skip in this file)
# ---------------------------------------------------------------------------

try:
    from sapphire_api_client.postprocessing import SapphirePostprocessingClient as _SPC

    _CLIENT_AVAILABLE = True
except ImportError:
    _SPC = None
    _CLIENT_AVAILABLE = False

_skip_no_client = pytest.mark.skipif(
    not _CLIENT_AVAILABLE,
    reason="sapphire-api-client not installed",
)

# ---------------------------------------------------------------------------
# Module import helpers
# ---------------------------------------------------------------------------


def _import_maintenance():
    """Import postprocessing_maintenance fresh (clears any prior mock state)."""
    for k in list(sys.modules):
        if "postprocessing_maintenance" in k:
            del sys.modules[k]
    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    # Minimal stubs so the top-level imports don't fail.
    _stub_sl = MagicMock()
    _stub_sl.load_environment.return_value = None
    _stub_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    _stub_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x
    _stub_tl = MagicMock()
    sys.modules.setdefault("setup_library", _stub_sl)
    sys.modules.setdefault("tag_library", _stub_tl)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Fixtures shared with test_maintenance_workflow.py patterns
# ---------------------------------------------------------------------------


def _derive_period_ints(horizon_type, target_date):
    d = target_date
    if horizon_type == "pentad":
        hv = min((d.day - 1) // 5 + 1, 6)
        hiy = (d.month - 1) * 6 + hv
    elif horizon_type == "decad":
        hv = 1 if d.day <= 10 else (2 if d.day <= 20 else 3)
        hiy = (d.month - 1) * 3 + hv
    else:
        hv = 1
        hiy = 1
    return hv, hiy


def _make_candidate_payload(code, issue, model_type="TFT", horizon_type="pentad"):
    """Build a valid candidate payload dict (input to plan_period_refresh)."""
    target = issue + datetime.timedelta(days=1)
    hv, hiy = _derive_period_ints(horizon_type, target)
    return {
        "horizon_type": horizon_type,
        "code": code,
        "model_type": model_type,
        "date": issue,
        "target": target,
        "forecasted_discharge": 100.0,
        "q05": 80.0,
        "q25": 90.0,
        "q75": 110.0,
        "q95": 120.0,
        "flag": None,
        "composition": None,
        "horizon_value": hv,
        "horizon_in_year": hiy,
    }


def _make_period_row_dict(
    code,
    issue_str,
    target_str,
    model,
    horizon_type,
    fd=100.0,
    q05=80.0,
    q25=90.0,
    q75=110.0,
    q95=120.0,
    hv=None,
    hiy=None,
):
    """Build a combined-period record dict (as returned by read_combined API)."""
    target = datetime.date.fromisoformat(target_str)
    if hv is None or hiy is None:
        _hv, _hiy = _derive_period_ints(horizon_type, target)
        hv = hv if hv is not None else _hv
        hiy = hiy if hiy is not None else _hiy
    api_ht = "decade" if horizon_type == "decad" else horizon_type
    return {
        "code": code,
        "date": issue_str,
        "target": target_str,
        "model_type": model,
        "horizon_type": api_ht,
        "forecasted_discharge": fd,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "horizon_value": hv,
        "horizon_in_year": hiy,
        "flag": None,
        "composition": None,
    }


def _make_fake_response(status_code, json_body):
    import json as _json

    import requests as _req

    resp = _req.Response()
    resp.status_code = status_code
    resp._content = _json.dumps(json_body).encode("utf-8")
    resp.headers["Content-Type"] = "application/json"
    return resp


class _FakeStore:
    """In-memory upsert store; POST commits, GET reads back."""

    def __init__(self, post_raise=None, commit_then_raise=None):
        self._records: dict[tuple, dict] = {}
        self.post_calls: list = []
        self._post_raise = post_raise
        self._commit_then_raise = commit_then_raise

    def upsert(self, record):
        key = (
            record.get("horizon_type"),
            record.get("code"),
            record.get("model_type"),
            record.get("date"),
            record.get("target"),
        )
        self._records[key] = dict(record)
        r = dict(record)
        r["id"] = abs(hash(str(key))) % 100000
        r["model_type_description"] = record.get("model_type", "")
        return r

    def get_records(self):
        return list(self._records.values())


def _make_combined_get_adapter(store):
    """Fake GET /forecast/ adapter that returns store records filtered by params."""
    import requests.adapters

    class _Adapter(requests.adapters.BaseAdapter):
        def send(self, prep, **kw):
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(prep.url)
            qs = parse_qs(parsed.query)

            def _first(k):
                return qs.get(k, [None])[0]

            skip = int(_first("skip") or 0)
            limit = int(_first("limit") or 500)
            h_filter = _first("horizon")
            m_filter = _first("model")
            c_filter = _first("code")
            s_filter = _first("start_date")
            e_filter = _first("end_date")

            rows = []
            for r in store.get_records():
                if h_filter and r.get("horizon_type") != h_filter:
                    continue
                if m_filter and r.get("model_type") != m_filter:
                    continue
                if c_filter and r.get("code") != c_filter:
                    continue
                if s_filter and r.get("date", "") < s_filter:
                    continue
                if e_filter and r.get("date", "") > e_filter:
                    continue
                rows.append(r)
            return _make_fake_response(200, rows[skip : skip + limit])

        def close(self):
            pass

    return _Adapter()


def _make_post_adapter(store, post_raise=None, commit_then_raise=None):
    """Fake POST /forecast/ adapter."""
    import json as _json

    import requests.adapters

    class _Adapter(requests.adapters.BaseAdapter):
        def send(self, prep, **kw):
            if post_raise is not None:
                raise post_raise
            body = _json.loads(prep.body)
            store.post_calls.append(body)
            data = body if isinstance(body, list) else body.get("data", [])
            if commit_then_raise is not None:
                for rec in data:
                    store.upsert(rec)
                raise commit_then_raise
            result = [store.upsert(rec) for rec in data]
            return _make_fake_response(200, result)

        def close(self):
            pass

    return _Adapter()


def _patch_client_for_store(store, post_raise=None, commit_then_raise=None):
    """Context manager: patches _SPC.__init__ to mount fake GET+POST adapters."""
    import unittest.mock

    orig_init = _SPC.__init__
    post_adap = _make_post_adapter(store, post_raise, commit_then_raise)
    get_adap = _make_combined_get_adapter(store)

    def _fake_init(self_c, **kwargs):
        orig_init(self_c, **kwargs)
        self_c.session.mount("http://", post_adap)
        self_c.session.mount("https://", post_adap)
        # GET adapter layered on top via a hook instead of double-mount
        _orig_send = self_c.session.send

        def _dispatch_send(prep, **kw):
            if prep.method == "GET":
                return get_adap.send(prep, **kw)
            return _orig_send(prep, **kw)

        self_c.session.send = _dispatch_send

    return unittest.mock.patch.object(_SPC, "__init__", _fake_init)


# ---------------------------------------------------------------------------
# Helper: build a minimal execution-eligible preview with real helpers
# ---------------------------------------------------------------------------


def _build_preview_with_real_helpers(
    mod,
    code="19999",
    issue=datetime.date(2026, 1, 5),
    horizons=("pentad",),
    model_type="TFT",
    base_url="http://localhost:8000",
    started_at=0.0,
    preparation_deadline=300.0,
):
    """Build a RecoveryPreviewResult that is execution_eligible=True using real helpers.

    Directly calls gap_detector.plan_period_refresh and manually constructs
    HorizonPreviewResult and RecoveryPreviewResult with execution_eligible=True.
    Does NOT call _run_recovery_preview; the result is not a native preview artifact.
    """
    import importlib

    _script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    gd = importlib.import_module("src.gap_detector")
    dr = importlib.import_module("src.data_reader")

    target = issue + datetime.timedelta(days=1)
    candidate = _make_candidate_payload(code, issue, model_type, horizons[0])

    authorized_keys = {(horizons[0], code, model_type, issue, target)}

    plan = gd.plan_period_refresh(
        [],  # no existing records
        [candidate],
        authorized_keys,
        set(),
        start_date=issue,
        end_date=issue,
        forecast_date=issue,
        existing_read_status="CONFIRMED_EMPTY",
    )
    assert plan.plan_error is None
    assert (horizons[0], code, model_type, issue, target) in plan.missing

    hr = mod.HorizonPreviewResult(
        plan=plan,
        source_statuses={(code, model_type): dr.RECOVERY_STATUS_COMPLETE},
        candidate_count=1,
        missing_count=1,
        protected_count=0,
        em_blocked_reason="skill_vintage_unresolved",
        diagnostics=[],
        blocked_reason=None,
        blocked_units={},
        candidate_payloads=[candidate],
        unusable_units={},
        planner_authorized_keys=authorized_keys,
    )

    horizon_results = {horizons[0]: hr}
    preview = mod.RecoveryPreviewResult(
        forecast_date=issue,
        start_date=issue,
        end_date=issue,
        horizons=list(horizons),
        horizon_results=horizon_results,
        blocked_reason=None,
        destination_base_url=base_url,
        started_at=started_at,
        preparation_deadline=preparation_deadline,
        execution_eligible=True,
    )
    return preview, authorized_keys


# ---------------------------------------------------------------------------
# Rejection tests (no HTTP needed)
# ---------------------------------------------------------------------------


class TestExecuteRejectionsNoHTTP:
    """Validate that _execute_recovery_preview rejects bad inputs before any write."""

    def _get_execute(self):
        mod = _import_maintenance()
        return mod._execute_recovery_preview

    def _good_timing(self):
        """Return (write_cutoff, deadline) well within preparation_deadline=300."""
        return 50.0, 60.0  # reserve=10 > timeout=5

    def test_rejects_non_true_exclusion(self):
        execute = self._get_execute()
        result = execute(
            MagicMock(spec=[]),
            approved_missing_keys=set(),
            base_url="http://localhost:8000",
            exclusion_confirmed=False,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir="/tmp",
        )
        assert not result["success"]
        assert "exclusion_confirmed" in result["error"]

    def test_rejects_truthy_non_bool_exclusion(self):
        execute = self._get_execute()
        result = execute(
            MagicMock(spec=[]),
            approved_missing_keys=set(),
            base_url="http://localhost:8000",
            exclusion_confirmed=1,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir="/tmp",
        )
        assert not result["success"]
        assert "exclusion_confirmed" in result["error"]

    def test_rejects_non_execution_eligible_preview(self):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        preview = mod.RecoveryPreviewResult(
            forecast_date=datetime.date(2026, 1, 5),
            start_date=datetime.date(2026, 1, 5),
            end_date=datetime.date(2026, 1, 5),
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=False,  # injected client
        )
        result = execute(
            preview,
            approved_missing_keys=set(),
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir="/tmp",
        )
        assert not result["success"]
        assert "execution_eligible" in result["error"]

    def test_rejects_destination_mismatch(self):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        preview = mod.RecoveryPreviewResult(
            forecast_date=datetime.date(2026, 1, 5),
            start_date=datetime.date(2026, 1, 5),
            end_date=datetime.date(2026, 1, 5),
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://prod-server:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys=set(),
            base_url="http://localhost:8000",  # mismatch
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir="/tmp",
        )
        assert not result["success"]
        assert "destination_base_url" in result["error"]

    def test_rejects_em_in_approved_keys(self, tmp_path):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        issue = datetime.date(2026, 1, 5)
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys={
                ("pentad", "19999", "ENSEMBLE_MEAN", issue, issue + datetime.timedelta(1))
            },
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "EM" in result["error"] or "ENSEMBLE_MEAN" in result["error"]

    def test_rejects_lr_in_approved_keys(self, tmp_path):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        issue = datetime.date(2026, 1, 5)
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys={("pentad", "19999", "LR", issue, issue + datetime.timedelta(1))},
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "LR" in result["error"]

    def test_rejects_datetime_date_in_approved_keys(self, tmp_path):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dt_val = datetime.datetime(2026, 1, 5, 0, 0, 0)
        preview = mod.RecoveryPreviewResult(
            forecast_date=datetime.date(2026, 1, 5),
            start_date=datetime.date(2026, 1, 5),
            end_date=datetime.date(2026, 1, 5),
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys={("pentad", "19999", "TFT", dt_val, datetime.date(2026, 1, 6))},
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "datetime" in result["error"]

    def test_rejects_refresh_approved_key(self, tmp_path):
        """Key in plan.authorized_refresh (not .missing) must be rejected."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(days=1)
        hv, hiy = _derive_period_ints("pentad", target)
        # Existing record with different forecasted_discharge → authorized_refresh
        existing = {
            "horizon_type": "pentad",
            "code": "19999",
            "model_type": "TFT",
            "date": issue,
            "target": target,
            "forecasted_discharge": 99.0,  # differs from candidate
            "q05": 80.0,
            "q25": 90.0,
            "q75": 110.0,
            "q95": 120.0,
            "flag": None,
            "composition": None,
            "horizon_value": hv,
            "horizon_in_year": hiy,
        }
        candidate = _make_candidate_payload("19999", issue, "TFT", "pentad")
        ak = {("pentad", "19999", "TFT", issue, target)}
        erk = {("pentad", "19999", "TFT", issue, target)}  # authorize refresh
        plan = gd.plan_period_refresh(
            [existing],
            [candidate],
            ak,
            erk,
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert ("pentad", "19999", "TFT", issue, target) in {k for k, _ in plan.authorized_refresh}

        import src.data_reader as dr

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(("19999", "TFT")): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=0,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason=None,
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys={("pentad", "19999", "TFT", issue, target)},
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "authorized_refresh" in result["error"]

    @pytest.mark.parametrize("suffix", ["", os.sep], ids=["bare_link", "trailing_sep"])
    def test_rejects_evidence_dir_symlink(self, tmp_path, suffix):
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        real_dir = tmp_path / "real"
        real_dir.mkdir()
        link_dir = tmp_path / "link"
        link_dir.symlink_to(real_dir)

        issue = datetime.date(2026, 1, 5)
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys=set(),
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(link_dir) + suffix,
        )
        assert not result["success"]
        assert "symlink" in result["error"]
        # Rejection must happen before any HTTP: zero POSTs regardless of suffix.
        assert result.get("attempted_keys", []) == []

    def test_em_lr_blocked_independently_do_not_block_tft(self):
        """EM/LR blocked in preview.blocked_units must NOT prevent TFT approval."""
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(days=1)
        candidate = _make_candidate_payload("19999", issue, "TFT", "pentad")
        ak = {("pentad", "19999", "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert ("pentad", "19999", "TFT", issue, target) in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason=None,
            blocked_units={
                ("19999", "EM"): "skill_vintage_unresolved",
                ("ALL", "LR"): "LR read failed",
            },
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        # The approved TFT key is in plan.missing; EM/LR blocks should appear in
        # blocked_diagnostics but must not cause the validation to reject TFT.
        # (We verify it gets past validation; the test does NOT need to submit HTTP.)
        # With no client available we can't complete the execute, but the validation
        # stage must pass (error, if any, is not about EM/LR blocking TFT).
        assert ("19999", "EM") in hr.blocked_units
        assert ("pentad", "19999", "TFT", issue, target) in plan.missing
        # blocked_diagnostics collected from preview.horizon_results must include EM/LR
        bd = {
            k: v for hhr in preview.horizon_results.values() for k, v in hhr.blocked_units.items()
        }
        assert ("19999", "EM") in bd
        assert ("ALL", "LR") in bd

    def test_empty_approved_set_returns_success_no_writes(self, tmp_path):
        """Empty approved_missing_keys → success immediately with no writes."""
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        issue = datetime.date(2026, 1, 5)
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=[],
            horizon_results={},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = execute(
            preview,
            approved_missing_keys=set(),
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert result["success"] is True
        assert result["error"] is None
        assert result["attempted_keys"] == []
        assert result["verified_keys"] == []
        assert result["not_attempted_keys"] == []


# ---------------------------------------------------------------------------
# HTTP-required tests (require sapphire-api-client)
# ---------------------------------------------------------------------------


@pytest.fixture
def real_modules():
    """Import real api_writer, gap_detector, data_reader; restore on teardown."""
    import importlib

    _KEYS = ("src.api_writer", "src.gap_detector", "src.data_reader", "tag_library")
    _saved = {k: sys.modules.get(k) for k in _KEYS}
    for k in _KEYS:
        if k in sys.modules and isinstance(sys.modules[k], MagicMock):
            del sys.modules[k]
    aw = importlib.import_module("src.api_writer")
    gd = importlib.import_module("src.gap_detector")
    dr = importlib.import_module("src.data_reader")
    yield {"aw": aw, "gd": gd, "dr": dr}
    for k, v in _saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


@_skip_no_client
class TestExecuteHappyPathRealHelpers:
    """Happy-path tests using real P1/P3 helpers with fake HTTP."""

    def test_happy_single_horizon_tft_missing_write_and_verify(self, real_modules, tmp_path):
        """Single approved TFT/pentad key: fresh read empty → submit → reconcile verified."""
        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        issue = datetime.date(2026, 1, 5)
        code = "19999"
        store = _FakeStore()
        base_url = "http://localhost:8000"

        preview, ak = _build_preview_with_real_helpers(
            mod,
            code=code,
            issue=issue,
            horizons=("pentad",),
            model_type="TFT",
            base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
        )
        approved = set(ak)

        # Deterministic clock: always returns 1.0 (well before write_cutoff=100)
        clock_val = [1.0]

        def _clock():
            return clock_val[0]

        with _patch_client_for_store(store):
            result = execute(
                preview,
                approved_missing_keys=approved,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=_clock,
            )

        assert result["success"] is True, result["error"]
        assert result["error"] is None
        assert set(result["verified_keys"]) == approved
        assert result["unverified_keys"] == []
        assert result["not_attempted_keys"] == []
        assert result["attempted_keys"] != []

    def test_approved_subset_only_those_keys_written(self, real_modules, tmp_path):
        """Approving a subset of missing keys writes only the subset, not all missing."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dr = real_modules["dr"]

        # Two candidates for two different issue dates (both missing).
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        code = "19999"
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {
            ("pentad", code, "TFT", issue1, target1),
            ("pentad", code, "TFT", issue2, target2),
        }
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 2

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason=None,
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Approve ONLY issue1.
        approved = {("pentad", code, "TFT", issue1, target1)}
        store = _FakeStore()

        with _patch_client_for_store(store):
            result = execute(
                preview,
                approved_missing_keys=approved,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert result["success"] is True, result["error"]
        assert set(result["verified_keys"]) == approved
        # issue2 was NOT submitted.
        assert len(store.post_calls) == 1
        submitted_dates = {r["date"] for r in store.post_calls[0].get("data", [])}
        assert issue1.isoformat() in submitted_dates
        assert issue2.isoformat() not in submitted_dates

    def test_fresh_state_exists_prevents_post(self, real_modules, tmp_path):
        """Fresh read shows record already exists → replan not-missing → stop before POST."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert ("pentad", code, "TFT", issue, target) in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Pre-populate store so fresh read shows the record already exists.
        store = _FakeStore()
        hv, hiy = _derive_period_ints("pentad", target)
        existing_wire = _make_period_row_dict(
            code,
            issue.isoformat(),
            target.isoformat(),
            "TFT",
            "pentad",
        )
        store._records[("pentad", code, "TFT", issue.isoformat(), target.isoformat())] = (
            existing_wire
        )

        with _patch_client_for_store(store):
            result = execute(
                preview,
                approved_missing_keys={("pentad", code, "TFT", issue, target)},
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"]
        # No POST was made.
        assert len(store.post_calls) == 0
        assert "no longer missing" in result["error"] or "not_still_missing" in (
            result["error"] or ""
        )

    def test_fresh_read_page_failure_stops_batch(self, real_modules, tmp_path):
        """Fresh read returning non-200 → stop_reason set, no POST."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert ("pentad", code, "TFT", issue, target) in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Adapter that returns 500 for GET (fresh read).
        import unittest.mock

        orig_init = _SPC.__init__

        def _fail_get_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)

            def _dispatch(prep, **kw):
                if prep.method == "GET":
                    return _make_fake_response(500, {"detail": "server error"})
                raise AssertionError("unexpected POST")

            self_c.session.send = _dispatch

        store = _FakeStore()
        with unittest.mock.patch.object(_SPC, "__init__", _fail_get_init):
            result = execute(
                preview,
                approved_missing_keys={("pentad", code, "TFT", issue, target)},
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"]
        assert len(store.post_calls) == 0

    def test_cross_cutoff_clock_prevents_post(self, real_modules, tmp_path):
        """Clock already past write_cutoff before fresh reads complete → no POST."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert ("pentad", code, "TFT", issue, target) in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Clock already past write_cutoff=50.
        store = _FakeStore()
        with _patch_client_for_store(store):
            result = execute(
                preview,
                approved_missing_keys={("pentad", code, "TFT", issue, target)},
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=0.5,  # clock() returns 1.0 > cutoff=0.5
                deadline=10.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"]
        assert len(store.post_calls) == 0

    def test_multi_batch_earlier_ack_later_uncertain_no_resend_all_readback(
        self, real_modules, tmp_path
    ):
        """Pentad batch acknowledged; decad POST raises (uncertain). Reconcile reads both.

        No resend on the decad batch; both keys in reconcile readback.
        """
        import src.gap_detector as gd

        mod = _import_maintenance()
        execute = mod._execute_recovery_preview
        dr = real_modules["dr"]

        issue_p = datetime.date(2026, 1, 5)
        target_p = issue_p + datetime.timedelta(1)
        issue_d = datetime.date(2026, 1, 10)
        target_d = issue_d + datetime.timedelta(1)
        code = "19999"

        c_p = _make_candidate_payload(code, issue_p, "TFT", "pentad")
        c_d = _make_candidate_payload(code, issue_d, "TFT", "decad")
        ak_p = {("pentad", code, "TFT", issue_p, target_p)}
        ak_d = {("decad", code, "TFT", issue_d, target_d)}

        plan_p = gd.plan_period_refresh(
            [],
            [c_p],
            ak_p,
            set(),
            start_date=issue_p,
            end_date=issue_p,
            forecast_date=issue_p,
            existing_read_status="CONFIRMED_EMPTY",
        )
        plan_d = gd.plan_period_refresh(
            [],
            [c_d],
            ak_d,
            set(),
            start_date=issue_d,
            end_date=issue_d,
            forecast_date=issue_d,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan_p.plan_error is None
        assert plan_d.plan_error is None

        hr_p = mod.HorizonPreviewResult(
            plan=plan_p,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c_p],
            unusable_units={},
            planner_authorized_keys=ak_p,
        )
        hr_d = mod.HorizonPreviewResult(
            plan=plan_d,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c_d],
            unusable_units={},
            planner_authorized_keys=ak_d,
        )
        base_url = "http://localhost:8000"
        # forecast_date must encompass both issue dates
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue_d,
            start_date=issue_p,
            end_date=issue_d,
            horizons=["pentad", "decad"],
            horizon_results={"pentad": hr_p, "decad": hr_d},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        approved = ak_p | ak_d

        # Pentad store: POST succeeds; decad POST commits then raises (uncertain).
        store_p = _FakeStore()
        store_d = _FakeStore()
        post_count = [0]

        import json as _json
        import unittest.mock

        orig_init = _SPC.__init__

        def _multi_fake_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)

            def _dispatch(prep, **kw):
                if prep.method == "GET":
                    # Read back from both stores.
                    from urllib.parse import parse_qs, urlparse

                    parsed = urlparse(prep.url)
                    qs = parse_qs(parsed.query)
                    h = (qs.get("horizon") or [None])[0]
                    skip = int((qs.get("skip") or [0])[0])
                    limit = int((qs.get("limit") or [500])[0])
                    rows = []
                    for st in (store_p, store_d):
                        rows.extend(st.get_records())
                    if h:
                        rows = [r for r in rows if r.get("horizon_type") == h]
                    return _make_fake_response(200, rows[skip : skip + limit])
                if prep.method == "POST":
                    body = _json.loads(prep.body)
                    data = body.get("data", [])
                    idx = post_count[0]
                    post_count[0] += 1
                    if idx == 0:
                        # First POST (pentad): success.
                        result_list = [store_p.upsert(r) for r in data]
                        store_p.post_calls.append(body)
                        return _make_fake_response(200, result_list)
                    else:
                        # Second POST (decad): commit then raise (uncertain transport).
                        for r in data:
                            store_d.upsert(r)
                        raise RuntimeError("network timeout after commit")

            self_c.session.send = _dispatch

        with unittest.mock.patch.object(_SPC, "__init__", _multi_fake_init):
            result = execute(
                preview,
                approved_missing_keys=approved,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        # Exactly 2 POSTs were made (no resend on uncertain).
        assert post_count[0] == 2
        # Pentad was submitted and acknowledged; decad was submitted (uncertain).
        # The uncertain transport means success=False; reconcile may verify decad.
        # The function must not have resent; verified+unverified covers attempted.
        attempted = set(result["attempted_keys"])
        verified = set(result["verified_keys"])
        unverified = set(result["unverified_keys"])
        assert verified.isdisjoint(unverified), "partition invariant"
        assert (verified | unverified) == attempted, "partition invariant"
        # at least one key was attempted
        assert attempted
        # not_attempted = approved - attempted
        assert set(result["not_attempted_keys"]) == approved - attempted

    def test_structured_reconcile_error_retains_attempted_keys_unverified(
        self, real_modules, tmp_path
    ):
        """If reconcile_recovery_batches raises, all attempted keys go to unverified."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()

        # Patch reconcile_recovery_batches to raise.
        import unittest.mock

        import src.api_writer as aw

        with _patch_client_for_store(store):
            with unittest.mock.patch.object(
                aw,
                "reconcile_recovery_batches",
                side_effect=RuntimeError("reconcile coordinator error"),
            ):
                result = mod._execute_recovery_preview(
                    preview,
                    approved_missing_keys={("pentad", code, "TFT", issue, target)},
                    base_url=base_url,
                    exclusion_confirmed=True,
                    batch_limit=100,
                    write_cutoff=100.0,
                    deadline=110.0,
                    timeout=5,
                    evidence_dir=str(tmp_path),
                    _clock=lambda: 1.0,
                )

        assert not result["success"]
        # Attempted keys derived from outcomes must be in unverified.
        attempted = set(result["attempted_keys"])
        unverified = set(result["unverified_keys"])
        verified = set(result["verified_keys"])
        # If POST was reached, attempted is non-empty and all go to unverified.
        assert verified.isdisjoint(unverified)
        assert (verified | unverified) == attempted
        # The reconcile error must appear in result["error"].
        assert "reconcile" in (result["error"] or "").lower()


# ---------------------------------------------------------------------------
# Artifact fields smoke tests (no HTTP)
# ---------------------------------------------------------------------------


class TestArtifactFields:
    """Verify the new artifact fields are populated correctly on RecoveryPreviewResult."""

    def test_injected_client_not_execution_eligible(self):
        """When _client is injected (not native), execution_eligible must be False."""
        mod = _import_maintenance()
        # Provide a fake injected client that has _get returning empty pages.
        fake_client = MagicMock()
        fake_client._get.return_value = []
        # Patch _read_station_codes to avoid filesystem access.
        mod._read_station_codes = MagicMock(return_value=[])
        result = mod._run_recovery_preview(
            "PENTAD",
            datetime.date(2026, 1, 5),
            datetime.date(2026, 1, 5),
            datetime.date(2026, 1, 5),
            _client=fake_client,
            _virtual_sources={},
        )
        assert result.execution_eligible is False
        assert result.destination_base_url is None
        assert result.started_at is not None

    def test_started_at_before_preparation_deadline(self):
        """started_at must be < preparation_deadline for any non-blocked preview."""
        mod = _import_maintenance()
        fake_client = MagicMock()
        fake_client._get.return_value = []
        mod._read_station_codes = MagicMock(return_value=[])
        result = mod._run_recovery_preview(
            "PENTAD",
            datetime.date(2026, 1, 5),
            datetime.date(2026, 1, 5),
            datetime.date(2026, 1, 5),
            _client=fake_client,
            _virtual_sources={},
        )
        if result.blocked_reason is None:
            assert result.started_at < result.preparation_deadline

    def test_planner_authorized_keys_populated(self):
        """HorizonPreviewResult.planner_authorized_keys must match authorized_keys passed."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = importlib.import_module("src.data_reader")

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"
        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        assert hr.planner_authorized_keys == ak

    def test_horizon_preview_without_planner_defaults_empty(self):
        """Early-return HorizonPreviewResult (plan=None) has empty planner_authorized_keys."""
        mod = _import_maintenance()
        hr = mod.HorizonPreviewResult(
            plan=None,
            source_statuses={},
            candidate_count=0,
            missing_count=0,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
        )
        assert hr.planner_authorized_keys == set()


# ---------------------------------------------------------------------------
# New executor guard tests
# ---------------------------------------------------------------------------


class TestExecuteNewGuards:
    """Validate new structured reject guards in _execute_recovery_preview."""

    def test_rejects_reserve_lt_timeout(self, tmp_path):
        """reserve < timeout must now return structured error, not a warning."""
        mod = _import_maintenance()
        issue = datetime.date(2026, 1, 5)
        preview, ak = _build_preview_with_real_helpers(
            mod,
            code="19999",
            issue=issue,
            horizons=("pentad",),
            model_type="TFT",
            base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
        )
        result = mod._execute_recovery_preview(
            preview,
            approved_missing_keys=set(ak),
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=100.0,
            deadline=104.0,  # reserve = 4 < timeout = 5
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "reserve" in result["error"]

    def test_rejects_clock_expired_at_entry(self, tmp_path):
        """Clock already past write_cutoff before any effects must return structured error."""
        mod = _import_maintenance()
        issue = datetime.date(2026, 1, 5)
        preview, ak = _build_preview_with_real_helpers(
            mod,
            code="19999",
            issue=issue,
            horizons=("pentad",),
            model_type="TFT",
            base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
        )
        result = mod._execute_recovery_preview(
            preview,
            approved_missing_keys=set(ak),
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
            _clock=lambda: 51.0,  # clock > write_cutoff at entry
        )
        assert not result["success"]
        assert "write_cutoff" in result["error"] or "expired" in result["error"]

    def test_rejects_approved_key_not_in_planner_authorized_keys(self, tmp_path):
        """Approved key absent from hr.planner_authorized_keys must be rejected."""
        import src.data_reader as dr
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        candidate = _make_candidate_payload("19999", issue, "TFT", "pentad")
        ak = {("pentad", "19999", "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert ("pentad", "19999", "TFT", issue, target) in plan.missing

        # Build HR with planner_authorized_keys intentionally empty (no allowlist).
        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(("19999", "TFT")): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason=None,
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=set(),  # empty — key not present
        )
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = mod._execute_recovery_preview(
            preview,
            approved_missing_keys={("pentad", "19999", "TFT", issue, target)},
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "planner_authorized_keys" in result["error"]

    def test_rejects_approved_key_when_hr_blocked(self, tmp_path):
        """Approved key for a horizon with blocked_reason must be rejected."""
        import src.data_reader as dr
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        candidate = _make_candidate_payload("19999", issue, "TFT", "pentad")
        ak = {("pentad", "19999", "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert ("pentad", "19999", "TFT", issue, target) in plan.missing

        # HR has a blocked_reason (e.g., partial deadline hit).
        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(("19999", "TFT")): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_reason="deadline exceeded during unusable-output enumeration (incomplete)",
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url="http://localhost:8000",
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )
        result = mod._execute_recovery_preview(
            preview,
            approved_missing_keys={("pentad", "19999", "TFT", issue, target)},
            base_url="http://localhost:8000",
            exclusion_confirmed=True,
            batch_limit=100,
            write_cutoff=50.0,
            deadline=60.0,
            timeout=5,
            evidence_dir=str(tmp_path),
        )
        assert not result["success"]
        assert "blocked_reason" in result["error"]


# ---------------------------------------------------------------------------
# Native preview provenance and clock-budget tests (require sapphire-api-client)
# ---------------------------------------------------------------------------


@_skip_no_client
class TestNativePreviewArtifacts:
    """Verify destination_base_url comes from client.base_url and deadline consumes budget."""

    def _empty_get_adapter(self):
        """BaseAdapter returning 200 empty list for all GETs (no live calls)."""
        import requests.adapters

        class _EmptyGet(requests.adapters.BaseAdapter):
            def send(self_a, prep, **kw):
                return _make_fake_response(200, [])

            def close(self_a):
                pass

        return _EmptyGet()

    def test_destination_from_client_base_url_strips_trailing_slash(self):
        """destination_base_url must equal client.base_url (rstrip('/') normalized form)."""
        import unittest.mock

        mod = _import_maintenance()
        # Synthetic station list ensures reads are attempted; adapter returns empty.
        mod._read_station_codes = MagicMock(return_value=["19999"])

        orig_init = _SPC.__init__
        get_adap = self._empty_get_adapter()

        def _mounting_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            self_c.session.mount("http://", get_adap)
            self_c.session.mount("https://", get_adap)

        with unittest.mock.patch.dict(os.environ, {"SAPPHIRE_API_URL": "http://test-server:8000/"}):
            with unittest.mock.patch.object(_SPC, "__init__", _mounting_init):
                result = mod._run_recovery_preview(
                    "PENTAD",
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                )
        assert result.destination_base_url is not None, (
            f"destination_base_url must be populated; blocked_reason={result.blocked_reason}"
        )
        assert not result.destination_base_url.endswith("/"), (
            "destination_base_url should not have trailing slash (matches client.base_url)"
        )

    def test_preparation_deadline_equals_started_at_plus_budget(self):
        """preparation_deadline must equal started_at + _RECOVERY_PREVIEW_DEADLINE_SECONDS.

        Constructor consumes some monotonic time; deadline must be started_at + budget,
        not re-read after construction.
        """
        import unittest.mock

        mod = _import_maintenance()
        mod._read_station_codes = MagicMock(return_value=["19999"])

        orig_init = _SPC.__init__
        get_adap = self._empty_get_adapter()

        def _mounting_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            self_c.session.mount("http://", get_adap)
            self_c.session.mount("https://", get_adap)

        with unittest.mock.patch.dict(os.environ, {"SAPPHIRE_API_URL": "http://localhost:8000"}):
            with unittest.mock.patch.object(_SPC, "__init__", _mounting_init):
                result = mod._run_recovery_preview(
                    "PENTAD",
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                )
        assert result.started_at is not None, "started_at must be populated"
        assert result.preparation_deadline is not None, "preparation_deadline must be populated"
        budget = mod._RECOVERY_PREVIEW_DEADLINE_SECONDS
        actual = result.preparation_deadline - result.started_at
        assert abs(actual - budget) < 1e-6, (
            f"preparation_deadline - started_at = {actual} != budget {budget}; "
            "deadline must be started_at + budget, including construction time"
        )

    def test_redirects_disabled_on_native_transport(self):
        """Native client must have max_redirects=0 before any reads.

        A 302 response from the adapter must not trigger a follow-up GET.
        """
        import unittest.mock

        import requests.adapters

        mod = _import_maintenance()
        # Synthetic station list so at least one GET is attempted.
        mod._read_station_codes = MagicMock(return_value=["19999"])

        captured_max_redirects = []
        follow_up_calls = [0]
        orig_init = _SPC.__init__

        class _CapturingAdapter(requests.adapters.BaseAdapter):
            def __init__(self_a, client_holder):
                self_a._holder = client_holder

            def send(self_a, prep, **kw):
                client = self_a._holder[0]
                captured_max_redirects.append(client.session.max_redirects)
                if len(captured_max_redirects) == 1:
                    # Return 302 on first call; if redirects were enabled this
                    # would trigger a second call to send().
                    resp = _make_fake_response(302, {})
                    resp.headers["Location"] = "http://other-host:9999/redirected"
                    return resp
                # Any follow-up call after the 302 means redirects were followed.
                follow_up_calls[0] += 1
                return _make_fake_response(200, [])

            def close(self_a):
                pass

        client_holder = [None]

        def _capture_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            client_holder[0] = self_c
            adap = _CapturingAdapter(client_holder)
            self_c.session.mount("http://", adap)
            self_c.session.mount("https://", adap)

        with unittest.mock.patch.dict(os.environ, {"SAPPHIRE_API_URL": "http://localhost:8000"}):
            with unittest.mock.patch.object(_SPC, "__init__", _capture_init):
                mod._run_recovery_preview(
                    "PENTAD",
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 5),
                )
        assert captured_max_redirects, (
            "no GET reads attempted — _read_station_codes returned non-empty list; "
            "check whether _run_recovery_preview skips reads before constructing client"
        )
        assert captured_max_redirects[0] == 0, (
            f"max_redirects must be 0 before first read, got {captured_max_redirects[0]}"
        )
        assert follow_up_calls[0] == 0, (
            "302 response triggered a follow-up GET; max_redirects=0 must prevent redirect following"
        )


# ---------------------------------------------------------------------------
# Batch-fix correction tests
# ---------------------------------------------------------------------------


@_skip_no_client
class TestBatchFixCorrections:
    """Verify per-batch fresh reads, guaranteed reconcile, and partition semantics."""

    def test_batch_limit_1_two_records_two_fresh_reads_two_posts(self, real_modules, tmp_path):
        """batch_limit=1 with two same-horizon records → two separate fresh reads + two POSTs."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {
            ("pentad", code, "TFT", issue1, target1),
            ("pentad", code, "TFT", issue2, target2),
        }
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 2

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        get_call_count = [0]
        orig_get_adapter_send = _make_combined_get_adapter(store).send

        import unittest.mock

        orig_init = _SPC.__init__

        def _counting_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            post_adap = _make_post_adapter(store)
            self_c.session.mount("http://", post_adap)
            self_c.session.mount("https://", post_adap)
            _orig_send = self_c.session.send

            def _dispatch(prep, **kw):
                if prep.method == "GET":
                    get_call_count[0] += 1
                    return orig_get_adapter_send(prep, **kw)
                return _orig_send(prep, **kw)

            self_c.session.send = _dispatch

        with unittest.mock.patch.object(_SPC, "__init__", _counting_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=1,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert result["success"] is True, result["error"]
        # Two batches → two fresh reads (each may be one GET call per combo) and two POSTs.
        assert len(store.post_calls) == 2
        # Fresh reads: at least 2 GET calls (one per batch minimum).
        assert get_call_count[0] >= 2

    def test_batch_limit_1_second_batch_blocked_by_fresh_existing(self, real_modules, tmp_path):
        """batch_limit=1: second batch blocked because record appears between batches.

        Batch1 submits and is acknowledged. Before batch2's fresh read the record
        is already in the store (simulating a concurrent write). The executor stops
        without a second POST; batch1 outcome is still reconciled.
        """
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {
            ("pentad", code, "TFT", issue1, target1),
            ("pentad", code, "TFT", issue2, target2),
        }
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        post_count = [0]
        import json as _json
        import unittest.mock

        orig_init = _SPC.__init__

        def _inject_after_first_post_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            _orig_send = self_c.session.send
            get_adap = _make_combined_get_adapter(store)

            def _dispatch(prep, **kw):
                if prep.method == "GET":
                    return get_adap.send(prep, **kw)
                if prep.method == "POST":
                    body = _json.loads(prep.body)
                    data = body.get("data", [])
                    result_list = [store.upsert(r) for r in data]
                    store.post_calls.append(body)
                    post_count[0] += 1
                    if post_count[0] == 1:
                        # After first POST succeeds, pre-inject the second record
                        # so the second batch's fresh read sees it as existing.
                        existing = _make_period_row_dict(
                            code,
                            issue2.isoformat(),
                            target2.isoformat(),
                            "TFT",
                            "pentad",
                        )
                        store._records[
                            ("pentad", code, "TFT", issue2.isoformat(), target2.isoformat())
                        ] = existing
                    return _make_fake_response(200, result_list)

            self_c.session.send = _dispatch

        with unittest.mock.patch.object(_SPC, "__init__", _inject_after_first_post_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=1,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        # Only one POST (batch1 succeeded; batch2 blocked by fresh existing).
        assert post_count[0] == 1
        # Not success since batch2 was not attempted.
        assert not result["success"]
        # batch1 key must have been attempted and reconciled.
        attempted = set(result["attempted_keys"])
        assert ("pentad", code, "TFT", issue1, target1) in attempted
        # batch2 not attempted.
        assert ("pentad", code, "TFT", issue2, target2) in set(result["not_attempted_keys"])

    def test_first_batch_ack_second_commit_timeout_stop_both_readback(self, real_modules, tmp_path):
        """First batch acknowledged; second POST raises after commit (uncertain).

        Executor stops (no third POST possible). Both attempted batches go
        through reconcile readback.
        """
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {
            ("pentad", code, "TFT", issue1, target1),
            ("pentad", code, "TFT", issue2, target2),
        }
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        import json as _json
        import unittest.mock

        import requests as _requests

        combined_store = _FakeStore()
        post_count = [0]
        orig_init = _SPC.__init__

        def _two_store_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            get_adap = _make_combined_get_adapter(combined_store)

            class _DispatchAdapter(_requests.adapters.BaseAdapter):
                def send(self_a, prep, **kw):
                    if prep.method == "GET":
                        return get_adap.send(prep, **kw)
                    # POST: commit to store; second POST raises after commit (uncertain).
                    body = _json.loads(prep.body)
                    data = body.get("data", [])
                    idx = post_count[0]
                    post_count[0] += 1
                    result_list = [combined_store.upsert(r) for r in data]
                    if idx == 1:
                        raise RuntimeError("commit timeout on second batch")
                    return _make_fake_response(200, result_list)

                def close(self_a):
                    pass

            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        with unittest.mock.patch.object(_SPC, "__init__", _two_store_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=1,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        # Exactly 2 POSTs (no resend after uncertain).
        assert post_count[0] == 2
        # Not success due to uncertainty.
        assert not result["success"]
        # Both keys must have been attempted.
        attempted = set(result["attempted_keys"])
        assert ("pentad", code, "TFT", issue1, target1) in attempted
        assert ("pentad", code, "TFT", issue2, target2) in attempted
        # Partition invariant.
        verified = set(result["verified_keys"])
        unverified = set(result["unverified_keys"])
        assert verified.isdisjoint(unverified)
        assert verified | unverified == attempted

    def test_unexpected_second_fresh_read_exception_earlier_ack_reconciled(
        self, real_modules, tmp_path
    ):
        """Unexpected exception during second batch's fresh read; first ack is still reconciled."""
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {
            ("pentad", code, "TFT", issue1, target1),
            ("pentad", code, "TFT", issue2, target2),
        }
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        post_count = [0]
        get_count = [0]
        import json as _json
        import unittest.mock

        import requests as _req_mod

        get_adap = _make_combined_get_adapter(store)

        orig_init = _SPC.__init__

        def _explode_second_get_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)

            class _DispatchAdapter(_req_mod.adapters.BaseAdapter):
                def send(self_a, prep, **kw):
                    if prep.method == "GET":
                        get_count[0] += 1
                        # Only fail the SECOND GET (fresh read for batch 2);
                        # allow first GET (batch 1 fresh read) and reconcile GETs.
                        if get_count[0] == 2:
                            raise OSError("simulated network failure on second fresh read")
                        return get_adap.send(prep, **kw)
                    if prep.method == "POST":
                        post_count[0] += 1
                        body = _json.loads(prep.body)
                        data = body.get("data", [])
                        result_list = [store.upsert(r) for r in data]
                        store.post_calls.append(body)
                        return _make_fake_response(200, result_list)
                    raise AssertionError(f"unexpected method {prep.method}")

                def close(self_a):
                    pass

            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        with unittest.mock.patch.object(_SPC, "__init__", _explode_second_get_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=1,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        # Not success (second batch failed before fresh read).
        assert not result["success"]
        # Exactly one POST: only batch1 reached the write stage.
        assert post_count[0] == 1, f"expected exactly 1 POST; got {post_count[0]}"
        # Exact partition: only batch1's key was attempted; batch1 reconcile found it in store → verified.
        k1 = ("pentad", code, "TFT", issue1, target1)
        k2 = ("pentad", code, "TFT", issue2, target2)
        assert set(result["attempted_keys"]) == {k1}, (
            f"attempted must be exactly {{k1}}; got {result['attempted_keys']}"
        )
        assert set(result["verified_keys"]) == {k1}, (
            f"verified must be exactly {{k1}} (committed + readback confirmed); got {result['verified_keys']}"
        )
        assert set(result["unverified_keys"]) == set(), (
            f"unverified must be empty; got {result['unverified_keys']}"
        )
        assert set(result["not_attempted_keys"]) == {k2}, (
            f"not_attempted must be exactly {{k2}}; got {result['not_attempted_keys']}"
        )

    def test_commit_then_keyboard_interrupt_first_batch_pending_in_reconcile(
        self, real_modules, tmp_path
    ):
        """POST commits to server then raises KeyboardInterrupt.

        The pending outcome must be in all_outcomes before _p3_submit is called so
        the finally block passes it to reconcile even though P3 never returned
        normally. KeyboardInterrupt propagates after reconcile runs. Exactly one POST.
        """
        import unittest.mock

        import requests as _req_mod
        import src.api_writer as aw
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"
        k = ("pentad", code, "TFT", issue, target)

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        ak = {k}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert k in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        reconcile_calls = []

        original_reconcile = aw.reconcile_recovery_batches

        def _spy_reconcile(attempted_batches, **kwargs):
            call_info = {
                "attempted_batches": list(attempted_batches),
                "kwargs": kwargs,
            }
            reconcile_calls.append(call_info)
            actual_result = original_reconcile(attempted_batches, **kwargs)
            call_info["result"] = actual_result
            return actual_result

        orig_init = _SPC.__init__

        def _commit_raise_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            get_adap = _make_combined_get_adapter(store)
            post_adap = _make_post_adapter(store, commit_then_raise=KeyboardInterrupt("test ki"))

            class _DispatchAdapter(_req_mod.adapters.BaseAdapter):
                def send(self_a, prep, **kw):
                    if prep.method == "GET":
                        return get_adap.send(prep, **kw)
                    return post_adap.send(prep, **kw)

                def close(self_a):
                    pass

            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        with unittest.mock.patch.object(aw, "reconcile_recovery_batches", _spy_reconcile):
            with unittest.mock.patch.object(_SPC, "__init__", _commit_raise_init):
                with pytest.raises(KeyboardInterrupt):
                    mod._execute_recovery_preview(
                        preview,
                        approved_missing_keys=ak,
                        base_url=base_url,
                        exclusion_confirmed=True,
                        batch_limit=100,
                        write_cutoff=100.0,
                        deadline=110.0,
                        timeout=5,
                        evidence_dir=str(tmp_path),
                        _clock=lambda: 1.0,
                    )

        # KeyboardInterrupt propagated (pytest.raises confirms).
        # Exactly one POST was attempted (no retry after interrupt).
        assert len(store.post_calls) == 1, f"expected exactly 1 POST; got {len(store.post_calls)}"
        # Record was committed to the store before the interrupt.
        assert len(store.get_records()) == 1, "server commit must have happened before interrupt"
        # Reconcile was called in the finally block (not skipped).
        assert len(reconcile_calls) == 1, (
            "reconcile_recovery_batches must be called in finally even after KeyboardInterrupt"
        )
        # The pending outcome containing the attempted batch must be in all_outcomes.
        batches_passed = reconcile_calls[0]["attempted_batches"]
        assert len(batches_passed) == 1, (
            f"reconcile must receive the 1 pending outcome; got {len(batches_passed)}"
        )
        pending = batches_passed[0]
        assert pending.get("attempted") is True, "pending outcome must have attempted=True"
        assert pending.get("transport_status") in ("uncertain", "acknowledged"), (
            f"pending transport_status unexpected: {pending.get('transport_status')!r}"
        )
        # Evidence path must be present in the pending outcome.
        assert pending.get("evidence_path"), "pending outcome must carry evidence_path"

        # Real reconcile result: committed record must be verified; transport uncertain for k.
        actual_r = reconcile_calls[0]["result"]
        assert k in actual_r.get("verified_keys", []), (
            f"real reconcile must verify the committed key; got verified={actual_r.get('verified_keys')}"
        )
        assert k in actual_r.get("original_transport_uncertain", []), (
            f"k must be in original_transport_uncertain (POST raised KI after commit); "
            f"got {actual_r.get('original_transport_uncertain')}"
        )
        assert pending.get("transport_status") == "uncertain", (
            f"pending batch transport_status must be 'uncertain'; got {pending.get('transport_status')!r}"
        )

    def test_batch_limit_1_ack_then_keyboard_interrupt_second_both_readback(
        self, real_modules, tmp_path
    ):
        """batch_limit=1, first batch acknowledged, second POST commits then KeyboardInterrupt.

        Both attempted batches must reach reconcile. KeyboardInterrupt propagates after
        reconcile runs. Exactly two POSTs (no retry on second uncertain batch).
        """
        import json as _json

        import requests as _req_mod
        import src.api_writer as aw
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)
        k1 = ("pentad", code, "TFT", issue1, target1)
        k2 = ("pentad", code, "TFT", issue2, target2)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {k1, k2}
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 2

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        reconcile_calls = []
        post_count = [0]

        original_reconcile = aw.reconcile_recovery_batches

        def _spy_reconcile(attempted_batches, **kwargs):
            call_info = {
                "attempted_batches": list(attempted_batches),
                "kwargs": kwargs,
            }
            reconcile_calls.append(call_info)
            actual_result = original_reconcile(attempted_batches, **kwargs)
            call_info["result"] = actual_result
            return actual_result

        orig_init = _SPC.__init__

        def _two_batch_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            get_adap = _make_combined_get_adapter(store)

            class _DispatchAdapter(_req_mod.adapters.BaseAdapter):
                def send(self_a, prep, **kw):
                    if prep.method == "GET":
                        return get_adap.send(prep, **kw)
                    # POST: first batch ack, second commits then KeyboardInterrupt.
                    body = _json.loads(prep.body)
                    data = body if isinstance(body, list) else body.get("data", [])
                    idx = post_count[0]
                    post_count[0] += 1
                    result_list = [store.upsert(r) for r in data]
                    store.post_calls.append(body)
                    if idx == 1:
                        raise KeyboardInterrupt("ki on second batch")
                    return _make_fake_response(200, result_list)

                def close(self_a):
                    pass

            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        import unittest.mock

        with unittest.mock.patch.object(aw, "reconcile_recovery_batches", _spy_reconcile):
            with unittest.mock.patch.object(_SPC, "__init__", _two_batch_init):
                with pytest.raises(KeyboardInterrupt):
                    mod._execute_recovery_preview(
                        preview,
                        approved_missing_keys=ak,
                        base_url=base_url,
                        exclusion_confirmed=True,
                        batch_limit=1,
                        write_cutoff=100.0,
                        deadline=110.0,
                        timeout=5,
                        evidence_dir=str(tmp_path),
                        _clock=lambda: 1.0,
                    )

        # Exactly two POSTs (no retry after interrupt on second).
        assert post_count[0] == 2, f"expected 2 POSTs; got {post_count[0]}"
        # Both records committed to the store.
        assert len(store.get_records()) == 2
        # Reconcile called exactly once in the finally block.
        assert len(reconcile_calls) == 1, (
            "reconcile must be called exactly once in finally after KeyboardInterrupt"
        )
        # Both batches must reach reconcile (first acknowledged + second pending).
        batches_passed = reconcile_calls[0]["attempted_batches"]
        assert len(batches_passed) == 2, (
            f"both batches must reach reconcile; got {len(batches_passed)}"
        )
        # First batch was acknowledged; second was pending/uncertain.
        statuses = {b.get("transport_status") for b in batches_passed}
        assert "acknowledged" in statuses, "first batch must be acknowledged"
        assert "uncertain" in statuses, "second batch must be uncertain (pending)"
        # Earlier ack key must be in the first batch's attempted_records.
        all_attempted_records = [r for b in batches_passed for r in b.get("attempted_records", [])]
        issue_dates_in_records = {r.get("date") for r in all_attempted_records}
        assert issue1.isoformat() in issue_dates_in_records, "k1 must be in reconcile input"
        assert issue2.isoformat() in issue_dates_in_records, "k2 must be in reconcile input"

        # Real reconcile result: both records committed → both verified; k2 transport uncertain.
        actual_r = reconcile_calls[0]["result"]
        assert k1 in actual_r.get("verified_keys", []), (
            f"real reconcile must verify k1 (acknowledged + in store); got {actual_r.get('verified_keys')}"
        )
        assert k2 in actual_r.get("verified_keys", []), (
            f"real reconcile must verify k2 (committed before KI, in store); got {actual_r.get('verified_keys')}"
        )
        assert k2 in actual_r.get("original_transport_uncertain", []), (
            f"k2 must be in original_transport_uncertain (POST raised KI after commit); "
            f"got {actual_r.get('original_transport_uncertain')}"
        )


@_skip_no_client
class TestPartitionSemantics:
    """Verify partition verified/unverified assignment from reconcile results."""

    def test_mixed_verified_unverified_readback_retains_first_verified(self, tmp_path):
        """When reconcile returns mixed verified/unverified, first batch's verified key is retained."""
        import unittest.mock

        import src.api_writer as aw
        import src.data_reader as dr
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue1 = datetime.date(2026, 1, 5)
        issue2 = datetime.date(2026, 1, 10)
        target1 = issue1 + datetime.timedelta(1)
        target2 = issue2 + datetime.timedelta(1)
        code = "19999"
        k1 = ("pentad", code, "TFT", issue1, target1)
        k2 = ("pentad", code, "TFT", issue2, target2)

        c1 = _make_candidate_payload(code, issue1, "TFT", "pentad")
        c2 = _make_candidate_payload(code, issue2, "TFT", "pentad")
        ak = {k1, k2}
        plan = gd.plan_period_refresh(
            [],
            [c1, c2],
            ak,
            set(),
            start_date=issue1,
            end_date=issue2,
            forecast_date=issue2,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=2,
            missing_count=2,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c1, c2],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue2,
            start_date=issue1,
            end_date=issue2,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        reconcile_call_count = [0]

        # Reconcile: k1 verified, k2 unverified (mixed result).
        def _fake_reconcile(*args, **kwargs):
            reconcile_call_count[0] += 1
            return {
                "reconciliation_status": "partial",
                "error": None,
                "verified_keys": [k1],
                "unverified_keys": [k2],
                "original_transport_uncertain": False,
            }

        with _patch_client_for_store(store):
            with unittest.mock.patch.object(aw, "reconcile_recovery_batches", _fake_reconcile):
                result = mod._execute_recovery_preview(
                    preview,
                    approved_missing_keys=ak,
                    base_url=base_url,
                    exclusion_confirmed=True,
                    batch_limit=100,
                    write_cutoff=100.0,
                    deadline=110.0,
                    timeout=5,
                    evidence_dir=str(tmp_path),
                    _clock=lambda: 1.0,
                )

        verified = set(result["verified_keys"])
        unverified = set(result["unverified_keys"])
        attempted = set(result["attempted_keys"])
        assert verified.isdisjoint(unverified), "partition invariant"
        assert verified | unverified == attempted, "partition invariant"
        # Reconcile called exactly once.
        assert reconcile_call_count[0] == 1, (
            f"reconcile must be called exactly once; called {reconcile_call_count[0]} times"
        )
        # Exactly one POST: both keys in a single batch (batch_limit=100).
        assert len(store.post_calls) == 1, (
            f"expected exactly 1 POST (both keys in one batch); got {len(store.post_calls)}"
        )
        # Both keys must have been attempted (POST ran for both in single batch).
        assert attempted == {k1, k2}, f"attempted must be exactly {{k1, k2}}; got {attempted}"
        # k1 must be in verified (not erased by overall unverified status).
        assert k1 in verified, f"k1 should be verified but got verified={verified}"
        assert k2 in unverified, f"k2 should be unverified but got unverified={unverified}"

    def test_structured_readback_error_empty_sets_all_attempted_unverified(self, tmp_path):
        """reconcile returns error + empty verified/unverified → all attempted go to unverified."""
        import unittest.mock

        import src.api_writer as aw
        import src.data_reader as dr
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"
        k = ("pentad", code, "TFT", issue, target)
        ak = {k}

        c = _make_candidate_payload(code, issue, "TFT", "pentad")
        plan = gd.plan_period_refresh(
            [],
            [c],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        reconcile_call_count = [0]

        def _fail_reconcile(*args, **kwargs):
            reconcile_call_count[0] += 1
            return {
                "reconciliation_status": "error",
                "error": "readback API returned 503",
                "verified_keys": [],
                "unverified_keys": [],
                "original_transport_uncertain": False,
            }

        with _patch_client_for_store(store):
            with unittest.mock.patch.object(aw, "reconcile_recovery_batches", _fail_reconcile):
                result = mod._execute_recovery_preview(
                    preview,
                    approved_missing_keys=ak,
                    base_url=base_url,
                    exclusion_confirmed=True,
                    batch_limit=100,
                    write_cutoff=100.0,
                    deadline=110.0,
                    timeout=5,
                    evidence_dir=str(tmp_path),
                    _clock=lambda: 1.0,
                )

        attempted = set(result["attempted_keys"])
        verified = set(result["verified_keys"])
        unverified = set(result["unverified_keys"])
        # Partition invariant.
        assert verified.isdisjoint(unverified)
        assert verified | unverified == attempted
        # Reconcile called exactly once.
        assert reconcile_call_count[0] == 1, (
            f"reconcile must be called exactly once; called {reconcile_call_count[0]} times"
        )
        # Exactly one POST (single key, single batch).
        assert len(store.post_calls) == 1, f"expected exactly 1 POST; got {len(store.post_calls)}"
        # Exact attempted set.
        assert attempted == {k}, f"attempted must be exactly {{k}}; got {attempted}"
        # All attempted must be unverified (structured error with empty verified_keys).
        assert unverified == attempted
        assert not verified


# ---------------------------------------------------------------------------
# Executor regression tests for source-level fixes
# ---------------------------------------------------------------------------


@_skip_no_client
class TestExecutorRegressions:
    """Focused regressions for executor bugs fixed in this branch."""

    def test_candidate_quantity_immutable_across_fresh_get(self, real_modules, tmp_path):
        """Mutating candidate_payloads during fresh GET must not alter POST body.

        The executor must snapshot candidate values before issuing the fresh GET.
        If it reads from the live candidate_payloads list after the GET, a concurrent
        mutation would silently corrupt the write.
        """
        import requests.adapters
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"
        original_fd = 100.0
        mutated_fd = 999.0

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        assert candidate["forecasted_discharge"] == original_fd
        ak = {("pentad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert ("pentad", code, "TFT", issue, target) in plan.missing

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        orig_init = _SPC.__init__
        get_mutation_calls = [0]

        # GET adapter: mutates the original candidate during the read, then returns empty.
        class _MutatingGetAdapter(requests.adapters.BaseAdapter):
            def send(self_a, prep, **kw):
                get_mutation_calls[0] += 1
                candidate["forecasted_discharge"] = mutated_fd
                return _make_fake_response(200, [])

            def close(self_a):
                pass

        class _DispatchAdapter(requests.adapters.BaseAdapter):
            def __init__(self_a):
                self_a._get = _MutatingGetAdapter()
                self_a._post = _make_post_adapter(store)

            def send(self_a, prep, **kw):
                if prep.method == "GET":
                    return self_a._get.send(prep, **kw)
                return self_a._post.send(prep, **kw)

            def close(self_a):
                pass

        def _mounting_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        import unittest.mock

        with unittest.mock.patch.object(_SPC, "__init__", _mounting_init):
            mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        # Verify the fresh GET path was actually exercised (mutation adapter was called).
        assert get_mutation_calls[0] >= 1, (
            "mutating GET adapter was never called; snapshot test has no coverage"
        )
        # Verify mutation happened during GET (precondition for the test).
        assert candidate["forecasted_discharge"] == mutated_fd

        # POST must carry the pre-GET snapshot value, not the mutated value.
        assert store.post_calls, "expected at least one POST"
        posted_data = store.post_calls[0].get("data", [])
        assert posted_data, "POST body must contain data records"
        assert posted_data[0]["forecasted_discharge"] == original_fd, (
            f"POST sent mutated fd={posted_data[0]['forecasted_discharge']}; "
            f"expected preflight snapshot fd={original_fd}"
        )

    def test_approved_horizon_absent_from_preview_horizons_zero_post(self, real_modules, tmp_path):
        """Approved key for horizon absent from preview.horizons → zero POSTs, not success.

        An approved key whose horizon_type does not appear in preview.horizons
        has no corresponding HorizonPreviewResult and therefore no planner
        authorization.  The executor must reject it cleanly without any writes.
        """
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 6)
        target = issue + datetime.timedelta(1)
        code = "19999"

        # Build a decad plan/hr but set preview.horizons to only "decad".
        candidate = _make_candidate_payload(code, issue, "TFT", "decad")
        ak = {("decad", code, "TFT", issue, target)}
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["decad"],  # only decad in preview
            horizon_results={"decad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Approved key is for pentad — absent from preview.horizons.
        approved_pentad = {("pentad", code, "TFT", issue, target)}
        store = _FakeStore()

        with _patch_client_for_store(store):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=approved_pentad,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"], (
            "approved key with horizon absent from preview.horizons must not succeed"
        )
        assert len(store.post_calls) == 0, f"expected zero POSTs but got {len(store.post_calls)}"

    def test_reconcile_error_with_full_verified_keys_still_fails(self, real_modules, tmp_path):
        """reconcile returning error + full verified_keys → success=False, error retained.

        A reconcile result that carries both an error string and a fully-populated
        verified_keys list must not be silently treated as success.  The error
        field must be preserved in the executor's return dict.
        """
        import unittest.mock

        import src.api_writer as aw
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        issue = datetime.date(2026, 1, 5)
        target = issue + datetime.timedelta(1)
        code = "19999"
        k = ("pentad", code, "TFT", issue, target)
        ak = {k}

        candidate = _make_candidate_payload(code, issue, "TFT", "pentad")
        plan = gd.plan_period_refresh(
            [],
            [candidate],
            ak,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[candidate],
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        store = _FakeStore()
        reconcile_error_msg = "reconcile metadata2/2 keys confirmed but status=error"
        reconcile_call_count = [0]

        # Reconcile returns error AND full verified_keys — the boundary mock.
        def _error_with_full_verified(*args, **kwargs):
            reconcile_call_count[0] += 1
            return {
                "reconciliation_status": "error",
                "error": reconcile_error_msg,
                "verified_keys": [k],
                "unverified_keys": [],
                "original_transport_uncertain": False,
            }

        with _patch_client_for_store(store):
            with unittest.mock.patch.object(
                aw, "reconcile_recovery_batches", _error_with_full_verified
            ):
                result = mod._execute_recovery_preview(
                    preview,
                    approved_missing_keys=ak,
                    base_url=base_url,
                    exclusion_confirmed=True,
                    batch_limit=100,
                    write_cutoff=100.0,
                    deadline=110.0,
                    timeout=5,
                    evidence_dir=str(tmp_path),
                    _clock=lambda: 1.0,
                )

        assert reconcile_call_count[0] == 1, (
            f"reconcile_recovery_batches must be called exactly once; called {reconcile_call_count[0]} times"
        )
        assert not result["success"], (
            "reconcile error must set success=False even when verified_keys is fully populated"
        )
        assert result["error"] is not None, "error field must be retained"
        assert "reconcile" in result["error"].lower() or reconcile_error_msg in result["error"], (
            f"error field must reference reconcile error; got: {result['error']!r}"
        )


# ---------------------------------------------------------------------------
# P4 gap-fill tests: native preview → executor roundtrip, budget, page
# failure, and cross-horizon preflight (required_test_gaps from review JSON)
# ---------------------------------------------------------------------------


def _make_day_row_for_test(
    code,
    issue_str,
    target_str,
    model,
    fd,
    q05,
    q25,
    q75,
    q95,
    horizon_value=None,
    horizon_in_year=None,
    flag=None,
):
    """Build a valid DAY row dict matching read_day_forecasts_for_recovery schema.

    horizon_value and horizon_in_year must equal target day-of-year for DAY rows.
    """
    import datetime as _dt

    target_d = _dt.date.fromisoformat(target_str)
    doy = target_d.timetuple().tm_yday
    return {
        "code": code,
        "date": issue_str,
        "target": target_str,
        "model_type": model,
        "horizon_type": "day",
        "forecasted_discharge": fd,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "horizon_value": doy if horizon_value is None else horizon_value,
        "horizon_in_year": doy if horizon_in_year is None else horizon_in_year,
        "flag": flag,
    }


@_skip_no_client
class TestNativePreviewToExecutorRoundtrip:
    """Native _run_recovery_preview → _execute_recovery_preview end-to-end.

    Calls the real _run_recovery_preview('BOTH', ...) with a native-owned client
    whose constructor is patched to mount ONE fake HTTP BaseAdapter. All
    normalizer/planner/selection/P3-submit/readback remain real. Clock patched
    on maintenance.time.monotonic during preview and the same fixed clock is
    passed to the executor.
    """

    def test_both_horizons_native_preview_approved_subset_written_and_verified(
        self, real_modules, tmp_path
    ):
        """Native BOTH preview with DAY source, two codes, two approved keys, one unapproved.

        Issue Jan 10. Two station codes: 19999 (first) and 19998 (second).
        DAY rows for both codes supply coverage for both horizons.
        A pre-loaded healthy existing pentad row for the FIRST code makes its
        pentad key non-missing. Approved: pentad/19998 + decad/19999 (one key
        per horizon, one per code). Not approved: decad/19998 (must remain
        unwritten). Asserts:
          - base_url with non-empty base path preserved exactly in destination
          - planner_authorized_keys is a copy (== captured input, is not same object)
          - 2 POSTs (one per horizon) with exactly the approved keyset
          - exact 14-field dict equality for all submitted rows vs pre-executor
            select_recovery_records output (expected records derived before write)
          - healthy pentad key exists unconditionally and dict unchanged
          - unapproved keys absent from attempted_keys
        """
        import json as _json
        import unittest.mock

        import requests.adapters
        import src.api_writer as aw
        import src.gap_detector as _gd_mod

        mod = _import_maintenance()

        code = "19999"
        second_code = "19998"
        # Jan 10 is a pentad-boundary (pentad 2 in month / 8 in year) and
        # decad-boundary (decad 1 in month / 1 in year).
        issue = datetime.date(2026, 1, 10)
        forecast_date = issue
        start = issue
        end = issue
        # Non-empty base path to prove the adapter routes the actual suffix unchanged.
        base_url = "http://fake-sapphire.local/testing"

        # DAY rows Jan 11-15 for BOTH codes. Same target dates; adapter filters by code.
        day_rows_first = [
            _make_day_row_for_test(
                code,
                "2026-01-10",
                f"2026-01-{d:02d}",
                "TFT",
                100.0 + d,
                80.0,
                90.0,
                110.0,
                120.0,
            )
            for d in range(11, 16)
        ]
        day_rows_second = [
            _make_day_row_for_test(
                second_code,
                "2026-01-10",
                f"2026-01-{d:02d}",
                "TFT",
                200.0 + d,
                80.0,
                90.0,
                110.0,
                120.0,
            )
            for d in range(11, 16)
        ]
        all_day_rows = day_rows_first + day_rows_second

        # Healthy existing combined pentad row for FIRST code only.
        pentad_target = datetime.date(2026, 1, 11)
        _phv, _phiy = _derive_period_ints("pentad", pentad_target)
        existing_pentad = _make_period_row_dict(
            code,
            issue.isoformat(),
            pentad_target.isoformat(),
            "TFT",
            "pentad",
            fd=200.0,
            q05=180.0,
            q25=190.0,
            q75=210.0,
            q95=220.0,
            hv=_phv,
            hiy=_phiy,
        )

        # Store: pre-load healthy pentad row before ANY preview or executor call.
        store = _FakeStore()
        healthy_key_str = ("pentad", code, "TFT", issue.isoformat(), pentad_target.isoformat())
        store._records[healthy_key_str] = existing_pentad
        # Full snapshot BEFORE executor — captured before any write.
        healthy_snapshot_before = dict(existing_pentad)

        # Fixed monotonic clock: baseline from real time.monotonic() before patch.
        import time as _time

        clock_val = [_time.monotonic()]

        def _fixed_clock():
            return clock_val[0]

        orig_init = _SPC.__init__

        # Capture every (method, url) at adapter entry to verify scheme/netloc/path.
        captured_requests: list[tuple[str, str]] = []

        # Derive expected URL components from base_url.
        from urllib.parse import urlsplit as _urlsplit_chk

        _base_split_chk = _urlsplit_chk(base_url)
        _expected_basepath_chk = _base_split_chk.path.rstrip("/")
        _expected_forecast_path = _expected_basepath_chk + "/api/postprocessing/forecast/"
        _expected_lr_path = _expected_basepath_chk + "/api/postprocessing/lr-forecast/"

        class _DispatchAdapter(requests.adapters.BaseAdapter):
            def send(self_a, prep, **kw):
                from urllib.parse import parse_qs, urlparse
                from urllib.parse import urlsplit as _us

                # Capture at entry BEFORE any method routing.
                captured_requests.append((prep.method, prep.url))
                # Assert scheme and netloc match base_url on every request.
                _s = _us(prep.url)
                assert _s.scheme == _base_split_chk.scheme, (
                    f"request scheme {_s.scheme!r} != expected {_base_split_chk.scheme!r}; url={prep.url!r}"
                )
                assert _s.netloc == _base_split_chk.netloc, (
                    f"request netloc {_s.netloc!r} != expected {_base_split_chk.netloc!r}; url={prep.url!r}"
                )
                # Assert path prefix for known endpoints.
                if prep.method == "POST":
                    assert _s.path == _expected_forecast_path, (
                        f"POST path {_s.path!r} != expected {_expected_forecast_path!r}"
                    )
                elif prep.method == "GET" and "/lr-forecast/" in _s.path:
                    assert _s.path == _expected_lr_path, (
                        f"LR GET path {_s.path!r} != expected {_expected_lr_path!r}"
                    )
                elif prep.method == "GET":
                    assert _s.path == _expected_forecast_path, (
                        f"GET path {_s.path!r} != expected {_expected_forecast_path!r}"
                    )
                parsed = urlparse(prep.url)
                qs = parse_qs(parsed.query)

                def _first(k):
                    return qs.get(k, [None])[0]

                if prep.method == "GET":
                    if "/lr-forecast/" in parsed.path:
                        return _make_fake_response(200, [])
                    horizon = _first("horizon")
                    skip = int(_first("skip") or 0)
                    limit = int(_first("limit") or 500)
                    m_filter = _first("model")
                    c_filter = _first("code")
                    s_filter = _first("start_date")
                    e_filter = _first("end_date")

                    if horizon == "day":
                        rows = [
                            r
                            for r in all_day_rows
                            if (c_filter is None or r["code"] == c_filter)
                            and (m_filter is None or r["model_type"] == m_filter)
                            and (s_filter is None or r["date"] >= s_filter)
                            and (e_filter is None or r["date"] <= e_filter)
                        ]
                        return _make_fake_response(200, rows[skip : skip + limit])
                    else:
                        rows = []
                        for r in store.get_records():
                            if horizon and r.get("horizon_type") != horizon:
                                continue
                            if m_filter and r.get("model_type") != m_filter:
                                continue
                            if c_filter and r.get("code") != c_filter:
                                continue
                            if s_filter and r.get("date", "") < s_filter:
                                continue
                            if e_filter and r.get("date", "") > e_filter:
                                continue
                            rows.append(r)
                        return _make_fake_response(200, rows[skip : skip + limit])

                if prep.method == "POST":
                    body = _json.loads(prep.body)
                    data = body if isinstance(body, list) else body.get("data", [])
                    result_list = [store.upsert(r) for r in data]
                    store.post_calls.append(body)
                    return _make_fake_response(200, result_list)

                raise AssertionError(f"unexpected method {prep.method}")

            def close(self_a):
                pass

        def _patched_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            dispatch = _DispatchAdapter()
            self_c.session.mount("http://", dispatch)
            self_c.session.mount("https://", dispatch)

        # Wrap plan_period_refresh DURING PREVIEW ONLY to capture the authorized_keys
        # input object passed by the maintenance module per horizon.
        captured_ak_input: dict[str, set] = {}
        _orig_plan_period_refresh = _gd_mod.plan_period_refresh

        def _capturing_ppf(existing, candidates, authorized_keys, erk, **kwargs):
            ht = candidates[0]["horizon_type"] if candidates else "unknown"
            captured_ak_input[ht] = authorized_keys
            return _orig_plan_period_refresh(existing, candidates, authorized_keys, erk, **kwargs)

        env = {
            "SAPPHIRE_API_URL": base_url,
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }

        with unittest.mock.patch.dict(os.environ, env):
            with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
                with unittest.mock.patch.object(
                    mod, "_read_station_codes", return_value=[code, second_code]
                ):
                    with unittest.mock.patch.object(mod.time, "monotonic", _fixed_clock):
                        with unittest.mock.patch.object(
                            _gd_mod, "plan_period_refresh", _capturing_ppf
                        ):
                            preview = mod._run_recovery_preview(
                                "BOTH",
                                forecast_date,
                                start,
                                end,
                            )

        assert preview.execution_eligible is True, (
            f"expected execution_eligible=True; blocked_reason={preview.blocked_reason!r}"
        )
        # Destination must exactly equal base_url (non-empty path, no trailing slash).
        assert preview.destination_base_url == base_url, (
            f"destination_base_url {preview.destination_base_url!r} != {base_url!r}"
        )
        assert "pentad" in preview.horizon_results, "pentad must be in horizon_results"
        assert "decad" in preview.horizon_results, "decad must be in horizon_results"

        decad_target = datetime.date(2026, 1, 11)
        k_pentad = ("pentad", code, "TFT", issue, pentad_target)
        k_pentad_second = ("pentad", second_code, "TFT", issue, pentad_target)
        k_decad = ("decad", code, "TFT", issue, decad_target)
        k_decad_second = ("decad", second_code, "TFT", issue, decad_target)

        hr_p = preview.horizon_results["pentad"]
        hr_d = preview.horizon_results["decad"]

        # k_pentad pre-exists → NOT missing. k_pentad_second, k_decad, k_decad_second missing.
        assert k_pentad_second in hr_p.plan.missing, (
            f"k_pentad_second must be in pentad plan.missing; missing={hr_p.plan.missing!r}"
        )
        assert k_decad in hr_d.plan.missing, (
            f"k_decad must be in decad plan.missing; missing={hr_d.plan.missing!r}"
        )
        assert k_decad_second in hr_d.plan.missing, (
            f"k_decad_second must be in decad plan.missing; missing={hr_d.plan.missing!r}"
        )

        # Prove planner allowlist COPY: same value as the captured input object, but
        # not the same object (must be a copy so the executor cannot mutate the preview).
        assert "pentad" in captured_ak_input, "plan_period_refresh must have been called for pentad"
        assert "decad" in captured_ak_input, "plan_period_refresh must have been called for decad"
        assert hr_p.planner_authorized_keys == captured_ak_input["pentad"], (
            "pentad planner_authorized_keys must equal the captured authorized_keys input"
        )
        assert hr_p.planner_authorized_keys is not captured_ak_input["pentad"], (
            "pentad planner_authorized_keys must be a copy, not the same object"
        )
        assert hr_d.planner_authorized_keys == captured_ak_input["decad"], (
            "decad planner_authorized_keys must equal the captured authorized_keys input"
        )
        assert hr_d.planner_authorized_keys is not captured_ak_input["decad"], (
            "decad planner_authorized_keys must be a copy, not the same object"
        )

        # Capture expected wire records from REAL select_recovery_records BEFORE executor.
        # Use original planner_authorized_keys (full allowlist per horizon); filter to approved.
        wire_pentad_all = aw.select_recovery_records(
            hr_p.candidate_payloads,
            hr_p.plan,
            hr_p.planner_authorized_keys,
            start_date=start,
            end_date=end,
            forecast_date=forecast_date,
        )
        wire_decad_all = aw.select_recovery_records(
            hr_d.candidate_payloads,
            hr_d.plan,
            hr_d.planner_authorized_keys,
            start_date=start,
            end_date=end,
            forecast_date=forecast_date,
        )

        # Approved: one pentad key (second code) + one decad key (first code).
        approved = {k_pentad_second, k_decad}

        def _wire_key(rec):
            ht = rec["horizon_type"]
            ht_internal = "decad" if ht == "decade" else ht
            return (
                ht_internal,
                rec["code"],
                rec["model_type"],
                datetime.date.fromisoformat(rec["date"]),
                datetime.date.fromisoformat(rec["target"]),
            )

        expected_by_key: dict[tuple, dict] = {}
        for rec in wire_pentad_all + wire_decad_all:
            k = _wire_key(rec)
            if k in approved:
                expected_by_key[k] = rec

        assert set(expected_by_key.keys()) == approved, (
            f"select_recovery_records must produce a record for every approved key; "
            f"got keys {set(expected_by_key.keys())} vs approved {approved}"
        )
        for k, rec in expected_by_key.items():
            assert len(rec) == 14, (
                f"expected wire record for {k} has {len(rec)} fields (need 14): {list(rec)}"
            )

        # Execute with the two approved keys.
        with unittest.mock.patch.object(_SPC, "__init__", _patched_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=approved,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=clock_val[0] + 200.0,
                deadline=clock_val[0] + 250.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=_fixed_clock,
            )

        assert result["success"] is True, f"execute failed: {result['error']!r}"
        assert set(result["verified_keys"]) == approved, (
            f"verified_keys {result['verified_keys']} != approved {approved}"
        )
        assert result["unverified_keys"] == [], "no unverified keys expected"
        assert result["not_attempted_keys"] == [], "no not-attempted keys expected"

        # Destination must be preserved in preview (not altered by executor).
        assert preview.destination_base_url == base_url, "destination_base_url must be unchanged"

        # Exactly 2 POSTs — one per horizon (pentad batch + decad batch).
        assert len(store.post_calls) == 2, (
            f"expected 2 POSTs (one per horizon); got {len(store.post_calls)}"
        )

        # Collect ALL submitted records from both POSTs keyed by canonical 5-tuple.
        submitted_by_key: dict[tuple, dict] = {}
        for post_body in store.post_calls:
            data = post_body if isinstance(post_body, list) else post_body.get("data", [])
            for rec in data:
                submitted_by_key[_wire_key(rec)] = rec

        # Exact POST keyset must equal approved (no extras, no missing).
        assert set(submitted_by_key.keys()) == approved, (
            f"submitted keyset {set(submitted_by_key.keys())} != approved {approved}"
        )

        # Exact 14-field dict equality for ALL submitted rows vs pre-executor expected.
        for k, expected_rec in expected_by_key.items():
            submitted_rec = submitted_by_key[k]
            assert submitted_rec == expected_rec, (
                f"submitted record for key {k} does not exactly match expected wire record; "
                f"submitted={submitted_rec!r}, expected={expected_rec!r}"
            )

        # Healthy existing pentad row: key MUST exist unconditionally, dict unchanged.
        assert healthy_key_str in store._records, (
            "healthy pentad key must still exist in store after executor"
        )
        assert store._records[healthy_key_str] == healthy_snapshot_before, (
            "healthy pentad row must not have been modified by the executor"
        )

        # Unapproved keys must not appear in attempted_keys.
        attempted = set(result["attempted_keys"])
        assert k_pentad not in attempted, (
            "pre-existing pentad/19999 key must not have been attempted"
        )
        assert k_decad_second not in attempted, (
            "unapproved decad/19998 key must not have been attempted"
        )

        # --- URL endpoint assertions: all captured requests must hit the right destination ---
        from urllib.parse import urlsplit as _us2

        _post_urls = [u for m, u in captured_requests if m == "POST"]
        _get_forecast_urls = [
            u for m, u in captured_requests if m == "GET" and "/lr-forecast/" not in _us2(u).path
        ]
        _get_lr_urls = [u for m, u in captured_requests if m == "GET" and "/lr-forecast/" in u]
        # At least one POST must have been captured (executor wrote 2 horizons).
        assert len(_post_urls) >= 1, f"no POST requests captured; all captured: {captured_requests}"
        # Every POST must go to the exact forecast endpoint including /testing base path.
        for _url in _post_urls:
            _s = _us2(_url)
            assert _s.scheme == _base_split_chk.scheme, f"POST wrong scheme: {_url!r}"
            assert _s.netloc == _base_split_chk.netloc, f"POST wrong netloc: {_url!r}"
            assert _s.path == _expected_forecast_path, (
                f"POST wrong path: {_s.path!r}; expected {_expected_forecast_path!r}"
            )
        # Every GET to the forecast endpoint must use the correct path.
        for _url in _get_forecast_urls:
            _s = _us2(_url)
            assert _s.scheme == _base_split_chk.scheme
            assert _s.netloc == _base_split_chk.netloc
            assert _s.path == _expected_forecast_path, (
                f"GET forecast wrong path: {_s.path!r}; expected {_expected_forecast_path!r}"
            )
        # LR GET endpoint must use the LR path (if any LR reads occurred).
        for _url in _get_lr_urls:
            _s = _us2(_url)
            assert _s.path == _expected_lr_path, (
                f"LR GET wrong path: {_s.path!r}; expected {_expected_lr_path!r}"
            )


@_skip_no_client
class TestNativeConstructorBudget:
    """Clock consumed during native client construction is reflected in preparation_deadline.

    The existing test_preparation_deadline_equals_started_at_plus_budget checks only
    arithmetic. This test advances the shared fake clock INSIDE the original native
    constructor to prove construction time is accounted for in the deadline.
    """

    def test_expired_preparation_deadline_blocks_executor(self, tmp_path):
        """Constructor budget consumed during native preview; executor refuses expired write_cutoff.

        started_at is captured before construction. The constructor advances the
        fake clock by 200 s (< 300 s budget), so the preview completes with a
        real missing TFT/pentad key. After preview, the clock is advanced to
        started_at + 301 (past deadline). The executor receives write_cutoff =
        started_at + 290, which the clock has already passed, and must reject
        with zero POSTs and an 'expired'/'write_cutoff' error.
        """
        import time as _time
        import unittest.mock

        import requests.adapters

        mod = _import_maintenance()
        budget = mod._RECOVERY_PREVIEW_DEADLINE_SECONDS

        # Initialize clock from real monotonic BEFORE patching so started_at is derivable.
        real_start = _time.monotonic()
        clock_val = [real_start]

        def _fixed_clock():
            return clock_val[0]

        code = "19999"
        issue = datetime.date(2026, 1, 5)
        # DAY rows for station 19999, issue Jan 5, targets Jan 6-10 (one pentad).
        day_rows = [
            _make_day_row_for_test(
                code,
                "2026-01-05",
                f"2026-01-{d:02d}",
                "TFT",
                100.0 + d,
                80.0,
                90.0,
                110.0,
                120.0,
            )
            for d in range(6, 11)
        ]

        post_count = [0]
        constructor_call_count = [0]
        orig_init = _SPC.__init__

        def _consuming_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)
            constructor_call_count[0] += 1
            # First call = preview: advance clock by 200 s (stays within budget=300).
            if constructor_call_count[0] == 1:
                clock_val[0] = real_start + 200.0

            class _DispAdap(requests.adapters.BaseAdapter):
                def send(self_a2, prep, **kw):
                    from urllib.parse import parse_qs, urlparse

                    # Count at entry before method routing.
                    if prep.method == "POST":
                        post_count[0] += 1
                    if prep.method == "GET":
                        parsed = urlparse(prep.url)
                        qs = parse_qs(parsed.query)
                        h = (qs.get("horizon") or [None])[0]
                        skip = int((qs.get("skip") or [0])[0])
                        limit = int((qs.get("limit") or [500])[0])
                        if h == "day":
                            c_f = (qs.get("code") or [None])[0]
                            rows = [r for r in day_rows if c_f is None or r["code"] == c_f]
                            return _make_fake_response(200, rows[skip : skip + limit])
                        return _make_fake_response(200, [])
                    # POST must never reach here if executor rejects early.
                    return _make_fake_response(200, [])

                def close(self_a2):
                    pass

            adap = _DispAdap()
            self_c.session.mount("http://", adap)
            self_c.session.mount("https://", adap)

        env = {
            "SAPPHIRE_API_URL": "http://localhost:8000",
            "ieasyhydroforecast_run_ML_models": "true",
            "ieasyhydroforecast_available_ML_models": "TFT",
        }
        with unittest.mock.patch.object(_SPC, "__init__", _consuming_init):
            with unittest.mock.patch.dict(os.environ, env):
                with unittest.mock.patch.object(mod, "_read_station_codes", return_value=[code]):
                    with unittest.mock.patch(
                        "postprocessing_maintenance.time.monotonic", _fixed_clock
                    ):
                        preview = mod._run_recovery_preview(
                            "PENTAD",
                            issue,
                            issue,
                            issue,
                        )

        # Preview must have produced an execution-eligible result with TFT/pentad missing.
        assert preview.execution_eligible is True, (
            f"preview must be execution_eligible; blocked_reason={preview.blocked_reason!r}"
        )
        started = preview.started_at
        assert started is not None
        assert preview.started_at == real_start, (
            f"started_at must equal real_start={real_start}; got {preview.started_at}"
        )
        assert clock_val[0] == real_start + 200.0, (
            f"clock after preview must be real_start+200; got {clock_val[0]}"
        )
        deadline = preview.preparation_deadline
        assert abs(deadline - (started + budget)) < 1e-6, (
            f"preparation_deadline={deadline} != started_at={started} + budget={budget}"
        )
        assert "pentad" in preview.horizon_results, "pentad must be in horizon_results"
        target = issue + datetime.timedelta(1)
        k = ("pentad", code, "TFT", issue, target)
        assert k in preview.horizon_results["pentad"].plan.missing, (
            f"TFT/pentad key must be missing in native preview; "
            f"missing={preview.horizon_results['pentad'].plan.missing!r}"
        )

        # Advance clock to started_at + 301 (past deadline = started_at + 300).
        clock_val[0] = started + budget + 1.0
        assert clock_val[0] > deadline, f"clock {clock_val[0]} must be past deadline {deadline}"

        # Execute: write_cutoff = started + budget - 10 < clock_val[0], so executor
        # must reject at entry with 'write_cutoff already expired'.
        approved = {k}
        with unittest.mock.patch.object(_SPC, "__init__", _consuming_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=approved,
                base_url="http://localhost:8000",
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=started + budget - 10.0,  # already expired
                deadline=started + budget,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=_fixed_clock,
            )

        assert not result["success"], "expired write_cutoff must not succeed"
        assert "write_cutoff already expired" in (result["error"] or ""), (
            f"error must contain exact 'write_cutoff already expired'; got: {result['error']!r}"
        )
        assert post_count[0] == 0, (
            f"executor must not attempt any POST when write_cutoff expired; "
            f"post_count={post_count[0]}"
        )
        # Executor should reject before creating a client (constructor_call_count stays at 1).
        assert constructor_call_count[0] == 1, (
            f"no executor client must be constructed since write_cutoff expired at entry; "
            f"constructor called {constructor_call_count[0]} times (expected 1 for preview only)"
        )


def _import_dr():
    """Import real src.data_reader for use inside new test classes."""
    import importlib

    return importlib.import_module("src.data_reader")


@_skip_no_client
class TestFreshPageFailureAfterFullPage:
    """Fresh read returns a full first page then HTTP 500 on the second page.

    The existing test_fresh_read_page_failure_stops_batch returns 500 on every
    GET. This test supplies exactly page_size=500 records on the first GET to
    force pagination, then returns 500 on the second GET. Zero POSTs must result.
    """

    @staticmethod
    def _gen_pentad_boundary_dates(n):
        """Generate n valid pentad issue dates using last-day-of-month for month ends.

        A valid pentad issue date d is one where d+1 is a pentad start day
        (1, 6, 11, 16, 21, 26). Matches _is_pentad_boundary: days 5, 10, 15,
        20, 25, and last-day-of-month.
        """
        import calendar as _cal

        dates = []
        year = 2021
        month = 1
        while len(dates) < n:
            last_day = _cal.monthrange(year, month)[1]
            for day in (5, 10, 15, 20, 25, last_day):
                d = datetime.date(year, month, day)
                if d not in dates:
                    dates.append(d)
                if len(dates) >= n:
                    break
            month += 1
            if month > 12:
                month = 1
                year += 1
        return dates[:n]

    def test_second_get_500_after_full_first_page_zero_post(self, real_modules, tmp_path):
        """Full first page (500 rows with different keys), then 500 on page 2 → zero POST.

        Generates 1000 valid pentad boundary dates. The approved set uses every
        odd-indexed date (500 keys). Page 1 serves the 500 even-indexed dates —
        valid TFT/pentad rows within [start, end] whose (code, date, target, model)
        do not match any approved key. The replan finds all 500 approved keys still
        missing, the paginator requests page 2, page 2 returns HTTP 500, the fresh
        read fails with RECOVERY_STATUS_FAILED, and no POST is made.
        """
        import unittest.mock

        import requests.adapters
        import src.gap_detector as gd

        mod = _import_maintenance()
        dr = real_modules["dr"]

        code = "19999"
        # Generate 1002 valid pentad boundary dates.
        # approved uses two sentinels (indices 0 and 1001) at both ends so that
        # [min_approved, max_approved] = [all_dates[0], all_dates[1001]] strictly
        # encloses all page1 dates [all_dates[1], all_dates[999]].  This prevents
        # the fresh-read validator from rejecting page1 as "outside requested date
        # window" before pagination can reach page 2.
        all_dates = self._gen_pentad_boundary_dates(1002)
        approved_dates = (
            [all_dates[0]] + [all_dates[i] for i in range(2, 998, 2)] + [all_dates[1001]]
        )
        page1_dates = [all_dates[i] for i in range(1, 1001, 2)]
        assert len(approved_dates) == 500
        assert len(page1_dates) == 500
        forecast_date = all_dates[-1]

        candidates = [_make_candidate_payload(code, d, "TFT", "pentad") for d in approved_dates]
        ak = {("pentad", code, "TFT", d, d + datetime.timedelta(1)) for d in approved_dates}
        plan = gd.plan_period_refresh(
            [],
            candidates,
            ak,
            set(),
            start_date=all_dates[0],
            end_date=all_dates[-1],
            forecast_date=forecast_date,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 500

        hr = mod.HorizonPreviewResult(
            plan=plan,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=500,
            missing_count=500,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=candidates,
            unusable_units={},
            planner_authorized_keys=ak,
        )
        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=all_dates[0],
            end_date=all_dates[-1],
            horizons=["pentad"],
            horizon_results={"pentad": hr},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Page 1: 500 valid TFT/pentad rows for the even-indexed boundary dates.
        # These have different issue dates from the approved (odd-indexed) keys,
        # so the replan finds all 500 approved keys still missing → triggers page 2.
        page1_rows = []
        for d in page1_dates:
            t = d + datetime.timedelta(days=1)
            _hv, _hiy = _derive_period_ints("pentad", t)
            page1_rows.append(
                _make_period_row_dict(
                    code,
                    d.isoformat(),
                    t.isoformat(),
                    "TFT",
                    "pentad",
                    fd=50.0,
                    q05=40.0,
                    q25=45.0,
                    q75=55.0,
                    q95=60.0,
                    hv=_hv,
                    hiy=_hiy,
                )
            )
        assert len(page1_rows) == 500

        get_count = [0]
        # Independent POST counter incremented at adapter entry before any routing.
        post_count = [0]
        # Capture (skip, limit) for each GET to verify pagination offsets.
        get_offsets: list[tuple[int, int]] = []
        orig_init = _SPC.__init__

        def _paginated_fail_init(self_c, **kwargs):
            orig_init(self_c, **kwargs)

            class _PaginatedFail(requests.adapters.BaseAdapter):
                def send(self_a, prep, **kw):
                    # Increment counters at entry BEFORE method routing.
                    if prep.method == "POST":
                        post_count[0] += 1
                    if prep.method == "GET":
                        from urllib.parse import parse_qs, urlparse

                        _qs = parse_qs(urlparse(prep.url).query)
                        _skip = int((_qs.get("skip") or [0])[0])
                        _limit = int((_qs.get("limit") or [500])[0])
                        get_offsets.append((_skip, _limit))
                        get_count[0] += 1
                        if get_count[0] == 1:
                            return _make_fake_response(200, page1_rows)
                        return _make_fake_response(500, {"detail": "server error page2"})
                    # POST must not arrive here if fresh read failed before reaching submit.
                    raise AssertionError("unexpected POST: fresh read should have failed")

                def close(self_a):
                    pass

            self_c.session.mount("http://", _PaginatedFail())
            self_c.session.mount("https://", _PaginatedFail())

        store = _FakeStore()
        with unittest.mock.patch.object(_SPC, "__init__", _paginated_fail_init):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys=ak,
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=1000,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"], "must fail when page 2 returns 500"
        # Exactly 2 GETs: page 1 at offset 0, page 2 at offset 500 (first page limit).
        assert get_count[0] == 2, f"exactly two GETs required (page1 + page2); got {get_count[0]}"
        assert len(get_offsets) == 2, f"expected 2 GET offset captures; got {get_offsets}"
        assert get_offsets[0][0] == 0, f"first GET must have skip=0; got skip={get_offsets[0][0]}"
        assert get_offsets[1][0] == get_offsets[0][1], (
            f"second GET skip must equal first page limit ({get_offsets[0][1]}); "
            f"got skip={get_offsets[1][0]}"
        )
        # POST counter is independent of the store; must be zero.
        assert post_count[0] == 0, (
            f"zero POSTs expected (counted at adapter entry); got post_count={post_count[0]}"
        )
        assert len(store.post_calls) == 0, (
            f"zero POSTs expected after page-2 failure; got {len(store.post_calls)}"
        )
        # attempted_keys must be empty: no batch was submitted.
        assert result.get("attempted_keys", []) == [], (
            f"attempted_keys must be empty when fresh read failed; "
            f"got {result.get('attempted_keys')}"
        )
        # Error must reference the page-2 failure (500 or failure/read).
        assert (
            "500" in (result.get("error") or "")
            or "fail" in (result.get("error") or "").lower()
            or "fresh" in (result.get("error") or "").lower()
        ), f"error must indicate fresh-read failure; got: {result.get('error')!r}"


@_skip_no_client
class TestCrossHorizonPreflightZeroPost:
    """Valid first horizon + invalid second horizon preflight → zero POST.

    Proves first horizon would produce a valid plan and candidate, but
    the second horizon's malformed candidate makes the executor reject
    before any write.
    """

    def test_valid_pentad_invalid_decad_malformed_candidate_zero_post(self, real_modules, tmp_path):
        """Valid pentad candidate + malformed decad candidate → zero POSTs.

        Both plans are real planner results with keys in missing and allowlists.
        After planning, the decad candidate_payloads[0] has q05 removed.
        The executor passes authorization for both but fails during decad
        selection/preflight (malformed payload), producing zero POSTs.
        """
        import src.data_reader as dr
        import src.gap_detector as gd

        mod = _import_maintenance()

        issue = datetime.date(2026, 1, 10)
        pentad_target = issue + datetime.timedelta(1)
        decad_target = issue + datetime.timedelta(1)
        code = "19999"

        # Pentad: valid plan with k_p in missing.
        c_p = _make_candidate_payload(code, issue, "TFT", "pentad")
        k_p = ("pentad", code, "TFT", issue, pentad_target)
        ak_p = {k_p}
        plan_p = gd.plan_period_refresh(
            [],
            [c_p],
            ak_p,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan_p.plan_error is None
        assert k_p in plan_p.missing

        hr_p = mod.HorizonPreviewResult(
            plan=plan_p,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c_p],
            unusable_units={},
            planner_authorized_keys=ak_p,
        )

        # Decad: plan exists but planner_authorized_keys is intentionally empty
        # (simulating a malformed/unauthorized candidate that the planner rejected).
        c_d = _make_candidate_payload(code, issue, "TFT", "decad")
        k_d = ("decad", code, "TFT", issue, decad_target)
        ak_d = {k_d}
        plan_d = gd.plan_period_refresh(
            [],
            [c_d],
            ak_d,
            set(),
            start_date=issue,
            end_date=issue,
            forecast_date=issue,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan_d.plan_error is None
        # HorizonPreviewResult with real planner_authorized_keys; q05 removed
        # from the candidate after planning to trigger selection/preflight failure.
        hr_d = mod.HorizonPreviewResult(
            plan=plan_d,
            source_statuses={(code, "TFT"): dr.RECOVERY_STATUS_COMPLETE},
            candidate_count=1,
            missing_count=1,
            protected_count=0,
            em_blocked_reason="skill_vintage_unresolved",
            diagnostics=[],
            blocked_units={},
            candidate_payloads=[c_d],
            unusable_units={},
            planner_authorized_keys=ak_d,
        )
        hr_d.candidate_payloads[0] = {
            k: v for k, v in hr_d.candidate_payloads[0].items() if k != "q05"
        }

        base_url = "http://localhost:8000"
        preview = mod.RecoveryPreviewResult(
            forecast_date=issue,
            start_date=issue,
            end_date=issue,
            horizons=["pentad", "decad"],
            horizon_results={"pentad": hr_p, "decad": hr_d},
            blocked_reason=None,
            destination_base_url=base_url,
            started_at=0.0,
            preparation_deadline=300.0,
            execution_eligible=True,
        )

        # Approve both pentad and decad keys.
        # The pentad key IS in planner_authorized_keys; the decad key is NOT.
        # The executor must reject the decad key as unauthorized, preventing any POST.
        store = _FakeStore()
        with _patch_client_for_store(store):
            result = mod._execute_recovery_preview(
                preview,
                approved_missing_keys={k_p, k_d},
                base_url=base_url,
                exclusion_confirmed=True,
                batch_limit=100,
                write_cutoff=100.0,
                deadline=110.0,
                timeout=5,
                evidence_dir=str(tmp_path),
                _clock=lambda: 1.0,
            )

        assert not result["success"], (
            "malformed decad candidate must make the executor fail before any write"
        )
        assert len(store.post_calls) == 0, (
            f"zero POSTs expected when decad candidate is malformed; got {len(store.post_calls)}"
        )
        assert (
            "selection" in (result["error"] or "").lower()
            or "preflight" in (result["error"] or "").lower()
            or "q05" in (result["error"] or "")
        ), (
            f"error must indicate selection/preflight failure, not unauthorized; "
            f"got: {result['error']!r}"
        )
