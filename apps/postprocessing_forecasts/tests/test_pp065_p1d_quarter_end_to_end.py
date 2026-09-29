"""PP-065 P1d: end-to-end chain tests (tests only, no production code).

Proves the already-implemented, already-reviewed pieces from P1a
(``derive_quarterly_from_monthly_same_issue``, ``src/aggregation.py``),
P1b (the quarter readers' native-row selection + decision-G LR fallback +
the writer's LR/EM skip, ``src/data_reader.py`` / ``src/api_writer.py``)
and P1c (the quarter-only Naive Mean / Skilled Mean helpers,
``src/ensemble_calculator.py``) work correctly TOGETHER as one chain, from
a mocked API boundary through to the records handed to the (mocked)
postprocessing API client.

Per
``doc/plans/issues/high_prio_gi_draft_pp_quarter_derived_models.md``
:2115-2129 (P1d), this file covers its three bullets:

1. One December-Q1 chain (raw monthly rows -> reader -> ensembles),
   exercised under both ``SAPPHIRE_SKILL_LEAD_AWARE`` states
   (``TestScenario1DecemberQ1Chain``).
2. The flag-OFF chain INCLUDING the writer, with a persisted non-native
   LR row present in the operational merge
   (``TestScenario2FlagOffWriterChain``).
3. tjhm Q4-2026, before and after "decision F" (the one-time operational
   DB cleanup described in
   ``high_prio_gi_draft_pp_quarter_calendar_window_validation.md``,
   "Decision F (tjhm only)" / PP-061) -- NOT a code change; this locks
   the already-implemented native-row rule's documented behaviour against
   each DB state as input (``TestScenario3TjhmDecisionF``).

Station code ``19999`` throughout (sentinel, never a real station), per
this repo's "no real station codes in tests" convention.
"""

import datetime as dt
import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import api_writer, data_reader, ensemble_calculator

CODE = "19999"
QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
SKILL_COLS = [
    "quarter_in_year",
    "code",
    "model_short",
    "sdivsigma",
    "nse",
    "delta",
    "accuracy",
    "mae",
    "n_pairs",
]

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_operational_long_term_module():
    """Import postprocessing_operational_long_term.py directly, the same

    way tests/test_lead_aware_write_side_dedup.py already does, to reuse
    its REAL ``_dedup_quarterly_joint`` -- the exact function the
    operational script runs against ``pd.concat([existing_q,
    quarterly_joint])`` (postprocessing_operational_long_term.py:220-226)
    -- without invoking the whole script (station-selection config,
    monthly skill/forecast reads, file_writer, etc. are unrelated to what
    P1d needs to prove here). The module configures logging and creates a
    ``logs/`` dir at import; accepted, matching the existing test file's
    precedent.
    """
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_long_term_p1d",
        os.path.join(SCRIPT_DIR, "postprocessing_operational_long_term.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_OPERATIONAL_LT = _load_operational_long_term_module()


def _write_quarter_config(config_dir, monkeypatch, *, lead, issue_day):
    monkeypatch.setenv("ieasyforecast_configuration_path", str(config_dir.parent))
    # A non-existent station-selection file (mirrors
    # tests/test_quarterly_api_writer.py::_write_quarter_config): without
    # this, api_writer._load_configured_codes joins the configuration path
    # with an EMPTY filename, tries to open the directory itself, and hits
    # an unhandled IsADirectoryError -- unrelated to anything P1d is
    # testing, so it is worked around here rather than investigated.
    monkeypatch.setenv("ieasyforecast_config_file_station_selection", "missing.json")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")
    (config_dir / "quarter.json").write_text(
        json.dumps({"operational_month_lead_time": lead, "operational_issue_day": issue_day})
    )


@pytest.fixture
def kghm_config(monkeypatch, tmp_path):
    """kghm-shaped quarter schedule: issue day 25, lead 1."""
    config_dir = tmp_path / "long_term"
    config_dir.mkdir()
    _write_quarter_config(config_dir, monkeypatch, lead=1, issue_day=25)
    return config_dir


@pytest.fixture
def tjhm_config(monkeypatch, tmp_path):
    """tjhm-shaped quarter schedule: issue day 1, lead 0."""
    config_dir = tmp_path / "long_term"
    config_dir.mkdir()
    _write_quarter_config(config_dir, monkeypatch, lead=0, issue_day=1)
    return config_dir


def _monthly_triplet_rows(issue_date, lead, model, values, code=CODE):
    """One same-issue monthly triplet, raw-API-shaped input for

    ``derive_quarterly_from_monthly_same_issue`` via ``_derive_quarterly_rows``
    (``src/data_reader.py:3497``): three rows at horizon_value
    lead/lead+1/lead+2, all issued on ``issue_date``. Mirrors
    ``tests/test_quarterly_data_reader.py::_quarter_derivation_rows``.
    """
    return [
        {
            "code": code,
            "date": issue_date,
            "model_type": model,
            "horizon_value": lead + i,
            "q50": value,
            "forecasted_discharge": value,
        }
        for i, value in enumerate(values)
    ]


def _direct_quarter_row(valid_from, valid_to, issue_date, *, model, q, horizon_value, code=CODE):
    """One raw API-shaped DIRECT quarterly row (mirrors

    ``tests/test_quarter_calendar_window.py::_quarter_row``)."""
    return {
        "horizon_type": "quarter",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "q": q,
        "q05": q - 10.0,
        "q10": q - 8.0,
        "q25": q - 5.0,
        "q50": q,
        "q75": q + 5.0,
        "q90": q + 8.0,
        "q95": q + 10.0,
        "id": 1,
        "model_type_description": model,
    }


def _api_fake(quarter_rows, monthly_rows):
    """Fake ``_read_long_forecasts_api`` -- the one API boundary every

    scenario below mocks. Filters by code, issue-date year range,
    ``horizon_type`` and (quarter direct reads only) ``horizon_value``, as
    the real call does, so a fake that ignores its arguments cannot mask a
    real wiring bug (mirrors
    ``tests/test_quarter_calendar_window.py::_quarter_api_fake``).
    """

    def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
        wanted_codes = {str(c) for c in codes}
        rows = (
            quarter_rows
            if horizon_type == "quarter"
            else (monthly_rows if horizon_type == "month" else [])
        )
        out = []
        for r in rows:
            if str(r["code"]) not in wanted_codes:
                continue
            issue_year = int(str(r["date"])[:4])
            if not (start_year <= issue_year <= end_year):
                continue
            if (
                horizon_type == "quarter"
                and horizon_value is not None
                and int(r.get("horizon_value", -999)) != horizon_value
            ):
                continue
            out.append(dict(r))
        return pd.DataFrame(out) if out else pd.DataFrame()

    return fake


def _mock_combined_client(rows):
    """MagicMock SapphirePostprocessingClient for

    ``read_quarterly_combined_forecasts`` (mirrors
    ``tests/test_quarter_calendar_window.py::_mock_combined_client``):
    returns `rows` verbatim so the REAL ``_normalize_combined_forecasts``
    runs on them.
    """
    client = MagicMock()
    client.readiness_check.return_value = True
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    client.read_long_term_forecasts.return_value = df
    return client


# ===========================================================================
# Scenario 1 -- one December-Q1 chain, both flags (P1d bullet 1)
# ===========================================================================


class TestScenario1DecemberQ1Chain:
    """Chain: mock ``_read_long_forecasts_api`` (data_reader.py:1430) ->

    REAL ``read_latest_quarterly_forecasts`` (data_reader.py:4001) -> REAL
    ``create_quarterly_ensemble_forecasts`` (ensemble_calculator.py:575),
    run under both ``SAPPHIRE_SKILL_LEAD_AWARE`` states.

    ``read_latest_quarterly_forecasts`` is chosen over
    ``read_quarterly_forecasts`` because it is the reader the REAL
    operational entry point actually calls
    (``postprocessing_operational_long_term.py:211``) -- the same
    function Scenario 2 below chains from, for consistency between the
    two "reader -> ensembles" halves of P1d.

    kghm-shaped (day 25, lead 1) same-issue monthly triplet issued
    2025-12-25, read via ``forecast_date=2025-12-25`` (issue date ==
    forecast_date, same pattern as
    ``TestA5DecemberQ1FlagOn::test_december_issued_q1_survives_latest_reader``
    in tests/test_quarter_calendar_window.py), targeting Q1 2026
    (Jan/Feb/Mar 2026, horizon_value 1/2/3):

    - GBT (one of the seven ``QUARTERLY_DERIVED_MODELS``): monthly values
      [100, 110, 120] -> derived quarter forecasted_discharge = mean =
      110.0.
    - LR_Base (``QUARTER_NATIVE_RAW_MODELS``, decision-G fallback -- NO
      native direct LR row is supplied to the API mock at all, so the
      fallback path is what produces this row): monthly values
      [80, 90, 100] -> mean = 90.0.

    Hand-computed expectations (see ``_expected_naive_mean`` /
    ``_expected_skilled_mean`` below for the arithmetic, independent of
    the SUT):

    - Naive Mean forecasted_discharge = mean(90.0, 110.0) = 100.0, all
      quantile columns null (both members are derived rows with the P1a
      helper's documented all-null-quantiles output contract).
    - Skilled Mean forecasted_discharge = the 1/MAE-weighted mean with
      mae_LR=1.0, mae_GBT=4.0 and ``eps = mean_mae / 100`` (the exact
      epsilon rule documented in
      ``ensemble_calculator._add_skilled_mean_aggregated_ens``) =
      9500/101 ~= 94.0594059405940594... (a repeating decimal; asserted
      via the formula below, not a truncated literal).
    """

    _ISSUE_DATE = "2025-12-25"
    _LEAD = 1
    _GBT_VALUES = [100.0, 110.0, 120.0]
    _LR_VALUES = [80.0, 90.0, 100.0]
    _MAE_LR = 1.0
    _MAE_GBT = 4.0

    def _monthly_rows(self):
        return _monthly_triplet_rows(
            self._ISSUE_DATE, self._LEAD, "GBT", self._GBT_VALUES
        ) + _monthly_triplet_rows(self._ISSUE_DATE, self._LEAD, "LR_Base", self._LR_VALUES)

    def _skill_stats(self):
        rows = [
            (1, CODE, "LR_Base", 0.3, 0.9, 5.0, 0.9, self._MAE_LR, 10),
            (1, CODE, "GBT", 0.3, 0.85, 5.0, 0.85, self._MAE_GBT, 10),
        ]
        return pd.DataFrame(rows, columns=SKILL_COLS)

    def _expected_naive_mean(self):
        return (90.0 + 110.0) / 2.0

    def _expected_skilled_mean(self):
        """Independent re-implementation of the documented 1/MAE-weighted

        formula (plan item 5) -- NOT a call into ensemble_calculator."""
        mean_mae = (self._MAE_LR + self._MAE_GBT) / 2.0
        eps = mean_mae / 100.0
        w_lr = 1.0 / (self._MAE_LR + eps)
        w_gbt = 1.0 / (self._MAE_GBT + eps)
        return (90.0 * w_lr + 110.0 * w_gbt) / (w_lr + w_gbt)

    @pytest.mark.parametrize("flag_on", [False, True], ids=["flag_off", "flag_on"])
    def test_december_q1_chain(self, monkeypatch, kghm_config, flag_on):
        if flag_on:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        else:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        fake = _api_fake(quarter_rows=[], monthly_rows=self._monthly_rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            quarterly_fc = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2025, 12, 25)
            )

        # --- Reader-level assertions (the derivation's own output) ---
        assert not quarterly_fc.empty
        assert set(quarterly_fc["year"].astype(int)) == {2026}
        assert set(quarterly_fc["quarter_in_year"].astype(int)) == {1}
        assert set(quarterly_fc["model_short"]) == {"LR_Base", "GBT"}

        lr_row = quarterly_fc[quarterly_fc["model_short"] == "LR_Base"].iloc[0]
        gbt_row = quarterly_fc[quarterly_fc["model_short"] == "GBT"].iloc[0]
        assert float(lr_row["forecasted_discharge"]) == 90.0
        assert float(gbt_row["forecasted_discharge"]) == 110.0
        for qcol in QUANTILE_COLS:
            assert pd.isna(lr_row[qcol]), "derivation output must null every quantile column"
            assert pd.isna(gbt_row[qcol])
        # horizon_value is populated under BOTH flags (P1b item 2, "Output
        # schema") -- the derivation's own output is flag-independent.
        assert int(lr_row["horizon_value"]) == self._LEAD
        assert int(gbt_row["horizon_value"]) == self._LEAD

        # --- Ensemble-level assertions ---
        result = ensemble_calculator.create_quarterly_ensemble_forecasts(
            quarterly_fc, self._skill_stats()
        )
        assert "EM" not in set(result["model_short"]), "quarter never forms EM (P1c decision 3)"

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        nm_row = nm.iloc[0]
        assert float(nm_row["forecasted_discharge"]) == pytest.approx(self._expected_naive_mean())
        assert nm_row["composition"] == "GBT, LR_Base"
        for qcol in QUANTILE_COLS:
            assert pd.isna(nm_row[qcol])

        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 1
        sm_row = sm.iloc[0]
        assert float(sm_row["forecasted_discharge"]) == pytest.approx(
            self._expected_skilled_mean(), rel=1e-9
        )
        assert sm_row["composition"] == "GBT, LR_Base"
        for qcol in QUANTILE_COLS:
            assert pd.isna(sm_row[qcol])

        # --- Flag-dependent machinery: horizon_value grouping ---
        # The derivation's own output (asserted above) is identical under
        # both flags; only the ENSEMBLE's own horizon_value bookkeeping
        # differs, per P1c's "Evaluate per group ... plus horizon_value
        # ONLY under flag ON" (item 5).
        if flag_on:
            assert int(nm_row["horizon_value"]) == self._LEAD
            assert int(sm_row["horizon_value"]) == self._LEAD
        else:
            assert pd.isna(nm_row["horizon_value"]), (
                "flag OFF must not carry a per-lead horizon_value on the generated ensemble row"
            )
            assert pd.isna(sm_row["horizon_value"])

    def test_december_q1_chain_two_lr_models_still_forms_no_em(self, monkeypatch, kghm_config):
        """Finding 4 (P1d review round 1), part (a): every other fixture
        in this file has only ONE LR model (either LR_Base alone as the
        fallback, or one native LR row) -- but the legacy EM gate
        (inside `_create_aggregated_ensemble_forecasts`) requires 2+ RAW
        LR models (from `AGGREGATED_EM_RAW_MODELS = {LR_BASE, LR_SM}`)
        before it even attempts to form EM, so "no EM appears" was true
        for a trivial reason with those fixtures, not because
        `_skip_em_for_quarter` was doing anything. This fixture adds a
        SECOND LR model (LR_SM, alongside LR_Base) as a decision-G
        monthly-derived fallback contributor, so that if the
        quarter-EM-skip guard were disabled, EM WOULD otherwise form --
        then asserts it still doesn't.

        Verified by mutation: monkeypatching `_skip_em_for_quarter` to
        always return False makes this test fail (an "EM" row,
        composition "LR_Base, LR_SM", then appears in the result).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        lr_sm_values = [50.0, 60.0, 70.0]
        monthly_rows = self._monthly_rows() + _monthly_triplet_rows(
            self._ISSUE_DATE, self._LEAD, "LR_SM", lr_sm_values
        )
        fake = _api_fake(quarter_rows=[], monthly_rows=monthly_rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            quarterly_fc = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2025, 12, 25)
            )
        # Two DISTINCT raw LR models present -- the shape the legacy EM
        # gate (2+ raw LR models) needs before it would even attempt EM.
        assert set(quarterly_fc["model_short"]) == {"LR_Base", "LR_SM", "GBT"}

        skill_stats = pd.concat(
            [
                self._skill_stats(),
                pd.DataFrame(
                    [(1, CODE, "LR_SM", 0.3, 0.9, 5.0, 0.9, 1.0, 10)],
                    columns=SKILL_COLS,
                ),
            ],
            ignore_index=True,
        )
        result = ensemble_calculator.create_quarterly_ensemble_forecasts(quarterly_fc, skill_stats)
        assert "EM" not in set(result["model_short"]), (
            "quarter never forms EM (P1c decision 3), even with 2+ raw "
            "LR models present -- the exact case the legacy EM gate "
            "alone would otherwise let through"
        )


# ===========================================================================
# Scenario 2 -- flag-OFF chain including the writer (P1d bullet 2)
# ===========================================================================


class TestScenario2FlagOffWriterChain:
    """Chain: mock ``_read_long_forecasts_api`` -> REAL

    ``read_latest_quarterly_forecasts`` -> REAL
    ``create_quarterly_ensemble_forecasts`` -> the REAL operational merge
    (``pd.concat([existing_q, quarterly_joint])`` +
    ``_dedup_quarterly_joint``, reproduced from
    ``postprocessing_operational_long_term.py:220-226`` via the loaded
    module) -> REAL ``_write_quarterly_ensemble_to_api`` (mocking only the
    postprocessing API client).

    kghm-shaped monthly triplet issued 2025-03-25 (day 25, lead 1) for Q2
    2025 (targets Apr/May/Jun 2025, horizon_value 1/2/3):

    - LR_Base (decision-G fallback, no native direct row in the READER's
      own API mock): [70, 80, 90] -> mean 80.0.
    - GBT (derived): [130, 140, 150] -> mean 140.0.

    The stale, non-native LR_Base row (date=2025-01-01, i.e. day 1 / lead
    3 months -- neither kghm's native day 25 nor lead 1) at the SAME
    (code, year=2025, quarter_in_year=2) key, with a deliberately
    implausible marker value (999.0) so it is unmistakable if it ever
    leaked into an ensemble or a written record, is fed in at TWO points:
    (1) via the READER's own mocked API boundary
    (``_read_long_forecasts_api``, ``horizon_type="quarter"``), proving
    the reader's OWN native-row filter (``_select_native_quarter_lr_rows``)
    rejects it before it ever reaches the ensemble step; and (2) via the
    operational merge's persisted-combined-forecasts mock
    (``read_quarterly_combined_forecasts``), proving the generic
    ``keep="last"`` dedup in the merge independently resolves a collision
    with a stale PERSISTED row (a different code path -- that reader never
    calls the native-row filter at all).
    """

    _ISSUE_DATE = "2025-03-25"
    _LEAD = 1
    _LR_VALUES = [70.0, 80.0, 90.0]
    _GBT_VALUES = [130.0, 140.0, 150.0]
    _MAE_LR = 2.0
    _MAE_GBT = 3.0
    _STALE_LR_VALUE = 999.0

    def _monthly_rows(self):
        return _monthly_triplet_rows(
            self._ISSUE_DATE, self._LEAD, "LR_Base", self._LR_VALUES
        ) + _monthly_triplet_rows(self._ISSUE_DATE, self._LEAD, "GBT", self._GBT_VALUES)

    def _skill_stats(self):
        rows = [
            (2, CODE, "LR_Base", 0.3, 0.9, 5.0, 0.9, self._MAE_LR, 10),
            (2, CODE, "GBT", 0.3, 0.85, 5.0, 0.85, self._MAE_GBT, 10),
        ]
        return pd.DataFrame(rows, columns=SKILL_COLS)

    def _expected_naive_mean(self):
        return (80.0 + 140.0) / 2.0

    def _expected_skilled_mean(self):
        mean_mae = (self._MAE_LR + self._MAE_GBT) / 2.0
        eps = mean_mae / 100.0
        w_lr = 1.0 / (self._MAE_LR + eps)
        w_gbt = 1.0 / (self._MAE_GBT + eps)
        return (80.0 * w_lr + 140.0 * w_gbt) / (w_lr + w_gbt)

    def _stale_non_native_direct_row(self):
        """The persisted, non-native, stale LR_Base row (date=2025-01-01,
        i.e. day 1 / lead 3 months -- neither kghm's native day 25 nor
        lead 1) at the SAME (code, year=2025, quarter_in_year=2) key as
        the fresh LR_Base fallback, with a deliberately implausible
        marker value (999.0). Raw API-shaped, so it can be fed through
        EITHER mocked API boundary (``_read_long_forecasts_api`` or the
        combined-forecasts client) unmodified."""
        return _direct_quarter_row(
            "2025-04-01",
            "2025-06-30",
            "2025-01-01",
            model="LR_Base",
            q=self._STALE_LR_VALUE,
            horizon_value=self._LEAD,
        )

    def test_flag_off_chain_including_writer(self, monkeypatch, kghm_config):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        # --- Reader ---
        # Finding 1 (P1d review round 1): the stale, non-native LR_Base row
        # is supplied HERE, via the reader's OWN mocked API boundary
        # (`_read_long_forecasts_api`, `horizon_type="quarter"` -- the
        # direct-row source `read_latest_quarterly_forecasts` reads under
        # flag OFF), so the REAL reader has to see it and its OWN
        # `_select_native_quarter_lr_rows` native-row filter has to reject
        # it (day 1 != kghm's native day 25) -- NOT a row injected only
        # after the reader already ran. Verified by mutation: forcing
        # `_select_native_quarter_lr_rows` to be an identity/no-op makes
        # this test fail (the 999.0 value would then survive into
        # `quarterly_fc` and override the clean 80.0 fallback via the
        # reader's own keep="last" dedup, since both rows share the same
        # (code, year, quarter_in_year, model_short) key).
        fake = _api_fake(
            quarter_rows=[self._stale_non_native_direct_row()],
            monthly_rows=self._monthly_rows(),
        )
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            quarterly_fc = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2025, 3, 25)
            )
        assert set(quarterly_fc["model_short"]) == {"LR_Base", "GBT"}
        assert set(quarterly_fc["quarter_in_year"].astype(int)) == {2}
        assert set(quarterly_fc["year"].astype(int)) == {2025}
        # The reader's own native-row filter must have rejected the stale
        # row: only the clean, monthly-derived fallback value (80.0)
        # survives for LR_Base.
        assert self._STALE_LR_VALUE not in set(quarterly_fc["forecasted_discharge"])
        lr_row_from_reader = quarterly_fc[quarterly_fc["model_short"] == "LR_Base"]
        assert len(lr_row_from_reader) == 1
        assert float(lr_row_from_reader.iloc[0]["forecasted_discharge"]) == 80.0

        # --- Ensembles (computed from the reader's output ONLY, before
        # the persisted stale row ever enters the picture) ---
        joint = ensemble_calculator.create_quarterly_ensemble_forecasts(
            quarterly_fc, self._skill_stats()
        )
        nm_before = joint[joint["model_short"] == "Naive Mean"].iloc[0]
        sm_before = joint[joint["model_short"] == "Skilled Mean"].iloc[0]
        assert float(nm_before["forecasted_discharge"]) == pytest.approx(
            self._expected_naive_mean()
        )
        assert float(sm_before["forecasted_discharge"]) == pytest.approx(
            self._expected_skilled_mean(), rel=1e-9
        )
        assert self._STALE_LR_VALUE not in set(joint["forecasted_discharge"])

        # --- The REAL operational merge: existing_q (persisted, stale,
        # non-native LR row) concatenated with the fresh ensemble output,
        # deduped exactly as postprocessing_operational_long_term.py does
        # it (:220-226). read_quarterly_combined_forecasts is the REAL
        # function; only the SapphirePostprocessingClient is mocked. This
        # is a SEPARATE code path from the reader's native-row filter
        # above (read_quarterly_combined_forecasts never calls
        # `_select_native_quarter_lr_rows`) -- it proves the merge's own
        # generic keep="last" dedup independently resolves a collision
        # with a stale PERSISTED row. ---
        client = _mock_combined_client([self._stale_non_native_direct_row()])
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            existing_q = data_reader.read_quarterly_combined_forecasts(codes=[CODE])
        assert not existing_q.empty
        assert self._STALE_LR_VALUE in set(existing_q["forecasted_discharge"])

        quarterly_joint = pd.concat([existing_q, joint], ignore_index=True)
        quarterly_joint = _OPERATIONAL_LT._dedup_quarterly_joint(quarterly_joint)

        # Naive/Skilled Mean survive the merge completely unchanged --
        # they were computed BEFORE the stale row ever entered the frame,
        # so there is no code path by which it could have contributed.
        nm_after = quarterly_joint[quarterly_joint["model_short"] == "Naive Mean"]
        sm_after = quarterly_joint[quarterly_joint["model_short"] == "Skilled Mean"]
        assert len(nm_after) == 1
        assert len(sm_after) == 1
        assert float(nm_after.iloc[0]["forecasted_discharge"]) == pytest.approx(
            self._expected_naive_mean()
        )
        assert float(sm_after.iloc[0]["forecasted_discharge"]) == pytest.approx(
            self._expected_skilled_mean(), rel=1e-9
        )
        assert nm_after.iloc[0]["composition"] == "GBT, LR_Base"
        assert sm_after.iloc[0]["composition"] == "GBT, LR_Base"

        # The stale value must not survive the merge at all: keep="last"
        # (fresh wins) collapses the 4-key (year, quarter, code,
        # model_short) collision between the persisted stale row and the
        # fresh derived LR_Base row.
        lr_rows_after = quarterly_joint[quarterly_joint["model_short"] == "LR_Base"]
        assert len(lr_rows_after) == 1
        assert float(lr_rows_after.iloc[0]["forecasted_discharge"]) == 80.0
        assert self._STALE_LR_VALUE not in set(quarterly_joint["forecasted_discharge"])

        # Finding 4 (P1d review round 1), part (b): add a PERSISTED EM row
        # to the writer's own input, so this file's writer-reaching test
        # exercises the writer's unconditional P1b item-3 EM-skip itself
        # (not just relying on other test files to cover it) -- built from
        # `nm_after` so it carries a consistent (code, year,
        # quarter_in_year, valid_from, valid_to) key, with an implausible
        # marker value (777.0). Verified by mutation: temporarily
        # disabling the writer's `model_upper in {...,"EM",...}` skip
        # branch makes this test fail (777.0 would then appear in
        # `records`).
        _EM_MARKER_VALUE = 777.0
        persisted_em_row = nm_after.iloc[0].copy()
        persisted_em_row["model_short"] = "EM"
        persisted_em_row["forecasted_discharge"] = _EM_MARKER_VALUE
        quarterly_joint_for_writer = pd.concat(
            [quarterly_joint, pd.DataFrame([persisted_em_row])], ignore_index=True
        )

        # --- Writer: mock only the postprocessing API client ---
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 3
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = api_writer._write_quarterly_ensemble_to_api(quarterly_joint_for_writer)
        assert result is True

        records = mock_client.write_long_forecasts.call_args[0][0]
        # No raw LR row, and no EM row (matched or persisted), is EVER
        # written for quarter (P1b item 3), even though a fresh LR_Base
        # row survives in quarterly_joint (the dedup above) and a
        # persisted EM row was fed into the writer's own input just above.
        assert all(rec["q"] != self._STALE_LR_VALUE for rec in records if rec.get("q") is not None)
        assert all(rec["q"] != _EM_MARKER_VALUE for rec in records if rec.get("q") is not None)
        assert all(
            rec["model_type"] not in {"LR_Base", "LR_SM", "LR_BASE", "EM", "ENSEMBLE_MEAN"}
            for rec in records
        )
        # Finding 3 (P1d review round 1): assert the COMPLETE emitted
        # model set (no more, no fewer) and the actual emitted `q` value
        # of each ensemble record, not just which models are absent.
        # Exactly the derived GBT row + Naive Mean + Skilled Mean survive
        # -- the persisted EM row above is skipped entirely, not merely
        # relabelled. Verified by mutation: corrupting the Naive/Skilled
        # Mean values in `quarterly_joint` before this write call (or
        # dropping those two rows from the writer's input) makes these
        # assertions fail.
        assert len(records) == 3
        emitted_model_types = {rec["model_type"] for rec in records}
        assert emitted_model_types == {"GBT", "Naive Mean", "Skilled Mean"}
        gbt_record = next(rec for rec in records if rec["model_type"] == "GBT")
        nm_record = next(rec for rec in records if rec["model_type"] == "Naive Mean")
        sm_record = next(rec for rec in records if rec["model_type"] == "Skilled Mean")
        assert gbt_record["q"] == 140.0
        assert nm_record["q"] == pytest.approx(self._expected_naive_mean())
        assert sm_record["q"] == pytest.approx(self._expected_skilled_mean(), rel=1e-9)


# ===========================================================================
# Scenario 3 -- tjhm Q4-2026, before and after decision F (P1d bullet 3)
# ===========================================================================


class TestScenario3TjhmDecisionF:
    """tjhm-shaped (day 1, lead 0) Q4-2026 chain, locking the

    ALREADY-IMPLEMENTED native-row rule's documented behaviour against
    two DB states that "decision F" (the one-time operational cleanup
    described in PP-064 Chunk C detail 3 / PP-061 --
    ``high_prio_gi_draft_pp_quarter_calendar_window_validation.md``
    :1023-1050 -- NOT a code change) is meant to transition between.

    Shared fixture, both tests: a genuine same-issue monthly triplet
    issued 2026-10-01 (tjhm native day 1, lead 0 -> target months
    Oct/Nov/Dec 2026, horizon_value 0/1/2 -> Q4 2026) for:

    - LR_Base: [200, 210, 220] -> mean 210.0 (the decision-G fallback
      candidate).
    - GBT: [300, 310, 320] -> mean 310.0 (always derived, unaffected by
      decision F).

    "Before" adds a DIRECT quarter row dated 2026-10-01 (day 1, lead 0 --
    i.e. it independently satisfies tjhm's OWN native-row predicate,
    `date.day == issue_day` and year-aware lead == `lead_time`) with an
    implausible aggregate value (555.0), reproducing the plan's own
    evidence: "the local DB has tjhm QUARTER rows dated 2026-10-01 at
    hv0, flag 0 (LR_BASE 4, LR_SM 4, ...). They pass the native rule and
    would suppress the fallback." "After" removes that row entirely
    (simulating decision F's cleanup), leaving only the genuine monthly
    triplet.

    Skilled Mean uses EQUAL mae for LR_Base and GBT (2.0 each) in both
    states, a deliberate simplification: scenario 1 already exercises the
    1/MAE weighted-average arithmetic with UNEQUAL weights, so this
    scenario keeps the arithmetic trivial (Skilled Mean == Naive Mean)
    and puts the whole test's signal on the decision-F value-selection
    question, not on re-proving the weighting formula a second time.
    """

    _ISSUE_DATE = "2026-10-01"
    _LEAD = 0
    _LR_VALUES = [200.0, 210.0, 220.0]
    _GBT_VALUES = [300.0, 310.0, 320.0]
    _STALE_NATIVE_LR_VALUE = 555.0
    _MAE = 2.0  # equal for both models -- see class docstring.

    def _monthly_rows(self):
        return _monthly_triplet_rows(
            self._ISSUE_DATE, self._LEAD, "LR_Base", self._LR_VALUES
        ) + _monthly_triplet_rows(self._ISSUE_DATE, self._LEAD, "GBT", self._GBT_VALUES)

    def _stale_native_direct_row(self):
        """The pre-decision-F row: date == 2026-10-01 (day 1, lead 0) --

        independently NATIVE-shaped for tjhm's own schedule, per the
        plan's own evidence (see class docstring)."""
        return _direct_quarter_row(
            "2026-10-01",
            "2026-12-31",
            "2026-10-01",
            # Spelling matched to the skill_stats fixture below (the
            # Skilled-Mean weight join keys on the RAW model_short string,
            # not the canonicalized form -- see
            # ensemble_calculator._add_skilled_mean_aggregated_ens).
            model="LR_Base",
            q=self._STALE_NATIVE_LR_VALUE,
            horizon_value=self._LEAD,
        )

    def _stale_native_direct_row_mismatched_spelling(self):
        """Same pre-decision-F native row as `_stale_native_direct_row`,
        but spelled "LR_BASE" (canonically equivalent to the monthly
        triplet's "LR_Base" -- see `canonical_model_short`, which
        upper-cases before comparing) instead of matching it exactly.

        Finding 2 (P1d review round 1): with matched spellings, the
        reader's generic ``keep="last"`` string-keyed dedup alone happens
        to produce "native row wins" even if
        `_suppress_lr_fallback_covered_by_direct` were a no-op, because
        both rows share one literal `model_short` string. A mismatched
        (but canonically equivalent) spelling is the only shape where
        ONLY the dedicated canonical-comparison suppression function can
        correctly prevent the fallback from surviving alongside the
        native row -- this is a legitimate real-world shape per this
        repo's own model-name canonicalization convention, not an
        artificial edge case.
        """
        return _direct_quarter_row(
            "2026-10-01",
            "2026-12-31",
            "2026-10-01",
            model="LR_BASE",
            q=self._STALE_NATIVE_LR_VALUE,
            horizon_value=self._LEAD,
        )

    def _skill_stats(self):
        rows = [
            (4, CODE, "LR_Base", 0.3, 0.9, 5.0, 0.9, self._MAE, 10),
            (4, CODE, "GBT", 0.3, 0.85, 5.0, 0.85, self._MAE, 10),
        ]
        return pd.DataFrame(rows, columns=SKILL_COLS)

    def _run_chain(self, monkeypatch, tjhm_config, *, quarter_rows):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _api_fake(quarter_rows=quarter_rows, monthly_rows=self._monthly_rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            quarterly_fc = data_reader.read_quarterly_forecasts([CODE], 2026, 2026)
        q4_2026 = quarterly_fc[
            (quarterly_fc["year"].astype(int) == 2026)
            & (quarterly_fc["quarter_in_year"].astype(int) == 4)
        ]
        joint = ensemble_calculator.create_quarterly_ensemble_forecasts(
            q4_2026, self._skill_stats()
        )
        return q4_2026, joint

    def test_before_decision_f_stale_native_row_wins_and_suppresses_fallback(
        self, monkeypatch, tjhm_config
    ):
        """BEFORE decision F: the stale, native-shaped hv0 row (per the

        plan, exactly the population F is meant to clean up) PASSES the
        native-row rule and therefore SUPPRESSES the monthly-derived LR
        fallback for this key -- this is the documented CURRENT (correct,
        per P1b's own rules) behaviour, and exactly the problem decision F
        exists to fix. It is NOT itself a bug in the P1b/P1c code under
        test here.
        """
        q4_2026, joint = self._run_chain(
            monkeypatch, tjhm_config, quarter_rows=[self._stale_native_direct_row()]
        )

        lr_rows = q4_2026[q4_2026["model_short"].str.upper() == "LR_BASE"]
        assert len(lr_rows) == 1
        # The stale NATIVE row wins over the genuine monthly-derived
        # fallback (210.0) -- P1b's native-row rule has no way to
        # distinguish a genuine native issuance from a stale aggregate
        # with the same shape; that distinction is decision F's job, not
        # code's.
        assert float(lr_rows.iloc[0]["forecasted_discharge"]) == self._STALE_NATIVE_LR_VALUE
        assert 210.0 not in set(q4_2026["forecasted_discharge"])

        gbt_rows = q4_2026[q4_2026["model_short"] == "GBT"]
        assert len(gbt_rows) == 1
        assert float(gbt_rows.iloc[0]["forecasted_discharge"]) == 310.0

        nm = joint[joint["model_short"] == "Naive Mean"].iloc[0]
        sm = joint[joint["model_short"] == "Skilled Mean"].iloc[0]
        # Naive/Skilled Mean are built from the STALE value (555.0), not
        # the genuine fallback (210.0) -- this is the "before" defect
        # decision F exists to fix, faithfully reproduced end-to-end.
        expected = (self._STALE_NATIVE_LR_VALUE + 310.0) / 2.0
        assert float(nm["forecasted_discharge"]) == pytest.approx(expected)
        assert float(sm["forecasted_discharge"]) == pytest.approx(expected)

    def test_before_decision_f_mismatched_lr_spelling_still_suppresses_fallback(
        self, monkeypatch, tjhm_config
    ):
        """Finding 2 (P1d review round 1): isolates
        `_suppress_lr_fallback_covered_by_direct`'s OWN contribution from
        the reader's generic keep="last" dedup, by spelling the native
        direct row "LR_BASE" while the monthly triplet feeding the
        decision-G fallback stays "LR_Base" (see
        `_stale_native_direct_row_mismatched_spelling`).

        Only the dedicated canonical-comparison suppression function can
        recognize these two differently-spelled rows as the SAME (code,
        model, year, quarter) key and drop the fallback; the generic
        dedup keys on the raw `model_short` string and would treat them
        as two independent rows. Verified by mutation: forcing
        `_suppress_lr_fallback_covered_by_direct` to be an identity/no-op
        makes this test fail (both the "LR_BASE" native row AND the
        "LR_Base" fallback row then survive into `q4_2026`, so
        `len(lr_rows) == 1` fails).
        """
        q4_2026, _joint = self._run_chain(
            monkeypatch,
            tjhm_config,
            quarter_rows=[self._stale_native_direct_row_mismatched_spelling()],
        )

        lr_rows = q4_2026[q4_2026["model_short"].str.upper() == "LR_BASE"]
        assert len(lr_rows) == 1
        assert lr_rows.iloc[0]["model_short"] == "LR_BASE"
        assert float(lr_rows.iloc[0]["forecasted_discharge"]) == self._STALE_NATIVE_LR_VALUE
        # The genuine monthly-derived fallback (210.0, spelled "LR_Base")
        # must have been suppressed, not merely out-voted by a dedup key
        # collision that never applied to these mismatched spellings.
        assert 210.0 not in set(q4_2026["forecasted_discharge"])

    def test_after_decision_f_fallback_lr_feeds_ensembles_and_is_never_written(
        self, monkeypatch, tjhm_config
    ):
        """AFTER decision F (the stale hv0 population cleared): the

        genuine monthly-derived LR fallback (210.0) now correctly feeds
        Q4 2026's Naive Mean and Skilled Mean, and (per the writer's
        pre-existing, unconditional P1b item 3 rule -- unrelated to
        decision F itself) no raw LR row is ever written, so none would
        be displayed.
        """
        q4_2026, joint = self._run_chain(monkeypatch, tjhm_config, quarter_rows=[])

        lr_rows = q4_2026[q4_2026["model_short"].str.upper() == "LR_BASE"]
        assert len(lr_rows) == 1
        assert float(lr_rows.iloc[0]["forecasted_discharge"]) == 210.0
        assert self._STALE_NATIVE_LR_VALUE not in set(q4_2026["forecasted_discharge"])

        nm = joint[joint["model_short"] == "Naive Mean"].iloc[0]
        sm = joint[joint["model_short"] == "Skilled Mean"].iloc[0]
        expected = (210.0 + 310.0) / 2.0
        assert float(nm["forecasted_discharge"]) == pytest.approx(expected)
        assert float(sm["forecasted_discharge"]) == pytest.approx(expected)
        assert self._STALE_NATIVE_LR_VALUE not in {
            nm["forecasted_discharge"],
            sm["forecasted_discharge"],
        }

        # Writer: the fallback-derived LR row is present in the input but
        # must never be written (P1b item 3), so it would never be
        # displayed either.
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_long_forecasts.return_value = 2
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = api_writer._write_quarterly_ensemble_to_api(joint)
        assert result is True
        records = mock_client.write_long_forecasts.call_args[0][0]
        assert all(
            rec["model_type"] not in {"LR_Base", "LR_SM", "LR_BASE", "EM", "ENSEMBLE_MEAN"}
            for rec in records
        )
        # Finding 3 (P1d review round 1): assert the COMPLETE emitted
        # model set -- the fallback-derived LR row and any EM are ABSENT,
        # and (this file's own "end-to-end" claim requires proving the
        # ensemble rows are not just non-LR but actually PRESENT with the
        # correct value) GBT/Naive Mean/Skilled Mean are all present with
        # their hand-computed values. Verified by mutation: dropping the
        # Naive/Skilled Mean rows from `joint` before this write call, or
        # corrupting their values, makes these assertions fail (the prior
        # membership-only checks above did not).
        written_models = {rec["model_type"] for rec in records}
        assert written_models == {"GBT", "Naive Mean", "Skilled Mean"}
        gbt_record = next(rec for rec in records if rec["model_type"] == "GBT")
        nm_record = next(rec for rec in records if rec["model_type"] == "Naive Mean")
        sm_record = next(rec for rec in records if rec["model_type"] == "Skilled Mean")
        assert gbt_record["q"] == 310.0
        assert nm_record["q"] == pytest.approx(expected)
        assert sm_record["q"] == pytest.approx(expected)
