"""
Tests for the PREPG-023 ensemble consumption gate (C1, C1a, C3).

The ensemble block in Quantile_Mapping_OP.main() -- download included --
is only processed when EITHER `ieasyhydroforecast_run_CM_models` (the
conceptual model, the known consumer) OR the new
`ieasyhydroforecast_ensemble_forcing_required` (default off) is on.
Both are parsed case-insensitively against "true" through the single
shared `_env_flag_is_true` helper.

Covers:

  1. Gate closed -> the DG client's ensemble endpoint is never called,
     no ensemble CSVs are written, main() exits 0, and the skip is
     logged exactly once, naming both gate variables and their values.
  2. Gate open via EITHER input, case-insensitively -- the full truth
     table, including the both-off / `ensemble_forcing_required`-absent
     default.
  3. Gate open with no configured HRUs (`ieasyhydroforecast_HRU_ENSEMBLE`
     unset / empty / the literal "None") -> a loud, non-zero config
     failure (C1a) naming the gate input that opened the gate and the
     missing HRU variable -- not a quiet exit 0.
  4. Gate closed with `ieasyhydroforecast_HRU_ENSEMBLE` unset does not
     crash (`.split(",")` on `None` raised `AttributeError` before this
     issue).
  5. Gate closed with `ieasyhydroforecast_OUTPUT_PATH_ENS` unset does
     not crash -- proves the output directory is created lazily, only
     once ensembles are actually required.

No real station or HRU codes are used anywhere in this file: the
synthetic HRU is "19999". Fakes/mocks only -- never a live Data
Gateway.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_ensemble_consumption_gate.py -v
"""

import errno
import json
import logging
import os
import sys
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client before importing -- it's a private package
# not installed in the test environment.
sys.modules.setdefault("sapphire_dg_client", MagicMock())
sys.modules.setdefault("sapphire_dg_client.client", MagicMock())
sys.modules.setdefault("sapphire_dg_client.SapphireDGClient", MagicMock())
sys.modules.setdefault("sapphire_dg_client.snow_model", MagicMock())

import Quantile_Mapping_OP as qm  # noqa: E402

HRU = "19999"

RUN_CM_MODELS_VAR = "ieasyhydroforecast_run_CM_models"
ENSEMBLE_FORCING_REQUIRED_VAR = "ieasyhydroforecast_ensemble_forcing_required"
HRU_ENSEMBLE_VAR = "ieasyhydroforecast_HRU_ENSEMBLE"

# G1: the two completion-banner prefixes, pinned independently of
# Quantile_Mapping_OP.py's own string literals so a regression that
# reintroduces the old unscoped "PREPROCESSING OF WEATHER DATA ...
# DONE"/"ECMWF IFS FORECASTS WRITTEN" wording (which over-claims
# ensemble success -- see the G1 test classes below) cannot pass by
# accident.
CM_BANNER_QMAP_TRUE = (
    "PREPROCESSING OF CONTROL MEMBER WEATHER DATA FROM DATA GATWAY DONE. "
    "DOWNSCALING WITH QUANTILE MAPPING DONE FOR THE CONTROL MEMBER."
)
CM_BANNER_QMAP_FALSE = (
    "PREPROCESSING OF CONTROL MEMBER WEATHER DATA FROM DATA GATWAY DONE "
    "BUT NO DOWNSCALING DONE.\n"
    "ERA5-LAND and ECMWF IFS CONTROL MEMBER FORECASTS WRITTEN WITHOUT DOWNSCALING."
)


def _make_dg_control_member_csv(code, date_str, t_value, p_value):
    """Minimal 7-header-row DG control-member CSV -- just enough for
    dg_utils.transform_data_file_control_member to parse without error,
    which is all these tests need from the control-member step on the
    way to the (possibly gated) ensemble section."""
    cols = ["Station", code, f"{code}.1", f"{code}.2"]
    header_rows = [[f"header_{i}", f"meta_{i}", f"meta_{i}", f"meta_{i}"] for i in range(7)]
    data_row = [[date_str, t_value, p_value, 0.0]]
    return pd.DataFrame(header_rows + data_row, columns=cols)


@pytest.fixture()
def gate_env(tmp_path, monkeypatch):
    """Minimal environment to run main() through the control-member step
    and up to the ensemble consumption gate.

    Deliberately does NOT create `ieasyhydroforecast_OUTPUT_PATH_ENS`'s
    directory and does NOT set `ieasyhydroforecast_HRU_ENSEMBLE`,
    `ieasyhydroforecast_run_CM_models`, or
    `ieasyhydroforecast_ensemble_forcing_required` -- every test sets
    exactly the combination it needs.
    """
    intermediate = tmp_path / "intermediate_data"
    dg_dir = intermediate / "dg_download"
    cm_dir = intermediate / "control_member"
    ens_dir = intermediate / "ensemble"
    config_dir = tmp_path / "config"
    models_dir = tmp_path / "models"
    for d in (dg_dir, cm_dir, config_dir, models_dir):
        d.mkdir(parents=True, exist_ok=True)
    # ens_dir is deliberately NOT created -- proving lazy creation (or
    # non-creation, when the gate stays closed) is the point of tests 3 and 5.

    # NOTE: deliberately do NOT create models_dir/qmap_params -- its
    # absence tells QM to skip quantile mapping (perform_qmapping=False),
    # which keeps the control-member success path minimal.
    config_file = config_dir / "data_gateway_name_twins.json"
    config_file.write_text(json.dumps({"gateway_name_twins": {}}))

    env_vars = {
        "ieasyforecast_intermediate_data_path": str(intermediate),
        "ieasyhydroforecast_OUTPUT_PATH_CM": "control_member",
        "ieasyhydroforecast_OUTPUT_PATH_ENS": "ensemble",
        "ieasyhydroforecast_OUTPUT_PATH_DG": "dg_download",
        "ieasyhydroforecast_HRU_CONTROL_MEMBER": HRU,
        "ieasyhydroforecast_API_KEY_GATEAWAY": "FAKE-KEY-DO-NOT-USE",
        "ieasyhydroforecast_Q_MAP_PARAM_PATH": "qmap_params",
        "ieasyhydroforecast_models_and_scalers_path": str(models_dir),
        "ieasyforecast_configuration_path": str(config_dir),
        "ieasyhydroforecast_config_file_data_gateway_name_twins": "data_gateway_name_twins.json",
    }
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)
    # Make sure the two gate inputs and HRU_ENSEMBLE start unset,
    # regardless of what may be set in the ambient shell/CI environment.
    for var in (HRU_ENSEMBLE_VAR, RUN_CM_MODELS_VAR, ENSEMBLE_FORCING_REQUIRED_VAR):
        monkeypatch.delenv(var, raising=False)

    return {"dg_dir": dg_dir, "cm_dir": cm_dir, "ens_dir": ens_dir, "intermediate": intermediate}


def _make_mock_dg(dg_dir):
    """A DG client double whose control-member call always succeeds
    immediately (so every test here can get past it regardless of the
    gate outcome) and whose ensemble call is a plain, inspectable mock."""
    cm_df = _make_dg_control_member_csv(HRU, "01.01.2024", 5.0, 2.0)
    cm_csv_path = str(dg_dir / "cm_19999.csv")

    def _cm_side_effect(**kwargs):
        cm_df.to_csv(cm_csv_path, index=False)
        return cm_csv_path

    mock_dg = MagicMock()
    mock_dg.operational.get_control_spinup_and_forecast.side_effect = _cm_side_effect
    mock_dg.ecmwf_ens.get_ensemble_forecast.return_value = []
    return mock_dg


def _run_main(mock_dg):
    """ExitStack with sl.load_environment neutralised, the DG client
    mocked, and API writes disabled -- mirrors the pattern used
    throughout this package's other main()-level tests."""
    stack = ExitStack()
    stack.enter_context(patch("Quantile_Mapping_OP.sl.load_environment"))
    stack.enter_context(
        patch(
            "Quantile_Mapping_OP.sapphire_dg_client.client.SapphireDGClient",
            return_value=mock_dg,
        )
    )
    stack.enter_context(patch.object(qm, "SAPPHIRE_API_AVAILABLE", False))
    return stack


# =====================================================================
# Unit tests for the shared helpers
# =====================================================================


class TestEnvFlagIsTrue:
    """Unit coverage for `_env_flag_is_true`, the single shared reader
    for both gate inputs."""

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "  true  ", "TrUe"])
    def test_true_case_insensitive(self, value):
        assert qm._env_flag_is_true(value) is True

    @pytest.mark.parametrize("value", [None, "false", "False", "no", "1", "yes", "", "   "])
    def test_everything_else_is_false(self, value):
        assert qm._env_flag_is_true(value) is False


class TestParseEnsembleHruList:
    """Unit coverage for `_parse_ensemble_hru_list`."""

    @pytest.mark.parametrize("value", [None, "", "   ", "None"])
    def test_no_hrus_sentinels(self, value):
        assert qm._parse_ensemble_hru_list(value) == []

    def test_splits_strips_and_orders(self):
        assert qm._parse_ensemble_hru_list(" 19999 , 28888 ") == ["19999", "28888"]

    def test_drops_empty_entries(self):
        assert qm._parse_ensemble_hru_list("19999,,28888,") == ["19999", "28888"]

    def test_deduplicates_preserving_first_order(self):
        assert qm._parse_ensemble_hru_list("19999,28888,19999") == ["19999", "28888"]


# =====================================================================
# 1 + 4 + 5. Gate closed
# =====================================================================


class TestGateClosed:
    @pytest.mark.parametrize("run_cm_models_value", [None, "False", "false", "no"])
    def test_gate_closed_no_download_exit_0_skip_logged_once(
        self, gate_env, monkeypatch, caplog, run_cm_models_value
    ):
        """Test 1 (+ 4): both gate inputs off/unset and
        ieasyhydroforecast_HRU_ENSEMBLE entirely unset -- no ensemble
        download is attempted, main() exits 0, no ensemble CSVs are
        written, and exactly one INFO line names both gate variables
        and their values."""
        if run_cm_models_value is None:
            monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        else:
            monkeypatch.setenv(RUN_CM_MODELS_VAR, run_cm_models_value)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)

        caplog.set_level(logging.INFO)
        mock_dg = _make_mock_dg(gate_env["dg_dir"])

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        mock_dg.ecmwf_ens.get_ensemble_forecast.assert_not_called()
        assert not list(gate_env["ens_dir"].glob("*_ensemble_forecast.csv"))

        skip_lines = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.INFO
            and RUN_CM_MODELS_VAR in r.getMessage()
            and ENSEMBLE_FORCING_REQUIRED_VAR in r.getMessage()
        ]
        assert len(skip_lines) == 1, (
            f"expected exactly one skip line naming both gate variables, got: {skip_lines}"
        )

    def test_gate_closed_hru_ensemble_unset_does_not_crash(self, gate_env, monkeypatch):
        """Test 4 in isolation: ieasyhydroforecast_HRU_ENSEMBLE is
        entirely unset (os.getenv returns None). Before PREPG-023,
        `.split(",")` on that None raised AttributeError; with the gate
        closed, HRU_ENSEMBLE is never parsed at all."""
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0

    def test_gate_closed_output_path_ens_unset_does_not_crash(self, gate_env, monkeypatch):
        """Test 5: ieasyhydroforecast_OUTPUT_PATH_ENS itself is unset
        (not just its directory absent). Before PREPG-023 this path was
        `os.path.join`'d unconditionally at import/setup time; joining a
        string with None raises TypeError. With the gate closed, the
        variable is never read, proving the directory is created
        lazily and only when required."""
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)
        monkeypatch.delenv("ieasyhydroforecast_OUTPUT_PATH_ENS", raising=False)

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        assert not gate_env["ens_dir"].exists()


# =====================================================================
# 2. Gate open truth table
# =====================================================================


class TestGateOpenTruthTable:
    """Test 2: the full OR-gate truth table, case-insensitive, pinning
    both "either input opens it" and "default off"."""

    @pytest.mark.parametrize(
        "run_cm_models_value, ensemble_forcing_required_value",
        [
            ("True", None),
            ("true", None),
            (None, "true"),
            ("True", "true"),
            ("false", "true"),
            ("true", "false"),
        ],
        ids=[
            "run_cm_models=True",
            "run_cm_models=true",
            "ensemble_forcing_required=true only",
            "both on",
            # F3: the two mixed explicit rows -- neither flag can
            # suppress the other. Without these, an implementation that
            # only consults ensemble_forcing_required when
            # run_CM_models is *unset* would pass every other row here
            # while breaking the main reason the new variable exists.
            "run_cm_models=false, ensemble_forcing_required=true",
            "run_cm_models=true, ensemble_forcing_required=false",
        ],
    )
    def test_gate_open_calls_client(
        self, gate_env, monkeypatch, run_cm_models_value, ensemble_forcing_required_value
    ):
        if run_cm_models_value is None:
            monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        else:
            monkeypatch.setenv(RUN_CM_MODELS_VAR, run_cm_models_value)
        if ensemble_forcing_required_value is None:
            monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        else:
            monkeypatch.setenv(ENSEMBLE_FORCING_REQUIRED_VAR, ensemble_forcing_required_value)
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            # The mocked ensemble endpoint returns no files, so
            # merge_ensemble_forecast's empty-list guard (PREPG-023 C2)
            # returns (None, {"P", "T"}) for this HRU, which the per-HRU
            # loop records as a failure; main() still exits non-zero
            # after the loop. This test only cares whether the client
            # was reached, not the resulting exit code -- see
            # TestPerHruIsolation below for the C2 continuation behaviour.
            with pytest.raises(SystemExit):
                qm.main()

        mock_dg.ecmwf_ens.get_ensemble_forecast.assert_called()

    @pytest.mark.parametrize(
        "run_cm_models_value, ensemble_forcing_required_value",
        [
            (None, None),
            ("False", "false"),
            ("false", None),
            (None, "false"),
        ],
        ids=[
            "both absent (the default)",
            "both explicitly off",
            "run_cm_models=false",
            "ensemble_forcing_required=false",
        ],
    )
    def test_gate_closed_does_not_call_client(
        self, gate_env, monkeypatch, run_cm_models_value, ensemble_forcing_required_value
    ):
        if run_cm_models_value is None:
            monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        else:
            monkeypatch.setenv(RUN_CM_MODELS_VAR, run_cm_models_value)
        if ensemble_forcing_required_value is None:
            monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        else:
            monkeypatch.setenv(ENSEMBLE_FORCING_REQUIRED_VAR, ensemble_forcing_required_value)
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        mock_dg.ecmwf_ens.get_ensemble_forecast.assert_not_called()


# =====================================================================
# 3. Gate open, no HRUs -> loud config failure (C1a)
# =====================================================================


class TestGateOpenNoHrusIsLoudFailure:
    """Test 3: with either gate input on and no ensemble HRUs
    configured, main() must fail loudly -- not exit 0 -- naming both
    the gate input that opened the gate and the missing HRU variable."""

    @pytest.mark.parametrize("hru_ensemble_value", [None, "", "None"])
    @pytest.mark.parametrize(
        "gate_var, gate_value",
        [
            (RUN_CM_MODELS_VAR, "true"),
            (ENSEMBLE_FORCING_REQUIRED_VAR, "true"),
        ],
        ids=["via run_CM_models", "via ensemble_forcing_required"],
    )
    def test_no_hrus_is_a_config_error(
        self, gate_env, monkeypatch, caplog, gate_var, gate_value, hru_ensemble_value
    ):
        # Only one gate input is on for each case -- pin that the error
        # message names that specific one, not just "some flag".
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.setenv(gate_var, gate_value)

        if hru_ensemble_value is None:
            monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)
        else:
            monkeypatch.setenv(HRU_ENSEMBLE_VAR, hru_ensemble_value)

        caplog.set_level(logging.ERROR)
        mock_dg = _make_mock_dg(gate_env["dg_dir"])

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        # Not a quiet skip: the DG ensemble endpoint must never be called.
        mock_dg.ecmwf_ens.get_ensemble_forecast.assert_not_called()
        # Not the directory-creation trap either.
        assert not gate_env["ens_dir"].exists()

        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert gate_var in error_text, error_text
        assert HRU_ENSEMBLE_VAR in error_text, error_text


# =====================================================================
# C2 -- one bad HRU must not abort the module mid-flight
# =====================================================================

# A second synthetic HRU, distinct from the module-level HRU ("19999")
# used everywhere else in this file. Never a real HRU/station code.
HRU_2 = "28888"


def _make_mock_dg_two_hru(dg_dir, ens_side_effect):
    """A DG client double whose control-member call always succeeds
    immediately and whose ensemble call uses a caller-supplied
    side_effect -- for tests that need per-HRU-specific ensemble
    responses (unlike `_make_mock_dg`, which returns a fixed value for
    every call)."""
    cm_df = _make_dg_control_member_csv(HRU, "01.01.2024", 5.0, 2.0)
    cm_csv_path = str(dg_dir / "cm_19999.csv")

    def _cm_side_effect(**kwargs):
        cm_df.to_csv(cm_csv_path, index=False)
        return cm_csv_path

    mock_dg = MagicMock()
    mock_dg.operational.get_control_spinup_and_forecast.side_effect = _cm_side_effect
    mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect
    return mock_dg


class TestPerHruIsolation:
    """PREPG-023 C2: the ensemble block's per-HRU unit (download through
    write) must isolate one HRU's failure from the rest of the loop --
    the second HRU is still attempted, the run summary counts per HRU,
    and main() exits non-zero only after the loop completes.

    Every test here runs with two configured ensemble HRUs (HRU="19999",
    HRU_2="28888") so "the next HRU is still attempted" is an
    observable fact (call count, CSVs written), not an assumption.
    """

    def test_5_missing_temperature_second_hru_still_attempted(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """Test 5: HRU "19999" has tp-only files (temperature missing);
        HRU "28888" has both. The second HRU must still be attempted,
        the summary must read attempted=2 written=1 failed=1 naming the
        failed HRU and {T}, and main() must exit non-zero after the
        loop (not mid-loop)."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            if hru_code == HRU:
                return [ensemble_csv_factory(HRU, model, "tp", ["01/01/2024"], [5.0])]
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0

        # The second HRU WAS attempted: 50 models x 2 HRUs = 100 calls,
        # not 50 (which would mean the loop stopped after HRU 1).
        assert mock_dg.ecmwf_ens.get_ensemble_forecast.call_count == 100
        hru_codes_called = {
            c.kwargs["hru_code"] for c in mock_dg.ecmwf_ens.get_ensemble_forecast.call_args_list
        }
        assert hru_codes_called == {HRU, HRU_2}

        # Only the successful HRU's CSVs were written.
        assert not (gate_env["ens_dir"] / f"{HRU}_P_ensemble_forecast.csv").exists()
        assert (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()
        assert (gate_env["ens_dir"] / f"{HRU_2}_T_ensemble_forecast.csv").exists()

        summary_lines = [
            r.getMessage() for r in caplog.records if "ensemble_hrus_attempted" in r.getMessage()
        ]
        assert len(summary_lines) == 1, summary_lines
        assert "ensemble_hrus_attempted=2" in summary_lines[0]
        assert "written=1" in summary_lines[0]
        assert "failed=1" in summary_lines[0]

        failed_lines = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.ERROR and HRU in r.getMessage() and "{T}" in r.getMessage()
        ]
        assert len(failed_lines) >= 1, [r.getMessage() for r in caplog.records]

    def test_8_missing_precipitation_second_hru_still_attempted(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """Test 8: the P branch is the sibling of the T branch and must
        not be left calling sys.exit -- same continuation behaviour as
        test 5, but with HRU "19999" missing precipitation (2t-only
        files) instead of temperature."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            if hru_code == HRU:
                return [ensemble_csv_factory(HRU, model, "2t", ["01/01/2024"], [270.0])]
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        assert mock_dg.ecmwf_ens.get_ensemble_forecast.call_count == 100
        assert (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()

        summary_lines = [
            r.getMessage() for r in caplog.records if "ensemble_hrus_attempted" in r.getMessage()
        ]
        assert len(summary_lines) == 1
        assert "ensemble_hrus_attempted=2" in summary_lines[0]
        assert "written=1" in summary_lines[0]
        assert "failed=1" in summary_lines[0]

        failed_lines = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.ERROR and HRU in r.getMessage() and "{P}" in r.getMessage()
        ]
        assert len(failed_lines) >= 1, [r.getMessage() for r in caplog.records]

    def test_9_empty_files_downloaded_fails_only_that_hru(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """Test 9: HRU "19999" downloads no files at all (every model
        call returns an empty list, no exception raised) -- distinct
        from a download ValueError. merge_ensemble_forecast returns
        (None, {"P", "T"}) for it rather than exiting the module, and
        HRU "28888" is still attempted and succeeds."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            if hru_code == HRU:
                return []
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        assert mock_dg.ecmwf_ens.get_ensemble_forecast.call_count == 100
        assert (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()

        summary_lines = [
            r.getMessage() for r in caplog.records if "ensemble_hrus_attempted" in r.getMessage()
        ]
        assert len(summary_lines) == 1
        assert "ensemble_hrus_attempted=2" in summary_lines[0]
        assert "written=1" in summary_lines[0]
        assert "failed=1" in summary_lines[0]

        failed_lines = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.ERROR
            and HRU in r.getMessage()
            and ("{P,T}" in r.getMessage() or "{T,P}" in r.getMessage())
        ]
        assert len(failed_lines) >= 1, [r.getMessage() for r in caplog.records]

    def test_10_download_valueerror_fails_only_that_hru(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """Test 10 (C2 scope): HRU "19999"'s download raises a
        non-matching ValueError (the ":964"-style "unexpected error"
        path, not the "no files" fallback trigger). This must fail only
        that HRU -- HRU "28888" is still attempted and succeeds -- and
        the redaction helper must still be applied to the logged
        message (the live API key must never appear in the log)."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        secret_message = (
            f"Failed to get data from api/calculations/ensemble?hru_code={HRU}"
            "&api_key=FAKE-KEY-DO-NOT-USE: "
            '{"message": "Internal server error", "success": false}'
        )

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            if hru_code == HRU:
                raise ValueError(secret_message)
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0

        # HRU "19999" fails on its very first model call (a ValueError
        # is not retried, and it doesn't match the "no files" fallback
        # trigger), so only 1 call for it; HRU "28888" is still fully
        # attempted: 50 calls.
        calls_by_hru = {}
        for c in mock_dg.ecmwf_ens.get_ensemble_forecast.call_args_list:
            calls_by_hru.setdefault(c.kwargs["hru_code"], 0)
            calls_by_hru[c.kwargs["hru_code"]] += 1
        assert calls_by_hru[HRU] == 1
        assert calls_by_hru[HRU_2] == 50
        assert (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()

        all_text = "\n".join(r.getMessage() for r in caplog.records)
        assert "FAKE-KEY-DO-NOT-USE" not in all_text
        assert "api_key=***" in all_text
        # Diagnostics preserved: this proves redaction, not suppression.
        assert "Internal server error" in all_text


# =====================================================================
# F1 -- the completion banner must tell the truth in all three states
# =====================================================================


class TestCompletionBannerTruthfulness:
    """PREPG-023 F1: the final "PREPROCESSING OF WEATHER DATA ... DONE"
    banner used to be logged unconditionally, before the deferred
    non-zero exit -- claiming success even when the ensemble stage was
    skipped by the gate or attempted and failed. It must now append a
    truthful ensemble-forcing status, and log at ERROR (not INFO) when
    any required ensemble HRU failed.
    """

    def _final_ensemble_status_line(self, caplog):
        lines = [r for r in caplog.records if "Ensemble forcing:" in r.getMessage()]
        assert len(lines) == 1, (
            f"expected exactly one completion-banner line, got: {[r.getMessage() for r in lines]}"
        )
        return lines[0]

    def test_gate_closed_message(self, gate_env, monkeypatch, caplog):
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)
        caplog.set_level(logging.INFO)

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.INFO
        assert "SKIPPED" in record.getMessage()
        # G1: pin the FULL line (prefix + suffix) -- the prefix alone
        # used to claim "ECMWF IFS FORECASTS WRITTEN" even though the
        # gate closed and nothing ensemble-related was written.
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_FALSE} Ensemble forcing: SKIPPED (consumption gate closed)."
        )
        self.closed_message = record.getMessage()

    def test_gate_open_success_message(self, gate_env, monkeypatch, caplog, ensemble_csv_factory):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(HRU, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.INFO
        assert "written for 1/1" in record.getMessage()
        assert "SKIPPED" not in record.getMessage()
        assert "FAILED" not in record.getMessage()
        # G1: pin the FULL line.
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_FALSE} Ensemble forcing: written for 1/1 HRU(s)."
        )
        self.success_message = record.getMessage()

    def test_gate_open_failure_message(self, gate_env, monkeypatch, caplog):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)
        caplog.set_level(logging.INFO)

        # The mocked ensemble endpoint returns no files for any model, so
        # merge_ensemble_forecast fails this (the only configured) HRU.
        mock_dg = _make_mock_dg(gate_env["dg_dir"])

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        record = self._final_ensemble_status_line(caplog)
        # The failure state must be visible without reading the whole
        # log: ERROR level, and the message must not claim success.
        assert record.levelno == logging.ERROR
        assert "FAILED" in record.getMessage()
        assert "SKIPPED" not in record.getMessage()
        # G1: pin the FULL line -- the prefix alone used to claim "DONE"
        # even though every configured ensemble HRU failed.
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_FALSE} Ensemble forcing: FAILED for 1/1 HRU(s) (written=0)."
        )
        self.failure_message = record.getMessage()

    def test_all_three_states_produce_different_messages(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """Runs all three states in one test and pins that the three
        final messages are pairwise distinct, and that the failure
        message does not claim success."""
        messages = {}

        # State 1: gate closed.
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)
        caplog.set_level(logging.INFO)
        mock_dg = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit):
                qm.main()
        messages["closed"] = self._final_ensemble_status_line(caplog).getMessage()
        caplog.clear()

        # State 2: gate open, ensemble succeeds.
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(HRU, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg_ok = _make_mock_dg(gate_env["dg_dir"])
        mock_dg_ok.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect
        with _run_main(mock_dg_ok):
            with pytest.raises(SystemExit):
                qm.main()
        messages["success"] = self._final_ensemble_status_line(caplog).getMessage()
        caplog.clear()

        # State 3: gate open, ensemble fails (no files downloaded).
        mock_dg_fail = _make_mock_dg(gate_env["dg_dir"])
        with _run_main(mock_dg_fail):
            with pytest.raises(SystemExit):
                qm.main()
        messages["failed"] = self._final_ensemble_status_line(caplog).getMessage()

        assert len({messages["closed"], messages["success"], messages["failed"]}) == 3, messages
        assert "FAILED" in messages["failed"]
        assert "written" not in messages["failed"] or "written=0" in messages["failed"]
        assert "SKIPPED" not in messages["failed"]


# =====================================================================
# G1 -- the completion banner's PREFIX must also tell the truth, in the
# states TestCompletionBannerTruthfulness above does not cover: a
# partial-failure run (some HRUs written, some failed), and both states
# again under perform_qmapping=True (that branch has its own prefix
# text and was not covered above at all).
# =====================================================================


@pytest.fixture()
def gate_env_qmapping(gate_env):
    """gate_env variant with quantile-mapping parameters present, so
    perform_qmapping=True.

    Quantile_Mapping_OP.py looks up the params CSVs by the CONTROL
    MEMBER HRU (`c_m_hru`, "19999" here -- the loop variable's value
    survives past the control-member loop) for BOTH the control-member
    and ensemble sections, regardless of which ensemble HRU is being
    processed. One param file pair, with rows for both synthetic HRUs
    used in this file (HRU="19999", HRU_2="28888"), therefore covers
    every test using this fixture. a=1.0, b=1.0, wet_day=0.0 makes
    quantile_mapping_ptf's `a * x**b` an identity transform -- these
    tests only care that quantile mapping RUNS without error, not its
    numeric output.
    """
    qmap_dir = os.path.join(
        os.environ["ieasyhydroforecast_models_and_scalers_path"],
        os.environ["ieasyhydroforecast_Q_MAP_PARAM_PATH"],
    )
    os.makedirs(qmap_dir, exist_ok=True)

    params_df = pd.DataFrame(
        {"code": [HRU, HRU_2], "a": [1.0, 1.0], "b": [1.0, 1.0], "wet_day": [0.0, 0.0]}
    )
    params_df.to_csv(os.path.join(qmap_dir, f"HRU{HRU}_P_params.csv"), index=False)
    params_df.to_csv(os.path.join(qmap_dir, f"HRU{HRU}_T_params.csv"), index=False)

    return gate_env


class TestCompletionBannerFullLinePartialAndQmapping:
    """G1: the completion banner's PREFIX ("PREPROCESSING OF ... DONE")
    was left unfixed by the F1 round -- it still unconditionally
    claimed the ensemble stage's work ("ECMWF IFS FORECASTS WRITTEN")
    even when the gate was closed or the run failed. Every assertion
    here checks the FULL final line (prefix + suffix together), for the
    two states TestCompletionBannerTruthfulness above does not reach:
    partial failure, and perform_qmapping=True (its own, separate
    prefix text).
    """

    def _final_ensemble_status_line(self, caplog):
        lines = [r for r in caplog.records if "Ensemble forcing:" in r.getMessage()]
        assert len(lines) == 1, (
            f"expected exactly one completion-banner line, got: {[r.getMessage() for r in lines]}"
        )
        return lines[0]

    # -----------------------------------------------------------------
    # perform_qmapping=False, partial failure (1 of 2 HRUs fail)
    # -----------------------------------------------------------------

    def test_partial_failure_qmapping_false(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            if hru_code == HRU:
                return []  # fails: no files at all
            model = int(models[0])
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.ERROR
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_FALSE} Ensemble forcing: FAILED for 1/2 HRU(s) (written=1)."
        )

    # -----------------------------------------------------------------
    # perform_qmapping=True: closed / success / partial-fail / all-fail
    # -----------------------------------------------------------------

    def test_gate_closed_qmapping_true(self, gate_env_qmapping, monkeypatch, caplog):
        monkeypatch.delenv(RUN_CM_MODELS_VAR, raising=False)
        monkeypatch.delenv(ENSEMBLE_FORCING_REQUIRED_VAR, raising=False)
        monkeypatch.delenv(HRU_ENSEMBLE_VAR, raising=False)
        caplog.set_level(logging.INFO)

        mock_dg = _make_mock_dg(gate_env_qmapping["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.INFO
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_TRUE} Ensemble forcing: SKIPPED (consumption gate closed)."
        )

    def test_gate_open_success_qmapping_true(
        self, gate_env_qmapping, monkeypatch, caplog, ensemble_csv_factory
    ):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(HRU, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg(gate_env_qmapping["dg_dir"])
        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.INFO
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_TRUE} Ensemble forcing: written for 1/1 HRU(s)."
        )

    def test_gate_open_all_failed_qmapping_true(self, gate_env_qmapping, monkeypatch, caplog):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, HRU)
        caplog.set_level(logging.INFO)

        # No files for any model -> merge_ensemble_forecast fails the
        # only configured HRU before quantile mapping is ever reached.
        mock_dg = _make_mock_dg(gate_env_qmapping["dg_dir"])
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.ERROR
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_TRUE} Ensemble forcing: FAILED for 1/1 HRU(s) (written=0)."
        )

    def test_partial_failure_qmapping_true(
        self, gate_env_qmapping, monkeypatch, caplog, ensemble_csv_factory
    ):
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            if hru_code == HRU:
                return []  # fails: no files at all
            model = int(models[0])
            p_file = ensemble_csv_factory(HRU_2, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(HRU_2, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env_qmapping["dg_dir"], ens_side_effect)
        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0
        record = self._final_ensemble_status_line(caplog)
        assert record.levelno == logging.ERROR
        assert record.getMessage() == (
            f"{CM_BANNER_QMAP_TRUE} Ensemble forcing: FAILED for 1/2 HRU(s) (written=1)."
        )


# =====================================================================
# F2 -- the per-HRU except Exception must log a traceback and re-raise
# process-wide faults
# =====================================================================


def _patched_to_csv_raising(target_substring, exc_factory):
    """Return a replacement for pandas.DataFrame.to_csv that raises
    `exc_factory()` only when the destination path contains
    `target_substring`, and otherwise delegates to the real to_csv."""
    original_to_csv = pd.DataFrame.to_csv

    def _patched(self, path_or_buf=None, *args, **kwargs):
        if isinstance(path_or_buf, str) and target_substring in path_or_buf:
            raise exc_factory()
        return original_to_csv(self, path_or_buf, *args, **kwargs)

    return _patched


class TestPerHruExceptHandling:
    """PREPG-023 F2/G2: the blanket `except Exception` around the
    per-HRU body must (a) log the full traceback (redacted) at ERROR,
    not just str(e), and (b) re-raise MemoryError and
    OSError(ENOSPC/EROFS/EDQUOT/ENOMEM) instead of isolating them, since
    those are process-wide faults, not HRU-local ones."""

    def test_type_error_isolated_and_traceback_logged(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """A TypeError raised while writing the FIRST HRU's ensemble CSV
        (e.g. a programming error) must fail only that HRU -- the SECOND
        HRU is still attempted and written -- and the traceback must
        appear in the log, not just the exception's message."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(hru_code, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(hru_code, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        patched_to_csv = _patched_to_csv_raising(
            f"{HRU}_P_ensemble_forecast.csv", lambda: TypeError("boom: not a real bug")
        )
        monkeypatch.setattr(pd.DataFrame, "to_csv", patched_to_csv)

        with _run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code != 0

        # Isolated: HRU_2 still written, HRU (the failing one) is not.
        assert not (gate_env["ens_dir"] / f"{HRU}_P_ensemble_forecast.csv").exists()
        assert (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()
        assert (gate_env["ens_dir"] / f"{HRU_2}_T_ensemble_forecast.csv").exists()

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        all_error_text = "\n".join(r.getMessage() for r in error_records)
        assert "TypeError" in all_error_text
        assert "boom: not a real bug" in all_error_text
        # The traceback itself (not just str(e)) must be present.
        assert "Traceback (most recent call last)" in all_error_text
        assert "raise exc_factory()" in all_error_text or "_patched" in all_error_text

    def test_os_error_enospc_propagates_and_stops_loop(
        self, gate_env, monkeypatch, caplog, ensemble_csv_factory
    ):
        """An OSError with errno=ENOSPC (disk full) while writing the
        FIRST HRU's ensemble CSV is not HRU-local -- it must propagate
        out of main() rather than being isolated, and the SECOND HRU
        must never be attempted (the loop stops)."""
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(hru_code, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(hru_code, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        def _raise_enospc():
            return OSError(errno.ENOSPC, "No space left on device")

        patched_to_csv = _patched_to_csv_raising(f"{HRU}_P_ensemble_forecast.csv", _raise_enospc)
        monkeypatch.setattr(pd.DataFrame, "to_csv", patched_to_csv)

        with _run_main(mock_dg):
            with pytest.raises(OSError) as exc_info:
                qm.main()

        assert exc_info.value.errno == errno.ENOSPC

        # The loop stopped: HRU_2 (the second HRU) was never attempted.
        hru_codes_called = {
            c.kwargs["hru_code"] for c in mock_dg.ecmwf_ens.get_ensemble_forecast.call_args_list
        }
        assert HRU_2 not in hru_codes_called
        assert not (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()

    @pytest.mark.parametrize(
        "exc_factory, expected_exception, expected_errno",
        [
            pytest.param(
                lambda: MemoryError("out of memory"),
                MemoryError,
                None,
                id="MemoryError",
            ),
            pytest.param(
                lambda: OSError(errno.EROFS, "Read-only file system"),
                OSError,
                errno.EROFS,
                id="OSError-EROFS",
            ),
            pytest.param(
                lambda: OSError(errno.EDQUOT, "Disk quota exceeded"),
                OSError,
                errno.EDQUOT,
                id="OSError-EDQUOT",
            ),
            pytest.param(
                lambda: OSError(errno.ENOMEM, "Cannot allocate memory"),
                OSError,
                errno.ENOMEM,
                id="OSError-ENOMEM",
            ),
        ],
    )
    def test_fatal_fault_propagates_and_stops_loop(
        self,
        gate_env,
        monkeypatch,
        caplog,
        ensemble_csv_factory,
        exc_factory,
        expected_exception,
        expected_errno,
    ):
        """G2/G5: MemoryError and OSError(EROFS/EDQUOT/ENOMEM) -- like
        the already-pinned OSError(ENOSPC) above -- are process-wide
        faults, not HRU-local ones, and must be pinned individually:
        before this test, only ENOSPC was actually covered even though
        MemoryError/EROFS were claimed protections, and EDQUOT/ENOMEM
        (G2) had no coverage at all. Each must propagate out of main()
        rather than being isolated as a per-HRU failure, and the SECOND
        HRU must never be attempted (the loop stops).
        """
        monkeypatch.setenv(RUN_CM_MODELS_VAR, "true")
        monkeypatch.setenv(HRU_ENSEMBLE_VAR, f"{HRU},{HRU_2}")
        caplog.set_level(logging.INFO)

        def ens_side_effect(hru_code, date, models, directory):
            model = int(models[0])
            p_file = ensemble_csv_factory(hru_code, model, "tp", ["01/01/2024"], [4.0])
            t_file = ensemble_csv_factory(hru_code, model, "2t", ["01/01/2024"], [270.0])
            return [p_file, t_file]

        mock_dg = _make_mock_dg_two_hru(gate_env["dg_dir"], ens_side_effect)

        patched_to_csv = _patched_to_csv_raising(f"{HRU}_P_ensemble_forecast.csv", exc_factory)
        monkeypatch.setattr(pd.DataFrame, "to_csv", patched_to_csv)

        with _run_main(mock_dg):
            with pytest.raises(expected_exception) as exc_info:
                qm.main()

        if expected_errno is not None:
            assert exc_info.value.errno == expected_errno

        # The loop stopped: HRU_2 (the second HRU) was never attempted.
        hru_codes_called = {
            c.kwargs["hru_code"] for c in mock_dg.ecmwf_ens.get_ensemble_forecast.call_args_list
        }
        assert HRU_2 not in hru_codes_called
        assert not (gate_env["ens_dir"] / f"{HRU_2}_P_ensemble_forecast.csv").exists()

        # G3: the run-so-far summary must survive the fatal raise -- the
        # normal after-loop summary is bypassed, so this partial one
        # (attempted=1, written=0, failed=0: the fault happened on the
        # first HRU's write, before it could be counted either way) is
        # the only aggregate status this run produces.
        summary_lines = [
            r.getMessage() for r in caplog.records if "ensemble_hrus_attempted" in r.getMessage()
        ]
        assert len(summary_lines) == 1, summary_lines
        assert "ensemble_hrus_attempted=1" in summary_lines[0]
        assert "written=0" in summary_lines[0]
        assert "failed=0" in summary_lines[0]
        assert HRU in summary_lines[0]
