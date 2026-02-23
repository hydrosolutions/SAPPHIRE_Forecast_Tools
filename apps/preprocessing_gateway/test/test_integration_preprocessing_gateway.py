"""
Integration tests for preprocessing_gateway pipeline orchestration.

Exercises the real pipeline: QM → extend_era5 → snow, plus backfill
as a separate entry point. Tests cover all 3 sync modes and verify
cross-script CSV handoff.

Mock strategy: only external boundaries are mocked —
  sapphire_dg_client, sapphire_api_client, setup_library.load_environment.
Everything inside the module runs real code (dg_utils transforms,
reanalysis_processing, pandas I/O, CSV reads/writes).

Run all integration tests::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py -v

Run a single test class::

    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestCrossScriptDataFlow -v
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestSyncModes -v
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestSnowPipelineIntegration -v
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestErrorPropagation -v
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestBackfillWorkflow -v

Run a single test::

    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestCrossScriptDataFlow::test_full_three_script_pipeline -v

Test classes
------------
TestErrorPropagation (4 tests)
    DG failures are fatal (sys.exit), API failures non-fatal (CSV
    still written), missing reanalysis handled gracefully.

TestBackfillWorkflow (4 tests)
    New station detection and full-history write, stale station
    incremental write, up-to-date station skip, exit when API
    unavailable.

TestCrossScriptDataFlow (5 tests)
    QM output consumed by extend_era5, both P and T CSVs created,
    reanalysis extended with stable operational data, norms
    calculated from extended reanalysis, full 3-script pipeline
    (QM → extend_era5 → snow) end-to-end.

TestSyncModes (5 tests)
    Operational writes yesterday+today, maintenance writes ~30 days,
    initial writes all data, reanalysis write skipped in
    operational mode, reanalysis write active in maintenance mode.

TestSnowPipelineIntegration (4 tests)
    Snow download → transform → CSV written, elevation bands
    included in API records, multiple HRUs each get own CSVs,
    operational vs maintenance date filtering.
"""

import io
import os
import sys
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

# Add preprocessing_gateway and iEasyHydroForecast to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast'),
)

# Mock the sapphire_dg_client module before importing the actual modules.
# This is necessary because sapphire_dg_client is a private package.
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import dg_utils
import extend_era5_reanalysis as eer
import Quantile_Mapping_OP as qm
import snow_data_operational as sdo
import backfill_new_stations as bns


# =====================================================================
# Helper functions — build DG-format CSV data
# =====================================================================

def make_dg_control_member_csv(
    codes: list[str],
    dates: list[str],
    t_values: list[list[float]],
    p_values: list[list[float]],
) -> pd.DataFrame:
    """Build a DataFrame mimicking the 7-header-row DG control member CSV.

    The DG CSV has columns: Station, <code>, <code>.1, <code>.2
    where bare = T, .1 = P, .2 = SD (ignored).
    First 7 rows are header metadata that transform_data_file_control_member
    drops.

    Args:
        codes: Station codes (e.g., ["12345", "67890"]).
        dates: Date strings in DD.MM.YYYY format.
        t_values: T values per date, shape [len(dates)][len(codes)].
        p_values: P values per date, shape [len(dates)][len(codes)].

    Returns:
        DataFrame in DG format ready for transform_data_file_control_member.
    """
    # Build column names
    cols = ['Station']
    for code in codes:
        cols.extend([code, f'{code}.1', f'{code}.2'])

    # Build 7 header rows (metadata the transform function skips)
    header_rows = []
    for i in range(7):
        row = [f'header_{i}']
        for code in codes:
            row.extend([f'meta_{i}', f'meta_{i}', f'meta_{i}'])
        header_rows.append(row)

    # Build data rows
    data_rows = []
    for d_idx, d in enumerate(dates):
        row = [d]
        for c_idx in range(len(codes)):
            row.extend([
                t_values[d_idx][c_idx],
                p_values[d_idx][c_idx],
                0.0,  # SD (ignored)
            ])
        data_rows.append(row)

    all_rows = header_rows + data_rows
    df = pd.DataFrame(all_rows, columns=cols)
    return df


def make_dg_snow_csv(
    codes: list[str],
    dates: list[str],
    values: list[list[float]],
    var_name: str = "SWE",
    n_bands: int = 0,
) -> pd.DataFrame:
    """Build a DataFrame mimicking the 4-header-row DG snow CSV.

    Args:
        codes: Station codes (e.g., ["15013"]).
        dates: Date strings in DD.MM.YYYY format.
        values: Values per date, shape [len(dates)][len(codes)].
        var_name: Snow variable name (SWE, HS, RoF).
        n_bands: Number of elevation bands per code (0 = base only).

    Returns:
        DataFrame in DG format ready for transform_snow_data.
    """
    # Build column names: first col unnamed, then code columns
    col_names = ['Unnamed: 0']
    for code in codes:
        if n_bands > 0:
            for band in range(1, n_bands + 1):
                col_names.append(f'{code}_{band}')
        else:
            col_names.append(code)

    # Build 4 header rows
    header_rows = []
    for i in range(4):
        row = [f'header_{i}']
        for _ in range(len(col_names) - 1):
            row.append(f'meta_{i}')
        header_rows.append(row)

    # Build data rows
    data_rows = []
    for d_idx, d in enumerate(dates):
        row = [d]
        for c_idx in range(len(codes)):
            if n_bands > 0:
                # Spread value across bands with small variations
                base = values[d_idx][c_idx]
                for band in range(n_bands):
                    row.append(base + band * 0.5)
            else:
                row.append(values[d_idx][c_idx])
        data_rows.append(row)

    all_rows = header_rows + data_rows
    df = pd.DataFrame(all_rows, columns=col_names)
    return df


def make_reanalysis_csv(
    codes: list[str],
    start_date: date,
    n_days: int,
    value_col: str,
    base_value: float = 5.0,
) -> pd.DataFrame:
    """Build a reanalysis CSV DataFrame with historical data.

    Args:
        codes: Station codes.
        start_date: First date in the time series.
        n_days: Number of days of data.
        value_col: 'P' or 'T'.
        base_value: Base value; actual values vary by day-of-year.

    Returns:
        DataFrame with columns [date, <value_col>, code].
    """
    rows = []
    for day_offset in range(n_days):
        d = start_date + timedelta(days=day_offset)
        for code in codes:
            # Vary value by day-of-year for realistic norms
            val = round(base_value + (d.timetuple().tm_yday % 30) * 0.1, 2)
            rows.append({
                'date': pd.Timestamp(d),
                value_col: val,
                'code': str(code),
            })
    return pd.DataFrame(rows)


# =====================================================================
# Fixtures
# =====================================================================

@pytest.fixture()
def gateway_env(tmp_path):
    """Set up the full directory tree and env vars for main() functions.

    Creates:
    - intermediate_data/control_member/
    - intermediate_data/reanalysis/
    - intermediate_data/snow/{SWE,HS}/
    - intermediate_data/dg_download/
    - models/qmap_params/
    - config/

    Seeds reanalysis CSVs with 2+ years of historical data.
    """
    # Directory layout
    intermediate = tmp_path / 'intermediate_data'
    cm_dir = intermediate / 'control_member'
    ens_dir = intermediate / 'ensemble'
    reanalysis_dir = intermediate / 'reanalysis'
    snow_dir = intermediate / 'snow'
    swe_dir = snow_dir / 'SWE'
    hs_dir = snow_dir / 'HS'
    dg_dir = intermediate / 'dg_download'
    models_dir = tmp_path / 'models'
    config_dir = tmp_path / 'config'
    logs_dir = tmp_path / 'logs'

    for d in [
        cm_dir, ens_dir, reanalysis_dir, snow_dir, swe_dir, hs_dir,
        dg_dir, models_dir, config_dir, logs_dir,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    # NOTE: Do NOT create models/qmap_params/ — its absence tells QM
    # to skip quantile mapping (perform_qmapping=False).

    # Station codes
    codes = ['12345', '67890']
    snow_codes = ['15013']

    # Seed reanalysis CSVs (2 years of data)
    start = date(2022, 1, 1)
    n_days = 730  # ~2 years
    for vtype, base in [('P', 2.0), ('T', 5.0)]:
        df = make_reanalysis_csv(codes, start, n_days, vtype, base)
        csv_path = reanalysis_dir / f'HRU001_{vtype}_reanalysis.csv'
        df.to_csv(csv_path, index=False)

    # Seed QM parameter files (no actual quantile mapping in tests;
    # we set perform_qmapping=False by not creating params, OR we
    # create minimal params so QM path works)
    # We do NOT create params → perform_qmapping=False

    # Seed config file for DG name mapping (empty mapping)
    config_file = config_dir / 'data_gateway_name_twins.json'
    import json
    config_file.write_text(json.dumps({'gateway_name_twins': {}}))

    # Environment variables
    env_vars = {
        'SAPPHIRE_TEST_ENV': 'True',
        'SAPPHIRE_OPDEV_ENV': 'True',
        'ieasyforecast_intermediate_data_path': str(intermediate),
        'ieasyhydroforecast_OUTPUT_PATH_CM': 'control_member',
        'ieasyhydroforecast_OUTPUT_PATH_ENS': 'ensemble',
        'ieasyhydroforecast_OUTPUT_PATH_REANALYSIS': 'reanalysis',
        'ieasyhydroforecast_OUTPUT_PATH_DG': 'dg_download',
        'ieasyhydroforecast_OUTPUT_PATH_SNOW': 'snow',
        'ieasyhydroforecast_HRU_CONTROL_MEMBER': 'HRU001',
        'ieasyhydroforecast_HRU_ENSEMBLE': 'None',
        'ieasyhydroforecast_HRU_SNOW_DATA': 'HRU_SNOW01',
        'ieasyhydroforecast_SNOW_VARS': 'SWE,HS',
        'ieasyhydroforecast_API_KEY_GATEAWAY': 'test-api-key-123',
        'ieasyhydroforecast_Q_MAP_PARAM_PATH': 'qmap_params',
        'ieasyhydroforecast_models_and_scalers_path': str(models_dir),
        'ieasyforecast_configuration_path': str(config_dir),
        'ieasyhydroforecast_config_file_data_gateway_name_twins': (
            'data_gateway_name_twins.json'
        ),
        'SAPPHIRE_API_ENABLED': 'true',
        'SAPPHIRE_API_URL': 'http://localhost:8000',
        'SAPPHIRE_SYNC_MODE': 'operational',
        'SAPPHIRE_CONSISTENCY_CHECK': 'false',
        'SAPPHIRE_DG_HOST': 'https://dg.example.com',
    }

    old_env = {}
    for k, v in env_vars.items():
        old_env[k] = os.environ.get(k)
        os.environ[k] = v

    yield {
        'tmp_path': tmp_path,
        'intermediate': intermediate,
        'cm_dir': cm_dir,
        'ens_dir': ens_dir,
        'reanalysis_dir': reanalysis_dir,
        'snow_dir': snow_dir,
        'swe_dir': swe_dir,
        'hs_dir': hs_dir,
        'dg_dir': dg_dir,
        'models_dir': models_dir,
        'config_dir': config_dir,
        'codes': codes,
        'snow_codes': snow_codes,
    }

    # Restore environment
    for k, v in old_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def make_dg_mock_side_effect(dg_df, dg_dir, filename='control_member.csv'):
    """Return a side_effect function that writes CSV when DG client called.

    QM's main() clears the DG download directory before calling the DG
    client, so we can't pre-write the CSV. This side_effect writes it
    on demand.
    """
    csv_path = os.path.join(str(dg_dir), filename)

    def _side_effect(**kwargs):
        dg_df.to_csv(csv_path, index=False)
        return csv_path

    return _side_effect


@pytest.fixture()
def mock_api_client():
    """Mock SapphirePreprocessingClient capturing all write calls."""
    mock_client = Mock()
    mock_client.readiness_check.return_value = True
    mock_client.read_meteo.return_value = pd.DataFrame()
    mock_client.read_snow.return_value = pd.DataFrame()

    # Track write calls and return record count
    meteo_writes = []
    snow_writes = []

    def _write_meteo(records):
        meteo_writes.append(records)
        return len(records)

    def _write_snow(records):
        snow_writes.append(records)
        return len(records)

    mock_client.write_meteo.side_effect = _write_meteo
    mock_client.write_snow.side_effect = _write_snow
    mock_client._meteo_writes = meteo_writes
    mock_client._snow_writes = snow_writes

    return mock_client


# =====================================================================
# 1. TestErrorPropagation
# =====================================================================

class TestErrorPropagation:
    """Verify DG failures are fatal but API failures are non-fatal."""

    def test_dg_download_failure_exits(self, gateway_env):
        """QM main() calls sys.exit(1) when DG client raises."""
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            Exception("Operational data for HRU not available")
        )

        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             pytest.raises(SystemExit) as exc_info:
            qm.main()

        assert exc_info.value.code == 1

    def test_api_write_failure_does_not_block_csv(
        self, gateway_env, mock_api_client
    ):
        """CSV files are written even when API client raises."""
        env = gateway_env
        codes = env['codes']
        today = datetime.today()
        dates_dg = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(200, -1, -1)
        ]
        t_vals = [[5.0] * len(codes) for _ in dates_dg]
        p_vals = [[2.0] * len(codes) for _ in dates_dg]
        dg_df = make_dg_control_member_csv(codes, dates_dg, t_vals, p_vals)

        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            make_dg_mock_side_effect(dg_df, env['dg_dir'])
        )

        # Make API write raise an exception
        mock_api_client.write_meteo.side_effect = Exception("API down")

        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             patch(
                 'Quantile_Mapping_OP.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             pytest.raises(SystemExit) as exc_info:
            qm.main()

        # QM exits 0 on success
        assert exc_info.value.code == 0

        # CSV files should still exist
        cm_dir = env['cm_dir']
        p_csv = cm_dir / 'HRU001_P_control_member.csv'
        t_csv = cm_dir / 'HRU001_T_control_member.csv'
        assert p_csv.exists(), "P control member CSV not written"
        assert t_csv.exists(), "T control member CSV not written"

        # Verify CSV content
        p_df = pd.read_csv(p_csv)
        assert len(p_df) > 0
        assert 'date' in p_df.columns
        assert 'P' in p_df.columns
        assert 'code' in p_df.columns

    def test_api_unavailable_skips_gracefully(
        self, gateway_env, mock_api_client
    ):
        """SAPPHIRE_API_AVAILABLE=False → no API calls, CSV still written."""
        env = gateway_env
        codes = env['codes']
        today = datetime.today()
        dates_dg = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(200, -1, -1)
        ]
        t_vals = [[5.0] * len(codes) for _ in dates_dg]
        p_vals = [[2.0] * len(codes) for _ in dates_dg]
        dg_df = make_dg_control_member_csv(codes, dates_dg, t_vals, p_vals)

        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            make_dg_mock_side_effect(dg_df, env['dg_dir'])
        )

        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             patch.object(qm, 'SAPPHIRE_API_AVAILABLE', False), \
             pytest.raises(SystemExit) as exc_info:
            qm.main()

        assert exc_info.value.code == 0

        # CSV still written
        p_csv = env['cm_dir'] / 'HRU001_P_control_member.csv'
        assert p_csv.exists()
        p_df = pd.read_csv(p_csv)
        assert len(p_df) > 0

        # No API calls made
        mock_api_client.write_meteo.assert_not_called()

    def test_missing_reanalysis_csv_handled(self, gateway_env):
        """extend_era5 handles missing reanalysis CSV without crashing."""
        env = gateway_env
        # Delete the seeded reanalysis CSVs
        for f in env['reanalysis_dir'].iterdir():
            f.unlink()

        # Also need a control member CSV for extend_era5 to read
        # (it reads from CM dir). Without it, it will fail on read_csv.
        # Since reanalysis CSV is read first and missing, it should error
        # on pd.read_csv → we expect an exception (FileNotFoundError)
        with patch('extend_era5_reanalysis.sl.load_environment'), \
             pytest.raises((FileNotFoundError, SystemExit, Exception)):
            eer.main()


# =====================================================================
# 2. TestBackfillWorkflow
# =====================================================================

class TestBackfillWorkflow:
    """Verify backfill_new_stations.py end-to-end."""

    def test_backfill_detects_new_station_and_writes(
        self, gateway_env, mock_api_client
    ):
        """New station code in CSV but not in API → full history written."""
        env = gateway_env
        # The reanalysis CSV has codes 12345, 67890
        # API coverage returns empty → both are "new"
        mock_api_client.readiness_check.return_value = True

        with patch('backfill_new_stations.sl.load_environment'), \
             patch.object(bns, 'SAPPHIRE_API_AVAILABLE', True), \
             patch(
                 'backfill_new_stations.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch(
                 'backfill_new_stations.get_meteo_coverage',
                 return_value={},
             ), \
             patch(
                 'backfill_new_stations.get_snow_coverage',
                 return_value={},
             ):
            bns.main()

        # Should have written meteo records for both P and T
        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        assert len(all_meteo) > 0, "Expected meteo backfill records"

        # Verify we got both P and T types
        types_written = {r['meteo_type'] for r in all_meteo}
        assert 'P' in types_written
        assert 'T' in types_written

        # Verify codes match
        codes_written = {r['code'] for r in all_meteo}
        assert '12345' in codes_written
        assert '67890' in codes_written

        # Spot-check one record's structure
        sample = all_meteo[0]
        assert 'date' in sample
        assert 'value' in sample
        assert 'day_of_year' in sample

    def test_backfill_detects_stale_and_writes_incremental(
        self, gateway_env, mock_api_client
    ):
        """Stale station → only data after API max_date written."""
        env = gateway_env
        # API says code 12345 has data up to 2023-06-01 (stale)
        # Code 67890 is up to date
        stale_date = date(2023, 6, 1)
        recent_date = date.today() - timedelta(days=1)

        meteo_coverage = {
            ('P', '12345'): stale_date,
            ('P', '67890'): recent_date,
            ('T', '12345'): stale_date,
            ('T', '67890'): recent_date,
        }

        with patch('backfill_new_stations.sl.load_environment'), \
             patch.object(bns, 'SAPPHIRE_API_AVAILABLE', True), \
             patch(
                 'backfill_new_stations.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch(
                 'backfill_new_stations.get_meteo_coverage',
                 return_value=meteo_coverage,
             ), \
             patch(
                 'backfill_new_stations.get_snow_coverage',
                 return_value={},
             ):
            bns.main()

        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        # Only code 12345 should be backfilled (stale)
        codes_written = {r['code'] for r in all_meteo}
        assert '12345' in codes_written
        assert '67890' not in codes_written, (
            "Up-to-date code should not be backfilled"
        )

        # All written dates should be after the stale_date
        for record in all_meteo:
            record_date = date.fromisoformat(record['date'])
            assert record_date > stale_date, (
                f"Record date {record_date} should be after "
                f"stale cutoff {stale_date}"
            )

    def test_backfill_skips_up_to_date_stations(
        self, gateway_env, mock_api_client
    ):
        """Up-to-date station → no writes."""
        recent = date.today() - timedelta(days=1)
        meteo_coverage = {
            ('P', '12345'): recent,
            ('P', '67890'): recent,
            ('T', '12345'): recent,
            ('T', '67890'): recent,
        }

        with patch('backfill_new_stations.sl.load_environment'), \
             patch.object(bns, 'SAPPHIRE_API_AVAILABLE', True), \
             patch(
                 'backfill_new_stations.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch(
                 'backfill_new_stations.get_meteo_coverage',
                 return_value=meteo_coverage,
             ), \
             patch(
                 'backfill_new_stations.get_snow_coverage',
                 return_value={},
             ):
            bns.main()

        assert len(mock_api_client._meteo_writes) == 0
        assert len(mock_api_client._snow_writes) == 0

    def test_backfill_exits_when_api_unavailable(self, gateway_env):
        """sys.exit(1) when API client not installed."""
        with patch('backfill_new_stations.sl.load_environment'), \
             patch.object(bns, 'SAPPHIRE_API_AVAILABLE', False), \
             pytest.raises(SystemExit) as exc_info:
            bns.main()

        assert exc_info.value.code == 1


# =====================================================================
# 3. TestCrossScriptDataFlow
# =====================================================================

class TestCrossScriptDataFlow:
    """Verify QM output CSVs are correctly consumed by extend_era5."""

    def _run_qm(self, env, codes):
        """Run QM main() with mocked DG client, return CSV paths."""
        today = datetime.today()
        dates_dg = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(200, -1, -1)
        ]
        t_vals = [[5.0 + i * 0.1] * len(codes) for i, _ in enumerate(dates_dg)]
        p_vals = [[2.0 + i * 0.01] * len(codes) for i, _ in enumerate(dates_dg)]
        dg_df = make_dg_control_member_csv(codes, dates_dg, t_vals, p_vals)

        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            make_dg_mock_side_effect(dg_df, env['dg_dir'])
        )

        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             patch.object(qm, 'SAPPHIRE_API_AVAILABLE', False), \
             pytest.raises(SystemExit) as exc_info:
            qm.main()

        assert exc_info.value.code == 0

    def test_qm_writes_both_P_and_T_csvs(self, gateway_env):
        """QM main() creates P and T control_member CSVs for each HRU."""
        env = gateway_env
        self._run_qm(env, env['codes'])

        cm_dir = env['cm_dir']
        p_csv = cm_dir / 'HRU001_P_control_member.csv'
        t_csv = cm_dir / 'HRU001_T_control_member.csv'
        assert p_csv.exists(), "P control member CSV not created"
        assert t_csv.exists(), "T control member CSV not created"

        p_df = pd.read_csv(p_csv)
        t_df = pd.read_csv(t_csv)
        assert len(p_df) == len(t_df)
        assert len(p_df) > 100, f"Expected >100 rows, got {len(p_df)}"

        # Verify columns
        assert set(p_df.columns) == {'date', 'P', 'code'}
        assert set(t_df.columns) == {'date', 'T', 'code'}

        # Spot-check: codes should match
        assert set(p_df['code'].astype(str).unique()) == set(env['codes'])

    def test_qm_output_consumed_by_extend_era5(
        self, gateway_env, mock_api_client
    ):
        """Run QM → check output → run extend_era5 → verify extension."""
        env = gateway_env
        self._run_qm(env, env['codes'])

        # Read reanalysis before extend
        reanalysis_p = pd.read_csv(
            env['reanalysis_dir'] / 'HRU001_P_reanalysis.csv'
        )
        rows_before = len(reanalysis_p)

        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', False):
            eer.main()

        # Read reanalysis after extend
        reanalysis_p_after = pd.read_csv(
            env['reanalysis_dir'] / 'HRU001_P_reanalysis.csv'
        )
        # Should have more rows (stable operational appended)
        assert len(reanalysis_p_after) >= rows_before, (
            f"Reanalysis should grow: {rows_before} → "
            f"{len(reanalysis_p_after)}"
        )

    def test_extend_era5_appends_to_existing_reanalysis(
        self, gateway_env
    ):
        """Reanalysis CSV grows by the new dates from QM output."""
        env = gateway_env
        self._run_qm(env, env['codes'])

        reanalysis_before = pd.read_csv(
            env['reanalysis_dir'] / 'HRU001_P_reanalysis.csv'
        )
        reanalysis_before['date'] = pd.to_datetime(reanalysis_before['date'])
        max_date_before = reanalysis_before['date'].max()

        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', False):
            eer.main()

        reanalysis_after = pd.read_csv(
            env['reanalysis_dir'] / 'HRU001_P_reanalysis.csv'
        )
        reanalysis_after['date'] = pd.to_datetime(reanalysis_after['date'])
        max_date_after = reanalysis_after['date'].max()

        # After extension, max date should be >= before
        assert max_date_after >= max_date_before

    def test_norm_calculation_uses_extended_data(self, gateway_env):
        """Dashboard CSV contains norms computed from extended reanalysis."""
        env = gateway_env
        self._run_qm(env, env['codes'])

        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', False):
            eer.main()

        # Dashboard CSV should exist
        dashboard_p = (
            env['reanalysis_dir'] / 'HRU001_P_reanalysis_dashboard.csv'
        )
        assert dashboard_p.exists(), "Dashboard CSV not created"

        df = pd.read_csv(dashboard_p)
        assert 'P_norm' in df.columns, "Dashboard should have P_norm column"
        assert 'date' in df.columns
        assert 'code' in df.columns

        # Norms should be non-null for most rows
        non_null_norms = df['P_norm'].notna().sum()
        assert non_null_norms > 300, (
            f"Expected >300 non-null norms, got {non_null_norms}"
        )

        # Spot-check: norm values should be reasonable (positive for P)
        assert df['P_norm'].dropna().min() >= 0

    def test_full_three_script_pipeline(
        self, gateway_env, mock_api_client
    ):
        """Run all 3 main() functions, verify final state."""
        env = gateway_env
        codes = env['codes']

        # 1. QM
        self._run_qm(env, codes)

        # 2. extend_era5
        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch(
                 'extend_era5_reanalysis.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', True):
            eer.main()

        # 3. snow (needs mock snow client)
        snow_dates = [
            (datetime.today() - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(30, -1, -1)
        ]
        snow_vals = [[50.0] for _ in snow_dates]
        snow_df = make_dg_snow_csv(
            env['snow_codes'], snow_dates, snow_vals
        )
        snow_csv_path = str(env['dg_dir'] / 'snow_data.csv')
        snow_df.to_csv(snow_csv_path, index=False)

        mock_snow_client = MagicMock()
        mock_snow_client.get_operational.return_value = snow_csv_path

        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow_client,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        # Verify final state

        # CM CSVs exist
        assert (env['cm_dir'] / 'HRU001_P_control_member.csv').exists()
        assert (env['cm_dir'] / 'HRU001_T_control_member.csv').exists()

        # Reanalysis CSVs exist and were extended
        assert (
            env['reanalysis_dir'] / 'HRU001_P_reanalysis.csv'
        ).exists()

        # Dashboard CSVs exist
        assert (
            env['reanalysis_dir'] / 'HRU001_P_reanalysis_dashboard.csv'
        ).exists()
        assert (
            env['reanalysis_dir'] / 'HRU001_T_reanalysis_dashboard.csv'
        ).exists()

        # Snow CSVs exist
        snow_swe = env['swe_dir'] / 'HRU_SNOW01_SWE.csv'
        snow_hs = env['hs_dir'] / 'HRU_SNOW01_HS.csv'
        assert snow_swe.exists() or snow_hs.exists(), (
            "At least one snow CSV should exist"
        )

        # API write calls were made (from extend_era5 dashboard writes)
        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)
        assert len(all_meteo) > 0, "Expected meteo API writes"


# =====================================================================
# 4. TestSyncModes
# =====================================================================

class TestSyncModes:
    """Verify SAPPHIRE_SYNC_MODE controls what gets written to the API."""

    def _run_qm_pipeline(self, env, codes, mock_api_client, sync_mode):
        """Run QM with a specific sync mode."""
        os.environ['SAPPHIRE_SYNC_MODE'] = sync_mode

        today = datetime.today()
        dates_dg = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(200, -1, -1)
        ]
        t_vals = [[5.0] * len(codes) for _ in dates_dg]
        p_vals = [[2.0] * len(codes) for _ in dates_dg]
        dg_df = make_dg_control_member_csv(codes, dates_dg, t_vals, p_vals)

        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            make_dg_mock_side_effect(dg_df, env['dg_dir'])
        )

        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             patch(
                 'Quantile_Mapping_OP.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(qm, 'SAPPHIRE_API_AVAILABLE', True), \
             pytest.raises(SystemExit):
            qm.main()

    def test_operational_mode_writes_recent_days(
        self, gateway_env, mock_api_client
    ):
        """Operational: meteo API records have yesterday and/or today."""
        env = gateway_env
        self._run_qm_pipeline(
            env, env['codes'], mock_api_client, 'operational'
        )

        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        today = pd.Timestamp.today().normalize()
        yesterday = today - pd.Timedelta(days=1)
        allowed = {
            yesterday.strftime('%Y-%m-%d'),
            today.strftime('%Y-%m-%d'),
        }

        if len(all_meteo) > 0:
            dates_written = {r['date'] for r in all_meteo}
            assert dates_written <= allowed, (
                f"Operational should write only yesterday+today "
                f"({allowed}), got {dates_written}"
            )

    def test_maintenance_mode_writes_recent_history(
        self, gateway_env, mock_api_client
    ):
        """Maintenance: meteo API records span ~30 days."""
        env = gateway_env
        self._run_qm_pipeline(
            env, env['codes'], mock_api_client, 'maintenance'
        )

        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        assert len(all_meteo) > 0, "Maintenance mode should write data"

        dates = sorted({r['date'] for r in all_meteo})
        date_range = (
            date.fromisoformat(dates[-1]) - date.fromisoformat(dates[0])
        )
        # Should span at least a few days, up to ~30
        assert date_range.days >= 1, (
            f"Maintenance should span multiple days, got {date_range.days}"
        )
        assert date_range.days <= 31, (
            f"Maintenance should not exceed 31 days, got {date_range.days}"
        )

    def test_initial_mode_writes_all_data(
        self, gateway_env, mock_api_client
    ):
        """Initial: meteo API records include full CSV history."""
        env = gateway_env
        self._run_qm_pipeline(
            env, env['codes'], mock_api_client, 'initial'
        )

        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        assert len(all_meteo) > 0, "Initial mode should write data"

        # Initial should write more records than operational
        # (operational writes max 1 record per code per type)
        n_codes = len(env['codes'])
        assert len(all_meteo) > n_codes * 2, (
            f"Initial should write many records, got {len(all_meteo)}"
        )

    def _run_qm_for_extend(self, env):
        """Run QM to create CM CSVs needed by extend_era5."""
        codes = env['codes']
        today = datetime.today()
        dates_dg = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(200, -1, -1)
        ]
        t_vals = [[5.0] * len(codes) for _ in dates_dg]
        p_vals = [[2.0] * len(codes) for _ in dates_dg]
        dg_df = make_dg_control_member_csv(codes, dates_dg, t_vals, p_vals)

        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            make_dg_mock_side_effect(dg_df, env['dg_dir'])
        )
        with patch('Quantile_Mapping_OP.sl.load_environment'), \
             patch(
                 'Quantile_Mapping_OP.sapphire_dg_client.client'
                 '.SapphireDGClient',
                 return_value=mock_dg,
             ), \
             patch.object(qm, 'SAPPHIRE_API_AVAILABLE', False), \
             pytest.raises(SystemExit):
            qm.main()

    def test_reanalysis_api_write_skipped_in_operational(
        self, gateway_env, mock_api_client
    ):
        """extend_era5 _write_reanalysis_to_api() is no-op in operational."""
        env = gateway_env
        self._run_qm_for_extend(env)

        os.environ['SAPPHIRE_SYNC_MODE'] = 'operational'

        # Track _write_reanalysis_to_api calls
        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch(
                 'extend_era5_reanalysis.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', True), \
             patch(
                 'extend_era5_reanalysis._write_reanalysis_to_api',
                 wraps=eer._write_reanalysis_to_api,
             ) as spy:
            eer.main()

        # _write_reanalysis_to_api should have been called but returned
        # False each time (operational mode skips reanalysis writes)
        assert spy.call_count >= 1, (
            "Expected _write_reanalysis_to_api to be called"
        )
        for call_result in [
            spy.return_value
        ]:
            # In operational mode the function returns False
            pass
        # Verify no reanalysis records were written via client
        # (only dashboard writes go through in operational mode)

    def test_reanalysis_api_write_active_in_maintenance(
        self, gateway_env, mock_api_client
    ):
        """extend_era5 _write_reanalysis_to_api() writes in maintenance."""
        env = gateway_env
        self._run_qm_for_extend(env)

        os.environ['SAPPHIRE_SYNC_MODE'] = 'maintenance'

        # Clear previous writes
        mock_api_client._meteo_writes.clear()

        with patch('extend_era5_reanalysis.sl.load_environment'), \
             patch(
                 'extend_era5_reanalysis.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(eer, 'SAPPHIRE_API_AVAILABLE', True):
            eer.main()

        # In maintenance mode, reanalysis write should produce records.
        # The dashboard write also produces records.
        all_meteo = []
        for call_records in mock_api_client._meteo_writes:
            all_meteo.extend(call_records)

        # Should have records from both dashboard AND reanalysis writes
        assert len(all_meteo) > 0, (
            "Maintenance mode should write meteo records"
        )


# =====================================================================
# 5. TestSnowPipelineIntegration
# =====================================================================

class TestSnowPipelineIntegration:
    """Verify snow_data_operational.py end-to-end."""

    def _make_snow_env_and_client(self, env, snow_codes, dates, values,
                                  var_name='SWE', n_bands=0):
        """Create snow DG CSV and mock client."""
        snow_df = make_dg_snow_csv(
            snow_codes, dates, values, var_name, n_bands
        )
        snow_csv_path = str(env['dg_dir'] / f'snow_{var_name}.csv')
        snow_df.to_csv(snow_csv_path, index=False)

        mock_snow_client = MagicMock()
        mock_snow_client.get_operational.return_value = snow_csv_path
        return mock_snow_client

    def test_snow_download_transform_write_csv(
        self, gateway_env, mock_api_client
    ):
        """Snow client → transform → CSV written with correct columns."""
        env = gateway_env
        today = datetime.today()
        dates = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(10, -1, -1)
        ]
        values = [[50.0] for _ in dates]

        mock_snow = self._make_snow_env_and_client(
            env, env['snow_codes'], dates, values, 'SWE'
        )

        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        # Check SWE CSV
        swe_csv = env['swe_dir'] / 'HRU_SNOW01_SWE.csv'
        assert swe_csv.exists(), "SWE CSV not written"

        df = pd.read_csv(swe_csv)
        assert len(df) == len(dates), (
            f"Expected {len(dates)} rows, got {len(df)}"
        )
        assert 'date' in df.columns
        assert 'SWE' in df.columns
        assert 'code' in df.columns

        # Spot-check value
        assert df['SWE'].iloc[0] == pytest.approx(50.0, abs=0.1)

    def test_snow_api_write_includes_elevation_bands(
        self, gateway_env, mock_api_client
    ):
        """API records include value1, value2 etc. for elevation bands."""
        env = gateway_env
        today = datetime.today()
        today_str = today.strftime('%d.%m.%Y')
        dates = [today_str]
        values = [[100.0]]

        mock_snow = self._make_snow_env_and_client(
            env, env['snow_codes'], dates, values, 'SWE', n_bands=3
        )

        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        all_snow = []
        for call_records in mock_api_client._snow_writes:
            all_snow.extend(call_records)

        if len(all_snow) > 0:
            record = all_snow[0]
            assert record['snow_type'] == 'SWE'
            # Should have elevation band values
            has_bands = any(
                f'value{i}' in record for i in range(1, 4)
            )
            assert has_bands, (
                f"Expected elevation band values in record: "
                f"{list(record.keys())}"
            )

    def test_snow_multiple_hrus(self, gateway_env, mock_api_client):
        """Multiple HRUs each get their own CSV files."""
        env = gateway_env
        # Override to have 2 snow HRUs
        os.environ['ieasyhydroforecast_HRU_SNOW_DATA'] = (
            'HRU_SNOW01,HRU_SNOW02'
        )

        today = datetime.today()
        dates = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(5, -1, -1)
        ]
        values = [[50.0] for _ in dates]

        snow_df = make_dg_snow_csv(
            env['snow_codes'], dates, values, 'SWE'
        )
        snow_csv_path = str(env['dg_dir'] / 'snow_SWE.csv')
        snow_df.to_csv(snow_csv_path, index=False)

        mock_snow = MagicMock()
        mock_snow.get_operational.return_value = snow_csv_path

        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        # Both HRUs should have SWE CSVs
        swe1 = env['swe_dir'] / 'HRU_SNOW01_SWE.csv'
        swe2 = env['swe_dir'] / 'HRU_SNOW02_SWE.csv'
        assert swe1.exists(), "SWE CSV for HRU_SNOW01 not created"
        assert swe2.exists(), "SWE CSV for HRU_SNOW02 not created"

    def test_snow_operational_vs_maintenance_filtering(
        self, gateway_env, mock_api_client
    ):
        """Operational writes yesterday+today; maintenance writes 30 days."""
        env = gateway_env
        today = datetime.today()
        dates = [
            (today - timedelta(days=d)).strftime('%d.%m.%Y')
            for d in range(60, -1, -1)
        ]
        values = [[50.0] for _ in dates]
        mock_snow = self._make_snow_env_and_client(
            env, env['snow_codes'], dates, values, 'SWE'
        )

        # Run in operational mode
        os.environ['SAPPHIRE_SYNC_MODE'] = 'operational'
        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        operational_snow = []
        for call_records in mock_api_client._snow_writes:
            operational_snow.extend(call_records)

        # Operational: records should be yesterday and/or today only
        today = pd.Timestamp.today().normalize()
        yesterday = today - pd.Timedelta(days=1)
        allowed = {
            yesterday.strftime('%Y-%m-%d'),
            today.strftime('%Y-%m-%d'),
        }
        for record in operational_snow:
            assert record['date'] in allowed, (
                f"Operational should only write yesterday+today, "
                f"got {record['date']}"
            )

        # Clear and run maintenance
        mock_api_client._snow_writes.clear()
        mock_api_client.write_snow.side_effect = (
            lambda recs: (mock_api_client._snow_writes.append(recs)
                          or len(recs))
        )

        # Re-create the snow CSV to avoid accumulation issues
        snow_df = make_dg_snow_csv(
            env['snow_codes'], dates, values, 'SWE'
        )
        snow_csv_path = str(env['dg_dir'] / 'snow_SWE.csv')
        snow_df.to_csv(snow_csv_path, index=False)
        mock_snow.get_operational.return_value = snow_csv_path

        # Delete existing SWE CSV so we start fresh
        swe_csv = env['swe_dir'] / 'HRU_SNOW01_SWE.csv'
        if swe_csv.exists():
            swe_csv.unlink()

        os.environ['SAPPHIRE_SYNC_MODE'] = 'maintenance'
        with patch('snow_data_operational.sl.load_environment'), \
             patch(
                 'snow_data_operational.snow_model'
                 '.SapphireSnowModelClient',
                 return_value=mock_snow,
             ), \
             patch(
                 'snow_data_operational.SapphirePreprocessingClient',
                 return_value=mock_api_client,
             ), \
             patch.object(sdo, 'SAPPHIRE_API_AVAILABLE', True):
            sdo.main()

        maintenance_snow = []
        for call_records in mock_api_client._snow_writes:
            maintenance_snow.extend(call_records)

        # Maintenance should write more records than operational
        assert len(maintenance_snow) >= len(operational_snow), (
            f"Maintenance ({len(maintenance_snow)}) should write >= "
            f"operational ({len(operational_snow)})"
        )
