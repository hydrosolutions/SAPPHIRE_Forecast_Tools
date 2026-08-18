"""Shared fixtures for preprocessing_gateway tests."""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import forecast_library as fl


@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library API client singletons between tests.

    Without this, a mock injected by one test leaks into subsequent tests
    because the singleton caches the first client instance it creates.
    """
    fl._reset_api_clients()
    yield
    fl._reset_api_clients()


@pytest.fixture
def ensemble_csv_factory(tmp_path):
    """Factory to create DG-format ensemble CSVs.

    Moved here from test_ensemble_transforms.py (PREPG-010) so it can be
    shared with other test modules that need real ensemble P/T CSVs on
    disk (e.g. main()-level transport-retry tests).
    """

    def _make(hru_code, ensemble_member, variable, dates, values, band_name="band_1000"):
        value_type = "P" if variable == "tp" else "T"
        filename = f"prefix_EM{ensemble_member:03d}_HRU{hru_code}_{variable}.csv"
        path = tmp_path / filename
        # Build DG format: 4 header rows + data rows
        header_rows = [
            [value_type, value_type],
            ["unit", "mm" if variable == "tp" else "K"],
            ["h", "h"],
            ["h", "h"],
        ]
        data_rows = [[d, str(v)] for d, v in zip(dates, values, strict=False)]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(all_rows, columns=["Unnamed: 0", band_name])
        df.to_csv(path, index=False)
        return str(path)

    return _make
