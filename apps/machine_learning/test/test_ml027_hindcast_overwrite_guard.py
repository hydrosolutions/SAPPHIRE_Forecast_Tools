"""Tests for the ML-027 overwrite guard in _write_ml_forecast_to_api.

ML-027: crud.create_forecast upserts on (horizon_type, code, model_type,
date, target) and applies every incoming field with setattr. `flag` is not
part of that key, so an incoming hindcast row (flag=3 or 4) for a key that
already holds an operational forecast (flag=0) would replace that row's
quantiles and flag -- and hindcast sets flag=3 on any null quantile, so a
failed hindcast could null out a good operational forecast.

_write_ml_forecast_to_api() now guards against this: before writing, it
reads the currently-stored flag=0 keys for the model/date-span covered by
any incoming flag 3/4 rows, and drops rows that would collide. The read
must fail closed (raise, not silently permit the write) if it cannot be
completed.

Uses a stateful fake SapphirePostprocessingClient (no deployed service, no
model run) that implements the upsert semantics of crud.create_forecast:
replacing the full row on a (model_type, code, date, target) key collision,
without flag being part of that key.

All station codes are fake, in the 19xxx range (repo convention).
"""

import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Mock heavy dependencies before importing from scr (matches the pattern used
# by the other utils_ml_forecast unit tests in this directory).
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

# Add module root and scr to path (matches other tests in this directory)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import scr.utils_ml_forecast as utils_ml_forecast  # noqa: E402
from scr.utils_ml_forecast import SapphireAPIError  # noqa: E402

pytestmark = pytest.mark.skipif(
    not utils_ml_forecast.SAPPHIRE_API_AVAILABLE,
    reason="sapphire-api-client not installed",
)

# Fake station code, 19xxx range per repo convention (no real station codes).
CODE = 19999


# ---------------------------------------------------------------------------
# Stateful fake API client
# ---------------------------------------------------------------------------


class FakeSapphirePostprocessingClient:
    """In-memory fake of SapphirePostprocessingClient.

    Reproduces the two facts about the real service that ML-027 depends on:
    - the unique key is (model_type, code, date, target) -- flag is NOT
      part of it;
    - a write to an existing key replaces the whole row (setattr on every
      field), matching crud.create_forecast's upsert.

    `read_should_fail`, when True, makes read_short_term_forecasts raise,
    simulating a transient read failure the guard must fail closed against.
    """

    def __init__(self, existing_rows=None, ready=True, read_should_fail=False):
        self._rows: dict[tuple, dict] = {}
        for r in existing_rows or []:
            self._rows[self._key(r)] = dict(r)
        self.ready = ready
        self.read_should_fail = read_should_fail
        self.write_calls: list[list[dict]] = []

    @staticmethod
    def _key(record: dict) -> tuple:
        return (record["model_type"], str(record["code"]), record["date"], record["target"])

    def readiness_check(self) -> bool:
        return self.ready

    def read_short_term_forecasts(
        self,
        horizon=None,
        code=None,
        model=None,
        start_date=None,
        end_date=None,
        target=None,
        start_target=None,
        end_target=None,
        skip=0,
        limit=100,
    ) -> pd.DataFrame:
        if self.read_should_fail:
            raise RuntimeError("simulated transient read failure")

        rows = list(self._rows.values())
        if model is not None:
            rows = [r for r in rows if r["model_type"] == model]
        if code is not None:
            rows = [r for r in rows if str(r["code"]) == str(code)]
        if start_date is not None:
            rows = [r for r in rows if r["date"] >= str(start_date)]
        if end_date is not None:
            rows = [r for r in rows if r["date"] <= str(end_date)]

        page = rows[skip : skip + limit]
        return pd.DataFrame(page) if page else pd.DataFrame()

    def write_forecasts(self, records: list[dict]) -> int:
        self.write_calls.append(records)
        for record in records:
            self._rows[self._key(record)] = dict(record)
        return len(records)

    def all_rows(self) -> list[dict]:
        return list(self._rows.values())


def _existing_record(code, model_type, date_, target, flag, q50=100.0):
    return {
        "horizon_type": "day",
        "code": str(int(code)),
        "model_type": model_type,
        "date": date_,
        "target": target,
        "flag": flag,
        "horizon_value": 1,
        "horizon_in_year": 1,
        "q05": q50 - 10,
        "q25": q50 - 5,
        "q75": q50 + 5,
        "q95": q50 + 10,
        "forecasted_discharge": q50,
    }


def _hindcast_frame(code, flag, date_, forecast_date, q50=None):
    """A single-row hindcast frame. q50=None reproduces the failed-hindcast
    case: all quantiles null, which is exactly when hindcast sets flag=3."""
    return pd.DataFrame(
        {
            "code": [code],
            "date": pd.to_datetime([date_]),
            "forecast_date": pd.to_datetime([forecast_date]),
            "flag": [flag],
            "Q5": [q50 - 10 if q50 is not None else None],
            "Q25": [q50 - 5 if q50 is not None else None],
            "Q50": [q50],
            "Q75": [q50 + 5 if q50 is not None else None],
            "Q95": [q50 + 10 if q50 is not None else None],
        }
    )


@pytest.fixture
def install_fake_client(monkeypatch):
    """Patch SapphirePostprocessingClient so _write_ml_forecast_to_api's
    internal `SapphirePostprocessingClient(base_url=..., batch_size=1)`
    call returns the given fake instance, regardless of constructor args."""

    def _install(fake_client):
        monkeypatch.setattr(
            utils_ml_forecast,
            "SapphirePostprocessingClient",
            lambda *args, **kwargs: fake_client,
        )
        monkeypatch.setattr(utils_ml_forecast, "SAPPHIRE_API_AVAILABLE", True)
        monkeypatch.delenv("SAPPHIRE_API_ENABLED", raising=False)
        return fake_client

    return _install


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOverwriteGuardProtectsOperationalRows:
    def test_flag3_hindcast_does_not_overwrite_existing_flag0_row(self, install_fake_client):
        """A hindcast row (flag=3, e.g. a failed hindcast with null
        quantiles) targeting a key that already holds a flag=0 operational
        forecast must be dropped -- the operational row must survive
        unchanged."""
        existing = _existing_record(CODE, "TiDE", "2024-06-05", "2024-06-06", flag=0, q50=42.0)
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[existing])
        )

        hindcast = _hindcast_frame(
            CODE, flag=3, date_="2024-06-06", forecast_date="2024-06-05", q50=None
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        # Benign no-op: the only row was dropped by the guard.
        assert result is False
        # The operational row in the fake store must be untouched.
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 0
        assert stored[0]["forecasted_discharge"] == 42.0
        # Nothing was ever sent to write_forecasts for this call.
        assert fake_client.write_calls == []

    def test_flag4_hindcast_does_not_overwrite_existing_flag0_row(self, install_fake_client):
        """A flag=4 hindcast row (not just flag=3) targeting a key that
        already holds a flag=0 operational forecast must also be dropped.
        Mutating _HINDCAST_FLAGS from (3, 4) to (3,) must fail this test."""
        existing = _existing_record(CODE, "TiDE", "2024-06-05", "2024-06-06", flag=0, q50=42.0)
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[existing])
        )

        hindcast = _hindcast_frame(
            CODE, flag=4, date_="2024-06-06", forecast_date="2024-06-05", q50=None
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert result is False
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 0
        assert stored[0]["forecasted_discharge"] == 42.0
        assert fake_client.write_calls == []

    def test_string_flag_hindcast_is_normalized_and_guarded(self, install_fake_client):
        """A flag stored as the string "3" must be normalized the same way
        the record builder coerces it (`int(row["flag"])`) before the guard
        tests membership in _HINDCAST_FLAGS -- an unnormalized
        `.isin((3, 4))` check would miss it and let it overwrite the
        operational row."""
        existing = _existing_record(CODE, "TiDE", "2024-06-05", "2024-06-06", flag=0, q50=42.0)
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[existing])
        )

        hindcast = _hindcast_frame(
            CODE, flag="3", date_="2024-06-06", forecast_date="2024-06-05", q50=None
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert result is False
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 0
        assert stored[0]["forecasted_discharge"] == 42.0
        assert fake_client.write_calls == []

    def test_hindcast_row_for_new_key_is_written(self, install_fake_client):
        """A hindcast row for a key with no existing row must be written."""
        fake_client = install_fake_client(FakeSapphirePostprocessingClient(existing_rows=[]))

        hindcast = _hindcast_frame(
            CODE, flag=4, date_="2024-06-06", forecast_date="2024-06-05", q50=42.0
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert result is True
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 4
        assert stored[0]["forecasted_discharge"] == 42.0

    def test_hindcast_row_for_flag1_key_is_written(self, install_fake_client):
        """A hindcast row targeting a key whose existing row is flag=1
        (not flag=0) is not protected and must be written -- the guard is
        scoped to flag=0 operational rows only."""
        existing = _existing_record(CODE, "TiDE", "2024-06-05", "2024-06-06", flag=1, q50=0.0)
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[existing])
        )

        hindcast = _hindcast_frame(
            CODE, flag=4, date_="2024-06-06", forecast_date="2024-06-05", q50=55.0
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert result is True
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 4
        assert stored[0]["forecasted_discharge"] == 55.0


class TestOperationalWritesUnaffected:
    def test_no_hindcast_rows_means_no_protection_read(self, install_fake_client, monkeypatch):
        """A frame with no flag 3/4 rows must behave exactly as before the
        guard existed, and must not trigger any protection read."""
        fake_client = install_fake_client(FakeSapphirePostprocessingClient(existing_rows=[]))

        read_calls = []
        original_read = fake_client.read_short_term_forecasts

        def _tracking_read(*args, **kwargs):
            read_calls.append((args, kwargs))
            return original_read(*args, **kwargs)

        monkeypatch.setattr(fake_client, "read_short_term_forecasts", _tracking_read)

        operational = pd.DataFrame(
            {
                "code": [CODE],
                "date": pd.to_datetime(["2024-06-06"]),
                "forecast_date": pd.to_datetime(["2024-06-05"]),
                "flag": [0],
                "Q5": [30.0],
                "Q25": [35.0],
                "Q50": [40.0],
                "Q75": [45.0],
                "Q95": [50.0],
            }
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(operational, "pentad", "TIDE")

        assert result is True
        assert read_calls == [], "Operational-only write must not perform any protection read"
        stored = fake_client.all_rows()
        assert len(stored) == 1
        assert stored[0]["flag"] == 0


class TestOverwriteGuardFailsClosed:
    def test_protection_read_failure_raises_and_writes_nothing(self, install_fake_client):
        """If the protection read cannot be completed, the guard must fail
        closed: raise SapphireAPIError and write nothing at all -- not even
        the non-colliding rows in the same frame."""
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[], read_should_fail=True)
        )

        hindcast = _hindcast_frame(
            CODE, flag=3, date_="2024-06-06", forecast_date="2024-06-05", q50=None
        )

        with pytest.raises(SapphireAPIError):
            utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert fake_client.write_calls == []
        assert fake_client.all_rows() == []

    def test_all_rows_dropped_by_guard_returns_false_not_raise(self, install_fake_client):
        """When every incoming row is protected and dropped, the outcome is
        the existing benign no-op path: return False, do not raise."""
        existing = _existing_record(CODE, "TiDE", "2024-06-05", "2024-06-06", flag=0, q50=42.0)
        fake_client = install_fake_client(
            FakeSapphirePostprocessingClient(existing_rows=[existing])
        )

        hindcast = _hindcast_frame(
            CODE, flag=3, date_="2024-06-06", forecast_date="2024-06-05", q50=None
        )

        result = utils_ml_forecast._write_ml_forecast_to_api(hindcast, "pentad", "TIDE")

        assert result is False
        assert fake_client.write_calls == []
