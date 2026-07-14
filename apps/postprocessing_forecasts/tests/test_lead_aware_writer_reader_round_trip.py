"""M1 P3b: writer -> reader round-trip verification for per-lead horizon_value.

Goal: prove — with an in-memory fake API client standing in for the SAPPHIRE
postprocessing service — that a long-term skill/ensemble row written with a
given per-lead ``horizon_value`` round-trips back at the SAME
``horizon_value`` for every monthly lead (month_0..month_3 ->
horizon_value 0,1,2,3) and for quarter/season leads. This is what makes the
P3 dashboard read (which merges on the lead key) actually find each lead's
row.

Harness note — reuse of the established pattern:
    The existing writer tests (``test_pp038_writer_reader.py``,
    ``test_api_writer_dedup.py``) patch
    ``src.api_writer.SapphirePostprocessingClient`` with a ``unittest.mock``
    object and inspect ``mock_client.write_*.call_args``. The existing reader
    tests patch the private ``_read_*_api`` helper or return a hand-built API
    frame. Neither exercises a genuine write-then-read symmetry.

    Here we keep the SAME injection seam (patch the module-level
    ``SapphirePostprocessingClient`` symbol in BOTH ``src.api_writer`` and
    ``src.data_reader``) but wire it to a small in-memory FAKE that stores
    what the writer sends and returns it, API-shaped, to the reader. This is
    the "fakes over mocks where practical" guidance in CLAUDE.md — the fake
    is the faithful stand-in for the service, so the assertions test the
    apps-side write->read contract, not a mock's call log.

    The fake mirrors the real client method signatures verified against the
    module venv:
      - write_skill_metrics(records) -> int
      - read_skill_metrics(horizon, code, ..., skip, limit) -> DataFrame
      - write_long_forecasts(records) -> int
      - read_long_term_forecasts(horizon_type, horizon_value, code, ...,
            skip, limit) -> DataFrame

    The writer already emits records keyed by ``horizon_in_year`` /
    ``model_type`` / ``horizon_value`` — exactly the API-response column
    names the reader normalizers expect — so the fake stores and replays
    records verbatim.

All tests use the fake station code "19999". These are REGRESSION-LOCK
tests: they pass against HEAD (the round-trip is already correct — PP-038
made the skill upsert key lead-aware, and M1 P1b/P2 carried horizon_value
through the aggregated writers). No production code is mutated. A failure
here would reveal a genuine round-trip gap.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import api_writer, data_reader
from src.api_writer import (
    SAPPHIRE_API_AVAILABLE,
    _write_monthly_ensemble_to_api,
    _write_quarterly_ensemble_to_api,
    _write_seasonal_ensemble_to_api,
    _write_skill_metrics_to_api,
)
from src.data_reader import (
    _normalize_monthly_forecasts,
    _read_long_forecasts_api,
    read_monthly_skill_metrics,
    read_quarterly_skill_metrics,
    read_seasonal_skill_metrics,
    read_skill_metrics,
)

STATION = "19999"

pytestmark = pytest.mark.skipif(
    not SAPPHIRE_API_AVAILABLE, reason="sapphire-api-client not installed"
)


# ===========================================================================
# In-memory fake API client (faithful service stand-in)
# ===========================================================================


class FakeRoundTripClient:
    """In-memory stand-in for SapphirePostprocessingClient.

    Stores whatever the writer sends and replays it, API-shaped, to the
    reader. Faithfully honours the ``horizon``/``horizon_type``/``code``/
    ``horizon_value`` filters and ``skip``/``limit`` pagination the readers
    rely on, so the read loops terminate exactly as they would against the
    real service.
    """

    def __init__(self):
        self.skill_records: list[dict] = []
        self.long_records: list[dict] = []

    # -- health ------------------------------------------------------------
    def readiness_check(self) -> bool:
        return True

    # -- skill metrics -----------------------------------------------------
    def write_skill_metrics(self, records) -> int:
        self.skill_records.extend(records)
        return len(records)

    def read_skill_metrics(
        self,
        horizon=None,
        code=None,
        model=None,
        start_date=None,
        end_date=None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        rows = [
            r
            for r in self.skill_records
            if (horizon is None or r.get("horizon_type") == horizon)
            and (code is None or str(r.get("code")) == str(code))
        ]
        return pd.DataFrame(rows[skip : skip + limit])

    # -- long forecasts (ensembles) ---------------------------------------
    def write_long_forecasts(self, records) -> int:
        self.long_records.extend(records)
        return len(records)

    def read_long_term_forecasts(
        self,
        horizon_type=None,
        horizon_value=None,
        code=None,
        model=None,
        start_date=None,
        end_date=None,
        valid_from=None,
        valid_to=None,
        skip: int = 0,
        limit: int = 100,
    ) -> pd.DataFrame:
        rows = [
            r
            for r in self.long_records
            if (horizon_type is None or r.get("horizon_type") == horizon_type)
            and (code is None or str(r.get("code")) == str(code))
            and (horizon_value is None or int(r.get("horizon_value")) == int(horizon_value))
        ]
        return pd.DataFrame(rows[skip : skip + limit])


@pytest.fixture
def fake_api(monkeypatch):
    """Install a shared FakeRoundTripClient behind both writer and reader.

    Both modules construct ``SapphirePostprocessingClient(base_url=...)``;
    patching the symbol in each module to a factory returning the SAME fake
    instance gives a single shared store for write-then-read.
    """
    monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
    fake = FakeRoundTripClient()

    def _factory(*args, **kwargs):
        return fake

    monkeypatch.setattr(api_writer, "SapphirePostprocessingClient", _factory)
    monkeypatch.setattr(data_reader, "SapphirePostprocessingClient", _factory)
    # The writer caches a singleton; make sure it is fresh for this test.
    api_writer._reset_api_client()
    return fake


# ===========================================================================
# Monthly skill metrics — per-lead round-trip (month_0..month_3)
# ===========================================================================


class TestMonthlySkillPerLeadRoundTrip:
    """Skill rows for the SAME (code, month, model) at leads 0..3 stay
    distinct through write AND read, each retrievable at its own lead."""

    def _four_lead_df(self):
        # Same (code, month_in_year=3, model), four distinct leads with
        # distinct nse so we can prove per-lead symmetry (no cross-lead
        # bleed).
        return pd.DataFrame(
            {
                "code": [STATION] * 4,
                "month_in_year": [3, 3, 3, 3],
                "model_short": ["GBT"] * 4,
                "horizon_value": [0, 1, 2, 3],
                "sdivsigma": [0.30, 0.35, 0.40, 0.45],
                "nse": [0.90, 0.85, 0.80, 0.75],
                "delta": [0.08, 0.09, 0.10, 0.11],
                "accuracy": [0.95, 0.92, 0.90, 0.88],
                "mae": [3.0, 3.5, 4.0, 4.5],
                "n_pairs": [12, 12, 12, 12],
            }
        )

    def test_four_leads_persist_distinct_on_write(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_skill_metrics_to_api(self._four_lead_df(), "month", 2025)

        assert len(fake_api.skill_records) == 4, (
            "Four distinct month leads must NOT be collapsed by the "
            f"upsert-key dedup; got {len(fake_api.skill_records)} rows"
        )
        assert {r["horizon_value"] for r in fake_api.skill_records} == {0, 1, 2, 3}

    def test_four_leads_read_back_at_own_horizon_value(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_skill_metrics_to_api(self._four_lead_df(), "month", 2025)

        result = read_monthly_skill_metrics(codes=[STATION])

        assert len(result) == 4
        assert set(result["horizon_value"]) == {0, 1, 2, 3}
        # Every row is the same month/model — only the lead differs.
        assert set(result["month_in_year"]) == {3}
        assert set(result["model_short"]) == {"GBT"}
        # Per-lead symmetry: the nse written for lead L reads back at lead L.
        expected_nse = {0: 0.90, 1: 0.85, 2: 0.80, 3: 0.75}
        got = dict(zip(result["horizon_value"], result["nse"], strict=True))
        assert got == pytest.approx(expected_nse)

    def test_flag_off_month_skill_leads_still_distinct(self, fake_api, monkeypatch):
        """PP-038 lead stratification is UNCONDITIONAL (not flag-gated).

        Flag OFF must behave identically for the month skill write path:
        four leads remain four rows.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "off")
        _write_skill_metrics_to_api(self._four_lead_df(), "month", 2025)

        result = read_monthly_skill_metrics(codes=[STATION])
        assert set(result["horizon_value"]) == {0, 1, 2, 3}


# ===========================================================================
# Quarter / season skill metrics — per-lead round-trip
# ===========================================================================


class TestAggregatedSkillPerLeadRoundTrip:
    """Quarter and season skill rows carry a per-lead horizon_value that
    round-trips unchanged and is not collapsed across distinct leads.

    Operationally an aggregated horizon usually has a single operational
    lead, but the DB upsert key still stratifies by horizon_value; we write
    TWO distinct leads to prove the key is lead-aware AND that a given lead
    reads back at its own value.
    """

    def _two_lead_df(self, period_col):
        return pd.DataFrame(
            {
                "code": [STATION, STATION],
                period_col: [2, 2],
                "model_short": ["LR_Base", "LR_Base"],
                "horizon_value": [1, 2],
                "sdivsigma": [0.40, 0.42],
                "nse": [0.86, 0.83],
                "delta": [0.10, 0.11],
                "accuracy": [0.91, 0.89],
                "mae": [3.9, 4.1],
                "n_pairs": [11, 11],
            }
        )

    def test_quarter_two_leads_round_trip(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_skill_metrics_to_api(self._two_lead_df("quarter_in_year"), "quarter", 2025)

        assert len(fake_api.skill_records) == 2
        result = read_quarterly_skill_metrics(codes=[STATION])
        assert set(result["horizon_value"]) == {1, 2}
        assert set(result["quarter_in_year"]) == {2}
        got = dict(zip(result["horizon_value"], result["nse"], strict=True))
        assert got == pytest.approx({1: 0.86, 2: 0.83})

    def test_season_two_leads_round_trip(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_skill_metrics_to_api(self._two_lead_df("season_in_year"), "season", 2025)

        assert len(fake_api.skill_records) == 2
        result = read_seasonal_skill_metrics(codes=[STATION])
        assert set(result["horizon_value"]) == {1, 2}
        assert set(result["season_in_year"]) == {2}
        got = dict(zip(result["horizon_value"], result["nse"], strict=True))
        assert got == pytest.approx({1: 0.86, 2: 0.83})

    def test_quarter_single_operational_lead_round_trips(self, fake_api, monkeypatch):
        """The common operational case: exactly one aggregated lead written.

        A single quarter lead (horizon_value=1) must survive write and read
        at the same value — this is the row the dashboard merge looks up.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        df = pd.DataFrame(
            {
                "code": [STATION],
                "quarter_in_year": [2],
                "model_short": ["LR_Base"],
                "horizon_value": [1],
                "sdivsigma": [0.40],
                "nse": [0.86],
                "delta": [0.10],
                "accuracy": [0.91],
                "mae": [3.9],
                "n_pairs": [11],
            }
        )
        _write_skill_metrics_to_api(df, "quarter", 2025)

        result = read_quarterly_skill_metrics(codes=[STATION])
        assert len(result) == 1
        assert int(result["horizon_value"].iloc[0]) == 1


# ===========================================================================
# Monthly ensemble (EM / Naive Mean / Skilled Mean) — per-lead round-trip
# ===========================================================================


class TestMonthlyEnsemblePerLeadRoundTrip:
    """Ensemble long_forecast rows carry per-lead horizon_value through the
    write and the long-forecast read path used to merge them back."""

    def _ensemble_df(self):
        # Four leads for the same (code, target year, target month); each
        # lead has a distinct q50 so we can prove per-lead symmetry.
        return pd.DataFrame(
            {
                "code": [STATION] * 4,
                "year": [2025] * 4,
                "month": [3] * 4,
                "model_short": ["EM", "EM", "EM", "EM"],
                "horizon_value": [0, 1, 2, 3],
                "forecasted_discharge": [120.0, 121.0, 122.0, 123.0],
                "q50": [120.0, 121.0, 122.0, 123.0],
                "valid_from": ["2025-03-01"] * 4,
                "valid_to": ["2025-03-31"] * 4,
                "composition": ["GBT, LR_Base"] * 4,
            }
        )

    def test_four_ensemble_leads_persist_distinct(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_monthly_ensemble_to_api(self._ensemble_df())

        assert len(fake_api.long_records) == 4
        assert {r["horizon_value"] for r in fake_api.long_records} == {0, 1, 2, 3}

    def test_four_ensemble_leads_read_back_at_own_value(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_monthly_ensemble_to_api(self._ensemble_df())

        raw = _read_long_forecasts_api([STATION], 2025, 2025)
        result = _normalize_monthly_forecasts(raw)

        assert set(result["horizon_value"]) == {0, 1, 2, 3}
        assert set(result["model_short"]) == {"EM"}
        # Per-lead symmetry: q50 written for lead L reads back at lead L.
        got = dict(zip(result["horizon_value"], result["q50"], strict=True))
        assert got == pytest.approx({0: 120.0, 1: 121.0, 2: 122.0, 3: 123.0})

    def test_read_can_filter_a_single_lead(self, fake_api, monkeypatch):
        """The reader can request one lead and get exactly that lead back."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_monthly_ensemble_to_api(self._ensemble_df())

        raw = _read_long_forecasts_api([STATION], 2025, 2025, horizon_value=2)
        result = _normalize_monthly_forecasts(raw)

        assert set(result["horizon_value"]) == {2}
        assert result["q50"].iloc[0] == pytest.approx(122.0)


# ===========================================================================
# Quarter ensemble — flag ON per-lead vs flag OFF legacy config lead
# ===========================================================================


class TestQuarterEnsembleFlagBehaviour:
    """The quarter ensemble writer is the one flag-gated horizon_value seam:
    flag ON uses the row's per-lead horizon_value; flag OFF uses the single
    deployment-configured quarter lead (quarter_horizon_value())."""

    def _quarter_df(self, horizon_value):
        return pd.DataFrame(
            {
                "code": [STATION],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["EM"],
                "horizon_value": [horizon_value],
                "forecasted_discharge": [200.0],
                "q50": [200.0],
                "valid_from": ["2025-04-01"],
                "valid_to": ["2025-06-30"],
                "date": ["2025-03-01"],
                "composition": ["GBT, LR_Base"],
            }
        )

    def test_flag_on_uses_row_horizon_value(self, fake_api, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        _write_quarterly_ensemble_to_api(self._quarter_df(3))

        assert len(fake_api.long_records) == 1
        assert int(fake_api.long_records[0]["horizon_value"]) == 3

        raw = _read_long_forecasts_api([STATION], 2025, 2025, horizon_type="quarter")
        assert int(raw["horizon_value"].iloc[0]) == 3

    def test_flag_off_uses_configured_quarter_lead(self, fake_api, monkeypatch):
        """Flag OFF: legacy behaviour — the writer ignores the row's
        horizon_value and stamps the single configured quarter lead."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "off")
        # Pin the configured lead so the assertion does not depend on
        # deployment config; prove the row's 3 is NOT used.
        monkeypatch.setattr(api_writer, "quarter_horizon_value", lambda: 1)

        _write_quarterly_ensemble_to_api(self._quarter_df(3))

        assert len(fake_api.long_records) == 1
        assert int(fake_api.long_records[0]["horizon_value"]) == 1, (
            "Flag OFF must use quarter_horizon_value() (1), not the row's "
            "per-lead horizon_value (3)"
        )


# ===========================================================================
# Non-month sentinel guard (flag ON and OFF) — no cross-horizon NULL tuples
# ===========================================================================


class TestNonMonthSentinelGuard:
    """A NON-month horizon with NO horizon_value column must normalise to the
    sentinel 0 on write — never NULL — and distinct periods must stay
    distinct (the cross-horizon NULL-tuple upsert hazard must not trigger)."""

    def _pentad_df_no_horizon_value(self):
        return pd.DataFrame(
            {
                "code": [STATION, STATION],
                "pentad_in_year": [5, 6],  # two distinct periods
                "model_short": ["LR", "LR"],
                # deliberately no horizon_value column
                "sdivsigma": [0.45, 0.46],
                "nse": [0.82, 0.81],
                "delta": [0.11, 0.12],
                "accuracy": [0.89, 0.88],
                "mae": [4.8, 4.9],
                "n_pairs": [14, 14],
            }
        )

    @pytest.mark.parametrize("flag", ["true", "off"])
    def test_pentad_sentinel_zero_and_distinct_periods(self, fake_api, monkeypatch, flag):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
        _write_skill_metrics_to_api(self._pentad_df_no_horizon_value(), "pentad", 2025)

        # Both periods persist; neither carries a NULL horizon_value.
        assert len(fake_api.skill_records) == 2
        for rec in fake_api.skill_records:
            assert rec["horizon_value"] == 0
            assert rec["horizon_value"] is not None

        # Round-trip read: sentinel preserved, both distinct pentads present.
        result = read_skill_metrics("pentad", codes=[STATION])
        assert set(result["pentad_in_year"]) == {5, 6}
        assert set(result["horizon_value"]) == {0}


# ===========================================================================
# Seasonal ensemble writer smoke — horizon_value round-trips
# ===========================================================================


def test_seasonal_ensemble_horizon_value_round_trip(fake_api, monkeypatch):
    """Seasonal ensemble rows carry season_in_year as horizon_value through
    the write and read paths."""
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
    df = pd.DataFrame(
        {
            "code": [STATION],
            "season_year": [2025],
            "season_in_year": [1],
            "model_short": ["EM"],
            "forecasted_discharge": [300.0],
            "q50": [300.0],
            "composition": ["GBT, LR_Base"],
        }
    )
    _write_seasonal_ensemble_to_api(df)

    assert len(fake_api.long_records) == 1
    written_hv = int(fake_api.long_records[0]["horizon_value"])

    raw = _read_long_forecasts_api([STATION], 2025, 2026, horizon_type="season")
    assert int(raw["horizon_value"].iloc[0]) == written_hv
