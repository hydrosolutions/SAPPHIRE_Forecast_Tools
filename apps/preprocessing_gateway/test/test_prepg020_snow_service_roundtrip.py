"""PREPG-020 final-stored-state test: real service upsert, not just an
outgoing python dict.

Review finding 3 on PREPG-020's first implementation pass: the
partial-row happy-path test
(test_api_integration.py::TestSnowNormPreservation::test_partial_row_preserves_stored_value_and_writes_incoming_band)
only inspects the dict ``dg_utils.write_snow_to_api()`` builds for
``client.write_snow(records)``. It never proves that dict survives the
preprocessing service's real upsert — which is where an *omitted*
field would acquire pydantic's ``None`` default and null a stored
value (``SnowBase`` declares every optional field with a default of
``None``; the unconditional ``setattr(existing, k, v)`` loop in
``crud.create_snow`` then writes that None). That mechanism is what
PREPG-020 is about, so this module closes the gap by actually routing
the generated payload through ``SnowBulkCreate`` and
``crud.create_snow`` against a real (in-memory SQLite) database,
mirroring the pattern in
sapphire/services/preprocessing/tests/conftest.py.

This module imports from ``sapphire/services/preprocessing/app`` —
read-only, for testing — and does NOT modify anything under
``sapphire/services/**`` (colleague-owned, per CLAUDE.md).

Requires sqlalchemy/pydantic/pydantic-settings, added as apps/
preprocessing_gateway dev dependencies for this test only (version
floors match sapphire/services/preprocessing/requirements.txt).
"""

import os
import pathlib
import sys
from datetime import date
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest


def _find_repo_root() -> pathlib.Path:
    """Locate the repo root from any CWD by finding the service's crud.py."""
    here = pathlib.Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "sapphire" / "services" / "preprocessing" / "app" / "crud.py"
        if candidate.is_file():
            return parent
    raise FileNotFoundError(
        "Could not locate sapphire/services/preprocessing/app/crud.py "
        f"from {here} or any of its parents."
    )


REPO_ROOT = _find_repo_root()
SERVICE_ROOT = REPO_ROOT / "sapphire" / "services" / "preprocessing"

# --- Environment setup (before ANY service `app.*` imports) ---
# Mirrors sapphire/services/preprocessing/tests/conftest.py: app/config.py
# reads these via pydantic-settings at import time, and they are
# required (no defaults), so they must be set first.
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("API_BASE_URL", "http://localhost:8000")
os.environ.setdefault("BATCH_SIZE", "1000")
os.environ.setdefault("CSV_FOLDER", "/tmp")

sys.path.insert(0, str(SERVICE_ROOT))

from app import crud  # noqa: E402
from app.database import Base  # noqa: E402
from app.models import Snow, SnowType  # noqa: E402
from app.schemas import SnowBulkCreate, SnowCreate  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

# preprocessing_gateway's own path, for dg_utils (same pattern as the
# other test modules in this directory).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import dg_utils  # noqa: E402


@pytest.fixture()
def service_db_session():
    """A fresh in-memory SQLite session using the service's real schema.

    A dedicated engine per test (StaticPool, isolated) rather than a
    shared module-level engine, so this test module has no ordering
    dependency on any other test.
    """
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = session_local()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


class TestPartialRowSurvivesRealServiceUpsert:
    """PREPG-020 review finding 3: the partial-row happy path must
    survive the real preprocessing-service upsert, not just look
    correct as an outgoing dict."""

    @patch("dg_utils.SapphirePreprocessingClient")
    def test_partial_row_final_stored_state_after_upsert(
        self, mock_client_class, service_db_session
    ):
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # 1. Seed an existing row via the service's own crud function
        # (the same call the service itself would have made on a
        # prior write) — this is the row a partial-row write must not
        # clobber.
        crud.create_snow(
            service_db_session,
            SnowBulkCreate(
                data=[
                    SnowCreate(
                        snow_type=SnowType.SWE,
                        code="19999",
                        date=date(2024, 1, 1),
                        value=50.0,
                    )
                ]
            ),
        )

        # 2. Run the real dg_utils.write_snow_to_api() gateway logic.
        # The mocked client.read_snow mirrors what the seeded row
        # above holds, exactly as a real API read would report it.
        os.environ["SAPPHIRE_API_ENABLED"] = "true"
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_snow.return_value = pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-01"]),
                    "code": ["19999"],
                    "snow_type": ["SWE"],
                    "value": [50.0],
                }
            )
            captured: list[dict] = []
            mock_client.write_snow.side_effect = lambda records: captured.extend(records) or len(
                records
            )
            mock_client_class.return_value = mock_client

            # Incoming: no main SWE value, but elevation band 1
            # present — the exact partial-row shape PREPG-020 is
            # about.
            data = pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-01"]),
                    "code": ["19999"],
                    "SWE": [np.nan],
                    "SWE_1": [12.5],
                }
            )

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru", mode="initial")
            assert result is True
        finally:
            os.environ.pop("SAPPHIRE_API_ENABLED", None)

        assert len(captured) == 1
        payload = captured[0]
        # Sanity check on the outgoing dict itself, matching the
        # existing (shallower) test in test_api_integration.py.
        assert payload["value"] == 50.0
        assert payload["value1"] == 12.5

        # 3. Route the generated payload through the REAL service
        # schema and the REAL unconditional-upsert crud function —
        # this is where an omitted field would acquire pydantic's
        # None default and null the stored value. We only import and
        # exercise sapphire/services/**, never modify it.
        bulk = SnowBulkCreate(data=[SnowCreate(**payload)])
        crud.create_snow(service_db_session, bulk)

        # 4. Reload from the database (not the in-memory ORM object
        # returned by step 3) and assert final stored state.
        service_db_session.expire_all()
        reloaded = (
            service_db_session.query(Snow)
            .filter_by(snow_type=SnowType.SWE, code="19999", date=date(2024, 1, 1))
            .one()
        )
        assert reloaded.value == 50.0, (
            "Stored main value must survive the upsert even though the "
            "incoming row had no SWE value — it must not be nulled by "
            "an omitted-field default."
        )
        assert reloaded.value1 == 12.5, "The incoming elevation band must be written."

        # Still exactly one row for this key — an upsert, not a
        # duplicate insert.
        assert service_db_session.query(Snow).count() == 1
