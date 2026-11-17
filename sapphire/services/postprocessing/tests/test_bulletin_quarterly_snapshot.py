"""
Tests for the quarterly_snapshot field on bulletin rows (PP-075 Q1).

Covers:
- POST → GET round-trip with actual DB commit + separate read
- Omission preserves existing snapshot
- Explicit null clears snapshot
- Object atomically replaces snapshot
- Legacy rows (no snapshot) return null
- Date precision, zero values, null bounds
- Validation rejections: nonfinite, inverted bounds, one-sided bounds,
  bad quarter dates, mid-month dates, non-month horizon with snapshot,
  missing required fields
- Primary fields unaffected by snapshot logic
- Migration upgrade/downgrade via actual revision functions through Operations.context
"""

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.crud import create_bulletin, get_bulletin
from app.models import Bulletin
from app.schemas import (
    BulletinBulkCreate,
    BulletinCreate,
    QuarterlySnapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _month_bulletin(**overrides) -> dict:
    """Minimal valid monthly bulletin payload (synthetic station 19999)."""
    base = {
        "horizon_type": "month",
        "year": 2026,
        "horizon_value": 1,
        "code": "19999",
        "model_type": "LR",
        "forecasted_discharge": 50.0,
        "fc_lower": 40.0,
        "fc_upper": 60.0,
    }
    base.update(overrides)
    return base


def _snapshot(**overrides) -> dict:
    """Minimal valid quarterly_snapshot payload."""
    base = {
        "model_type": "LR",
        "valid_from": "2026-04-01",
        "valid_to": "2026-06-30",
        "forecasted_discharge": 100.0,
        "fc_lower": 80.0,
        "fc_upper": 120.0,
    }
    base.update(overrides)
    return base


def _post_bulletin(client, bulletin_dict: dict):
    return client.post("/bulletin/", json={"data": [bulletin_dict]})


def _get_bulletins(client, **params):
    return client.get("/bulletin/", params=params)


# ---------------------------------------------------------------------------
# Round-trip: POST then separate GET confirms DB persistence
# ---------------------------------------------------------------------------

class TestQuarterlySnapshotRoundTrip:
    def test_post_with_snapshot_persists_and_get_returns_it(self, client):
        payload = _month_bulletin(quarterly_snapshot=_snapshot())
        post_resp = _post_bulletin(client, payload)
        assert post_resp.status_code == 201

        get_resp = _get_bulletins(client, horizon="month", year=2026, horizon_value=1)
        assert get_resp.status_code == 200
        rows = get_resp.json()
        assert len(rows) == 1
        qs = rows[0]["quarterly_snapshot"]
        assert qs is not None
        assert qs["model_type"] == "LR"
        assert qs["valid_from"] == "2026-04-01"
        assert qs["valid_to"] == "2026-06-30"
        assert qs["forecasted_discharge"] == 100.0
        assert qs["fc_lower"] == 80.0
        assert qs["fc_upper"] == 120.0

    def test_post_without_snapshot_returns_null(self, client):
        post_resp = _post_bulletin(client, _month_bulletin())
        assert post_resp.status_code == 201

        get_resp = _get_bulletins(client, horizon="month", year=2026, horizon_value=1)
        assert get_resp.json()[0]["quarterly_snapshot"] is None

    def test_legacy_row_snapshot_null(self, db_session):
        """Rows inserted directly (bypassing schema) have null snapshot."""
        row = Bulletin(
            horizon_type="month",
            year=2025,
            horizon_value=3,
            code="19999",
            model_type="LR",
            forecasted_discharge=42.0,
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        assert row.quarterly_snapshot is None

    def test_snapshot_exact_json_round_trip(self, client):
        """Dates survive DB round-trip as ISO strings; enums as values."""
        payload = _month_bulletin(
            quarterly_snapshot=_snapshot(
                model_type="GBT",
                valid_from="2026-01-01",
                valid_to="2026-03-31",
                forecasted_discharge=0.0,
                fc_lower=0.0,
                fc_upper=0.0,
            )
        )
        _post_bulletin(client, payload)
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        qs = rows[0]["quarterly_snapshot"]
        assert qs["model_type"] == "GBT"
        assert qs["valid_from"] == "2026-01-01"
        assert qs["valid_to"] == "2026-03-31"
        assert qs["forecasted_discharge"] == 0.0
        assert qs["fc_lower"] == 0.0
        assert qs["fc_upper"] == 0.0

    def test_snapshot_nonzero_decimal_precision_round_trip(self, client):
        """Non-zero finite float values survive JSON → JSON-column → JSON unchanged."""
        payload = _month_bulletin(
            quarterly_snapshot=_snapshot(
                forecasted_discharge=123.5,
                fc_lower=100.25,
                fc_upper=150.75,
            )
        )
        _post_bulletin(client, payload)
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        qs = rows[0]["quarterly_snapshot"]
        # Values chosen as exact IEEE-754 binary fractions so == holds without tolerance.
        assert qs["forecasted_discharge"] == 123.5
        assert qs["fc_lower"] == 100.25
        assert qs["fc_upper"] == 150.75

    def test_snapshot_with_null_bounds_round_trip(self, client):
        """Both bounds null: persisted and returned as null."""
        payload = _month_bulletin(
            quarterly_snapshot=_snapshot(
                forecasted_discharge=None, fc_lower=None, fc_upper=None
            )
        )
        _post_bulletin(client, payload)
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        qs = rows[0]["quarterly_snapshot"]
        assert qs["fc_lower"] is None
        assert qs["fc_upper"] is None
        assert qs["forecasted_discharge"] is None


# ---------------------------------------------------------------------------
# Omission / null / replacement semantics
# ---------------------------------------------------------------------------

class TestSnapshotUpdateSemantics:
    def test_omitted_snapshot_on_update_preserves_stored_value(self, client):
        """Updating a row without sending quarterly_snapshot keeps existing snapshot."""
        _post_bulletin(client, _month_bulletin(quarterly_snapshot=_snapshot()))
        # Second POST omits quarterly_snapshot entirely
        _post_bulletin(client, _month_bulletin(forecasted_discharge=99.0))
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert rows[0]["quarterly_snapshot"] is not None
        assert rows[0]["quarterly_snapshot"]["forecasted_discharge"] == 100.0
        assert rows[0]["forecasted_discharge"] == 99.0

    def test_explicit_null_clears_snapshot(self, client):
        """Sending quarterly_snapshot: null replaces the stored snapshot with null."""
        _post_bulletin(client, _month_bulletin(quarterly_snapshot=_snapshot()))
        _post_bulletin(client, _month_bulletin(quarterly_snapshot=None))
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert rows[0]["quarterly_snapshot"] is None

    def test_explicit_object_replaces_snapshot(self, client):
        """Sending a new snapshot object atomically replaces the stored one."""
        _post_bulletin(client, _month_bulletin(quarterly_snapshot=_snapshot(forecasted_discharge=100.0)))
        _post_bulletin(
            client,
            _month_bulletin(quarterly_snapshot=_snapshot(forecasted_discharge=200.0)),
        )
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert rows[0]["quarterly_snapshot"]["forecasted_discharge"] == 200.0

    def test_omission_on_new_row_stores_null(self, client):
        """Creating a row without quarterly_snapshot stores null, not an error."""
        _post_bulletin(client, _month_bulletin())
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert rows[0]["quarterly_snapshot"] is None

    def test_primary_fields_unaffected_by_snapshot_omission(self, client):
        """primary fields update correctly even when quarterly_snapshot is omitted."""
        _post_bulletin(client, _month_bulletin(forecasted_discharge=50.0))
        _post_bulletin(client, _month_bulletin(forecasted_discharge=77.7))
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert rows[0]["forecasted_discharge"] == pytest.approx(77.7)

    def test_primary_optional_fields_reset_to_null_snapshot_preserved(self, client):
        """Omitting optional primary fields resets them to null; snapshot survives intact.

        Pins the required behaviour: a blanket exclude_unset=True on the upsert
        would skip the null-reset and would fail this test.
        """
        # Create: nonnull optional primary fields (fc_lower, fc_upper) + snapshot
        _post_bulletin(client, _month_bulletin(quarterly_snapshot=_snapshot()))

        # Upsert omitting fc_lower, fc_upper, AND quarterly_snapshot
        update_payload = {
            "horizon_type": "month",
            "year": 2026,
            "horizon_value": 1,
            "code": "19999",
            "model_type": "LR",
            "forecasted_discharge": 55.0,
        }
        _post_bulletin(client, update_payload)

        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        assert len(rows) == 1
        row = rows[0]
        # Optional primary fields omitted → reset to null
        assert row["fc_lower"] is None
        assert row["fc_upper"] is None
        # Snapshot omitted → preserved unchanged, all fields intact
        assert row["quarterly_snapshot"] == _snapshot()


# ---------------------------------------------------------------------------
# Validation rejections
# ---------------------------------------------------------------------------

class TestSnapshotValidationRejections:
    def _assert_422(self, client, payload):
        resp = _post_bulletin(client, payload)
        assert resp.status_code == 422, f"Expected 422, got {resp.status_code}: {resp.text}"

    def test_reject_nonfinite_forecasted_discharge(self, client):
        # Send "inf" as a JSON string; Pydantic lax-mode coerces to float("inf"),
        # then _reject_nonfinite raises → 422.  Python float("inf") cannot be
        # serialized by httpx's json= path (stdlib json.dumps rejects it).
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(forecasted_discharge="inf")
        ))

    def test_reject_nonfinite_fc_lower(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(fc_lower="nan", fc_upper=120.0)
        ))

    def test_reject_nonfinite_fc_upper(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(fc_lower=80.0, fc_upper="-inf")
        ))

    def test_reject_inverted_bounds(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(fc_lower=120.0, fc_upper=80.0)
        ))

    def test_reject_one_sided_bounds_lower_only(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(fc_lower=80.0, fc_upper=None)
        ))

    def test_reject_one_sided_bounds_upper_only(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(fc_lower=None, fc_upper=120.0)
        ))

    def test_reject_valid_from_mid_month(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(valid_from="2026-04-15", valid_to="2026-06-30")
        ))

    def test_reject_valid_to_mid_month(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(valid_from="2026-04-01", valid_to="2026-05-31")
        ))

    def test_reject_mismatched_quarter_dates(self, client):
        """valid_from and valid_to in different quarters."""
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(valid_from="2026-01-01", valid_to="2026-06-30")
        ))

    def test_reject_cross_year_quarter_dates(self, client):
        """valid_from and valid_to in different years."""
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(valid_from="2025-10-01", valid_to="2026-12-31")
        ))

    def test_reject_snapshot_on_nonmonth_horizon(self, client):
        """quarterly_snapshot is not valid on a pentad bulletin row."""
        payload = _month_bulletin(horizon_type="pentad", quarterly_snapshot=_snapshot())
        self._assert_422(client, payload)

    def test_reject_snapshot_on_quarter_horizon(self, client):
        payload = _month_bulletin(horizon_type="quarter", quarterly_snapshot=_snapshot())
        self._assert_422(client, payload)

    # All six snapshot fields are required (even though the three numeric ones are
    # nullable). Omitting any one of them must yield 422. For the numeric fields
    # the base snapshot uses explicit nulls for the other two numerics so the
    # omission is the only invalid aspect — this would escape if any numeric field
    # gained a default of None.
    _NULL_NUMERICS = {
        "forecasted_discharge": None,
        "fc_lower": None,
        "fc_upper": None,
    }

    @pytest.mark.parametrize("omit_field,numeric_override", [
        ("model_type", {}),
        ("valid_from", {}),
        ("valid_to", {}),
        ("forecasted_discharge", _NULL_NUMERICS),
        ("fc_lower", _NULL_NUMERICS),
        ("fc_upper", _NULL_NUMERICS),
    ])
    def test_reject_missing_required_field(self, client, omit_field, numeric_override):
        snap = {**_snapshot(), **numeric_override}
        del snap[omit_field]
        self._assert_422(client, _month_bulletin(quarterly_snapshot=snap))

    def test_reject_invalid_model_type(self, client):
        self._assert_422(client, _month_bulletin(
            quarterly_snapshot=_snapshot(model_type="NONEXISTENT_MODEL")
        ))


# ---------------------------------------------------------------------------
# Direct schema-level nonfinite rejection (bypasses HTTP transport)
# ---------------------------------------------------------------------------

class TestQuarterlySnapshotDirectValidation:
    """Unit tests for _reject_nonfinite using actual Python float specials.

    These tests instantiate QuarterlySnapshot directly, so they don't depend
    on the JSON serialization layer and exercise the validator with the same
    float("inf"/"nan"/"-inf") values a caller could produce programmatically.
    """

    _VALID_BASE = {
        "model_type": "LR",
        "valid_from": "2026-04-01",
        "valid_to": "2026-06-30",
        "fc_lower": 80.0,
        "fc_upper": 120.0,
    }

    def _make(self, **overrides):
        return {**self._VALID_BASE, **overrides}

    def test_direct_reject_inf_forecasted_discharge(self):
        with pytest.raises(ValidationError, match="finite"):
            QuarterlySnapshot(**self._make(forecasted_discharge=float("inf")))

    def test_direct_reject_nan_fc_lower(self):
        with pytest.raises(ValidationError, match="finite"):
            QuarterlySnapshot(**self._make(forecasted_discharge=100.0, fc_lower=float("nan")))

    def test_direct_reject_neg_inf_fc_upper(self):
        with pytest.raises(ValidationError, match="finite"):
            QuarterlySnapshot(**self._make(forecasted_discharge=100.0, fc_upper=float("-inf")))

    def test_direct_accept_finite_values(self):
        snap = QuarterlySnapshot(**self._make(forecasted_discharge=100.0))
        assert snap.forecasted_discharge == 100.0

    def test_direct_accept_null_bounds(self):
        snap = QuarterlySnapshot(**self._make(forecasted_discharge=None, fc_lower=None, fc_upper=None))
        assert snap.fc_lower is None
        assert snap.fc_upper is None


# ---------------------------------------------------------------------------
# Quarter date boundary coverage
# ---------------------------------------------------------------------------

class TestSnapshotQuarterDates:
    @pytest.mark.parametrize("valid_from,valid_to", [
        ("2026-01-01", "2026-03-31"),  # Q1
        ("2026-04-01", "2026-06-30"),  # Q2
        ("2026-07-01", "2026-09-30"),  # Q3
        ("2026-10-01", "2026-12-31"),  # Q4
    ])
    def test_all_four_quarters_accepted(self, client, valid_from, valid_to):
        payload = _month_bulletin(
            horizon_value=1,
            quarterly_snapshot=_snapshot(valid_from=valid_from, valid_to=valid_to),
        )
        resp = _post_bulletin(client, payload)
        assert resp.status_code == 201

    def test_dates_stored_as_iso_strings(self, client):
        """Dates survive the JSON column as ISO strings, not datetime objects."""
        payload = _month_bulletin(
            quarterly_snapshot=_snapshot(valid_from="2026-07-01", valid_to="2026-09-30")
        )
        _post_bulletin(client, payload)
        rows = _get_bulletins(client, horizon="month", year=2026, horizon_value=1).json()
        qs = rows[0]["quarterly_snapshot"]
        assert qs["valid_from"] == "2026-07-01"
        assert qs["valid_to"] == "2026-09-30"


# ---------------------------------------------------------------------------
# Migration: upgrade / downgrade via actual revision functions on SQLite
# ---------------------------------------------------------------------------

class TestMigrationUpgradeDowngrade:
    """
    Verify the actual migration revision adds and removes the column correctly.

    Loads the migration file via importlib and invokes its upgrade() and
    downgrade() functions through Operations.context() so the test fails if
    those functions become no-ops or the file is removed. A fresh SQLite
    engine is used to avoid contaminating the test suite's shared in-memory DB.
    SQLite supports DROP COLUMN directly (≥3.35.0); no batch_alter_table needed.
    """

    def test_upgrade_adds_column_and_downgrade_removes_it(self):
        import importlib.util
        from pathlib import Path

        from alembic.operations import Operations
        from alembic.runtime.migration import MigrationContext

        # Load the actual migration module without adding it to sys.modules.
        migration_path = (
            Path(__file__).parent.parent
            / "alembic" / "versions"
            / "c3d4e5f6a7b8_add_quarterly_snapshot_to_bulletins.py"
        )
        spec = importlib.util.spec_from_file_location("_migration_c3d4e5f6a7b8", migration_path)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)

        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

        # Build the pre-migration table and seed a legacy row.
        with engine.begin() as conn:
            conn.execute(sa.text(
                """
                CREATE TABLE bulletins (
                    id INTEGER PRIMARY KEY,
                    horizon_type TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    horizon_value INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    model_type TEXT NOT NULL,
                    forecasted_discharge REAL
                )
                """
            ))
            conn.execute(sa.text(
                "INSERT INTO bulletins VALUES (1, 'month', 2025, 3, '19999', 'LR', 42.0)"
            ))

        # --- upgrade: invoke actual migration.upgrade() ---
        with engine.begin() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.upgrade()

        insp = sa.inspect(engine)
        col_names = [c["name"] for c in insp.get_columns("bulletins")]
        assert "quarterly_snapshot" in col_names

        # Legacy row must have null in the new column.
        with engine.connect() as conn:
            row = conn.execute(
                sa.text("SELECT quarterly_snapshot FROM bulletins WHERE id = 1")
            ).fetchone()
            assert row[0] is None

        # --- downgrade: invoke actual migration.downgrade() ---
        with engine.begin() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                migration.downgrade()

        insp = sa.inspect(engine)
        col_names = [c["name"] for c in insp.get_columns("bulletins")]
        assert "quarterly_snapshot" not in col_names

        # Seeded row's primary values must survive both upgrade and downgrade.
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT id, horizon_type, year, horizon_value, code,"
                    " model_type, forecasted_discharge FROM bulletins WHERE id = 1"
                )
            ).fetchone()
            assert tuple(row) == (1, "month", 2025, 3, "19999", "LR", 42.0)
