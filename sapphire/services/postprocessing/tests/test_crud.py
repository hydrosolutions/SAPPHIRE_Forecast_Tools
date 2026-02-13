"""
CRUD tests for the postprocessing service.

Tests the SQLAlchemy CRUD functions directly (no HTTP layer) using
SQLite in-memory via the _fallback_upsert path.
"""

from datetime import date

import pytest

from app import crud
from app.models import Forecast, LongForecast, LRForecast, SkillMetric
from app.schemas import (
    ForecastBulkCreate,
    LongForecastBulkCreate,
    LRForecastBulkCreate,
    SkillMetricBulkCreate,
)
from factories import (
    make_forecast,
    make_long_forecast,
    make_lr_forecast,
    make_skill_metric,
)


# -------------------------------------------------------------------
# Forecast CRUD
# -------------------------------------------------------------------

class TestForecastCRUD:
    """Tests for create_forecast / get_forecast."""

    def test_create_single_forecast(self, db_session):
        item = make_forecast()
        bulk = ForecastBulkCreate(data=[item])
        results = crud.create_forecast(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.model_type == "LR"
        assert r.horizon_type == "pentad"
        assert r.forecasted_discharge == 100.0
        assert r.q05 == 80.0
        assert r.target == date(2024, 6, 20)

    def test_create_bulk_forecasts(self, db_session):
        items = [
            make_forecast(code="15013"),
            make_forecast(code="15014"),
            make_forecast(code="15015"),
        ]
        bulk = ForecastBulkCreate(data=items)
        results = crud.create_forecast(db_session, bulk)

        assert len(results) == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_upsert_updates_existing(self, db_session):
        """Insert then re-insert with same unique keys but new values."""
        item1 = make_forecast(forecasted_discharge=100.0)
        crud.create_forecast(db_session, ForecastBulkCreate(data=[item1]))

        item2 = make_forecast(forecasted_discharge=999.0)
        results = crud.create_forecast(
            db_session, ForecastBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].forecasted_discharge == 999.0
        # Should be exactly 1 row in the table, not 2
        total = db_session.query(Forecast).count()
        assert total == 1

    def test_filter_by_code(self, db_session):
        items = [
            make_forecast(code="15013"),
            make_forecast(code="15014"),
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        results = crud.get_forecast(db_session, code="15013")
        assert len(results) == 1
        assert results[0].code == "15013"

    def test_filter_by_date_range(self, db_session):
        items = [
            make_forecast(code="15013", date=date(2024, 6, 10)),
            make_forecast(code="15014", date=date(2024, 6, 15)),
            make_forecast(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        results = crud.get_forecast(
            db_session, start_date="2024-06-12", end_date="2024-06-18"
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_empty_results(self, db_session):
        results = crud.get_forecast(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# LongForecast CRUD
# -------------------------------------------------------------------

class TestLongForecastCRUD:
    """Tests for create_long_forecast / get_long_forecast."""

    def test_create(self, db_session):
        item = make_long_forecast()
        bulk = LongForecastBulkCreate(data=[item])
        results = crud.create_long_forecast(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.model_type == "GBT"
        assert r.horizon_type == "month"
        assert r.q == 123.45
        assert r.valid_from == date(2024, 7, 1)
        assert r.valid_to == date(2024, 7, 31)

    def test_upsert(self, db_session):
        item1 = make_long_forecast(q=100.0)
        crud.create_long_forecast(
            db_session, LongForecastBulkCreate(data=[item1])
        )

        item2 = make_long_forecast(q=200.0)
        results = crud.create_long_forecast(
            db_session, LongForecastBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].q == 200.0
        assert db_session.query(LongForecast).count() == 1

    def test_filter_by_horizon_type_and_value(self, db_session):
        items = [
            make_long_forecast(horizon_type="month", horizon_value=1),
            make_long_forecast(
                horizon_type="month", horizon_value=2,
                valid_from=date(2024, 8, 1), valid_to=date(2024, 8, 31),
            ),
            make_long_forecast(
                horizon_type="quarter", horizon_value=1,
                valid_from=date(2024, 10, 1), valid_to=date(2024, 12, 31),
            ),
        ]
        crud.create_long_forecast(
            db_session, LongForecastBulkCreate(data=items)
        )

        results = crud.get_long_forecast(
            db_session, horizon_type="month", horizon_value=1
        )
        assert len(results) == 1
        assert results[0].horizon_value == 1

    def test_empty(self, db_session):
        results = crud.get_long_forecast(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# LRForecast CRUD
# -------------------------------------------------------------------

class TestLRForecastCRUD:
    """Tests for create_lr_forecast / get_lr_forecast."""

    def test_create(self, db_session):
        item = make_lr_forecast()
        bulk = LRForecastBulkCreate(data=[item])
        results = crud.create_lr_forecast(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.slope == 1.2
        assert r.intercept == 10.0
        assert r.forecasted_discharge == 106.0
        assert r.rsquared == 0.85

    def test_upsert(self, db_session):
        item1 = make_lr_forecast(forecasted_discharge=106.0)
        crud.create_lr_forecast(
            db_session, LRForecastBulkCreate(data=[item1])
        )

        item2 = make_lr_forecast(forecasted_discharge=999.0)
        results = crud.create_lr_forecast(
            db_session, LRForecastBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].forecasted_discharge == 999.0
        assert db_session.query(LRForecast).count() == 1

    def test_filter_by_horizon(self, db_session):
        items = [
            make_lr_forecast(
                horizon_type="pentad", code="15013",
                date=date(2024, 6, 15),
            ),
            make_lr_forecast(
                horizon_type="decade", code="15013",
                date=date(2024, 6, 15),
            ),
        ]
        crud.create_lr_forecast(
            db_session, LRForecastBulkCreate(data=items)
        )

        results = crud.get_lr_forecast(db_session, horizon="pentad")
        assert len(results) == 1
        assert results[0].horizon_type == "pentad"

    def test_empty(self, db_session):
        results = crud.get_lr_forecast(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# SkillMetric CRUD
# -------------------------------------------------------------------

class TestSkillMetricCRUD:
    """Tests for create_skill_metric / get_skill_metric."""

    def test_create(self, db_session):
        item = make_skill_metric()
        bulk = SkillMetricBulkCreate(data=[item])
        results = crud.create_skill_metric(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.model_type == "LR"
        assert r.nse == 0.75
        assert r.accuracy == 0.85
        assert r.n_pairs == 50

    def test_upsert(self, db_session):
        item1 = make_skill_metric(nse=0.75)
        crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=[item1])
        )

        item2 = make_skill_metric(nse=0.99)
        results = crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].nse == 0.99
        assert db_session.query(SkillMetric).count() == 1

    def test_filter_by_model(self, db_session):
        items = [
            make_skill_metric(model_type="LR", horizon_in_year=33),
            make_skill_metric(model_type="EM", horizon_in_year=34),
        ]
        crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=items)
        )

        results = crud.get_skill_metric(db_session, model="LR")
        assert len(results) == 1
        assert results[0].model_type == "LR"

    def test_empty(self, db_session):
        results = crud.get_skill_metric(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# Edge cases
# -------------------------------------------------------------------

class TestCRUDEdgeCases:
    """Cross-cutting edge case tests."""

    def test_null_optional_fields(self, db_session):
        """Create a forecast with all optional fields set to None."""
        item = make_forecast(
            target=None, flag=None, composition=None,
            q05=None, q25=None, q50=None, q75=None, q95=None,
            forecasted_discharge=None,
        )
        results = crud.create_forecast(
            db_session, ForecastBulkCreate(data=[item])
        )

        assert len(results) == 1
        r = results[0]
        assert r.target is None
        assert r.flag is None
        assert r.q05 is None
        assert r.forecasted_discharge is None

    def test_empty_bulk_returns_empty_list(self, db_session):
        """Passing an empty data list returns an empty list."""
        bulk = ForecastBulkCreate(data=[])
        results = crud.create_forecast(db_session, bulk)
        assert results == []

    def test_pagination_skip_limit(self, db_session):
        """Create several records and verify skip/limit work correctly."""
        items = [
            make_forecast(code=f"1{i:04d}", date=date(2024, 6, 15))
            for i in range(5)
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        page1 = crud.get_forecast(db_session, skip=0, limit=2)
        assert len(page1) == 2

        page2 = crud.get_forecast(db_session, skip=2, limit=2)
        assert len(page2) == 2

        page3 = crud.get_forecast(db_session, skip=4, limit=2)
        assert len(page3) == 1

    def test_skip_beyond_total(self, db_session):
        """Skipping past all records returns empty list."""
        item = make_forecast()
        crud.create_forecast(db_session, ForecastBulkCreate(data=[item]))

        results = crud.get_forecast(db_session, skip=100, limit=10)
        assert results == []

    def test_mixed_insert_and_update(self, db_session):
        """Batch containing both new and existing records."""
        # Insert first record
        item1 = make_forecast(code="15013", forecasted_discharge=100.0)
        crud.create_forecast(db_session, ForecastBulkCreate(data=[item1]))

        # Batch: update existing + insert new
        item1_updated = make_forecast(
            code="15013", forecasted_discharge=999.0
        )
        item2_new = make_forecast(code="15014", forecasted_discharge=200.0)
        results = crud.create_forecast(
            db_session, ForecastBulkCreate(data=[item1_updated, item2_new])
        )

        assert len(results) == 2
        assert db_session.query(Forecast).count() == 2
        by_code = {r.code: r for r in results}
        assert by_code["15013"].forecasted_discharge == 999.0
        assert by_code["15014"].forecasted_discharge == 200.0

    def test_fallback_upsert_path_used_for_sqlite(self, db_session):
        """Verify SQLite triggers the fallback (non-PG) upsert path."""
        assert "sqlite" in str(db_session.bind.url)

        item = make_forecast()
        results = crud.create_forecast(
            db_session, ForecastBulkCreate(data=[item])
        )
        # If fallback path works, we get results with valid IDs
        assert len(results) == 1
        assert results[0].id >= 1
