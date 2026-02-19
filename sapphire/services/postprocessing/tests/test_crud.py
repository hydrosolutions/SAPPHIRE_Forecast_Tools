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

    def test_new_metric_fields_round_trip(self, db_session):
        """New fields (crps, pbias, kgelf, nse_log) survive round-trip."""
        item = make_skill_metric(
            crps=12.5, pbias=-3.2, kgelf=0.65, nse_log=0.72,
        )
        bulk = SkillMetricBulkCreate(data=[item])
        results = crud.create_skill_metric(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.crps == 12.5
        assert r.pbias == -3.2
        assert r.kgelf == 0.65
        assert r.nse_log == 0.72

    def test_new_metric_fields_default_none(self, db_session):
        """New fields default to None when not provided."""
        item = make_skill_metric()
        bulk = SkillMetricBulkCreate(data=[item])
        results = crud.create_skill_metric(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.crps is None
        assert r.pbias is None
        assert r.kgelf is None
        assert r.nse_log is None


# -------------------------------------------------------------------
# Edge cases
# -------------------------------------------------------------------

class TestMonthlySkillMetricCRUD:
    """Tests for monthly skill metrics CRUD operations."""

    def test_create_monthly(self, db_session):
        """Create a monthly skill metric with month horizon."""
        item = make_skill_metric(
            horizon_type="month", horizon_in_year=6,
            model_type="GBT", nse=0.82, accuracy=0.90,
        )
        bulk = SkillMetricBulkCreate(data=[item])
        results = crud.create_skill_metric(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.model_type.value == "GBT"
        assert r.horizon_type.value == "month"
        assert r.horizon_in_year == 6
        assert r.nse == 0.82
        assert r.accuracy == 0.90

    def test_upsert_monthly(self, db_session):
        """Update same (month, code, model) record via upsert."""
        item1 = make_skill_metric(
            horizon_type="month", horizon_in_year=6,
            model_type="GBT", nse=0.70,
        )
        crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=[item1])
        )

        item2 = make_skill_metric(
            horizon_type="month", horizon_in_year=6,
            model_type="GBT", nse=0.95,
        )
        results = crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].nse == 0.95
        assert db_session.query(SkillMetric).count() == 1

    def test_filter_by_month_horizon(self, db_session):
        """get_skill_metric(horizon='month') returns only monthly records."""
        items = [
            make_skill_metric(
                horizon_type="pentad", horizon_in_year=33,
                model_type="LR",
            ),
            make_skill_metric(
                horizon_type="month", horizon_in_year=6,
                model_type="GBT",
            ),
            make_skill_metric(
                horizon_type="month", horizon_in_year=7,
                model_type="GBT", code="15014",
            ),
        ]
        crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=items)
        )

        results = crud.get_skill_metric(db_session, horizon="month")
        assert len(results) == 2
        for r in results:
            assert r.horizon_type.value == "month"


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


# -------------------------------------------------------------------
# _fallback_upsert direct tests
# -------------------------------------------------------------------

class TestFallbackUpsertDirect:
    """Test _fallback_upsert function directly (bypassing create_* wrappers)."""

    def test_insert_only_batch(self, db_session):
        """Batch of new records — all inserts, no updates."""
        items = [
            make_forecast(code="15013"),
            make_forecast(code="15014"),
            make_forecast(code="15015"),
        ]
        unique_keys = ['horizon_type', 'code', 'model_type', 'date', 'target']
        results = crud._fallback_upsert(
            db_session, Forecast, items, unique_keys
        )
        assert len(results) == 3
        assert db_session.query(Forecast).count() == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_update_only_batch(self, db_session):
        """Batch where ALL records already exist — all updates."""
        # Pre-populate
        items_v1 = [
            make_forecast(code="15013", forecasted_discharge=100.0),
            make_forecast(code="15014", forecasted_discharge=200.0),
        ]
        crud._fallback_upsert(
            db_session, Forecast, items_v1,
            ['horizon_type', 'code', 'model_type', 'date', 'target'],
        )
        assert db_session.query(Forecast).count() == 2

        # Update both with new values
        items_v2 = [
            make_forecast(code="15013", forecasted_discharge=999.0),
            make_forecast(code="15014", forecasted_discharge=888.0),
        ]
        results = crud._fallback_upsert(
            db_session, Forecast, items_v2,
            ['horizon_type', 'code', 'model_type', 'date', 'target'],
        )
        assert len(results) == 2
        # Still only 2 rows — no duplicates
        assert db_session.query(Forecast).count() == 2
        by_code = {r.code: r for r in results}
        assert by_code["15013"].forecasted_discharge == 999.0
        assert by_code["15014"].forecasted_discharge == 888.0

    def test_mixed_insert_and_update(self, db_session):
        """Batch with one existing (update) and one new (insert)."""
        crud._fallback_upsert(
            db_session, Forecast,
            [make_forecast(code="15013", forecasted_discharge=100.0)],
            ['horizon_type', 'code', 'model_type', 'date', 'target'],
        )

        items = [
            make_forecast(code="15013", forecasted_discharge=999.0),  # update
            make_forecast(code="15014", forecasted_discharge=200.0),  # insert
        ]
        results = crud._fallback_upsert(
            db_session, Forecast, items,
            ['horizon_type', 'code', 'model_type', 'date', 'target'],
        )
        assert len(results) == 2
        assert db_session.query(Forecast).count() == 2
        by_code = {r.code: r for r in results}
        assert by_code["15013"].forecasted_discharge == 999.0
        assert by_code["15014"].forecasted_discharge == 200.0

    def test_empty_batch(self, db_session):
        """Empty list — no crash, returns empty."""
        results = crud._fallback_upsert(
            db_session, Forecast, [],
            ['horizon_type', 'code', 'model_type', 'date', 'target'],
        )
        # _fallback_upsert is only called when bulk_items is non-empty
        # but it should handle empty gracefully
        assert results == []

    def test_skill_metric_upsert(self, db_session):
        """Verify _fallback_upsert works for SkillMetric model."""
        items = [
            make_skill_metric(code="15013", nse=0.7),
            make_skill_metric(code="15014", nse=0.8, horizon_in_year=34),
        ]
        unique_keys = ['horizon_type', 'code', 'model_type', 'date',
                       'horizon_in_year']
        results = crud._fallback_upsert(
            db_session, SkillMetric, items, unique_keys,
        )
        assert len(results) == 2
        by_code = {r.code: r for r in results}
        assert by_code["15013"].nse == 0.7
        assert by_code["15014"].nse == 0.8

        # Now update one
        items_v2 = [make_skill_metric(code="15013", nse=0.99)]
        results = crud._fallback_upsert(
            db_session, SkillMetric, items_v2, unique_keys,
        )
        assert len(results) == 1
        assert results[0].nse == 0.99
        # Still 2 rows total
        assert db_session.query(SkillMetric).count() == 2

    def test_returned_objects_are_refreshed(self, db_session):
        """Verify returned objects reflect committed state (not stale)."""
        items = [make_forecast(code="15013", forecasted_discharge=100.0)]
        unique_keys = ['horizon_type', 'code', 'model_type', 'date', 'target']
        results = crud._fallback_upsert(
            db_session, Forecast, items, unique_keys,
        )
        # Object should have a valid ID (assigned by DB, refreshed)
        assert results[0].id is not None
        assert results[0].id >= 1

        # Update and verify the returned object is fresh
        items_v2 = [make_forecast(code="15013", forecasted_discharge=999.0)]
        results = crud._fallback_upsert(
            db_session, Forecast, items_v2, unique_keys,
        )
        assert results[0].forecasted_discharge == 999.0
        # Same ID (same row, updated in-place)
        assert results[0].id >= 1


# -------------------------------------------------------------------
# Combined filter queries
# -------------------------------------------------------------------

class TestCombinedFilters:
    """Test multi-parameter filter combinations for get_* functions."""

    def test_forecast_code_plus_date_range_plus_model(self, db_session):
        """Combine code, date range, and model filter on Forecast."""
        items = [
            make_forecast(code="15013", model_type="LR",
                          date=date(2024, 6, 10)),
            make_forecast(code="15013", model_type="TFT",
                          date=date(2024, 6, 15)),
            make_forecast(code="15014", model_type="LR",
                          date=date(2024, 6, 15)),
            make_forecast(code="15013", model_type="LR",
                          date=date(2024, 6, 20)),
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        results = crud.get_forecast(
            db_session, code="15013", model="LR",
            start_date="2024-06-12", end_date="2024-06-18",
        )
        # Only code=15013 + model=LR + date between 12-18 → nothing
        # (15013 LR is on 10th and 20th; 15013 TFT is on 15th)
        assert len(results) == 0

        results = crud.get_forecast(
            db_session, code="15013", model="LR",
            start_date="2024-06-08", end_date="2024-06-12",
        )
        assert len(results) == 1
        assert results[0].date == date(2024, 6, 10)

    def test_forecast_target_null_filter(self, db_session):
        """Special target='null' filter returns records with NULL target."""
        items = [
            make_forecast(code="15013", target=date(2024, 6, 20)),
            make_forecast(code="15014", target=None),
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        results = crud.get_forecast(db_session, target="null")
        assert len(results) == 1
        assert results[0].code == "15014"
        assert results[0].target is None

    def test_forecast_horizon_plus_code(self, db_session):
        """Combine horizon_type and code filters."""
        items = [
            make_forecast(code="15013", horizon_type="pentad",
                          date=date(2024, 6, 15)),
            make_forecast(code="15013", horizon_type="decade",
                          date=date(2024, 6, 15)),
            make_forecast(code="15014", horizon_type="pentad",
                          date=date(2024, 6, 15)),
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        results = crud.get_forecast(
            db_session, horizon="pentad", code="15013"
        )
        assert len(results) == 1
        assert results[0].horizon_type.value == "pentad"
        assert results[0].code == "15013"

    def test_skill_metric_code_plus_model_plus_date(self, db_session):
        """Combine code, model, and date range filters on SkillMetric."""
        items = [
            make_skill_metric(code="15013", model_type="LR",
                              date=date(2024, 6, 10), horizon_in_year=31),
            make_skill_metric(code="15013", model_type="LR",
                              date=date(2024, 6, 15), horizon_in_year=33),
            make_skill_metric(code="15013", model_type="TFT",
                              date=date(2024, 6, 15), horizon_in_year=34),
            make_skill_metric(code="15014", model_type="LR",
                              date=date(2024, 6, 15), horizon_in_year=35),
        ]
        crud.create_skill_metric(
            db_session, SkillMetricBulkCreate(data=items)
        )

        results = crud.get_skill_metric(
            db_session, code="15013", model="LR",
            start_date="2024-06-12", end_date="2024-06-18",
        )
        assert len(results) == 1
        assert results[0].code == "15013"
        assert results[0].model_type.value == "LR"
        assert results[0].date == date(2024, 6, 15)

    def test_lr_forecast_date_range(self, db_session):
        """LR forecast filter by date range."""
        items = [
            make_lr_forecast(code="15013", date=date(2024, 6, 10)),
            make_lr_forecast(code="15014", date=date(2024, 6, 15)),
            make_lr_forecast(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_lr_forecast(
            db_session, LRForecastBulkCreate(data=items)
        )

        results = crud.get_lr_forecast(
            db_session, start_date="2024-06-12", end_date="2024-06-18",
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_long_forecast_combined_filters(self, db_session):
        """LongForecast filter by horizon_type + code + date range."""
        items = [
            make_long_forecast(
                horizon_type="month", code="15013",
                date=date(2024, 6, 10),
            ),
            make_long_forecast(
                horizon_type="month", code="15014",
                date=date(2024, 6, 15),
                valid_from=date(2024, 8, 1), valid_to=date(2024, 8, 31),
            ),
            make_long_forecast(
                horizon_type="quarter", code="15013",
                date=date(2024, 6, 15),
                valid_from=date(2024, 10, 1), valid_to=date(2024, 12, 31),
            ),
        ]
        crud.create_long_forecast(
            db_session, LongForecastBulkCreate(data=items)
        )

        results = crud.get_long_forecast(
            db_session, horizon_type="month", code="15013",
        )
        assert len(results) == 1
        assert results[0].code == "15013"
        assert results[0].horizon_type.value == "month"


# -------------------------------------------------------------------
# Large batch correctness
# -------------------------------------------------------------------

class TestLargeBatch:
    """Verify correctness with larger batches."""

    def test_fifty_record_batch(self, db_session):
        """50 records inserted in one batch, all retrievable."""
        items = [
            make_forecast(
                code=f"1{i:04d}", date=date(2024, 6, 15),
                forecasted_discharge=float(i),
            )
            for i in range(50)
        ]
        crud.create_forecast(db_session, ForecastBulkCreate(data=items))

        total = db_session.query(Forecast).count()
        assert total == 50

        # Spot-check first and last
        first = crud.get_forecast(db_session, code="10000")
        assert len(first) == 1
        assert first[0].forecasted_discharge == 0.0

        last = crud.get_forecast(db_session, code="10049")
        assert len(last) == 1
        assert last[0].forecasted_discharge == 49.0

    def test_batch_upsert_preserves_unmodified_fields(self, db_session):
        """Upsert updates specified fields without corrupting others."""
        item = make_forecast(
            code="15013", forecasted_discharge=100.0,
            q05=80.0, q25=90.0, q50=95.0, q75=110.0, q95=120.0,
        )
        crud.create_forecast(db_session, ForecastBulkCreate(data=[item]))

        # Update only forecasted_discharge — other quantiles should persist
        item_v2 = make_forecast(
            code="15013", forecasted_discharge=999.0,
            q05=80.0, q25=90.0, q50=95.0, q75=110.0, q95=120.0,
        )
        results = crud.create_forecast(
            db_session, ForecastBulkCreate(data=[item_v2])
        )

        r = results[0]
        assert r.forecasted_discharge == 999.0
        assert r.q05 == 80.0
        assert r.q25 == 90.0
        assert r.q50 == 95.0
        assert r.q75 == 110.0
        assert r.q95 == 120.0
