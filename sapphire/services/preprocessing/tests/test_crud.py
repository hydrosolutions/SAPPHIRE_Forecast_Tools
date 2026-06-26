"""
CRUD tests for the preprocessing service.

Tests the SQLAlchemy CRUD functions directly (no HTTP layer) using
SQLite in-memory databases.
"""

import random
from datetime import date

import pytest

from app import crud
from app.models import Runoff, Hydrograph, Meteo, Snow
from app.schemas import (
    RunoffBulkCreate,
    HydrographBulkCreate,
    MeteoBulkCreate,
    SnowBulkCreate,
)
from factories import (
    make_hydrograph,
    make_meteo,
    make_runoff,
    make_snow,
)


# -------------------------------------------------------------------
# Runoff CRUD
# -------------------------------------------------------------------

class TestRunoffCRUD:
    """Tests for create_runoff / get_runoff."""

    def test_create_single_runoff(self, db_session):
        item = make_runoff()
        bulk = RunoffBulkCreate(data=[item])
        results = crud.create_runoff(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.horizon_type == "pentad"
        assert r.discharge == 100.0
        assert r.predictor == 80.0
        assert r.horizon_value == 3
        assert r.horizon_in_year == 33

    def test_create_bulk_runoffs(self, db_session):
        items = [
            make_runoff(code="15013"),
            make_runoff(code="15014"),
            make_runoff(code="15015"),
        ]
        bulk = RunoffBulkCreate(data=items)
        results = crud.create_runoff(db_session, bulk)

        assert len(results) == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_upsert_updates_existing(self, db_session):
        """Insert then re-insert with same unique keys but new values."""
        item1 = make_runoff(discharge=100.0)
        crud.create_runoff(db_session, RunoffBulkCreate(data=[item1]))

        item2 = make_runoff(discharge=999.0)
        results = crud.create_runoff(
            db_session, RunoffBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].discharge == 999.0
        # Should be exactly 1 row in the table, not 2
        total = db_session.query(Runoff).count()
        assert total == 1

    def test_filter_by_code(self, db_session):
        items = [
            make_runoff(code="15013"),
            make_runoff(code="15014"),
        ]
        crud.create_runoff(db_session, RunoffBulkCreate(data=items))

        results = crud.get_runoff(db_session, code="15013")
        assert len(results) == 1
        assert results[0].code == "15013"

    def test_filter_by_date_range(self, db_session):
        items = [
            make_runoff(code="15013", date=date(2024, 6, 10)),
            make_runoff(code="15014", date=date(2024, 6, 15)),
            make_runoff(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_runoff(db_session, RunoffBulkCreate(data=items))

        results = crud.get_runoff(
            db_session, start_date="2024-06-12", end_date="2024-06-18"
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_empty_results(self, db_session):
        results = crud.get_runoff(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# Hydrograph CRUD
# -------------------------------------------------------------------

class TestHydrographCRUD:
    """Tests for create_hydrograph / get_hydrograph."""

    def test_create_single_hydrograph(self, db_session):
        item = make_hydrograph()
        bulk = HydrographBulkCreate(data=[item])
        results = crud.create_hydrograph(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.horizon_type == "pentad"
        assert r.mean == 95.0
        assert r.q50 == 95.0
        assert r.norm == 90.0

    def test_create_bulk_hydrographs(self, db_session):
        items = [
            make_hydrograph(code="15013"),
            make_hydrograph(code="15014"),
            make_hydrograph(code="15015"),
        ]
        bulk = HydrographBulkCreate(data=items)
        results = crud.create_hydrograph(db_session, bulk)

        assert len(results) == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_upsert_updates_existing(self, db_session):
        """Insert then re-insert with same unique keys but new values."""
        item1 = make_hydrograph(mean=95.0)
        crud.create_hydrograph(db_session, HydrographBulkCreate(data=[item1]))

        item2 = make_hydrograph(mean=200.0)
        results = crud.create_hydrograph(
            db_session, HydrographBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].mean == 200.0
        total = db_session.query(Hydrograph).count()
        assert total == 1

    def test_filter_by_code(self, db_session):
        items = [
            make_hydrograph(code="15013"),
            make_hydrograph(code="15014"),
        ]
        crud.create_hydrograph(db_session, HydrographBulkCreate(data=items))

        results = crud.get_hydrograph(db_session, code="15013")
        assert len(results) == 1
        assert results[0].code == "15013"

    def test_filter_by_date_range(self, db_session):
        items = [
            make_hydrograph(code="15013", date=date(2024, 6, 10)),
            make_hydrograph(code="15014", date=date(2024, 6, 15)),
            make_hydrograph(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_hydrograph(db_session, HydrographBulkCreate(data=items))

        results = crud.get_hydrograph(
            db_session, start_date="2024-06-12", end_date="2024-06-18"
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_empty_results(self, db_session):
        results = crud.get_hydrograph(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# Meteo CRUD
# -------------------------------------------------------------------

class TestMeteoCRUD:
    """Tests for create_meteo / get_meteo."""

    def test_create_single_meteo(self, db_session):
        item = make_meteo()
        bulk = MeteoBulkCreate(data=[item])
        results = crud.create_meteo(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.meteo_type == "T"
        assert r.value == 22.5
        assert r.norm == 20.0

    def test_create_bulk_meteo(self, db_session):
        items = [
            make_meteo(code="15013"),
            make_meteo(code="15014"),
            make_meteo(code="15015"),
        ]
        bulk = MeteoBulkCreate(data=items)
        results = crud.create_meteo(db_session, bulk)

        assert len(results) == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_upsert_updates_existing(self, db_session):
        """Insert then re-insert with same unique keys but new values."""
        item1 = make_meteo(value=22.5)
        crud.create_meteo(db_session, MeteoBulkCreate(data=[item1]))

        item2 = make_meteo(value=99.9)
        results = crud.create_meteo(
            db_session, MeteoBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].value == 99.9
        total = db_session.query(Meteo).count()
        assert total == 1

    def test_filter_by_code(self, db_session):
        items = [
            make_meteo(code="15013"),
            make_meteo(code="15014"),
        ]
        crud.create_meteo(db_session, MeteoBulkCreate(data=items))

        results = crud.get_meteo(db_session, code="15013")
        assert len(results) == 1
        assert results[0].code == "15013"

    def test_filter_by_date_range(self, db_session):
        items = [
            make_meteo(code="15013", date=date(2024, 6, 10)),
            make_meteo(code="15014", date=date(2024, 6, 15)),
            make_meteo(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_meteo(db_session, MeteoBulkCreate(data=items))

        results = crud.get_meteo(
            db_session, start_date="2024-06-12", end_date="2024-06-18"
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_empty_results(self, db_session):
        results = crud.get_meteo(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# Snow CRUD
# -------------------------------------------------------------------

class TestSnowCRUD:
    """Tests for create_snow / get_snow."""

    def test_create_single_snow(self, db_session):
        item = make_snow()
        bulk = SnowBulkCreate(data=[item])
        results = crud.create_snow(db_session, bulk)

        assert len(results) == 1
        r = results[0]
        assert r.id is not None
        assert r.code == "15013"
        assert r.snow_type == "HS"
        assert r.value == 50.0
        assert r.norm == 45.0

    def test_create_bulk_snow(self, db_session):
        items = [
            make_snow(code="15013"),
            make_snow(code="15014"),
            make_snow(code="15015"),
        ]
        bulk = SnowBulkCreate(data=items)
        results = crud.create_snow(db_session, bulk)

        assert len(results) == 3
        codes = {r.code for r in results}
        assert codes == {"15013", "15014", "15015"}

    def test_upsert_updates_existing(self, db_session):
        """Insert then re-insert with same unique keys but new values."""
        item1 = make_snow(value=50.0)
        crud.create_snow(db_session, SnowBulkCreate(data=[item1]))

        item2 = make_snow(value=999.0)
        results = crud.create_snow(
            db_session, SnowBulkCreate(data=[item2])
        )

        assert len(results) == 1
        assert results[0].value == 999.0
        total = db_session.query(Snow).count()
        assert total == 1

    def test_filter_by_code(self, db_session):
        items = [
            make_snow(code="15013"),
            make_snow(code="15014"),
        ]
        crud.create_snow(db_session, SnowBulkCreate(data=items))

        results = crud.get_snow(db_session, code="15013")
        assert len(results) == 1
        assert results[0].code == "15013"

    def test_filter_by_date_range(self, db_session):
        items = [
            make_snow(code="15013", date=date(2024, 6, 10)),
            make_snow(code="15014", date=date(2024, 6, 15)),
            make_snow(code="15015", date=date(2024, 6, 20)),
        ]
        crud.create_snow(db_session, SnowBulkCreate(data=items))

        results = crud.get_snow(
            db_session, start_date="2024-06-12", end_date="2024-06-18"
        )
        assert len(results) == 1
        assert results[0].code == "15014"

    def test_empty_results(self, db_session):
        results = crud.get_snow(db_session, code="NONEXISTENT")
        assert results == []


# -------------------------------------------------------------------
# Snow pagination stable ordering (PREPG-009)
# -------------------------------------------------------------------

class TestSnowPaginationOrdering:
    """Regression tests for PREPG-009.

    get_snow() must apply a stable ORDER BY (snow_type, code, date, id)
    before offset/limit so that whole-table pagination is complete,
    non-overlapping, and stable across repeated calls. Without the
    ORDER BY, OFFSET/LIMIT over a large snow table returns a
    nondeterministic, potentially incomplete subset.
    """

    @staticmethod
    def _expected_key(row):
        # SnowType is a str-Enum; .value gives the stable string form.
        snow_type = getattr(row.snow_type, "value", row.snow_type)
        return (snow_type, row.code, row.date, row.id)

    def _seed_shuffled_snow(self, db_session):
        """Seed > 1 page of snow rows across multiple codes and years,
        inserting them in deliberately shuffled order.

        Returns the total number of seeded rows.
        """
        # Dummy placeholder station codes only (no real station codes).
        codes = ["19999", "29999", "39999"]
        years = [2020, 2021, 2022, 2023]
        days = list(range(1, 9))
        # 3 codes * 4 years * 8 days = 96 rows; with page_size=10 this
        # spans many pages, well past a single page.

        items = []
        for code in codes:
            for year in years:
                for day in days:
                    items.append(
                        make_snow(
                            snow_type="SWE",
                            code=code,
                            date=date(year, 1, day),
                            value=float(year * 100 + day),
                            norm=float(year),
                        )
                    )

        # Deterministic shuffle so insertion order does NOT match the
        # expected domain order (snow_type, code, date, id).
        random.Random(1234).shuffle(items)

        crud.create_snow(db_session, SnowBulkCreate(data=items))
        return len(items)

    def _paginate_all(self, db_session, page_size):
        """Walk every page via get_snow(skip, limit) and concatenate."""
        all_rows = []
        skip = 0
        while True:
            page = crud.get_snow(db_session, skip=skip, limit=page_size)
            if not page:
                break
            # A page may never exceed the requested limit.
            assert len(page) <= page_size
            all_rows.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return all_rows

    def test_pagination_complete_nonoverlapping_ordered(self, db_session):
        total = self._seed_shuffled_snow(db_session)
        page_size = 10
        assert total > page_size  # sanity: more than one page

        rows = self._paginate_all(db_session, page_size)

        ids = [r.id for r in rows]
        # Complete: every seeded row returned exactly once.
        assert len(ids) == total
        # Non-overlapping: no row appears in more than one page.
        assert len(set(ids)) == total
        # The id set matches every row actually stored.
        stored_ids = {r.id for r in db_session.query(Snow).all()}
        assert set(ids) == stored_ids

        # Ordered by (snow_type, code, date, id).
        keys = [self._expected_key(r) for r in rows]
        assert keys == sorted(keys)

    def test_pagination_stable_across_repeated_calls(self, db_session):
        self._seed_shuffled_snow(db_session)
        page_size = 10

        first = [r.id for r in self._paginate_all(db_session, page_size)]
        second = [r.id for r in self._paginate_all(db_session, page_size)]

        # Repeated paginated calls return the same ordered sequence.
        assert first == second


# -------------------------------------------------------------------
# Edge cases
# -------------------------------------------------------------------

class TestCRUDEdgeCases:
    """Cross-cutting edge case tests."""

    def test_null_optional_fields_runoff(self, db_session):
        """Create runoff with discharge=None and predictor=None."""
        item = make_runoff(discharge=None, predictor=None)
        results = crud.create_runoff(
            db_session, RunoffBulkCreate(data=[item])
        )

        assert len(results) == 1
        r = results[0]
        assert r.discharge is None
        assert r.predictor is None

    def test_empty_bulk_returns_empty_list_runoff(self, db_session):
        """Passing an empty data list returns an empty list."""
        bulk = RunoffBulkCreate(data=[])
        results = crud.create_runoff(db_session, bulk)
        assert results == []

    def test_empty_bulk_returns_empty_list_hydrograph(self, db_session):
        """Passing an empty data list returns an empty list."""
        bulk = HydrographBulkCreate(data=[])
        results = crud.create_hydrograph(db_session, bulk)
        assert results == []

    def test_empty_bulk_returns_empty_list_meteo(self, db_session):
        """Passing an empty data list returns an empty list."""
        bulk = MeteoBulkCreate(data=[])
        results = crud.create_meteo(db_session, bulk)
        assert results == []

    def test_empty_bulk_returns_empty_list_snow(self, db_session):
        """Passing an empty data list returns an empty list."""
        bulk = SnowBulkCreate(data=[])
        results = crud.create_snow(db_session, bulk)
        assert results == []

    def test_pagination_skip_limit(self, db_session):
        """Create several records and verify skip/limit work correctly."""
        items = [
            make_runoff(code=f"1{i:04d}", date=date(2024, 6, 15))
            for i in range(5)
        ]
        crud.create_runoff(db_session, RunoffBulkCreate(data=items))

        page1 = crud.get_runoff(db_session, skip=0, limit=2)
        assert len(page1) == 2

        page2 = crud.get_runoff(db_session, skip=2, limit=2)
        assert len(page2) == 2

        page3 = crud.get_runoff(db_session, skip=4, limit=2)
        assert len(page3) == 1

    def test_mixed_insert_and_update(self, db_session):
        """Batch containing both new and existing records."""
        # Insert first record
        item1 = make_runoff(code="15013", discharge=100.0)
        crud.create_runoff(db_session, RunoffBulkCreate(data=[item1]))

        # Batch: update existing + insert new
        item1_updated = make_runoff(code="15013", discharge=999.0)
        item2_new = make_runoff(code="15014", discharge=200.0)
        results = crud.create_runoff(
            db_session, RunoffBulkCreate(data=[item1_updated, item2_new])
        )

        assert len(results) == 2
        assert db_session.query(Runoff).count() == 2
        by_code = {r.code: r for r in results}
        assert by_code["15013"].discharge == 999.0
        assert by_code["15014"].discharge == 200.0

    def test_meteo_filter_by_type(self, db_session):
        """Filter meteo records by meteo_type."""
        items = [
            make_meteo(meteo_type="T", code="15013"),
            make_meteo(meteo_type="P", code="15013", date=date(2024, 6, 16)),
        ]
        crud.create_meteo(db_session, MeteoBulkCreate(data=items))

        results = crud.get_meteo(db_session, meteo_type="T")
        assert len(results) == 1
        assert results[0].meteo_type == "T"

    def test_snow_filter_by_type(self, db_session):
        """Filter snow records by snow_type."""
        items = [
            make_snow(snow_type="HS", code="15013"),
            make_snow(snow_type="SWE", code="15013", date=date(2024, 6, 16)),
        ]
        crud.create_snow(db_session, SnowBulkCreate(data=items))

        results = crud.get_snow(db_session, snow_type="HS")
        assert len(results) == 1
        assert results[0].snow_type == "HS"
