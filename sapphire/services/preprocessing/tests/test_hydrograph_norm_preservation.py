"""
Service-side locking tests for hydrograph ``norm`` preservation.

Context
-------
The hydrograph bulk upsert (``app/crud.py:create_hydrograph``) is keyed on
``(horizon_type, code, date)`` and performs a blind ``setattr`` of every field
from ``model_dump()`` (no ``exclude_unset``). This means a re-POST of an existing
key overwrites *all* stored columns with the payload values, including ``norm``.

The long-horizon (month/quarter/season) writer must preserve a previously stored
numeric ``norm`` across a norm-absent recalculation. It does so by *re-sending*
the existing numeric ``norm`` in the payload (read-merge, "mechanism B"). These
tests lock the exact service behavior that mechanism relies on:

1. A re-POST carrying the same ``norm`` preserves it while updating other fields.
2. A re-POST carrying ``norm=None`` clobbers a good stored ``norm`` -- which is
   precisely *why* the writer must re-send the numeric norm.

All station codes are the placeholder "19999"; no real codes/values are used.
"""


def _month_row(**overrides):
    """Build a single MONTH-horizon hydrograph row for the placeholder station.

    Matches ``HydrographCreate`` (app/schemas.py): ``horizon_value`` and
    ``horizon_in_year`` must be >= 1, ``day_of_year`` in [1, 366].
    """
    row = {
        "horizon_type": "month",
        "code": "19999",
        "date": "2026-07-01",
        "horizon_value": 7,
        "horizon_in_year": 7,
        "day_of_year": 182,
        "norm": 100.5,
        "previous": None,
        "current": None,
    }
    row.update(overrides)
    return row


class TestHydrographNormPreservation:
    """Lock the upsert semantics the long-horizon writer's norm-merge depends on."""

    def test_norm_preserved_when_repost_carries_same_norm(self, client):
        # Arrange: initial row stores a good numeric norm, no previous/current.
        first = client.post(
            "/hydrograph/", json={"data": [_month_row()]}
        )
        assert first.status_code == 201

        # Act: re-POST the SAME (horizon_type, code, date) key. This mimics the
        # writer's read-merge output: norm re-sent unchanged, previous/current
        # now populated from a fresh recalculation.
        repost = client.post(
            "/hydrograph/",
            json={"data": [_month_row(norm=100.5, previous=200.0, current=300.0)]},
        )
        assert repost.status_code == 201

        resp = client.get(
            "/hydrograph/", params={"horizon": "month", "code": "19999"}
        )
        assert resp.status_code == 200
        rows = resp.json()

        # Assert: single row for the key; norm survived, other fields updated.
        assert len(rows) == 1
        row = rows[0]
        assert row["code"] == "19999"
        assert row["horizon_type"] == "month"
        assert row["norm"] == 100.5
        assert row["previous"] == 200.0
        assert row["current"] == 300.0

    def test_norm_is_clobbered_when_repost_omits_norm(self, client):
        # Arrange: initial row stores a good numeric norm.
        first = client.post(
            "/hydrograph/", json={"data": [_month_row(norm=100.5)]}
        )
        assert first.status_code == 201

        # Act: re-POST the same key WITHOUT carrying the norm forward (norm=None),
        # changing previous/current. The blind setattr overwrites every column.
        repost = client.post(
            "/hydrograph/",
            json={"data": [_month_row(norm=None, previous=200.0, current=300.0)]},
        )
        assert repost.status_code == 201

        resp = client.get(
            "/hydrograph/", params={"horizon": "month", "code": "19999"}
        )
        assert resp.status_code == 200
        rows = resp.json()

        # Assert: the good norm was overwritten with None. This documents WHY the
        # long-horizon writer MUST re-send the numeric norm (mechanism B).
        assert len(rows) == 1
        row = rows[0]
        assert row["norm"] is None
        assert row["previous"] == 200.0
        assert row["current"] == 300.0
