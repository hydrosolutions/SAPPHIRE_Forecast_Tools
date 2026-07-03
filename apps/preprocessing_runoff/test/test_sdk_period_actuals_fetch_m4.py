"""LOCKED tests for ``_fetch_sdk_period_actuals`` (M4) — real iEH HF SDK shape.

``get_data_values_for_site`` returns a paginated, nested dict:

    {"count": int, "next": url|None, "previous": url|None,
     "results": [{"station_code": ..., "data": [{"variable_code": ...,
                  "values": [{"value": ..., "timestamp_local": ...,
                              "timestamp_utc": ...}, ...]}]}]}

These tests pin the correct traversal (results -> data -> values), the
pagination follow (``next`` + ``page`` filter), variable_code filtering, and
fail-safe behaviour on malformed/empty responses. They must fail against the
old flat-list-of-dicts parsing and pass against the fixed implementation.

Fake station code '19999'; no real discharge values.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import sync_short_horizon_hydrograph as shh  # noqa: E402

CODE = "19999"
TARGET_YEAR = 2024


class _FakeSdk:
    """Fake SDK whose ``get_data_values_for_site`` serves pages by filter["page"]."""

    def __init__(self, pages: dict[int, object]) -> None:
        self._pages = pages
        self.calls: list[dict] = []

    def get_data_values_for_site(self, filters):
        self.calls.append(dict(filters))
        page = filters.get("page", 1)
        return self._pages[page]


def _series(variable_code: str, points: list[dict]) -> dict:
    return {"variable_code": variable_code, "unit": "m3/s", "values": points}


def _point(value: float, timestamp_local: str, timestamp_utc: str | None = None) -> dict:
    return {
        "value": value,
        "value_type": "M",
        "timestamp_local": timestamp_local,
        "timestamp_utc": timestamp_utc,
        "value_code": None,
    }


def _result(*series: dict) -> dict:
    return {
        "station_id": 1,
        "station_code": CODE,
        "station_name": "Test station",
        "station_type": "hydro",
        "data": list(series),
    }


def _page(results: list[dict], next_url: str | None = None, previous_url: str | None = None):
    return {
        "count": len(results),
        "next": next_url,
        "previous": previous_url,
        "results": results,
    }


def test_decode_and_pagination_and_variable_filter_decad():
    """Two pages, a decoy series, current/previous split by shifted year."""
    # Page 1: current-year WDDCA point (period-8-mapping) + a decoy series
    # (different variable_code) whose timestamp would map to a DIFFERENT
    # period (period 1) if it were wrongly included.
    page1 = _page(
        results=[
            _result(
                _series(
                    "WDDA",  # decoy - must be ignored
                    [_point(999.9, "2024-01-05T08:00:00", "2024-01-05T02:00:00Z")],
                ),
                _series(
                    "WDDCA",
                    [_point(12.3, "2024-03-10T08:00:00", "2024-03-10T02:00:00Z")],
                ),
            )
        ],
        next_url="http://example.invalid/api?page=2",
    )
    # Page 2: previous-year WDDCA point.
    page2 = _page(
        results=[
            _result(
                _series(
                    "WDDCA",
                    [_point(45.6, "2023-06-15T08:00:00", "2023-06-15T02:00:00Z")],
                )
            )
        ],
        next_url=None,
    )
    fake_sdk = _FakeSdk({1: page1, 2: page2})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "decade", TARGET_YEAR)

    # 2024-03-10 + 1 day = 2024-03-11 -> decad 8 (March, decad 2 of month).
    assert sdk_current == {8: 12.3}
    # 2023-06-15 + 1 day = 2023-06-16 -> decad 17 (June, decad 2 of month).
    assert sdk_previous == {17: 45.6}
    # The decoy's period (1) must never appear - proves variable_code filtering.
    assert 1 not in sdk_current

    # Pagination was followed: two calls, second one requesting page=2.
    assert len(fake_sdk.calls) == 2
    assert fake_sdk.calls[0].get("page") is None
    assert fake_sdk.calls[1]["page"] == 2
    # page_size is always requested to avoid the default-10 truncation.
    assert fake_sdk.calls[0]["page_size"] == 1000


def test_decode_pentad_basic():
    page = _page(
        results=[
            _result(
                _series(
                    "WDFA",
                    [_point(78.9, "2024-05-14T08:00:00", "2024-05-14T02:00:00Z")],
                )
            )
        ],
        next_url=None,
    )
    fake_sdk = _FakeSdk({1: page})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "pentad", TARGET_YEAR)

    # 2024-05-14 + 1 day = 2024-05-15 -> pentad 27 (May, 3rd pentad of month).
    assert sdk_current == {27: 78.9}
    assert sdk_previous == {}


def test_malformed_response_none_returns_empty_and_does_not_raise():
    fake_sdk = _FakeSdk({1: None})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "decade", TARGET_YEAR)

    assert sdk_current == {}
    assert sdk_previous == {}


def test_malformed_response_list_returns_empty_and_does_not_raise():
    fake_sdk = _FakeSdk({1: [{"station_code": CODE}]})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "decade", TARGET_YEAR)

    assert sdk_current == {}
    assert sdk_previous == {}


def test_malformed_response_non_list_results_returns_empty_and_does_not_raise():
    fake_sdk = _FakeSdk({1: {"count": 0, "next": None, "previous": None, "results": "oops"}})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "decade", TARGET_YEAR)

    assert sdk_current == {}
    assert sdk_previous == {}


def test_empty_results_returns_empty_dicts():
    fake_sdk = _FakeSdk({1: _page(results=[], next_url=None)})

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(fake_sdk, CODE, "decade", TARGET_YEAR)

    assert sdk_current == {}
    assert sdk_previous == {}


def test_sdk_call_exception_returns_empty_and_does_not_raise():
    class _RaisingSdk:
        def get_data_values_for_site(self, filters):
            raise RuntimeError("boom")

    sdk_current, sdk_previous = shh._fetch_sdk_period_actuals(
        _RaisingSdk(), CODE, "pentad", TARGET_YEAR
    )

    assert sdk_current == {}
    assert sdk_previous == {}
