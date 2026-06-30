from __future__ import annotations

import math

import pytest

from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.norms import NormResolver

STATION_CODE = "19999"


def _resolver(fake_client_factory, *, hydrograph_rows=None, runoff_rows=None) -> NormResolver:
    client = fake_client_factory(
        hydrograph_rows=hydrograph_rows or [],
        runoff_rows=runoff_rows or [],
    )
    return NormResolver(ForecastSkillEvalConfig(), client)


def _observed_rows(
    horizon: str,
    period_key: int,
    values_by_year: dict[int, float],
) -> list[dict[str, object]]:
    return [
        {
            "horizon": horizon,
            "code": STATION_CODE,
            "date": f"{year}-01-01",
            "horizon_in_year": period_key,
            "discharge": discharge,
        }
        for year, discharge in values_by_year.items()
    ]


def _other_year_values(scored_year: int, value: float, count: int = 10) -> dict[int, float]:
    return {year: value for year in range(scored_year - count, scored_year)}


def _month_norm_row(period_key: int, norm: float | None, **overrides: object) -> dict[str, object]:
    return {
        "horizon": "month",
        "code": STATION_CODE,
        "horizon_in_year": period_key,
        "horizon_value": 1,
        "norm": norm,
    } | overrides


def test_stored_norm_used_with_horizon_provenance(fake_client_factory) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            _month_norm_row(4, 42.0, count=30),
        ],
    )

    result = resolver.resolve("month", STATION_CODE, 4, 2024)

    assert result.norm == 42.0
    assert result.provenance == "official"
    assert not result.excluded
    assert result.reason is None


def test_calculated_fallback_for_decade_overrides_horizon_provenance(
    fake_client_factory,
) -> None:
    values_by_year = _other_year_values(2024, 20.0) | {2024: 200.0}
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            {
                "horizon": "decade",
                "code": STATION_CODE,
                "horizon_in_year": 3,
                "norm": None,
            },
        ],
        runoff_rows=_observed_rows("decade", 3, values_by_year),
    )

    result = resolver.resolve("decade", STATION_CODE, 3, 2024)

    assert result.norm == 20.0
    assert result.provenance == "calculated"
    assert result.provenance != "official"
    assert not result.excluded


def test_leave_one_out_excludes_scored_year_from_calculated_norm(
    fake_client_factory,
) -> None:
    values_by_year = _other_year_values(2024, 10.0) | {2024: 1000.0}
    resolver = _resolver(
        fake_client_factory,
        runoff_rows=_observed_rows("pentad", 8, values_by_year),
    )

    result = resolver.resolve("pentad", STATION_CODE, 8, 2024)

    assert result.norm == 10.0
    assert result.norm != pytest.approx(sum(values_by_year.values()) / len(values_by_year))
    assert result.provenance == "calculated"
    assert not result.excluded


def test_calculated_fallback_excludes_when_distinct_years_lt_min_years(
    fake_client_factory,
) -> None:
    values_by_year = _other_year_values(2024, 15.0, count=9) | {2024: 150.0}
    resolver = _resolver(
        fake_client_factory,
        runoff_rows=_observed_rows("day", 15, values_by_year),
    )

    result = resolver.resolve("day", STATION_CODE, 15, 2024)

    assert result.norm is None
    assert result.provenance is None
    assert result.excluded
    assert result.reason == "norm_unavailable_lt_min_years"


def test_calculated_mapped_stored_norm_below_min_years_falls_back_to_loo(
    fake_client_factory,
) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            {
                "horizon": "day",
                "code": STATION_CODE,
                "horizon_in_year": 20,
                "norm": 999.0,
                "count": 9,
            },
        ],
        runoff_rows=_observed_rows("day", 20, _other_year_values(2024, 12.0)),
    )

    result = resolver.resolve("day", STATION_CODE, 20, 2024)

    assert result.norm == 12.0
    assert result.provenance == "calculated"
    assert not result.excluded


def test_official_month_stored_norm_with_null_count_is_used(
    fake_client_factory,
) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            _month_norm_row(2, 33.0, count=None),
        ],
    )

    result = resolver.resolve("month", STATION_CODE, 2, 2024)

    assert result.norm == 33.0
    assert result.provenance == "official"
    assert not result.excluded


def test_duplicate_conflicting_stored_keys_are_excluded(
    fake_client_factory,
) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            _month_norm_row(3, 21.0),
            _month_norm_row(3, 22.0),
        ],
    )

    result = resolver.resolve("month", STATION_CODE, 3, 2024)

    assert result.norm is None
    assert result.provenance is None
    assert result.excluded
    assert result.reason == "norm_duplicate_conflict"


def test_long_term_missing_stored_norm_is_excluded(fake_client_factory) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            _month_norm_row(4, None),
        ],
        runoff_rows=_observed_rows("month", 4, _other_year_values(2024, 18.0)),
    )

    result = resolver.resolve("month", STATION_CODE, 4, 2024)

    assert result.norm is None
    assert result.provenance is None
    assert result.excluded
    assert result.reason == "norm_unavailable_long_term"


def test_long_term_stored_norm_joins_on_horizon_in_year_not_horizon_value(
    fake_client_factory,
) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            _month_norm_row(4, 44.0),
        ],
    )

    matched = resolver.resolve("month", STATION_CODE, 4, 2024)
    horizon_value_match = resolver.resolve("month", STATION_CODE, 1, 2024)

    assert matched.norm == 44.0
    assert matched.provenance == "official"
    assert not matched.excluded
    assert horizon_value_match.norm is None
    assert horizon_value_match.provenance is None
    assert horizon_value_match.excluded
    assert horizon_value_match.reason == "norm_unavailable_long_term"


@pytest.mark.parametrize("stored_norm", [0.0, -1.0, math.nan])
def test_non_positive_or_nan_stored_norm_is_treated_as_missing(
    fake_client_factory,
    stored_norm: float,
) -> None:
    resolver = _resolver(
        fake_client_factory,
        hydrograph_rows=[
            {
                "horizon": "pentad",
                "code": STATION_CODE,
                "horizon_in_year": 11,
                "norm": stored_norm,
                "count": 30,
            },
        ],
        runoff_rows=_observed_rows("pentad", 11, _other_year_values(2024, 17.0)),
    )

    result = resolver.resolve("pentad", STATION_CODE, 11, 2024)

    assert result.norm == 17.0
    assert result.provenance == "calculated"
    assert not result.excluded
