from __future__ import annotations

from datetime import date, timedelta

import pytest

from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.observed_truth import ObservedTruthConfig, ObservedTruthProvider


def _daily_rows(
    *,
    year: int,
    month: int,
    count: int | None = None,
    value: float | None = None,
) -> list[dict[str, object]]:
    rows = []
    day = date(year, month, 1)
    while day.month == month and (count is None or len(rows) < count):
        rows.append(
            {
                "horizon": "day",
                "code": "19999",
                "date": day.isoformat(),
                "discharge": float(day.day if value is None else value),
            }
        )
        day += timedelta(days=1)
    return rows


def _provider(fake_client_factory, rows: list[dict[str, object]]) -> ObservedTruthProvider:
    return ObservedTruthProvider(
        ForecastSkillEvalConfig(station_filter=["19999"]),
        client=fake_client_factory(runoff_rows=rows),
    )


def _ledger_reasons(result) -> list[str]:
    return [entry.reason for entry in result.ledger]


def test_short_term_observed_truth_passes_runoff_through(fake_client_factory) -> None:
    provider = _provider(
        fake_client_factory,
        [
            {
                "horizon": "day",
                "code": "19999",
                "horizon_in_year": 12,
                "year": 2024,
                "discharge": 10.5,
            },
            {
                "horizon": "day",
                "code": "19999",
                "horizon_in_year": 13,
                "year": 2024,
                "discharge": None,
            },
            {
                "horizon": "day",
                "code": "19999",
                "horizon_in_year": 0,
                "year": 2024,
                "discharge": 99.0,
            },
        ],
    )

    result = provider.observed_for("day")

    assert result.values == {("19999", 12, 2024): 10.5}
    assert _ledger_reasons(result) == ["observed_missing", "observed_missing"]


def test_daily_to_month_uses_mean_and_requires_half_the_days(fake_client_factory) -> None:
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=1),
            *_daily_rows(year=2024, month=2, count=14, value=2.0),
        ],
    )

    result = provider.observed_for("month")

    assert result.values == {("19999", 1, 2024): 16.0}
    assert _ledger_reasons(result) == ["observed_incomplete_month"]


def test_daily_to_quarter_uses_monthly_means_and_requires_at_least_two_months(
    fake_client_factory,
) -> None:
    # Q1 (months 1-3): all 3 complete → scored; mean = (3+6+9)/3 = 6.0.
    # Q2 (months 4-6): months 4 and 6 present (2 of 3) → now also scored; mean = (12+18)/2 = 15.0.
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=1, value=3.0),
            *_daily_rows(year=2024, month=2, value=6.0),
            *_daily_rows(year=2024, month=3, value=9.0),
            *_daily_rows(year=2024, month=4, value=12.0),
            *_daily_rows(year=2024, month=6, value=18.0),
        ],
    )

    result = provider.observed_for("quarter")

    assert result.values == {("19999", 1, 2024): 6.0, ("19999", 2, 2024): 15.0}
    assert _ledger_reasons(result) == []


def test_daily_to_default_season_uses_april_to_september(fake_client_factory) -> None:
    # 2024: all 6 season months present → scored; mean = (4+5+6+7+8+9)/6 = 6.5.
    # 2025: 5 of 6 season months present (missing July) → now also scored (5 >= threshold=3);
    # mean = (14+15+16+18+19)/5 = 16.4.
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=4, value=4.0),
            *_daily_rows(year=2024, month=5, value=5.0),
            *_daily_rows(year=2024, month=6, value=6.0),
            *_daily_rows(year=2024, month=7, value=7.0),
            *_daily_rows(year=2024, month=8, value=8.0),
            *_daily_rows(year=2024, month=9, value=9.0),
            *_daily_rows(year=2025, month=4, value=14.0),
            *_daily_rows(year=2025, month=5, value=15.0),
            *_daily_rows(year=2025, month=6, value=16.0),
            *_daily_rows(year=2025, month=8, value=18.0),
            *_daily_rows(year=2025, month=9, value=19.0),
        ],
    )

    result = provider.observed_for("season")

    assert result.values[("19999", 1, 2024)] == pytest.approx(6.5)
    assert result.values[("19999", 1, 2025)] == pytest.approx(16.4)
    assert _ledger_reasons(result) == []


def test_daily_to_season_honors_non_default_month_bounds(fake_client_factory) -> None:
    provider = ObservedTruthProvider(
        ForecastSkillEvalConfig(station_filter=["19999"]),
        client=fake_client_factory(
            runoff_rows=[
                *_daily_rows(year=2024, month=6, value=6.0),
                *_daily_rows(year=2024, month=7, value=7.0),
                *_daily_rows(year=2024, month=8, value=8.0),
            ]
        ),
        observed_config=ObservedTruthConfig(season_start_month=6, season_end_month=8),
    )

    result = provider.observed_for("season")

    assert result.values == {("19999", 1, 2024): 7.0}
    assert result.ledger == ()


# --- Fix 1: relaxed completeness thresholds ---


def test_quarter_with_two_of_three_months_is_scored(fake_client_factory) -> None:
    # Q1 has months 1 and 2 (missing month 3): 2 of 3 → now scored (threshold >= 2).
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=1, value=3.0),
            *_daily_rows(year=2024, month=2, value=6.0),
        ],
    )

    result = provider.observed_for("quarter")

    assert ("19999", 1, 2024) in result.values
    assert result.values[("19999", 1, 2024)] == pytest.approx(4.5)
    assert _ledger_reasons(result) == []


def test_quarter_with_one_of_three_months_is_excluded(fake_client_factory) -> None:
    # Q1 has only month 1 (1 of 3): still excluded (threshold >= 2).
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=1, value=3.0),
        ],
    )

    result = provider.observed_for("quarter")

    assert ("19999", 1, 2024) not in result.values
    assert _ledger_reasons(result) == ["observed_incomplete_quarter"]


def test_season_with_half_or_more_months_present_is_scored(fake_client_factory) -> None:
    # Season Apr-Sep = 6 months; threshold = ceil(0.5 * 6) = 3.
    # 3 months present (4, 5, 6) → exactly at threshold → scored.
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=4, value=4.0),
            *_daily_rows(year=2024, month=5, value=5.0),
            *_daily_rows(year=2024, month=6, value=6.0),
        ],
    )

    result = provider.observed_for("season")

    assert ("19999", 1, 2024) in result.values
    assert result.values[("19999", 1, 2024)] == pytest.approx(5.0)
    assert _ledger_reasons(result) == []


def test_season_with_fewer_than_half_months_present_is_excluded(fake_client_factory) -> None:
    # Season Apr-Sep = 6 months; threshold = 3.
    # Only 2 months present (4, 5) → below threshold → excluded.
    provider = _provider(
        fake_client_factory,
        [
            *_daily_rows(year=2024, month=4, value=4.0),
            *_daily_rows(year=2024, month=5, value=5.0),
        ],
    )

    result = provider.observed_for("season")

    assert ("19999", 1, 2024) not in result.values
    assert _ledger_reasons(result) == ["observed_incomplete_season"]
