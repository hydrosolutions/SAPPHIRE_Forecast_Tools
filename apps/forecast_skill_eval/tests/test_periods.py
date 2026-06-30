from __future__ import annotations

import pytest

from forecast_skill_eval.periods import (
    join_key_for_horizon,
    long_term_calendar_period,
    normalize_horizon,
    short_term_join_key,
)


def test_horizon_in_year_is_short_term_key_and_decad_normalizes() -> None:
    assert normalize_horizon("decad") == "decade"

    for horizon in ("day", "pentad", "decad", "decade"):
        assert short_term_join_key(horizon) == "horizon_in_year"
        assert join_key_for_horizon(horizon) == "horizon_in_year"


def test_horizon_value_is_never_returned_as_a_join_key() -> None:
    for horizon in ("day", "pentad", "decade", "month", "quarter", "season"):
        assert join_key_for_horizon(horizon) != "horizon_value"


@pytest.mark.parametrize(
    ("horizon", "valid_from", "valid_to", "expected"),
    [
        ("month", "2024-02-01", "2024-02-29", (2, True)),
        ("quarter", "2024-04-01", "2024-06-30", (2, True)),
        ("season", "2024-04-01", "2024-09-30", (1, True)),
        ("month", "2024-02-15", "2024-03-14", (2, False)),
        ("quarter", "2024-02-01", "2024-04-30", (1, False)),
        ("season", "2024-04-15", "2024-09-30", (1, False)),
    ],
)
def test_long_term_calendar_key_and_rolling_detection(
    horizon: str,
    valid_from: str,
    valid_to: str,
    expected: tuple[int, bool],
) -> None:
    assert long_term_calendar_period(horizon, valid_from, valid_to) == expected
