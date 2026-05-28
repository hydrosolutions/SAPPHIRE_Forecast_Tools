"""PP-036 archive-union contract tests for ML forecast reads."""

import os
import sys
from dataclasses import dataclass, field
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_reader import _normalize_ml_forecasts, _read_ml_forecasts_pp_api

STATION_CODE = "19999"


@dataclass
class FakePostprocessingClient:
    """In-memory postprocessing client keyed by (horizon, skip)."""

    pages: dict[tuple[str, int], pd.DataFrame]
    calls: list[dict] = field(default_factory=list)

    def readiness_check(self):
        return True

    def read_short_term_forecasts(self, **kwargs):
        self.calls.append(dict(kwargs))
        horizon = kwargs["horizon"]
        skip = kwargs["skip"]
        page = self.pages.get((horizon, skip), pd.DataFrame())
        return page.copy()


def _period_row(date, value, horizon_type="pentad"):
    return pd.DataFrame(
        {
            "code": [STATION_CODE],
            "date": [date],
            "target": [(pd.Timestamp(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")],
            "forecasted_discharge": [value],
            "horizon_type": [horizon_type],
        }
    )


def _day_fan(date, values):
    start = pd.Timestamp(date) + pd.Timedelta(days=1)
    return pd.DataFrame(
        {
            "code": [STATION_CODE] * len(values),
            "date": [date] * len(values),
            "target": [
                (start + pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(len(values))
            ],
            "forecasted_discharge": values,
            "horizon_type": ["day"] * len(values),
        }
    )


def _read_with_fake_client(
    pages,
    horizon_type="pentad",
    start_year=2024,
    end_year=2024,
):
    fake_client = FakePostprocessingClient(pages=pages)
    with (
        patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True),
        patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
        patch(
            "src.data_reader.SapphirePostprocessingClient",
            create=True,
            return_value=fake_client,
        ),
    ):
        result = _read_ml_forecasts_pp_api(
            "TFT",
            horizon_type,
            codes=[STATION_CODE],
            start_year=start_year,
            end_year=end_year,
        )
    return result, fake_client


def _called_horizons(fake_client):
    return [call["horizon"] for call in fake_client.calls if call["skip"] == 0]


def _assert_called_archives(fake_client, expected):
    assert set(_called_horizons(fake_client)) == set(expected)


def _assert_single_code_date_model(result, expected_dates):
    keys = result[["code", "date", "model_short"]].drop_duplicates()
    assert len(keys) == len(result)
    actual = set(
        zip(
            keys["code"],
            keys["date"].dt.strftime("%Y-%m-%d"),
            keys["model_short"],
            strict=True,
        )
    )
    assert actual == {(STATION_CODE, date, "TFT") for date in expected_dates}


class TestMlHorizonArchiveSplit:
    """PP-036: DAY archive and period archive form a cutover union."""

    @pytest.mark.parametrize(
        ("horizon_type", "api_horizon", "issue_date", "value"),
        [
            ("pentad", "pentad", "2024-01-05", 101.0),
            ("decad", "decade", "2024-01-10", 202.0),
        ],
    )
    def test_period_history_without_day_rows_is_returned_and_normalized(
        self,
        horizon_type,
        api_horizon,
        issue_date,
        value,
    ):
        period_rows = _period_row(issue_date, value, horizon_type=api_horizon)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): pd.DataFrame(),
                (api_horizon, 0): period_rows,
            },
            horizon_type=horizon_type,
        )

        assert_frame_equal(raw.reset_index(drop=True), period_rows)
        _assert_called_archives(fake_client, ["day", api_horizon])

        result = _normalize_ml_forecasts(raw, "TFT", horizon_type)
        _assert_single_code_date_model(result, [issue_date])
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(value)

    @pytest.mark.parametrize(
        ("horizon_type", "api_horizon", "issue_date", "values", "expected"),
        [
            (
                "pentad",
                "pentad",
                "2024-01-05",
                [10.0, 20.0, 30.0, 40.0, 50.0],
                30.0,
            ),
            ("decad", "decade", "2024-01-10", list(range(10, 110, 10)), 55.0),
        ],
    )
    def test_day_history_without_period_rows_is_returned_unchanged(
        self,
        horizon_type,
        api_horizon,
        issue_date,
        values,
        expected,
    ):
        day_rows = _day_fan(issue_date, values)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): day_rows,
                (api_horizon, 0): pd.DataFrame(),
            },
            horizon_type=horizon_type,
        )

        assert_frame_equal(raw.reset_index(drop=True), day_rows)
        _assert_called_archives(fake_client, ["day", api_horizon])

        result = _normalize_ml_forecasts(raw, "TFT", horizon_type)
        _assert_single_code_date_model(result, [issue_date])
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(expected)

    def test_day_and_period_same_issue_date_uses_day_without_duplicate(self):
        day_rows = _day_fan("2024-01-05", [10.0, 20.0, 30.0, 40.0, 50.0])
        period_rows = _period_row("2024-01-05", 900.0)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): day_rows,
                ("pentad", 0): period_rows,
            }
        )

        _assert_called_archives(fake_client, ["day", "pentad"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")

        _assert_single_code_date_model(result, ["2024-01-05"])
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_period_rows_do_not_fill_day_era_outage_dates(self):
        day_rows = pd.concat(
            [
                _day_fan("2024-01-05", [10.0, 20.0, 30.0, 40.0, 50.0]),
                _day_fan("2024-01-15", [60.0, 70.0, 80.0, 90.0, 100.0]),
            ],
            ignore_index=True,
        )
        period_rows = _period_row("2024-01-10", 777.0)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): day_rows,
                ("pentad", 0): period_rows,
            }
        )

        _assert_called_archives(fake_client, ["day", "pentad"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")

        _assert_single_code_date_model(result, ["2024-01-05", "2024-01-15"])
        assert "2024-01-10" not in set(result["date"].dt.strftime("%Y-%m-%d"))

    def test_no_ml_history_in_either_archive_preserves_none(self):
        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): pd.DataFrame(),
                ("pentad", 0): pd.DataFrame(),
            }
        )

        assert raw is None
        _assert_called_archives(fake_client, ["day", "pentad"])

    def test_recalc_straddling_day_cutover_uses_older_period_and_newer_day_rows(self):
        day_rows = _day_fan("2024-01-10", [20.0, 30.0, 40.0, 50.0, 60.0])
        period_rows = _period_row("2024-01-05", 111.0)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): day_rows,
                ("pentad", 0): period_rows,
            },
            start_year=2023,
            end_year=2024,
        )

        _assert_called_archives(fake_client, ["day", "pentad"])
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")

        _assert_single_code_date_model(result, ["2024-01-05", "2024-01-10"])
        by_date = {
            row.date.strftime("%Y-%m-%d"): row.forecasted_discharge for row in result.itertuples()
        }
        assert by_date["2024-01-05"] == pytest.approx(111.0)
        assert by_date["2024-01-10"] == pytest.approx(40.0)

    @pytest.mark.parametrize(
        ("horizon_type", "api_horizon", "issue_date", "values"),
        [
            ("pentad", "pentad", "2024-01-05", [1.0, 2.0, 3.0, 4.0, 5.0]),
            ("decad", "decade", "2024-01-10", list(range(1, 11))),
        ],
    )
    def test_api_call_sequence_reads_day_and_requested_period_archive(
        self,
        horizon_type,
        api_horizon,
        issue_date,
        values,
    ):
        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): _day_fan(issue_date, values),
                (api_horizon, 0): pd.DataFrame(),
            },
            horizon_type=horizon_type,
        )

        assert raw is not None
        _assert_called_archives(fake_client, ["day", api_horizon])
        for call in fake_client.calls:
            assert call["code"] == STATION_CODE
            assert call["model"] == "TFT"
            assert call["start_date"] == "2024-01-01"
            assert call["end_date"] == "2024-12-31"

    def test_paginates_both_archives_before_building_union(self):
        day_page_1 = _day_fan("2024-01-10", [1.0] * 1000)
        day_page_2 = _day_fan("2024-01-15", [2.0])
        period_page_1 = _period_row("2024-01-05", 3.0)
        period_page_1 = pd.concat([period_page_1] * 1000, ignore_index=True)
        period_page_2 = _period_row("2024-01-01", 4.0)

        raw, fake_client = _read_with_fake_client(
            {
                ("day", 0): day_page_1,
                ("day", 1000): day_page_2,
                ("pentad", 0): period_page_1,
                ("pentad", 1000): period_page_2,
            },
            start_year=2023,
            end_year=2024,
        )

        assert raw is not None
        calls = [(call["horizon"], call["skip"]) for call in fake_client.calls]
        assert ("day", 0) in calls
        assert ("day", 1000) in calls
        assert ("pentad", 0) in calls
        assert ("pentad", 1000) in calls
