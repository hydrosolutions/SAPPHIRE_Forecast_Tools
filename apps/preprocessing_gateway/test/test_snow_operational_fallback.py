"""
Tests for PREPG-025: the per-issue-date snow-forecast fallback that
tolerates a single missing day in `client.get_operational`'s response.

`get_operational` returns all-or-nothing from its `start_date` to the
forecast horizon, so one missing interior day voids the entire response.
`snow_data_operational._assemble_snow_forecast_fallback` recovers from
exactly that condition by assembling the recent window from per-issue-date
`snow-forecast` calls, whose overlapping windows cover a single absent
issue date. See
doc/plans/issues/mid_prio_gi_draft_prepg_snow_single_day_gap_tolerance.md

Also covers a 2026-09 out-of-loop review round that found three defects
in the first pass of this fallback:

1. HIGH: the assembled window was never floored at `required_start`
   (yesterday), so it could overwrite already-written history with
   stale same-day forecast values via the downstream
   `drop_duplicates(keep="last")` merge.
2. MEDIUM: the required horizon assumed today's own issuance was
   present, which it routinely is not early in the day -- the fallback
   would fail completeness on an ordinary morning.
3. MEDIUM: with no existing CSV, the completeness gate validated the
   fetched response against itself (whatever codes came back defined
   "complete"), so a code missing from every issuance was never
   flagged.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_snow_operational_fallback.py -v
"""

import os
import re
import sys
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client package before importing any module under
# test -- it's a private package not installed in the test environment.
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import dg_utils
import snow_data_operational as sdo

TEST_CODE = "19999"
TEST_HRU = "19999"
OTHER_CODE = "28888"

_ISSUE_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def _snow_forecast_csv_bytes(dates, code, values, source="ERA5"):
    """Build DG-format snow-forecast CSV bytes matching what
    `dg_utils.transform_snow_data` expects: first column renamed
    'date', first 4 rows dropped as metadata, 'Source' column ignored.
    """
    header_rows = [["h", "h", "h"]] * 4
    data_rows = [
        [d.strftime("%d/%m/%Y"), str(v), source] for d, v in zip(dates, values, strict=True)
    ]
    all_rows = header_rows + data_rows
    df = pd.DataFrame(all_rows, columns=["Timestamp", code, "Source"])
    return df.to_csv(index=False).encode("utf-8")


def _write_forecast_csv(directory, name, dates, code, value):
    """Write a snow-forecast CSV for one issue date to `directory` and
    return its path -- used by `fetch_snow_forecast_for_issue_date`
    side_effect functions below."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{name}.csv")
    content = _snow_forecast_csv_bytes(dates, code, [value] * len(dates))
    with open(path, "wb") as f:
        f.write(content)
    return path


class FakeResponse:
    """Minimal stand-in for `requests.Response`, as consumed by
    `dg_utils.fetch_snow_forecast_for_issue_date` (only `.headers` and
    `.content` are touched)."""

    def __init__(self, content: bytes, headers: dict | None = None):
        self.content = content
        self.headers = headers or {}


class FakeSnowClient:
    """Fake `sapphire_dg_client` client that only implements the
    request path `dg_utils.fetch_snow_forecast_for_issue_date` depends
    on: `_call_api` and `_save_file`.

    `value_by_parameter` maps the UPPERCASE `parameter=` value the
    endpoint must carry to the (single, constant) value to return for
    that variable. The CSV content is generated dynamically from the
    issue date actually present in the requested endpoint's `date=`
    query value, covering `issue_date .. issue_date + window - 1` --
    i.e. it honours the real forward-window contract each issuance has,
    rather than returning the same fixed date span regardless of which
    issue date was asked for. This is what pins PREPG-025's
    variable-identity fix: a request that (like the upstream client
    bug) sent `param=<lower>` instead of `parameter=<UPPER>` would not
    match any of these keys and would raise, not silently return HS
    for everything.
    """

    def __init__(self, value_by_parameter: dict, window: int, code: str = TEST_CODE):
        self.value_by_parameter = value_by_parameter
        self.window = window
        self.code = code
        self.calls: list[str] = []

    def _call_api(self, method, endpoint):
        self.calls.append(endpoint)
        match = _ISSUE_DATE_RE.search(endpoint)
        assert match, f"endpoint carries no date=YYYY-MM-DD: {endpoint}"
        issue_date = match.group(1)
        for param_value, value in self.value_by_parameter.items():
            if f"parameter={param_value}" in endpoint:
                dates = pd.date_range(issue_date, periods=self.window, freq="D")
                content = _snow_forecast_csv_bytes(dates, self.code, [value] * len(dates))
                return FakeResponse(content)
        raise ValueError(f"No data found for endpoint {endpoint}")

    @staticmethod
    def _save_file(response, directory, filename):
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, filename)
        with open(path, "wb") as f:
            f.write(response.content)
        return path


# =============================================================================
# Gap-error classification (dg_utils)
# =============================================================================


class TestGapErrorClassification:
    """dg_utils.is_snow_operational_gap_error /
    is_snow_forecast_no_data_error only match their specific marker
    text, case-insensitively."""

    @pytest.mark.parametrize(
        "message",
        [
            "Operational data for HRU 19999 is not available for date 2026-09-01",
            "Failed to get data from api/x: NOT AVAILABLE FOR DATE 2026-09-01",
        ],
    )
    def test_gap_error_recognized(self, message):
        assert dg_utils.is_snow_operational_gap_error(Exception(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "connection timed out",
            "Unauthorized. Please check your API key!",
            "No reanalysis data available for spin-up",
        ],
    )
    def test_other_errors_not_recognized_as_gap(self, message):
        assert dg_utils.is_snow_operational_gap_error(Exception(message)) is False

    def test_no_data_found_recognized(self):
        assert dg_utils.is_snow_forecast_no_data_error(Exception("No data found for this date"))

    def test_other_errors_not_recognized_as_no_data(self):
        assert not dg_utils.is_snow_forecast_no_data_error(Exception("connection timed out"))


# =============================================================================
# fetch_snow_forecast_for_issue_date (dg_utils) -- variable identity, unit
# =============================================================================


class TestFetchSnowForecastForIssueDate:
    """dg_utils.fetch_snow_forecast_for_issue_date must call the
    endpoint with `parameter=<UPPER>`, not the broken client's
    `param=<lower>` (PREPG-025)."""

    @pytest.mark.parametrize(
        "variable, expected_param", [("HS", "HS"), ("SWE", "SWE"), ("RoF", "ROF")]
    )
    def test_request_carries_correct_parameter_name_and_case(
        self, tmp_path, variable, expected_param
    ):
        client = FakeSnowClient({expected_param: 1.0}, window=dg_utils.SNOW_FORECAST_WINDOW_DAYS)

        dg_utils.fetch_snow_forecast_for_issue_date(
            client, TEST_HRU, variable, "2026-09-04", str(tmp_path)
        )

        assert len(client.calls) == 1
        endpoint = client.calls[0]
        assert f"parameter={expected_param}" in endpoint
        # The upstream client bug sends "param=<lower>"; make sure our
        # fix does not also send that (mere substring of "parameter="
        # would be a false pass, so check the lowercase key-with-equals
        # form specifically).
        assert "param=" not in endpoint


# =============================================================================
# _assemble_snow_forecast_fallback -- overlap resolution, completeness,
# the required-window floor, variable identity end-to-end
# =============================================================================


class TestAssembleSnowForecastFallback:
    def test_overlap_resolution_prefers_shorter_lead(self, tmp_path):
        """With both an N-1 and an N-2 day issuance available for a
        target date, the N-1 (shorter lead) value is the one written --
        AND the exact returned date range is pinned, not just the
        winning value. This is the direct regression test for review
        defect #1 (the missing required_start floor): a stale, much
        older issuance (2026-08-25) is also present here and would,
        without the floor, leak dates before required_start into the
        result.
        """
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS  # 10

        # Required window (yesterday .. today + window - 2) for
        # 2026-09-04 is 2026-09-03 .. 2026-09-12.
        #
        # - 2026-09-04 (today):    covers 09-04 .. 09-13, value 5.0.
        # - 2026-09-03 (yesterday): covers 09-03 .. 09-12, value 9.0.
        # - 2026-08-25 (today - window, the oldest candidate issue
        #   date): covers 08-25 .. 09-03, value 1.0. Its own window
        #   reaches all the way to 09-03, and WITHOUT the
        #   required_start floor, its 08-25 .. 09-02 rows would have
        #   leaked into the result -- exactly what would silently
        #   overwrite real history at the caller.
        issuances = {
            "2026-09-04": (pd.date_range("2026-09-04", periods=window, freq="D"), 5.0),
            "2026-09-03": (pd.date_range("2026-09-03", periods=window, freq="D"), 9.0),
            "2026-08-25": (pd.date_range("2026-08-25", periods=window, freq="D"), 1.0),
        }

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date not in issuances:
                raise ValueError("No data found for that date")
            dates, value = issuances[issue_date]
            return _write_forecast_csv(directory, f"forecast_{issue_date}", dates, TEST_CODE, value)

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE},
                reference_date=reference_date,
            )

        assert result is not None

        # Exact returned date range: floored at required_start
        # (2026-09-03, yesterday) -- 08-25 .. 09-02 must be entirely
        # absent, even though the 08-25 issuance's own window reaches
        # into that range. NOT capped at the upper end: 2026-09-13,
        # supplied only by today's issuance and beyond the required
        # end (09-12), is legitimate and must survive.
        expected_dates = set(pd.date_range("2026-09-03", "2026-09-13", freq="D"))
        assert set(result["date"]) == expected_dates
        assert result["date"].min() == pd.Timestamp("2026-09-03")
        assert (result["date"] >= pd.Timestamp("2026-09-03")).all()
        # The stale 08-25 issuance's marker value must not appear
        # anywhere in the result.
        assert 1.0 not in result["SWE"].values

        def value_on(date_str):
            row = result[(result["date"] == pd.Timestamp(date_str)) & (result["code"] == TEST_CODE)]
            assert len(row) == 1, f"expected exactly one row for {date_str}"
            return row["SWE"].iloc[0]

        # 2026-09-05 is covered by both issuances (lead 1 from
        # 09-04, lead 2 from 09-03) -- the newer/shorter-lead
        # issuance (09-04, value 5.0) must win.
        assert value_on("2026-09-05") == 5.0
        # 2026-09-03 is covered by the 09-03 issuance (lead 0) and the
        # 08-25 issuance (lead 9) -- the newer one (09-03) must win.
        assert value_on("2026-09-03") == 9.0
        # Sole source for 09-13 is the 09-04 issuance (09-03's window
        # ends at 09-12).
        assert value_on("2026-09-13") == 5.0

        # The issue date must not be written out.
        assert "_issue_date" not in result.columns

    def test_incomplete_window_returns_none(self, tmp_path):
        """A missing required (date, code) pair means the whole
        fallback assembly fails closed -- no partial window."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        # Only one issuance succeeds, covering 09-04 .. 09-13. The
        # required window is 09-03 .. 09-12 (yesterday through the
        # horizon yesterday's own issuance would supply), so 09-03 is
        # missing.
        dates = pd.date_range("2026-09-04", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date != "2026-09-04":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 1.0)

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE},
                reference_date=reference_date,
            )

        assert result is None

    def test_no_issue_dates_available_returns_none(self, tmp_path):
        """Every issue date fails -> no data to assemble at all."""
        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=ValueError("No data found for that date"),
        ):
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE},
                reference_date=pd.Timestamp("2026-09-04"),
            )
        assert result is None

    def test_completes_from_yesterdays_issuance_alone_when_today_absent(self, tmp_path):
        """Review defect #2: today's own issuance is routinely absent
        early in the day (measured 2026-09-04) -- the fallback must
        still complete using ONLY yesterday's issuance, not require
        today's."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        # Yesterday's issuance alone: covers 09-03 .. 09-12, which is
        # exactly the required window for reference_date 09-04.
        dates = pd.date_range("2026-09-03", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            # Today's issuance (2026-09-04) is deliberately absent.
            if issue_date != "2026-09-03":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 4.0)

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE},
                reference_date=reference_date,
            )

        assert result is not None
        expected_dates = set(pd.date_range("2026-09-03", "2026-09-12", freq="D"))
        assert set(result["date"]) == expected_dates
        assert (result["SWE"] == 4.0).all()

    def test_missing_baseline_code_never_covered_returns_none(self, tmp_path):
        """Review defect #3: an independent multi-code baseline
        (`existing_codes` with more codes than the fetched issuances
        ever return) must still fail completeness for the code that
        never appears -- the gate must not validate itself only
        against whichever codes happened to come back."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        # Yesterday's issuance fully covers the required window, but
        # only for TEST_CODE -- OTHER_CODE never appears in any
        # issuance's CSV at all.
        dates = pd.date_range("2026-09-03", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date != "2026-09-03":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 1.0)

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE, OTHER_CODE},
                reference_date=reference_date,
            )

        assert result is None

    @pytest.mark.parametrize(
        "existing_codes", [None, set(), frozenset()], ids=["none", "empty-set", "empty-frozenset"]
    )
    def test_no_existing_codes_baseline_refuses_without_fetching(self, tmp_path, existing_codes):
        """Review defect #3, direct guard test: with no codes baseline
        at all (fresh CSV), the fallback refuses immediately -- it
        does not even attempt to fetch, since there is nothing
        trustworthy to validate completeness against."""
        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date"
        ) as mock_fetch:
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes=existing_codes,
                reference_date=pd.Timestamp("2026-09-04"),
            )

        assert result is None
        mock_fetch.assert_not_called()

    @pytest.mark.parametrize(
        "variable, expected_param", [("HS", "HS"), ("SWE", "SWE"), ("RoF", "ROF")]
    )
    def test_variable_identity_end_to_end(self, tmp_path, variable, expected_param):
        """Distinct HS/SWE/RoF payloads: the request carries
        `parameter=<UPPER>` and the assembled values are the requested
        variable's, not cross-contaminated with another variable's
        data (the upstream client bug this wrapper avoids). Uses
        FakeSnowClient, whose fake response respects the real
        per-issuance forward-window contract instead of returning a
        fixed date span regardless of which issue date was requested.
        """
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        value_by_parameter = {"HS": 111.0, "SWE": 222.0, "ROF": 333.0}
        expected_value = value_by_parameter[expected_param]

        client = FakeSnowClient(value_by_parameter, window=window)

        result = sdo._assemble_snow_forecast_fallback(
            client=client,
            hru=TEST_HRU,
            variable=variable,
            dg_path=str(tmp_path),
            existing_codes={TEST_CODE},
            reference_date=reference_date,
        )

        assert result is not None
        assert variable in result.columns
        assert (result[variable] == expected_value).all()

        # Every request the fake client saw must carry the correct
        # parameter, and never the broken "param=" form.
        assert client.calls
        for endpoint in client.calls:
            assert f"parameter={expected_param}" in endpoint
            assert "param=" not in endpoint


# =============================================================================
# get_snow_data_operational -- trigger condition, happy path
# =============================================================================


class TestFallbackTriggerCondition:
    """The fallback must trigger ONLY on the 'not available for date'
    response; every other failure keeps today's exact behaviour: log
    and return False, without attempting the fallback."""

    @patch("snow_data_operational._assemble_snow_forecast_fallback")
    def test_other_error_does_not_trigger_fallback(self, mock_fallback, tmp_path):
        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception("connection timed out")

        result = sdo.get_snow_data_operational(
            client=mock_client,
            hru=TEST_HRU,
            variable="SWE",
            date="2024-01-01",
            dg_path=str(tmp_path / "dg"),
            save_path=str(tmp_path / "save"),
        )

        assert result is False
        mock_fallback.assert_not_called()

    @patch("snow_data_operational._check_snow_consistency")
    @patch("dg_utils.write_snow_to_api")
    @patch("snow_data_operational._assemble_snow_forecast_fallback")
    def test_gap_error_triggers_fallback(self, mock_fallback, mock_write_api, mock_check, tmp_path):
        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )
        mock_fallback.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-03"]),
                "code": [TEST_CODE],
                "SWE": [1.0],
            }
        )
        mock_write_api.return_value = True
        mock_check.return_value = True

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)

        result = sdo.get_snow_data_operational(
            client=mock_client,
            hru=TEST_HRU,
            variable="SWE",
            date="2024-01-01",
            dg_path=str(tmp_path / "dg"),
            save_path=save_path,
        )

        assert result is True
        mock_fallback.assert_called_once()


class TestHappyPathNoFallback:
    """When get_operational succeeds, nothing new (the fallback)
    executes."""

    @patch("snow_data_operational._check_snow_consistency")
    @patch("dg_utils.write_snow_to_api")
    @patch("snow_data_operational.pd.read_csv")
    @patch("snow_data_operational.dg_utils.transform_snow_data")
    @patch("snow_data_operational._assemble_snow_forecast_fallback")
    def test_fallback_not_called_on_success(
        self, mock_fallback, mock_transform, mock_read_csv, mock_write_api, mock_check, tmp_path
    ):
        mock_transform.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "code": [TEST_CODE],
                "SWE": [100.0],
            }
        )
        mock_read_csv.return_value = pd.DataFrame({"raw": ["data"]})
        mock_write_api.return_value = True
        mock_check.return_value = True

        mock_client = Mock()
        mock_client.get_operational.return_value = "/tmp/fake.csv"

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)

        result = sdo.get_snow_data_operational(
            client=mock_client,
            hru=TEST_HRU,
            variable="SWE",
            date="2024-01-01",
            dg_path=str(tmp_path / "dg"),
            save_path=save_path,
        )

        assert result is True
        mock_fallback.assert_not_called()


class TestIncompleteWindowWritesNothing:
    """A missing required (date, code) coverage returns False and
    performs no write at all -- no CSV, no API POST."""

    @patch("dg_utils.write_snow_to_api")
    def test_no_csv_and_no_api_write_on_incomplete_window(self, mock_write_api, tmp_path):
        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)
        file_path = os.path.join(save_path, "SWE", f"{TEST_HRU}_SWE.csv")

        with patch("snow_data_operational._assemble_snow_forecast_fallback", return_value=None):
            result = sdo.get_snow_data_operational(
                client=mock_client,
                hru=TEST_HRU,
                variable="SWE",
                date="2024-01-01",
                dg_path=str(tmp_path / "dg"),
                save_path=save_path,
            )

        assert result is False
        assert not os.path.exists(file_path)
        mock_write_api.assert_not_called()


class TestSnowPreservationReadErrorEscapesFallbackPath:
    """PREPG-020: a SnowPreservationReadError from write_snow_to_api
    must still escape uncaught when the data came via the PREPG-025
    fallback path, not just the primary get_operational path (the
    existing test at test_api_integration.py:941 covers only primary
    success)."""

    @patch("snow_data_operational._check_snow_consistency")
    @patch("dg_utils.write_snow_to_api")
    @patch("snow_data_operational._assemble_snow_forecast_fallback")
    def test_preservation_error_propagates_uncaught(
        self, mock_fallback, mock_write_api, mock_check, tmp_path
    ):
        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )
        mock_fallback.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-03"]),
                "code": [TEST_CODE],
                "SWE": [1.0],
            }
        )
        mock_write_api.side_effect = dg_utils.SnowPreservationReadError("boom")
        mock_check.return_value = True

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)

        with pytest.raises(dg_utils.SnowPreservationReadError):
            sdo.get_snow_data_operational(
                client=mock_client,
                hru=TEST_HRU,
                variable="SWE",
                date="2024-01-01",
                dg_path=str(tmp_path / "dg"),
                save_path=save_path,
            )


# =============================================================================
# get_snow_data_operational -- full-stack positive fallback: what
# actually reaches the CSV and the API write
# =============================================================================


class TestFullStackPositiveFallback:
    """End-to-end through get_snow_data_operational: one missing
    interior issuance, assembly succeeds via overlap, and the complete
    window reaches both sinks -- while pre-existing history strictly
    before the required window (yesterday) is left untouched (review
    defect #1's regression, exercised at the full-stack level rather
    than just inside the assembler).

    `dg_utils.write_snow_to_api` runs for real here (only the
    underlying `SapphirePreprocessingClient` is faked) so the actual
    POST payload -- after write_snow_to_api's own `date >= yesterday`
    operational-window filter -- can be inspected directly, the same
    pattern `test_api_integration.py::TestWriteSnowToApi` uses.
    """

    @patch("snow_data_operational._check_snow_consistency")
    @patch("dg_utils.SapphirePreprocessingClient")
    def test_missing_interior_issuance_recovers_and_preserves_history(
        self, mock_client_class, mock_check, tmp_path, monkeypatch
    ):
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

        mock_api_client = Mock()
        mock_api_client.readiness_check.return_value = True
        mock_api_client.read_snow.return_value = pd.DataFrame()
        mock_api_client.write_snow.return_value = 0
        mock_client_class.return_value = mock_api_client

        mock_check.return_value = True

        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)
        file_path = os.path.join(save_path, "SWE", f"{TEST_HRU}_SWE.csv")

        # Pre-existing CSV: real history, including a date
        # (2026-09-02) that is BEFORE required_start (2026-09-03,
        # yesterday) but still inside the wide candidate issue-date
        # range (today - window .. today) some issuances below reach
        # into. This is exactly the row review defect #1 protects: it
        # must survive untouched.
        historical = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-08-01", "2026-09-02"]),
                "code": [TEST_CODE, TEST_CODE],
                "SWE": [42.0, 55.0],
            }
        )
        historical.to_csv(file_path, index=False)

        # Issuances: 09-03 (yesterday) and 09-04 (today) both present;
        # 09-01 -- an interior issue date within the wide candidate
        # range -- is absent (simulating the measured gap), covered
        # instead by its neighbours' overlapping windows. 08-25
        # (today - window, the oldest candidate) is ALSO present and
        # would, without review defect #1's fix, leak a stale forecast
        # value (999.0) into 2026-09-02's slot.
        issuances = {
            "2026-08-25": (pd.date_range("2026-08-25", periods=window, freq="D"), 999.0),
            "2026-09-03": (pd.date_range("2026-09-03", periods=window, freq="D"), 7.0),
            "2026-09-04": (pd.date_range("2026-09-04", periods=window, freq="D"), 8.0),
        }

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date not in issuances:
                raise ValueError("No data found for that date")
            dates, value = issuances[issue_date]
            return _write_forecast_csv(directory, f"forecast_{issue_date}", dates, TEST_CODE, value)

        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )

        with (
            patch("pandas.Timestamp.today", return_value=reference_date),
            patch(
                "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
                side_effect=fetch_side_effect,
            ),
        ):
            result = sdo.get_snow_data_operational(
                client=mock_client,
                hru=TEST_HRU,
                variable="SWE",
                date="2024-01-01",
                dg_path=str(tmp_path / "dg"),
                save_path=save_path,
            )

        assert result is True

        # Every required (date, code) in the window -- yesterday
        # through the horizon yesterday's issuance alone can supply --
        # computed independently of the assembler's own math, so this
        # doesn't just re-check the production formula against itself.
        required_dates = list(
            pd.date_range(
                reference_date - pd.Timedelta(days=1),
                reference_date + pd.Timedelta(days=window - 2),
                freq="D",
            )
        )

        # -- What reached the CSV --
        written = pd.read_csv(file_path)
        written["date"] = pd.to_datetime(written["date"])
        written["code"] = written["code"].astype(str)

        # Old history well outside the fallback window: untouched.
        row_0801 = written[written["date"] == pd.Timestamp("2026-08-01")]
        assert len(row_0801) == 1
        assert row_0801["SWE"].iloc[0] == 42.0

        # The critical regression check for review defect #1:
        # 2026-09-02 is BEFORE required_start (2026-09-03, yesterday),
        # so even though the 08-25 issuance's forward window nominally
        # reaches it, the fallback must not have touched it -- its
        # original historical value must survive.
        row_0902 = written[written["date"] == pd.Timestamp("2026-09-02")]
        assert len(row_0902) == 1
        assert row_0902["SWE"].iloc[0] == 55.0

        # No fallback-sourced value leaked into any date before the
        # required window (yesterday): none of the stale/forecast
        # marker values appear on a pre-window date.
        stale_fallback_values = {999.0, 7.0, 8.0}
        pre_window = written[written["date"] < pd.Timestamp("2026-09-03")]
        assert not pre_window["SWE"].isin(stale_fallback_values).any()

        # The recovered window itself: present and correct. 09-03 is
        # sourced by its own issuance (7.0), not the older 08-25
        # issuance (999.0).
        row_0903 = written[written["date"] == pd.Timestamp("2026-09-03")]
        assert len(row_0903) == 1
        assert row_0903["SWE"].iloc[0] == 7.0

        # Completeness, not just the one date probed above: a
        # reviewer flagged that checking only 2026-09-03 would still
        # pass if some other required date were silently dropped from
        # the CSV. Every required date for TEST_CODE must be present.
        written_code_dates = set(written.loc[written["code"] == TEST_CODE, "date"])
        missing_from_csv = [d for d in required_dates if d not in written_code_dates]
        assert not missing_from_csv, f"required dates missing from CSV: {missing_from_csv}"

        # -- What reached the API POST --
        # write_snow_to_api ran for real here; its own operational
        # `date >= yesterday` filter has already been applied by the
        # time client.write_snow(records) is called, so this checks
        # the actual outbound payload, not merely write_snow_to_api's
        # input.
        assert mock_api_client.write_snow.called
        posted_records = mock_api_client.write_snow.call_args[0][0]
        assert posted_records, "expected at least one record to be posted"
        posted_dates = {pd.Timestamp(r["date"]) for r in posted_records}
        assert not any(d < pd.Timestamp("2026-09-03") for d in posted_dates), (
            "no date earlier than yesterday may reach the API POST"
        )
        # And the recovered window's value made it through, not the
        # stale older-issuance one.
        posted_by_date = {pd.Timestamp(r["date"]): r["value"] for r in posted_records}
        assert posted_by_date[pd.Timestamp("2026-09-03")] == 7.0

        # Completeness for the POST payload too, same reasoning as the
        # CSV check above: every required date for TEST_CODE must be
        # present in what was actually posted, not just 2026-09-03.
        posted_code_dates = {
            pd.Timestamp(r["date"]) for r in posted_records if str(r["code"]) == TEST_CODE
        }
        missing_from_post = [d for d in required_dates if d not in posted_code_dates]
        assert not missing_from_post, f"required dates missing from API POST: {missing_from_post}"
