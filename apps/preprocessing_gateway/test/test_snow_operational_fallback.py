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

Also covers two 2026-09 out-of-loop review rounds. First round (all
FIXED):

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

Second round, an end-to-end review of the committed result, found three
more HIGH defects plus three MEDIUM (all FIXED):

H1. The completeness check only looked at (date, code) presence, never
    at whether a usable value existed -- a blank (NaN) row from a
    newer issuance could beat a valid older one in the overlap dedup,
    pass completeness, and overwrite a real value with NaN downstream.
H2. Every fetch/read failure other than "No data found" was treated as
    skippable (log + continue), so a transport error on a recent
    issuance could let an older, stale issuance stand in for data that
    genuinely failed to download rather than being confirmed absent.
H3. `required_end = today + window - 2` is reachable only from
    yesterday's issuance -- but if yesterday is the genuinely missing
    run (and today's is routinely unpublished), the newest available
    issuance is today-2, whose window ends one day short. One missing
    day could still stop ingestion in exactly the case this fallback
    exists to fix.
M1. Conflicting duplicate (date, code) rows within a single issuance
    had identical sort keys, so `keep="last"` picked arbitrarily and
    completeness passed regardless.
M2. The assembler and `write_snow_to_api` each independently read
    "today"; a clock rollover between the two calls could drop a row
    from the API write that the assembler's completeness check had
    already accepted for the CSV.
M3. A tz-aware `reference_date` raised during the floor comparison
    before any controlled `None` return.

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

    A `None` entry in `values` renders as an empty CSV cell -- a blank
    upstream value, which `pd.read_csv` (and hence
    `transform_snow_data`) parses as NaN, exactly like a genuinely
    blank field in a real Data Gateway response (PREPG-025 review
    fix H1).
    """
    header_rows = [["h", "h", "h"]] * 4
    data_rows = [
        [d.strftime("%d/%m/%Y"), "" if v is None else str(v), source]
        for d, v in zip(dates, values, strict=True)
    ]
    all_rows = header_rows + data_rows
    df = pd.DataFrame(all_rows, columns=["Timestamp", code, "Source"])
    return df.to_csv(index=False).encode("utf-8")


def _write_forecast_csv(directory, name, dates, code, value):
    """Write a snow-forecast CSV for one issue date to `directory` and
    return its path -- used by `fetch_snow_forecast_for_issue_date`
    side_effect functions below. `value` is the same for every date in
    `dates`; use `_write_forecast_csv_values` for per-date values
    (e.g. one blank day among otherwise-valid ones)."""
    return _write_forecast_csv_values(directory, name, dates, code, [value] * len(dates))


def _write_forecast_csv_values(directory, name, dates, code, values):
    """Like `_write_forecast_csv`, but with one value per date in
    `dates` (may include `None` for a blank cell)."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{name}.csv")
    content = _snow_forecast_csv_bytes(dates, code, values)
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

        # Required window (yesterday .. today + window - 3, PREPG-025
        # review fix H3) for 2026-09-04 is 2026-09-03 .. 2026-09-11.
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
        # end (09-11), is legitimate and must survive.
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
        # required window is 09-03 .. 09-11 (yesterday through the
        # horizon guaranteed even with today AND yesterday both
        # missing, PREPG-025 review fix H3), so 09-03 is missing.
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
        # Yesterday's issuance alone: covers 09-03 .. 09-12, which
        # covers the required window for reference_date 09-04
        # (09-03 .. 09-11) with a day to spare.
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

    def test_newest_blank_older_valid_wins(self, tmp_path):
        """PREPG-025 review fix H1: a blank (NaN) value from the
        NEWER issuance must not beat a real value from an OLDER one.
        transform_snow_data preserves NaN and the completeness check
        only looks at (date, code) presence, so without dropping
        blanks before overlap resolution, the newest-wins rule would
        pick the blank and a real historical value could be
        overwritten with NaN downstream."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        older_dates = pd.date_range("2026-09-02", periods=window, freq="D")  # 09-02..09-11
        newer_dates = pd.date_range("2026-09-03", periods=window, freq="D")  # 09-03..09-12
        newer_values = [None if d == pd.Timestamp("2026-09-05") else 99.0 for d in newer_dates]

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date == "2026-09-02":
                return _write_forecast_csv(directory, "older", older_dates, TEST_CODE, 42.0)
            if issue_date == "2026-09-03":
                return _write_forecast_csv_values(
                    directory, "newer", newer_dates, TEST_CODE, newer_values
                )
            raise ValueError("No data found for that date")

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
        assert result["SWE"].notna().all(), "no blank value should survive into the result"

        def value_on(date_str):
            row = result[(result["date"] == pd.Timestamp(date_str)) & (result["code"] == TEST_CODE)]
            assert len(row) == 1
            return row["SWE"].iloc[0]

        # The newer issuance is blank at 09-05 -- the older, valid
        # issuance's value must win instead of a NaN.
        assert value_on("2026-09-05") == 42.0
        # Sanity check: where the newer issuance IS valid, it still
        # wins normally (shortest lead).
        assert value_on("2026-09-03") == 99.0

    def test_all_issuances_blank_for_a_date_fails_completeness(self, tmp_path):
        """PREPG-025 review fix H1: if EVERY issuance is blank for a
        required (date, code), no row survives to satisfy
        completeness -- the fallback must fail honestly (return None)
        rather than accept a blank value."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        older_dates = pd.date_range("2026-09-02", periods=window, freq="D")
        newer_dates = pd.date_range("2026-09-03", periods=window, freq="D")
        blank_date = pd.Timestamp("2026-09-08")
        older_values = [None if d == blank_date else 42.0 for d in older_dates]
        newer_values = [None if d == blank_date else 99.0 for d in newer_dates]

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date == "2026-09-02":
                return _write_forecast_csv_values(
                    directory, "older", older_dates, TEST_CODE, older_values
                )
            if issue_date == "2026-09-03":
                return _write_forecast_csv_values(
                    directory, "newer", newer_dates, TEST_CODE, newer_values
                )
            raise ValueError("No data found for that date")

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

    def test_completes_from_two_days_ago_when_today_and_yesterday_both_missing(self, tmp_path):
        """PREPG-025 review fix H3: even when BOTH today's issuance
        (routinely unpublished early in the day) AND yesterday's
        issuance (the genuine gap this fallback exists to tolerate)
        are missing, the issuance from two days ago alone must still
        satisfy completeness -- required_end = today + window - 3,
        not today + window - 2 or - 1."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        # 2026-09-02 (today - 2) covers 09-02 .. 09-11, which is
        # exactly the required window (09-03 .. 09-11) with a day to
        # spare on the near side.
        dates = pd.date_range("2026-09-02", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            # Today (2026-09-04) and yesterday (2026-09-03) are both
            # absent -- exactly the compound gap H3 fixes.
            if issue_date != "2026-09-02":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 6.0)

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
        required_dates = set(pd.date_range("2026-09-03", "2026-09-11", freq="D"))
        assert required_dates <= set(result["date"])
        assert (result["SWE"] == 6.0).all()

    def test_conflicting_duplicate_rows_within_one_issuance_are_rejected(self, tmp_path):
        """PREPG-025 review fix M1: two DIFFERENT values for the same
        (date, code) within a SINGLE issuance are a data-integrity
        problem, not something the later keep="last" dedup should
        resolve arbitrarily by whichever way pandas' stable sort
        happens to break the tie -- both conflicting rows must be
        dropped, so this key falls back to another issuance instead."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        # An older, clean issuance covering the whole range -- what
        # the conflicting date should fall back to once the
        # conflicting rows are dropped from the newer issuance.
        older_dates = pd.date_range("2026-09-02", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date == "2026-09-02":
                return _write_forecast_csv(directory, "older", older_dates, TEST_CODE, 15.0)
            if issue_date == "2026-09-03":
                # Hand-built CSV: 2026-09-05 appears TWICE with
                # different values -- an internally conflicting
                # issuance.
                header_rows = [["h", "h", "h"]] * 4
                data_rows = [
                    ["03/09/2026", "20.0", "ERA5"],
                    ["04/09/2026", "20.0", "ERA5"],
                    ["05/09/2026", "20.0", "ERA5"],
                    ["05/09/2026", "999.0", "ERA5"],  # conflicting duplicate
                    ["06/09/2026", "20.0", "ERA5"],
                    ["07/09/2026", "20.0", "ERA5"],
                    ["08/09/2026", "20.0", "ERA5"],
                    ["09/09/2026", "20.0", "ERA5"],
                    ["10/09/2026", "20.0", "ERA5"],
                    ["11/09/2026", "20.0", "ERA5"],
                    ["12/09/2026", "20.0", "ERA5"],
                ]
                df = pd.DataFrame(
                    header_rows + data_rows, columns=["Timestamp", TEST_CODE, "Source"]
                )
                os.makedirs(directory, exist_ok=True)
                path = os.path.join(directory, "conflicting.csv")
                df.to_csv(path, index=False)
                return path
            raise ValueError("No data found for that date")

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

        def value_on(date_str):
            row = result[(result["date"] == pd.Timestamp(date_str)) & (result["code"] == TEST_CODE)]
            assert len(row) == 1
            return row["SWE"].iloc[0]

        # Neither of the conflicting issuance's two values for 09-05
        # (20.0, 999.0) may win by accident -- both were dropped from
        # that issuance, so the older, clean issuance's value is used.
        assert value_on("2026-09-05") == 15.0
        assert 999.0 not in result["SWE"].values
        # A non-conflicting date from the SAME (newer) issuance is
        # unaffected and still wins normally (shortest lead).
        assert value_on("2026-09-04") == 20.0

    def test_valid_blank_duplicate_within_issuance_is_not_a_conflict(self, tmp_path):
        """PREPG-025 review follow-up: the H1 blank filter and the M1
        conflicting-duplicate rejection interact. Both were correct in
        isolation, but in the wrong relative order: if the SAME
        issuance carries a (date, code) key TWICE -- once with a
        valid value, once blank -- and the conflict check (which only
        compares (date, code), not the value) runs first, it
        misclassifies the pair as a genuine conflict and drops BOTH
        rows, discarding the valid value along with the blank. An
        older issuance's stale value would then win completeness by
        default, or, with no older issuance available, ingestion would
        fail outright despite a perfectly good value having been
        downloaded. The blank filter must run first, per issuance, so
        a valid/blank pair collapses to the single valid row and is
        never treated as a conflict at all."""
        reference_date = pd.Timestamp("2026-09-04")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS

        # Older issuance: covers the whole range with a STALE marker
        # value (5.0) -- this must NOT be what wins for 09-05.
        older_dates = pd.date_range("2026-09-02", periods=window, freq="D")

        # Newer issuance: 2026-09-05 appears TWICE -- once valid
        # (20.0), once blank -- everything else is a single valid row.
        newer_dates = list(pd.date_range("2026-09-03", periods=window, freq="D"))
        dup_index = newer_dates.index(pd.Timestamp("2026-09-05"))
        newer_dates = (
            newer_dates[: dup_index + 1]
            + [pd.Timestamp("2026-09-05")]
            + newer_dates[dup_index + 1 :]
        )
        newer_values = [20.0] * len(newer_dates)
        newer_values[dup_index + 1] = None  # the duplicate's blank entry

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date == "2026-09-02":
                return _write_forecast_csv(directory, "older", older_dates, TEST_CODE, 5.0)
            if issue_date == "2026-09-03":
                return _write_forecast_csv_values(
                    directory, "newer", newer_dates, TEST_CODE, newer_values
                )
            raise ValueError("No data found for that date")

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

        # Not a completeness failure: the valid row for 09-05 must
        # have survived the (wrongly-ordered) conflict check.
        assert result is not None

        def value_on(date_str):
            row = result[(result["date"] == pd.Timestamp(date_str)) & (result["code"] == TEST_CODE)]
            assert len(row) == 1, f"expected exactly one row for {date_str}"
            return row["SWE"].iloc[0]

        # The newer issuance's valid value must win -- not the older,
        # stale issuance's value (5.0), and not NaN.
        assert value_on("2026-09-05") == 20.0
        # Sanity check: an unaffected date from the same newer
        # issuance still wins normally.
        assert value_on("2026-09-04") == 20.0

    def test_tz_aware_reference_date_normalises_instead_of_crashing(self, tmp_path):
        """PREPG-025 review fix M3: a tz-aware reference_date must not
        raise during the floor comparison -- it is normalised to
        naive (tzinfo dropped) before use, matching the endpoint's
        timezone-less `date=YYYY-MM-DD` contract."""
        tz_aware_reference_date = pd.Timestamp("2026-09-04").tz_localize("UTC")
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        # Same shape as the "today absent" fixture: yesterday's
        # issuance alone should be enough.
        dates = pd.date_range("2026-09-03", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date != "2026-09-03":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 4.0)

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            # Must not raise (TypeError: tz-naive vs tz-aware).
            result = sdo._assemble_snow_forecast_fallback(
                client=Mock(),
                hru=TEST_HRU,
                variable="SWE",
                dg_path=str(tmp_path),
                existing_codes={TEST_CODE},
                reference_date=tz_aware_reference_date,
            )

        assert result is not None
        required_dates = set(pd.date_range("2026-09-03", "2026-09-11", freq="D"))
        assert required_dates <= set(result["date"])
        assert (result["SWE"] == 4.0).all()


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


class TestTransportErrorAbortsFallback:
    """PREPG-025 review fix H2: a fetch/read failure that is NOT a
    confirmed absent issuance ("No data found") must abort the whole
    fallback, even when older issuances would, on their own, have
    covered the full required window. Silently substituting older
    data for an issuance that merely failed to download would mask a
    genuine transport failure rather than reporting it."""

    @patch("pandas.Timestamp.today")
    @patch("dg_utils.write_snow_to_api")
    def test_transport_error_on_recent_issuance_leaves_csv_untouched_and_no_api_write(
        self, mock_write_api, mock_today, tmp_path
    ):
        window = dg_utils.SNOW_FORECAST_WINDOW_DAYS
        reference_date = pd.Timestamp("2026-09-04")
        mock_today.return_value = reference_date

        save_path = str(tmp_path / "save")
        os.makedirs(os.path.join(save_path, "SWE"), exist_ok=True)
        file_path = os.path.join(save_path, "SWE", f"{TEST_HRU}_SWE.csv")

        # A pre-existing CSV is required for a codes baseline (the
        # fallback refuses outright with none); it also lets this test
        # assert the file is left untouched, not merely absent.
        historical = pd.DataFrame(
            {"date": pd.to_datetime(["2026-08-01"]), "code": [TEST_CODE], "SWE": [7.0]}
        )
        historical.to_csv(file_path, index=False)

        # today - 2 (2026-09-02) succeeds on its own and, by itself,
        # already covers the ENTIRE required window (09-03 .. 09-11)
        # -- so the pre-fix behaviour of skipping a failed fetch would
        # have "succeeded" using this older, potentially stale data,
        # silently masking a transport failure on the newer, correct
        # issuance that genuinely exists.
        older_dates = pd.date_range("2026-09-02", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date == "2026-09-02":
                return _write_forecast_csv(directory, "older", older_dates, TEST_CODE, 11.0)
            if issue_date == "2026-09-03":
                # A transport-style failure, NOT "No data found" --
                # the fix must treat this as fatal, not skippable.
                raise Exception("Connection reset by peer")
            raise ValueError("No data found for that date")

        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )

        with patch(
            "snow_data_operational.dg_utils.fetch_snow_forecast_for_issue_date",
            side_effect=fetch_side_effect,
        ):
            result = sdo.get_snow_data_operational(
                client=mock_client,
                hru=TEST_HRU,
                variable="SWE",
                date="2024-01-01",
                dg_path=str(tmp_path / "dg"),
                save_path=save_path,
            )

        assert result is False
        mock_write_api.assert_not_called()

        # The pre-existing CSV must be left exactly as it was -- no
        # partial/stale write from the aborted fallback.
        written = pd.read_csv(file_path)
        assert len(written) == 1
        assert written["SWE"].iloc[0] == 7.0
        assert pd.to_datetime(written["date"].iloc[0]) == pd.Timestamp("2026-08-01")


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
                reference_date + pd.Timedelta(days=window - 3),
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


class TestSingleReferenceDateSharedWithApiWrite:
    """PREPG-025 review fix M2: the assembler and write_snow_to_api
    must share ONE captured reference date on the fallback path, not
    each independently read wall-clock "today" -- a rollover between
    two separate reads could drop a row from the API write that the
    assembler's completeness check had already accepted for the CSV.

    write_snow_to_api runs for REAL here (only the underlying
    SapphirePreprocessingClient is faked), so its own internal "today"
    read genuinely does or doesn't fire depending on whether the fix
    threads reference_date through, the same pattern
    TestFullStackPositiveFallback uses.
    """

    @patch("snow_data_operational._check_snow_consistency")
    @patch("dg_utils.SapphirePreprocessingClient")
    def test_reference_date_read_exactly_once_and_shared(
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
        pd.DataFrame(
            {"date": pd.to_datetime(["2026-08-01"]), "code": [TEST_CODE], "SWE": [1.0]}
        ).to_csv(file_path, index=False)

        dates = pd.date_range("2026-09-03", periods=window, freq="D")

        def fetch_side_effect(client, hru, variable, issue_date, directory):
            if issue_date != "2026-09-03":
                raise ValueError("No data found for that date")
            return _write_forecast_csv(directory, "forecast", dates, TEST_CODE, 3.0)

        mock_client = Mock()
        mock_client.get_operational.side_effect = Exception(
            "Operational data for HRU 19999 is not available for date 2026-09-01"
        )

        # A single-element side_effect: exactly one real
        # pd.Timestamp.today() call is allowed across the ENTIRE
        # fallback path, including write_snow_to_api's own internal
        # clock read (which runs for real here, unmocked). If either
        # the assembler or write_snow_to_api independently reads
        # "today" a second time -- the defect this fix removes -- that
        # second call raises StopIteration and fails the test loudly,
        # rather than silently using a second, possibly different,
        # value.
        with (
            patch("pandas.Timestamp.today", side_effect=[reference_date]) as mock_today,
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
        assert mock_today.call_count == 1

        # And the POST payload actually used that single reference
        # date's "yesterday" boundary (2026-09-03), not a second,
        # independently-read value.
        assert mock_api_client.write_snow.called
        posted_records = mock_api_client.write_snow.call_args[0][0]
        assert posted_records
        posted_dates = {pd.Timestamp(r["date"]) for r in posted_records}
        assert all(d >= pd.Timestamp("2026-09-03") for d in posted_dates)
