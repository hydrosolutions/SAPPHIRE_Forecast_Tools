"""PP-076: recover only missing ensembles at each mode's latest raw issuance."""

import datetime as dt
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import postprocessing_maintenance_long_term as maintenance
import pytest
from src import api_writer, ensemble_calculator, gap_detector

ASOF = dt.date(2030, 2, 28)
MODELS = {"Naive Mean", "Skilled Mean"}


def raw_issue(lead=1, issue="2030-02-25", month=3, code="SYNTHETIC_A"):
    return pd.DataFrame(
        [
            dict(
                code=code,
                year=2030,
                month=month,
                month_in_year=month,
                horizon_value=lead,
                date=issue,
                model_short=model,
                q=point,
                q50=median,
                valid_from=f"2030-{month:02d}-01",
                valid_to=(pd.Timestamp(2030, month, 1) + pd.offsets.MonthEnd()).strftime(
                    "%Y-%m-%d"
                ),
                q05=1.0,
                q10=2.0,
                q25=3.0,
                q75=80.0,
                q90=90.0,
                q95=100.0,
            )
            for model, point, median in [("LR_Base", 0.0, 40.0), ("GBT", 20.0, 60.0)]
        ]
    )


def skills(raw):
    result = raw[["month_in_year", "code", "model_short", "horizon_value"]].drop_duplicates().copy()
    return result.assign(sdivsigma=0.2, nse=0.9, delta=5.0, accuracy=0.9, mae=2.0, n_pairs=20)


def detect(frame, leads=frozenset({0, 1, 2, 3}), **kwargs):
    return gap_detector.detect_missing_monthly_ensembles(
        frame, forecast_date=ASOF, configured_leads=leads, **kwargs
    )


def generated(raw):
    return ensemble_calculator.create_monthly_ensemble_forecasts(raw, skills(raw)).query(
        "model_short in @MODELS"
    )


@pytest.fixture(autouse=True)
def thresholds(monkeypatch):
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", ".6")
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold", ".8")
    monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", ".8")


@pytest.mark.parametrize("flag", ["false", "true"])
def test_latest_issue_global_per_mode_includes_zero_and_ignores_old_targets(monkeypatch, flag):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
    latest_dates = {0: "2030-02-25", 1: "2030-02-25", 2: "2030-02-20", 3: "2030-02-18"}
    frames = []
    for lead, issue in latest_dates.items():
        frames += [raw_issue(lead, issue, lead + 2), raw_issue(lead, "2030-01-25", lead + 1)]
    # A station absent from the latest issue must not fall back to its older issue.
    frames.append(raw_issue(1, "2030-02-20", 3, "SYNTHETIC_OLD"))
    combined = pd.concat(frames, ignore_index=True)
    original = combined.copy(deep=True)
    gaps = detect(combined)
    assert len(gaps) == 8
    assert set(gaps.horizon_value) == {0, 1, 2, 3}
    assert set(zip(gaps.horizon_value, gaps.month, gaps.date, strict=True)) == {
        (lead, lead + 2, pd.Timestamp(issue)) for lead, issue in latest_dates.items()
    }
    assert set(gaps.code) == {"SYNTHETIC_A"}
    pd.testing.assert_frame_equal(combined, original)


def test_old_ensemble_cannot_mask_latest_and_ensemble_cannot_choose_issue():
    old = raw_issue(issue="2030-02-20")
    old_ensembles = generated(old)
    # A future ensemble must neither select an issue nor obscure the latest raw gap.
    newer_ensemble = old_ensembles.assign(date="2030-02-27")
    combined = pd.concat([old, old_ensembles, newer_ensemble, raw_issue()], ignore_index=True)
    gaps = detect(combined)
    assert len(gaps) == 2
    assert set(gaps.date) == {pd.Timestamp("2030-02-25")}


@pytest.mark.parametrize("invalid", [None, pd.NaT, "NaT", "", "bad", "2030-03-01", 12])
def test_invalid_missing_future_dates_do_not_replace_latest(invalid, caplog):
    combined = pd.concat([raw_issue(), raw_issue(issue=invalid)], ignore_index=True)
    with caplog.at_level("INFO"):
        gaps = detect(combined)
    assert len(gaps) == 2
    assert set(gaps.date) == {pd.Timestamp("2030-02-25")}
    assert "issue dates=2" in caplog.text


@pytest.mark.parametrize("lead", [None, np.inf, 1.5, -1, 9, "bad"])
def test_invalid_unconfigured_leads_excluded(lead, caplog):
    combined = pd.concat([raw_issue(), raw_issue(lead=lead)], ignore_index=True)
    with caplog.at_level("INFO"):
        gaps = detect(combined)
    assert len(gaps) == 2
    assert set(gaps.horizon_value) == {1}
    assert "unconfigured leads=2" in caplog.text


def test_issue_window_anchored_to_captured_date_inclusive():
    at_boundary = raw_issue(issue="2029-11-28")
    assert len(detect(at_boundary)) == 2
    assert detect(raw_issue(issue="2029-11-27")).empty


@pytest.mark.parametrize("point", [None, np.nan, np.inf, -np.inf])
def test_nonfinite_ensemble_is_missing(point):
    raw = raw_issue()
    existing = generated(raw)
    existing.loc[existing.model_short == "Naive Mean", "forecasted_discharge"] = point
    # Stored point absence must not be hidden by raw quantile or q columns.
    existing["q50"] = np.nan
    combined = pd.concat([raw, existing], ignore_index=True)
    gaps = detect(combined)
    assert gaps.model_short.tolist() == ["Naive Mean"]


def test_zero_ensemble_point_is_complete_even_without_quantiles():
    raw = raw_issue()
    existing = generated(raw)
    existing["forecasted_discharge"] = 0.0
    existing[existing.filter(regex="^q[0-9]").columns] = np.nan
    assert detect(pd.concat([raw, existing], ignore_index=True)).empty


def test_builder_separates_issue_dates_and_preserves_raw_dates_and_quantiles():
    raw = pd.concat([raw_issue(issue="2030-02-20"), raw_issue()], ignore_index=True)
    raw.loc[2:, "q"] = [100.0, 200.0]
    original = raw.copy(deep=True)
    result = ensemble_calculator.create_monthly_ensemble_forecasts(raw, skills(raw))
    naive = result[result.model_short == "Naive Mean"]
    assert dict(zip(naive.date, naive.forecasted_discharge, strict=True)) == {
        pd.Timestamp("2030-02-20"): 10.0,
        pd.Timestamp("2030-02-25"): 150.0,
    }
    pd.testing.assert_frame_equal(raw, original)
    result_raw = result[~result.model_short.isin(MODELS)].reset_index(drop=True)
    assert result_raw.date.tolist() == original.date.tolist()
    pd.testing.assert_frame_equal(
        result_raw[original.filter(regex="^q").columns], original.filter(regex="^q")
    )


@pytest.mark.parametrize("invalid", [None, "bad"])
def test_builder_never_mixes_missing_issue_with_valid_issue(invalid):
    raw = raw_issue()
    raw.loc[1, "date"] = invalid
    result = ensemble_calculator.create_monthly_ensemble_forecasts(raw, skills(raw))
    assert result[result.model_short.isin(MODELS)].empty


def test_mixed_calendar_date_types_normalize_for_exact_gap_join():
    raw = raw_issue()
    raw["date"] = pd.Series([dt.date(2030, 2, 25), "2030-02-25T17:15:00"], dtype=object)
    existing = generated(raw)
    existing["date"] = "2030-02-25T00:00:00+02:00"
    assert detect(pd.concat([raw, existing], ignore_index=True)).empty


@pytest.fixture
def prepare_maintenance(monkeypatch):
    def prepare(combined):
        monkeypatch.setattr(
            maintenance.data_reader,
            "read_monthly_combined_forecasts",
            MagicMock(return_value=combined),
        )
        monkeypatch.setattr(
            maintenance.data_reader,
            "read_skill_metrics",
            MagicMock(return_value=skills(combined[~combined.model_short.isin(MODELS)])),
        )
        second_read = MagicMock(side_effect=AssertionError("must use same combined snapshot"))
        monkeypatch.setattr(maintenance.data_reader, "read_monthly_forecasts", second_read)
        monkeypatch.setattr(
            maintenance,
            "available_monthly_batch_modes",
            lambda: ["month_0", "month_1", "month_2", "month_3"],
        )
        monkeypatch.setattr(maintenance, "operational_lead_for_mode", lambda mode: int(mode[-1]))
        monkeypatch.setattr(maintenance.pt, "log_most_recent_forecasts_monthly", MagicMock())
        return second_read

    return prepare


@pytest.mark.parametrize("flag", ["false", "true"])
def test_maintenance_writes_only_missing_latest_and_rerun_is_noop(
    monkeypatch, prepare_maintenance, flag
):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
    raw = raw_issue()
    existing = generated(raw).query("model_short == 'Naive Mean'")
    combined = pd.concat([raw, existing, raw_issue(issue="2030-01-25", month=2)], ignore_index=True)
    prepare_maintenance(combined)
    writer = MagicMock(return_value=True)
    monkeypatch.setattr(maintenance.api_writer, "_write_monthly_ensemble_to_api", writer)
    errors = []
    completed, gaps = maintenance._run_monthly_gap_fill(
        ["SYNTHETIC_A"], 3, errors, forecast_date=ASOF
    )
    assert completed and not errors
    assert gaps.model_short.tolist() == ["Skilled Mean"]
    written = writer.call_args.args[0]
    assert len(written) == 1
    assert written.iloc[0].model_short == "Skilled Mean"
    assert written.iloc[0].horizon_value == 1
    assert written.iloc[0].date == pd.Timestamp("2030-02-25")
    assert written.iloc[0].forecasted_discharge == pytest.approx(10.0)
    prepare_maintenance(pd.concat([combined, written], ignore_index=True))
    writer.reset_mock()
    completed, gaps = maintenance._run_monthly_gap_fill(
        ["SYNTHETIC_A"], 3, errors, forecast_date=ASOF
    )
    assert not completed and gaps.empty and not errors
    writer.assert_not_called()


def test_latest_insufficient_members_has_no_old_model_fallback(monkeypatch, prepare_maintenance):
    raw = pd.concat([raw_issue(issue="2030-02-20"), raw_issue().iloc[:1]], ignore_index=True)
    prepare_maintenance(raw)
    writer = MagicMock(return_value=True)
    monkeypatch.setattr(maintenance.api_writer, "_write_monthly_ensemble_to_api", writer)
    completed, gaps = maintenance._run_monthly_gap_fill(["SYNTHETIC_A"], 3, [], forecast_date=ASOF)
    assert not completed and len(gaps) == 2
    writer.assert_not_called()


def test_api_failure_propagates_to_errors(monkeypatch, prepare_maintenance):
    prepare_maintenance(raw_issue())
    monkeypatch.setattr(
        maintenance.api_writer, "_write_monthly_ensemble_to_api", lambda data: False
    )
    errors = []
    completed, _ = maintenance._run_monthly_gap_fill(["SYNTHETIC_A"], 3, errors, forecast_date=ASOF)
    assert not completed
    assert errors == ["Monthly gap-fill save failed: API write returned False"]


def test_final_api_records_preserve_issue_lead_bounds_composition(monkeypatch, prepare_maintenance):
    raw = pd.concat([raw_issue(lead, month=lead + 2) for lead in range(4)], ignore_index=True)
    raw.loc[raw.model_short == "GBT", "q"] = 20.2469
    prepare_maintenance(raw)
    client = MagicMock()
    client.readiness_check.return_value = True
    monkeypatch.setattr(api_writer, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(api_writer, "_get_postprocessing_client", lambda: client)
    monkeypatch.setattr(api_writer, "_check_write_codes", lambda *args: None)
    monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
    errors = []
    completed, _ = maintenance._run_monthly_gap_fill(["SYNTHETIC_A"], 3, errors, forecast_date=ASOF)
    assert completed and not errors
    records = client.write_long_forecasts.call_args.args[0]
    assert len(records) == 8
    for record in records:
        lead = record["horizon_value"]
        assert record["date"] == "2030-02-25"
        assert record["valid_from"] == f"2030-{lead + 2:02d}-01"
        assert record["valid_to"] == raw.loc[raw.horizon_value == lead, "valid_to"].iloc[0]
        assert record["composition"] == "GBT, LR_Base"
        assert record["q"] == 10.123
        assert record["q50"] == pytest.approx(50.0)


def test_real_combined_normalizer_cannot_manufacture_month_zero():
    from src.data_reader import _normalize_monthly_combined_forecasts

    valid = raw_issue(0, "2030-02-20", 2)
    invalid = pd.concat(
        [raw_issue(lead, "2030-02-25", 2) for lead in (None, 0.5, "bad", np.inf)], ignore_index=True
    )
    raw = pd.concat([valid, invalid], ignore_index=True).rename(
        columns={"model_short": "model_type"}
    )
    normalized = _normalize_monthly_combined_forecasts(raw)
    assert normalized.horizon_value.iloc[:2].eq(0).all()
    assert normalized.horizon_value.iloc[2:4].isna().all()
    assert normalized.horizon_value.iloc[4:6].eq(0.5).all()
    selected = gap_detector.select_latest_monthly_issues(
        normalized,
        forecast_date=ASOF,
        configured_leads={0, 1, 2, 3},
    )
    assert len(selected) == 2
    assert set(selected.date) == {pd.Timestamp("2030-02-20")}
    assert set(selected.horizon_value) == {0}


def test_latest_null_points_do_not_fall_back_to_old_finite_members(
    monkeypatch, prepare_maintenance
):
    latest = raw_issue().assign(q=np.nan, q50=np.nan)
    combined = pd.concat([raw_issue(issue="2030-02-20"), latest], ignore_index=True)
    prepare_maintenance(combined)
    writer = MagicMock(return_value=True)
    monkeypatch.setattr(maintenance.api_writer, "_write_monthly_ensemble_to_api", writer)
    completed, gaps = maintenance._run_monthly_gap_fill(["SYNTHETIC_A"], 3, [], forecast_date=ASOF)
    assert not completed
    assert len(gaps) == 2
    assert set(gaps.date) == {pd.Timestamp("2030-02-25")}
    writer.assert_not_called()


def test_nullable_stored_point_is_missing():
    raw = raw_issue()
    existing = generated(raw).assign(q50=np.nan)
    existing["forecasted_discharge"] = pd.Series(
        [pd.NA, pd.NA], index=existing.index, dtype="Float64"
    )
    assert len(detect(pd.concat([raw, existing], ignore_index=True))) == 2
