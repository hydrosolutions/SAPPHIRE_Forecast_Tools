"""Additional monthly models survive quarterly reads, scoring and persistence."""
import datetime as dt
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src import api_writer, data_reader
from src.aggregation import aggregate_monthly_obs_to_quarterly
from src.model_names import QUARTERLY_RAW_MODELS, canonical_model_short_series
from src.skill_metrics import calculate_quarterly_skill_metrics
from src.ensemble_calculator import create_quarterly_ensemble_forecasts

MODELS = ['LR_Base', 'LR_SM', 'GBT', 'LR_SM_DT', 'LR_SM_ROF',
          'MC_ALD', 'SM_GBT', 'SM_GBT_LR', 'SM_GBT_Norm']


@pytest.fixture
def monthly_inputs(monkeypatch, tmp_path):
    (tmp_path / 'quarter.json').write_text(json.dumps({
        'operational_month_lead_time': 1, 'operational_issue_day': 25,
    }))
    monkeypatch.setenv('ieasyforecast_configuration_path', str(tmp_path))
    monkeypatch.setenv('ieasyhydroforecast_ml_long_term_configuration', '.')
    monkeypatch.setenv('ieasyhydroforecast_ml_long_term_supported_modes', 'quarter')
    monkeypatch.setenv('ieasyhydroforecast_min_pairs_long_term_quarter', '5')
    monkeypatch.setenv('SAPPHIRE_API_ENABLED', 'true')
    monkeypatch.setenv('ieasyforecast_config_file_station_selection', 'missing.json')
    forecasts, observations = [], []
    for year in range(2019, 2025):
        for month in (4, 5, 6):
            q = float(20 + (year - 2019) * 5 + month)
            observations.append(dict(code='19999', year=year, month=month, discharge_avg=q))
            for model in MODELS:
                forecasts.append(dict(
                    code='19999', year=year, month=month, model_short=model,
                    date=f'{year}-03-25', horizon_value=month-3,
                    valid_from=f'{year}-{month:02d}-01',
                    valid_to=f'{year}-{month:02d}-{30 if month in (4,6) else 31}',
                    q=q + 0.1, q50=None,
                ))
    return pd.DataFrame(forecasts), pd.DataFrame(observations)


@pytest.mark.parametrize('flag', ['true', 'false'])
def test_extra_models_reach_historical_latest_skill_and_api(monthly_inputs, monkeypatch, flag):
    monkeypatch.setenv('SAPPHIRE_SKILL_LEAD_AWARE', flag)
    monthly, observations = monthly_inputs
    raw = monthly.rename(columns={'model_short': 'model_type'})

    direct = raw[raw.model_type.isin(['LR_Base', 'LR_SM']) & raw.month.eq(4)].copy()
    direct['valid_to'] = direct.year.astype(str) + '-06-30'
    direct['q'] += 1.0  # Direct LR values differ from their first monthly value.

    def read_api(codes, start_year, end_year, horizon_type='month', **kwargs):
        return raw.copy() if horizon_type == 'month' else direct.copy()

    with patch.object(data_reader, 'read_monthly_forecasts', return_value=monthly), \
         patch.object(data_reader, '_read_long_forecasts_api', side_effect=read_api):
        historical = data_reader.read_quarterly_forecasts(['19999'], 2019, 2024)
        latest = data_reader.read_latest_quarterly_forecasts(['19999'], dt.date(2024, 3, 25))

    assert set(canonical_model_short_series(historical.model_short)) == QUARTERLY_RAW_MODELS
    assert len(historical) == 6 * len(MODELS)
    assert set(latest.model_short) == set(MODELS)
    assert set(latest.horizon_value) == {1}
    assert set(latest.date) == {pd.Timestamp('2024-03-25')}
    assert latest.forecasted_discharge.notna().all()
    assert latest.q50.isna().all()

    obs = aggregate_monthly_obs_to_quarterly(observations)
    skills, joint, _ = calculate_quarterly_skill_metrics(obs, historical)
    raw_skills = skills[skills.model_short.isin(MODELS)]
    assert set(raw_skills.model_short) == set(MODELS)
    assert set(raw_skills.n_pairs) == {6}
    assert raw_skills.nse.gt(.99).all()
    em = joint[joint.model_short.eq('EM')]
    assert not em.empty
    assert em.composition.str.contains('LR_Base').all()
    assert em.composition.str.contains('GBT').all()
    assert em.composition.str.contains('MC_ALD').all()
    assert em.composition.str.split(', ').map(len).eq(len(MODELS)).all()
    naive = joint[joint.model_short.eq('Naive Mean')]
    assert not naive.empty
    assert naive.composition.str.contains('MC_ALD').all()
    operational = create_quarterly_ensemble_forecasts(latest, skills)
    assert set(MODELS).issubset(set(operational.model_short))
    operational_em = operational[operational.model_short.eq('EM')]
    assert len(operational_em) == 1
    assert operational_em.composition.str.split(', ').map(len).eq(len(MODELS)).all()
    assert operational_em.forecasted_discharge.iloc[0] == pytest.approx(latest.forecasted_discharge.mean())
    assert operational[operational.model_short.eq('Naive Mean')].composition.str.contains('MC_ALD').all()

    client = MagicMock()
    client.readiness_check.return_value = True
    with patch.object(api_writer, 'SAPPHIRE_API_AVAILABLE', True), \
         patch.object(api_writer, '_get_postprocessing_client', return_value=client):
        assert api_writer._write_quarterly_ensemble_to_api(latest)
        api_writer._write_skill_metrics_to_api(raw_skills, 'quarter', 2026)
    records = client.write_long_forecasts.call_args.args[0]
    assert {r['model_type'] for r in records} == set(MODELS)
    assert {r['date'] for r in records} == {'2024-03-25'}
    assert {r['horizon_value'] for r in records} == {1}
    skill_records = client.write_skill_metrics.call_args.args[0]
    assert {r['model_type'] for r in skill_records} == set(MODELS)


def test_quarterly_eligibility_does_not_expand_seasonal_models():
    frame = pd.DataFrame({'model_short': MODELS + ['EM', 'Naive Mean', 'Skilled Mean', 'UNKNOWN']})
    quarter = data_reader._filter_supported_aggregated_forecast_models(frame, horizon_type='quarter')
    season = data_reader._filter_supported_aggregated_forecast_models(frame)
    assert set(quarter.model_short) == set(MODELS + ['EM', 'Naive Mean', 'Skilled Mean'])
    assert set(season.model_short) == {'LR_Base', 'LR_SM', 'EM', 'Naive Mean', 'Skilled Mean'}


@pytest.mark.parametrize("direct_first", [True, False])
def test_direct_and_derived_date_formats_both_reach_skill(monthly_inputs, monkeypatch, direct_first):
    """Reader output must not lose direct LR rows when mixed with derived MC rows."""
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
    monthly, observations = monthly_inputs
    raw = monthly[monthly.model_short.eq("LR_SM") & monthly.month.eq(4)].copy()
    raw = raw.rename(columns={"model_short": "model_type"})
    raw["valid_to"] = raw.year.astype(str) + "-06-30"
    raw["q"] = raw["q"] + 1  # distinct direct three-month point prediction
    with patch.object(data_reader, "read_monthly_forecasts", return_value=monthly), \
         patch.object(data_reader, "_read_long_forecasts_api", return_value=raw):
        forecasts = data_reader.read_quarterly_forecasts(["19999"], 2019, 2024)
    forecasts = forecasts[forecasts.model_short.isin(["LR_SM", "MC_ALD"])].copy()
    forecasts = forecasts.sort_values("model_short", ascending=direct_first)
    # Actual reader output contains both date-only and timestamp strings.
    assert forecasts.valid_from.str.len().nunique() == 2
    obs = aggregate_monthly_obs_to_quarterly(observations)
    skills, _, _ = calculate_quarterly_skill_metrics(obs, forecasts)
    raw_skills = skills[skills.model_short.isin(["LR_SM", "MC_ALD"])]
    assert set(raw_skills.model_short) == {"LR_SM", "MC_ALD"}
    assert set(raw_skills.n_pairs) == {6}
    assert raw_skills.nse.notna().all()
