# DOC-004: Update doc/data_flow_long_term.md with seasonal pipeline and model output specs

**Priority**: Medium
**Module**: doc, long_term_forecasting, postprocessing_forecasts
**Assigned**: @sandrohurni, @mabesa (to cross-check)
**Status**: Draft
**Created**: 2026-03-26

## Problem

`doc/data_flow_long_term.md` only documents the monthly pipeline. The seasonal
and quarterly forecast paths exist in code and produce data in the database, but
are absent from the documentation. Additionally, the document incorrectly states
that all models produce full quantile distributions — this is only true for
MC_ALD.

These gaps were identified during a database review on 2026-03-26. The full
evidence is in `doc/dev/lt_forecast_db_dump_2026-03-26.md` (gitignored, local
only).

## Documentation gaps to address

### 1. Seasonal pipeline (missing entirely)

The seasonal forecast path is undocumented:
- `aggregate_monthly_fc_to_seasonal()` in `postprocessing_forecasts/src/aggregation.py`
  creates seasonal records by averaging monthly forecasts for Apr–Sep.
- `_write_aggregated_forecasts_to_api()` in `api_writer.py` writes them with
  `horizon_type=season`, `horizon_value=1`, `date=valid_from` (= YYYY-04-01).
- `SAPPHIRE_SEASON_START_MONTH` (default 4) and `SAPPHIRE_SEASON_END_MONTH`
  (default 9) control the season window.
- The `date` field on seasonal records is set to the season start date
  (YYYY-04-01), not the forecast production date.

**Action**: Add a "Seasonal Aggregation" section to the data flow doc, including
a Mermaid diagram showing monthly records → aggregation → seasonal records.

### 2. Model output capabilities (incorrect/incomplete)

The doc states (line 121–122): "Each forecast includes a full quantile
distribution (q05–q95) and a validity period." This is only true for MC_ALD.

Actual model output capabilities (verified against upstream `lt_forecasting`
library code):

| Model class | Models | q (point) | q05–q95 | q50 | q_loc | q_xgb/lgbm/catboost |
|---|---|---|---|---|---|---|
| `UncertaintyMixtureModel` | MC_ALD | yes | yes | yes | yes | no |
| `LinearRegressionModel` (BayesianRidge) | LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF | yes | q05,q10,q25,q75,q90,q95 | **no** (0.50 excluded from BAYESIAN_QUANTILES) | no | no |
| `SciRegressor` | GBT, SM_GBT, SM_GBT_LR, SM_GBT_Norm | yes | no | no | no | yes |

**Action**: Replace the blanket "full quantile distribution" claim with this
table. Note that q50 is absent for LR models (upstream design choice in
`BAYESIAN_QUANTILES`).

### 3. Key Data Transformations table (line 208)

The table shows `q50 | Median forecast | 45.2` as if all models produce q50.
This is misleading — only MC_ALD populates q50. LR models populate q (the point
forecast) but not q50. GBT models populate q only.

**Action**: Update the table to show `q` as the primary forecast field and note
that `q50` is only populated by MC_ALD.

### 4. Differences from Short-Term table (line 238)

The table says long-term horizon types are only `month`. Should include `season`
(and potentially `quarter`).

**Action**: Update to `month, season` (and `quarter` if deployed).

### 5. Season/quarter config files

The module loads mode configs dynamically at runtime from an external config
directory (via `.env` → `CONFIG_PATH/LT_CONFIGS/{forecast_mode}.json`). Only
`config_monthly.json` is in the repo. Season and quarter configs live in the
operator's external config directory.

`lt_schedule_query.py` maps:
```python
HORIZON_TYPE_TO_SKILL = {
    "month": "MONTHLY",
    "quarter": "QUARTERLY",
    "season": "SEASONAL",
}
```

**Action**: Document that season/quarter configs are external, and describe the
config structure needed for a seasonal mode (`calendar_month_adjustment=false`,
`target_start_month`, `target_end_month`).

## Acceptance criteria

- [ ] `doc/data_flow_long_term.md` includes a seasonal aggregation section with
      Mermaid diagram
- [ ] Model output table accurately reflects which fields each model class
      produces
- [ ] `date=YYYY-04-01` convention for seasonal records is documented
- [ ] `SAPPHIRE_SEASON_START_MONTH`/`END_MONTH` env vars are documented
- [ ] Differences table updated to include `season` horizon type
- [ ] External config file convention documented

## References

- Evidence: `doc/dev/lt_forecast_db_dump_2026-03-26.md` (local, gitignored)
- Seasonal aggregation code: `apps/postprocessing_forecasts/src/aggregation.py:278-344`
- API writer: `apps/postprocessing_forecasts/src/api_writer.py:986-1013`
- Season env vars: `apps/postprocessing_forecasts/README.md:220-221`
- Schedule query: `apps/long_term_forecasting/lt_schedule_query.py:44-48`
- Upstream quantile constants: `lt_forecasting/forecast_models/LINEAR_REGRESSION.py:44-45`
