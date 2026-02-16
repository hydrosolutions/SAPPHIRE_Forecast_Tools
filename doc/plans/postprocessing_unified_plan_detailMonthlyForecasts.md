# Phase 4a: Monthly Skill Metrics — Detailed Implementation Plan

> **Parent document:** [`postprocessing_unified_plan.md`](postprocessing_unified_plan.md) — see Phase 4 section for overall scope (monthly + quarterly + seasonal) and how this fits into the broader postprocessing refactoring (Phases 1–5).
>
> **This document:** Detailed step-by-step implementation plan for **monthly skill metrics only** (Phase 4a). Quarterly and seasonal will follow in Phase 4b (deferred).

---

## Quick Reference

| Question | Answer |
|----------|--------|
| What is this? | Implementation plan for monthly skill metrics in `postprocessing_forecasts` |
| Where does it fit? | Phase 4a of `postprocessing_unified_plan.md` |
| What temporal resolution? | Monthly only (Phase 4b covers quarterly + seasonal) |
| Which forecasts? | Long-term forecasts from `long_term_forecasting` module (stored in `long_forecasts` table) |
| Which observations? | Daily runoff aggregated to monthly means (from preprocessing API) |
| Point metrics? | NSE, MAE, accuracy, sdivsigma — using Q50 as point forecast |
| Probabilistic metric? | CRPS — using full quantile distribution (Q05–Q95) |
| sapphire-api-client changes? | **Not implemented here** — see [Pre-requisite](#pre-requisite-sapphire-api-client-llm-instructions) for LLM instructions to be executed in the separate `sapphire-api-client` repo |

---

## Context

The postprocessing module currently computes skill metrics at **pentad** (5-day) and **decad** (10-day) resolutions. Phase 4 extends this to monthly, quarterly, and seasonal resolutions. This plan covers **monthly only** (Phase 4a) — quarterly and seasonal will follow in Phase 4b.

Monthly skill metrics require:
- Reading monthly **forecasts** from the `long_forecasts` table (produced by `long_term_forecasting` module)
- Aggregating daily **observations** to monthly means (from preprocessing API)
- Computing **point metrics** (NSE, MAE, accuracy, sdivsigma) using Q50 as forecast vs observed mean
- Computing **CRPS** (probabilistic metric) using the full quantile distribution (Q05–Q95)
- Writing results to CSV + API

**What already exists:**
- API schema supports `horizon_type=month` for skill metrics (`sapphire/services/postprocessing/app/models.py`)
- `long_forecasts` table stores monthly forecasts with quantiles Q05–Q95 (from `long_term_forecasting` module)
- `calculate_all_skill_metrics()` computes point metrics and is reusable at any resolution
- Daily runoff available via preprocessing API (`horizon="day"`)
- Monthly aggregation logic exists in `apps/long_term_forecasting/post_process_lt_forecast.py:168-242`

---

## Pre-requisite: sapphire-api-client (LLM Instructions)

> **IMPORTANT — Out of scope for this repo.** The `sapphire-api-client` is a separate project (`hydrosolutions/sapphire-api-client`). We do **not** implement these changes here. Instead, this section provides **detailed instructions for an LLM** (or developer) working in that separate repository. Once completed there, we bump the pinned commit hash in `apps/postprocessing_forecasts/pyproject.toml` and run `uv sync`.

### Instructions for the sapphire-api-client repository

**Repo**: `hydrosolutions/sapphire-api-client`
**File to modify**: `src/sapphire_api_client/postprocessing.py`

**Task**: Add three items to `SapphirePostprocessingClient`: `read_long_forecasts()`, `write_long_forecasts()`, and `prepare_long_forecast_records()`. These must be consistent with the server-side API defined in the SAPPHIRE postprocessing service.

#### Server-side API reference (already deployed)

The postprocessing service exposes two endpoints for long forecasts. The client methods must match these exactly.

**GET `/long-forecast/`** — query parameters:

| Parameter | Type | Maps to DB column | Filter behavior |
|-----------|------|-------------------|-----------------|
| `horizon_type` | string | `LongForecast.horizon_type` | `== horizon_type` (enum: `day`, `pentad`, `decade`, `month`, `quarter`, `season`) |
| `horizon_value` | int | `LongForecast.horizon_value` | `== horizon_value` |
| `code` | string | `LongForecast.code` | `== code` |
| `model` | string | `LongForecast.model_type` | `== model` (**note**: query param is `model`, DB column is `model_type`) |
| `start_date` | string (YYYY-MM-DD) | `LongForecast.date` | `>= start_date` (forecast issue date) |
| `end_date` | string (YYYY-MM-DD) | `LongForecast.date` | `<= end_date` |
| `valid_from` | string (YYYY-MM-DD) | `LongForecast.valid_from` | `>= valid_from` |
| `valid_to` | string (YYYY-MM-DD) | `LongForecast.valid_to` | `<= valid_to` |
| `skip` | int (default 0) | — | Pagination offset |
| `limit` | int (default 100) | — | Max records returned |

**POST `/long-forecast/`** — request body schema:

```json
{
  "data": [
    {
      "horizon_type": "month",
      "horizon_value": 1,
      "code": "15013",
      "date": "2024-06-15",
      "model_type": "GBT",
      "valid_from": "2024-07-01",
      "valid_to": "2024-07-31",
      "flag": 0,
      "composition": "",
      "q": 123.45,
      "q_obs": 120.0,
      "q_xgb": 125.0,
      "q_lgbm": 124.0,
      "q_catboost": 123.0,
      "q_loc": 122.0,
      "q05": 100.0,
      "q10": 110.0,
      "q25": 115.0,
      "q50": 123.0,
      "q75": 130.0,
      "q90": 135.0,
      "q95": 140.0
    }
  ]
}
```

Upsert on unique key: `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)` — 7 fields.

Returns HTTP 201 with list of created/updated records (each includes an `id` field and a computed `model_type_description` string).

**Response fields per record** (all fields from the request, plus):

| Field | Type | Notes |
|-------|------|-------|
| `id` | int | Auto-generated primary key |
| `model_type_description` | string | Computed from `model_type` enum. Has human-readable names for short-term models (e.g., `"TFT"` → `"Temporal Fusion Transformer (TFT)"`). **Long-term models currently fall through to the raw value** (e.g., `"GBT"` → `"GBT"`). Descriptions will be added server-side in `models.py ModelType.description` before this client work begins. |

#### LongForecast record fields (all columns)

Required fields (form the unique key):
- `horizon_type`: string — one of `"day"`, `"pentad"`, `"decade"`, `"month"`, `"quarter"`, `"season"`
- `horizon_value`: int — which period (e.g., month 1–12, quarter 1–4)
- `code`: string — station code (max 10 chars)
- `date`: date — forecast issue date (when the forecast was made)
- `model_type`: string — one of `"TSMixer"`, `"TiDE"`, `"TFT"`, `"EM"`, `"NE"`, `"RRAM"`, `"LR"`, `"GBT"`, `"LR_Base"`, `"LR_SM"`, `"LR_SM_DT"`, `"LR_SM_ROF"`, `"MC_ALD"`, `"SM_GBT"`, `"SM_GBT_LR"`, `"SM_GBT_Norm"`, `"Skilled Mean"`, `"Naive Mean"`
- `valid_from`: date — start of the target period the forecast covers
- `valid_to`: date — end of the target period the forecast covers

Optional fields (nullable):
- `flag`: int
- `composition`: string (max 100 chars) — which models composed the ensemble
- `q`: float — combined/ensemble forecast value
- `q_obs`: float — observed value
- `q_xgb`: float — XGBoost model prediction
- `q_lgbm`: float — LightGBM model prediction
- `q_catboost`: float — CatBoost model prediction
- `q_loc`: float — local model prediction
- `q05`, `q10`, `q25`, `q50`, `q75`, `q90`, `q95`: float — quantile predictions

#### Existing client patterns to follow

The `SapphirePostprocessingClient` in `src/sapphire_api_client/postprocessing.py` already has `read_forecasts()` / `write_forecasts()`. The new methods must follow the identical pattern. Here is the exact existing code to use as template:

```python
# EXISTING read_forecasts — use as template for read_long_forecasts
def read_forecasts(
    self,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    target: Optional[Union[str, date]] = None,
    start_target: Optional[Union[str, date]] = None,
    end_target: Optional[Union[str, date]] = None,
    skip: int = 0,
    limit: int = 100,
) -> pd.DataFrame:
    params: Dict[str, Any] = {"skip": skip, "limit": limit}
    if horizon:
        params["horizon"] = horizon
    if code:
        params["code"] = code
    if model:
        params["model"] = model
    if start_date:
        params["start_date"] = str(start_date)
    if end_date:
        params["end_date"] = str(end_date)
    if target:
        params["target"] = str(target)
    if start_target:
        params["start_target"] = str(start_target)
    if end_target:
        params["end_target"] = str(end_target)

    records = self._get("/forecast/", params=params)
    return pd.DataFrame(records) if records else pd.DataFrame()

# EXISTING write_forecasts — use as template for write_long_forecasts
def write_forecasts(self, records: List[Dict[str, Any]]) -> int:
    return self._post_batched("/forecast/", records)
```

The base class `SapphireAPIClient` (in `client.py`) provides:
- `self._get(endpoint, params)` → `List[Dict[str, Any]]` — GET with retry logic
- `self._post_batched(endpoint, records)` → `int` — POST in batches of `self.batch_size` (default 1000), wraps each batch as `{"data": batch}`, returns total count

#### Implementation: `read_long_forecasts()`

```python
# ==================== LONG FORECASTS ====================

def read_long_forecasts(
    self,
    horizon_type: Optional[str] = None,
    horizon_value: Optional[int] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    valid_from: Optional[Union[str, date]] = None,
    valid_to: Optional[Union[str, date]] = None,
    skip: int = 0,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Read long forecast data from the API.

    Long forecasts are produced by the long_term_forecasting module and
    stored in the long_forecasts table. They include quantile predictions
    (Q05-Q95) and a validity period (valid_from/valid_to).

    Args:
        horizon_type: Horizon type filter ("month", "quarter", "season")
        horizon_value: Horizon value filter (e.g., 1-12 for months)
        code: Station code filter
        model: Model type filter (GBT, LR_Base, SM_GBT, MC_ALD, etc.)
        start_date: Start date filter for forecast issue date (inclusive)
        end_date: End date filter for forecast issue date (inclusive)
        valid_from: Filter: valid_from >= this value
        valid_to: Filter: valid_to <= this value
        skip: Pagination offset
        limit: Maximum records

    Returns:
        DataFrame with long forecast data. Empty DataFrame if no records.
        Columns include: horizon_type, horizon_value, code, date,
        model_type, valid_from, valid_to, flag, composition,
        q, q_obs, q_xgb, q_lgbm, q_catboost, q_loc,
        q05, q10, q25, q50, q75, q90, q95,
        id, model_type_description
    """
    params: Dict[str, Any] = {"skip": skip, "limit": limit}
    if horizon_type:
        params["horizon_type"] = horizon_type
    if horizon_value is not None:
        params["horizon_value"] = horizon_value
    if code:
        params["code"] = code
    if model:
        params["model"] = model
    if start_date:
        params["start_date"] = str(start_date)
    if end_date:
        params["end_date"] = str(end_date)
    if valid_from:
        params["valid_from"] = str(valid_from)
    if valid_to:
        params["valid_to"] = str(valid_to)

    records = self._get("/long-forecast/", params=params)
    return pd.DataFrame(records) if records else pd.DataFrame()
```

**Critical differences from `read_forecasts()`:**
- Endpoint is `/long-forecast/` (not `/forecast/`)
- Has `horizon_type` param (not `horizon`) — matches server query param name
- Has `horizon_value` param (int) — use `is not None` check (not truthiness), because `0` is a valid value
- Has `valid_from` and `valid_to` params (not `target`/`start_target`/`end_target`)
- No `target`, `start_target`, `end_target` params (long forecasts use valid_from/valid_to instead)

#### Implementation: `write_long_forecasts()`

```python
def write_long_forecasts(self, records: List[Dict[str, Any]]) -> int:
    """
    Write long forecast records to the API.

    Records are upserted based on the unique key:
    (horizon_type, horizon_value, code, date, model_type, valid_from, valid_to).

    Args:
        records: List of long forecast record dicts. Required keys:
            horizon_type, horizon_value, code, date, model_type,
            valid_from, valid_to. Optional keys: flag, composition,
            q, q_obs, q_xgb, q_lgbm, q_catboost, q_loc,
            q05, q10, q25, q50, q75, q90, q95.

    Returns:
        Number of records written
    """
    return self._post_batched("/long-forecast/", records)
```

#### Implementation: `prepare_long_forecast_records()` (static helper)

```python
@staticmethod
def prepare_long_forecast_records(
    df: pd.DataFrame,
    horizon_type: str,
    horizon_value: int,
    model_type: str,
    code_col: str = "code",
    date_col: str = "date",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
) -> List[Dict[str, Any]]:
    """
    Prepare long forecast records from a DataFrame.

    Converts a DataFrame to a list of dicts ready for write_long_forecasts().
    Handles NaN→None conversion for all nullable float fields.

    Args:
        df: Source DataFrame with forecast data
        horizon_type: Horizon type ("month", "quarter", "season")
        horizon_value: Which period (e.g., month number 1-12)
        model_type: Model type string (e.g., "GBT", "LR_Base")
        code_col: Column name for station code
        date_col: Column name for forecast issue date
        valid_from_col: Column name for validity start date
        valid_to_col: Column name for validity end date

    Returns:
        List of record dicts ready for write_long_forecasts()
    """
    quantile_cols = [
        "q", "q_obs", "q_xgb", "q_lgbm", "q_catboost", "q_loc",
        "q05", "q10", "q25", "q50", "q75", "q90", "q95",
    ]

    records = []
    for _, row in df.iterrows():
        record: Dict[str, Any] = {
            "horizon_type": horizon_type,
            "horizon_value": horizon_value,
            "code": str(row[code_col]),
            "date": str(row[date_col]),
            "model_type": model_type,
            "valid_from": str(row[valid_from_col]),
            "valid_to": str(row[valid_to_col]),
        }

        # Optional fields
        if "flag" in df.columns:
            val = row.get("flag")
            record["flag"] = int(val) if pd.notna(val) else None
        if "composition" in df.columns:
            val = row.get("composition")
            record["composition"] = val if pd.notna(val) else None

        # Quantile predictions — NaN→None
        for col in quantile_cols:
            if col in df.columns:
                val = row.get(col)
                record[col] = float(val) if pd.notna(val) else None

        records.append(record)
    return records
```

#### Placement in the file

Add the three methods after the existing `# ==================== SKILL METRICS ====================` section (after `prepare_skill_metric_records()`), using this section header:

```python
# ==================== LONG FORECASTS ====================
```

Also update the class docstring to mention long forecasts:

```python
class SapphirePostprocessingClient(SapphireAPIClient):
    """
    Client for the SAPPHIRE Postprocessing API.

    Provides methods for reading and writing:
    - Forecasts
    - Linear regression forecasts
    - Skill metrics
    - Long forecasts (monthly/quarterly/seasonal with quantiles)
    ...
    """
```

#### Tests required

Follow the existing test patterns in the sapphire-api-client repo. At minimum:

1. **`test_read_long_forecasts_builds_params`** — verify all query params are passed correctly (especially `horizon_type` not `horizon`, and `horizon_value` with `is not None` check)
2. **`test_read_long_forecasts_returns_dataframe`** — mock `_get` returning records → DataFrame
3. **`test_read_long_forecasts_empty`** — mock `_get` returning `[]` → empty DataFrame
4. **`test_write_long_forecasts_calls_post_batched`** — verify endpoint is `/long-forecast/`
5. **`test_prepare_long_forecast_records`** — verify NaN→None, date→str, all quantile cols
6. **`test_prepare_long_forecast_records_missing_optional_cols`** — only required cols present

#### Verification after implementation

```bash
# In the sapphire-api-client repo:
pytest tests/ -v

# After bumping the hash in this repo:
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

**After completion in sapphire-api-client**: Commit, push, then in this repo update the pinned commit hash in `apps/postprocessing_forecasts/pyproject.toml` and run `uv sync`.

---

## Step 1: Monthly observation aggregation — `data_reader.py`

**File**: `apps/postprocessing_forecasts/src/data_reader.py`

**Reuse** core logic from `apps/long_term_forecasting/post_process_lt_forecast.py:168-242` (`calculate_lt_statistics_calendar_month`), simplified — no leave-one-out, just aggregate all years.

```python
def read_monthly_observations(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Aggregate daily runoff to monthly mean discharge.

    Reads daily runoff via preprocessing API (primary) or CSV (fallback).
    Requires >= 50% non-missing days per month.

    Returns:
        DataFrame: [code, year, month, month_in_year, discharge_avg, delta]
    """
```

**Data source**: `SapphirePreprocessingClient.read_runoff(horizon="day", code=code, ...)` with paginated reads.

**Core aggregation**:
1. Parse dates → extract year, month, days_in_month
2. `groupby(['code', 'year', 'month']).agg(mean, count, first)`
3. Filter: `non_missing_days >= days_in_month * 0.5`
4. Compute delta per (code, month_in_year): `delta = 0.674 * std(discharge_avg)` across all years for that station+month. This matches the pentad/decad convention (`forecast_library.py:1519`).
5. Return with `month_in_year = month`

---

## Step 2: Monthly forecast reading — `data_reader.py`

**File**: `apps/postprocessing_forecasts/src/data_reader.py`

```python
def read_monthly_forecasts(
    codes: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Read monthly long-term forecasts from postprocessing API.

    Returns:
        DataFrame: [code, year, month, model_type, model_short,
                     q50, q05, q10, q25, q75, q90, q95,
                     valid_from, valid_to, date, flag]
    """
```

**Data source**: `SapphirePostprocessingClient.read_long_forecasts(horizon_type="month", code=code, ...)` with paginated reads (batch_size=1000).

**Normalization**: The API `model_type` value is used directly as `model_short`. No `model_long` mapping is needed — long display names are resolved server-side via `ModelType.description` (see INFRA-005 decision). Long-term model types:

| API model_type / model_short | Source | Notes |
|------------------------------|--------|-------|
| LR_Base | `long_term_forecasting` | Linear Regression Base |
| LR_SM | `long_term_forecasting` | LR with Snow/Meteo |
| LR_SM_DT | `long_term_forecasting` | LR with Snow/Meteo/DT |
| LR_SM_ROF | `long_term_forecasting` | LR with Snow/Meteo/Runoff |
| SM_GBT | `long_term_forecasting` | SciRegressor GBT |
| SM_GBT_LR | `long_term_forecasting` | SciRegressor GBT+LR |
| SM_GBT_Norm | `long_term_forecasting` | SciRegressor GBT Norm |
| MC_ALD | `long_term_forecasting` | Uncertainty Mixture ALD |
| GBT | `long_term_forecasting` | Gradient Boosted Trees |
| Naive Mean | **Computed in postprocessing** | Climatological mean monthly discharge (no-skill baseline) |
| Skilled Mean | **Computed in postprocessing** | Skill-weighted average of individual model forecasts |

**`Naive Mean` and `Skilled Mean` (decided 2026-02-16):** These reference baselines are not produced by `long_term_forecasting`. They are computed during skill metric calculation in Step 4:
- **Naive Mean**: For each (station, month), the mean of all years' monthly observed discharge. Written to `long_forecasts` table with `model_type="Naive Mean"` and `q50 = climatological_mean`. Quantile columns are not populated (CRPS not applicable for a point baseline).
- **Skilled Mean**: Weighted average of individual model Q50 forecasts, weighted by their skill scores. The exact weighting scheme (inverse MAE, NSE-weighted, etc.) to be defined during implementation. Written with `model_type="Skilled Mean"`.

**Extract month**: From `valid_from` date → month (1–12).

---

## Step 3: CRPS implementation — `skill_metrics.py`

**File**: `apps/postprocessing_forecasts/src/skill_metrics.py`

```python
def calculate_crps(
    observed: np.ndarray,
    quantile_forecasts: np.ndarray,
    quantile_levels: np.ndarray,
) -> float:
    """Continuous Ranked Probability Score from quantile forecasts.

    Uses the quantile decomposition:
      CRPS ≈ (2/N) * Σ_i Σ_j ρ_τj(y_i - q_ij) * Δτ_j
    where ρ_τ(u) = u*(τ - 1(u<0)) is the quantile (pinball) loss.

    Args:
        observed: shape (N,) — observed values
        quantile_forecasts: shape (N, K) — forecasted quantiles
        quantile_levels: shape (K,) — e.g. [0.05, 0.10, ..., 0.95]

    Returns:
        Mean CRPS across all observations (lower is better).
    """
```

Trapezoidal integration of quantile (pinball) loss. No external dependency needed.

---

## Step 4: Monthly skill metrics — `skill_metrics.py`

**File**: `apps/postprocessing_forecasts/src/skill_metrics.py`

```python
def calculate_monthly_skill_metrics(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    timing_stats=None,
) -> tuple[pd.DataFrame, pd.DataFrame, Any]:
    """Calculate monthly skill metrics for long-term forecasts.

    Point metrics (Q50 vs observed): NSE, MAE, accuracy, sdivsigma
    Probabilistic metric: CRPS (using Q05–Q95)

    Args:
        observations: [code, year, month, month_in_year, discharge_avg, delta]
        forecasts: [code, year, month, model_short,
                    q50, q05, q10, q25, q75, q90, q95]

    Returns:
        (skill_stats_df, joint_forecasts_df, timing_stats)
        skill_stats_df: [month_in_year, code, model_short,
                         sdivsigma, nse, delta, accuracy, mae, n_pairs, crps]
    """
```

**Pattern** (follows `calculate_skill_metrics_pentad()`):
1. Merge forecasts with observations on `[code, year, month]`
2. Use `q50` as point forecast → rename to `forecasted_discharge`
3. GroupBy `[month_in_year, code, model_short]`
4. Apply `calculate_all_skill_metrics()` — **reuse existing function**
5. Compute CRPS per group using quantile columns
6. Ensemble creation: threshold filtering + ensemble mean (reuse logic from pentad/decad)

**Delta for accuracy metric (decided 2026-02-16)**: Computed on-the-fly inside `read_monthly_observations()` as `delta = 0.674 * std(discharge_avg)` per (station, month_in_year), using all available years of monthly observations. This is self-contained — no cross-module dependency on `long_term_forecasting`. Consistent with how pentad/decad delta is computed in `forecast_library.py:1519`.

**Key differences from pentad/decad**:
- Uses `month_in_year` (1–12) instead of `pentad_in_year` (1–72)
- Has CRPS column (additional probabilistic metric)
- Model types are LT models (LR_Base, SM_GBT, MC_ALD) not short-term (LR, TFT, TiDE)

---

## Step 5: Extend `api_writer.py` for monthly horizon

**File**: `apps/postprocessing_forecasts/src/api_writer.py`

Extend `_write_skill_metrics_to_api()` (line 309-315):
- Add `elif horizon_type == "month": horizon_in_year_col = "month_in_year"`
- Extend `model_type_map` to include LT models: `LR_BASE → LR_Base`, `SM_GBT → SM_GBT`, `MC_ALD → MC_ALD`, `GBT → GBT`, etc.

---

## Step 6: Monthly save function — `file_writer.py`

**File**: `apps/postprocessing_forecasts/src/file_writer.py`

```python
def save_monthly_skill_metrics(data: pd.DataFrame):
    """Save monthly skill metrics to CSV + API.
    Pattern matches save_pentadal_skill_metrics / save_decadal_skill_metrics.
    """
```

Env var: `ieasyforecast_monthly_skill_metrics_file`

---

## Step 7: Extend `recalculate_skill_metrics.py`

**File**: `apps/postprocessing_forecasts/recalculate_skill_metrics.py`

Add monthly block after pentad/decad:

```python
if prediction_mode in ("MONTHLY", "ALL"):
    observations = data_reader.read_monthly_observations(codes, start_year, end_year)
    forecasts = data_reader.read_monthly_forecasts(codes, start_year, end_year)
    monthly_skill_stats, monthly_joint, timing_stats = (
        skill_metrics.calculate_monthly_skill_metrics(observations, forecasts, timing_stats)
    )
    file_writer.save_monthly_skill_metrics(monthly_skill_stats)
```

Extend `SAPPHIRE_PREDICTION_MODE`: add `MONTHLY` and `ALL`. `BOTH` stays = pentad + decad (backward compat).

**Entry point scope (decided 2026-02-16):**
- **`recalculate_skill_metrics.py`:** Supports `MONTHLY` and `ALL`. This is the definite entry point for monthly skill metric recalculation.
- **`postprocessing_operational.py` / `postprocessing_maintenance.py`:** Pentad/decad only for now. Whether monthly postprocessing needs its own operational entry point (~2x/month) or gap-fill is an **open question** to be decided when Phase 4a implementation begins.

---

## Files Modified

| File | Change |
|------|--------|
| ~~`sapphire-api-client/.../postprocessing.py`~~ | **Out of scope** — see [Pre-requisite](#pre-requisite-sapphire-api-client-llm-instructions) for LLM instructions |
| `apps/postprocessing_forecasts/pyproject.toml` | Bump sapphire-api-client commit hash (after pre-requisite is done) |
| `apps/postprocessing_forecasts/src/data_reader.py` | Add `read_monthly_observations()`, `read_monthly_forecasts()` |
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Add `calculate_crps()`, `calculate_monthly_skill_metrics()` |
| `apps/postprocessing_forecasts/src/api_writer.py` | Extend horizon_type → "month", add LT model types |
| `apps/postprocessing_forecasts/src/file_writer.py` | Add `save_monthly_skill_metrics()` |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py` | Add monthly block |

## Existing Code to Reuse

| Function | Location | Reuse |
|----------|----------|-------|
| `calculate_all_skill_metrics()` | `src/skill_metrics.py:~420` | Point metrics — identical for monthly |
| `calculate_lt_statistics_calendar_month()` | `long_term_forecasting/post_process_lt_forecast.py:168` | 50% coverage aggregation — adapt without leave-one-out |
| `_write_skill_metrics_to_api()` | `src/api_writer.py:263` | Extend, don't rewrite |
| `save_pentadal_skill_metrics()` | `src/file_writer.py:273` | Template for monthly version |
| `_read_skill_metrics_api()` | `src/data_reader.py:130` | Paginated read + normalization pattern |
| Ensemble creation | `src/skill_metrics.py:~490-560` | Threshold filter + ensemble mean |

---

## Tests Required

### Unit tests
- `test_calculate_crps()` — known distributions, edge cases (equal quantiles, observed outside range)
- `test_read_monthly_observations()` — mock preprocessing API, 50% coverage filter
- `test_read_monthly_forecasts()` — mock postprocessing API, normalization
- `test_calculate_monthly_skill_metrics()` — hand-calculated metrics

### Edge case tests
- Empty observations / empty forecasts
- All-NaN quantile columns
- Single station, single month
- Dec → Jan year transition
- Station with 1 year of data only

### Integration tests
- Full pipeline: read obs + forecasts → calculate → save CSV + API
- Monthly block in `recalculate_skill_metrics.py`

### API failure tests
- API unavailable → graceful fallback
- API disabled → skip
- Readiness check fails → skip

---

## Implementation Order

1. ~~**[Separate repo]** `sapphire-api-client`: `read_long_forecasts` + `write_long_forecasts` + tests~~ — **DONE** (LT forecast support added to sapphire-api-client)
2. ~~**[This repo]** Bump pinned hash in `pyproject.toml` → `uv sync`~~ — **DONE** (hash `a457728`, all 7 modules synced)
3. ~~`data_reader.py`: `read_monthly_observations()` + `read_monthly_forecasts()` + tests~~ — **DONE** (25 unit + 35 edge case + 17 integration tests)
4. ~~`skill_metrics.py`: `calculate_crps()` + tests~~ — **DONE** (17 tests: basic, hand-calculated, edge cases)
5. ~~`skill_metrics.py`: `calculate_monthly_skill_metrics()` + tests~~ — **DONE** (21 tests: basic, multi-model, ensemble, Naive Mean, edge cases)
6. `api_writer.py`: extend horizon/model mappings + tests ← **NEXT**
7. `file_writer.py`: `save_monthly_skill_metrics()` + tests
8. `recalculate_skill_metrics.py`: monthly block + integration tests
9. Full test suite — zero skips
10. Update Phase 4 checklist in `postprocessing_unified_plan.md`

---

## Verification

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

Zero skips except `SAPPHIRE_API_AVAILABLE` guards.

---

## Revision History

| Date | Changes |
|------|---------|
| 2026-02-16 | Initial detailed plan created |
| 2026-02-16 | Clarified sapphire-api-client is out of scope — section rewritten as comprehensive LLM instructions for separate repo. Added: Quick Reference table, cross-references to master document, full server-side API reference (GET/POST endpoints, query params, filter behavior, request/response schemas), complete LongForecast record field documentation (all 20+ columns with types and constraints), exact existing client code as template, complete implementation code for all 3 methods (read/write/prepare), critical differences callout (horizon_type vs horizon, is not None for int params), file placement instructions, 6 required test cases, verification commands. |
| 2026-02-16 | Applied 5 decisions from unified plan review: (1) Delta/accuracy: `read_monthly_observations()` now computes `delta = 0.674 * std` on-the-fly, returns it as a column. Step 4 delta guidance updated. (2) Removed `model_long` from all function signatures, return types, and groupby keys per INFRA-005. (3) Added `Skilled Mean` and `Naive Mean` to model mapping table with computation notes. (4) Added LR_SM, LR_SM_DT, LR_SM_ROF to model table (were missing). (5) Step 7: added entry point scope note (recalculate supports MONTHLY/ALL; operational/maintenance monthly is open question). |
| 2026-02-16 | Marked Step 1 (sapphire-api-client LT support) as DONE. Next: Step 2 (bump pinned hash + uv sync). |
| 2026-02-16 | Marked Steps 3–4 as DONE. Step 3: monthly readers (77 tests). Step 4: CRPS (17 tests). Next: Step 5. |
| 2026-02-16 | Marked Step 5 as DONE. calculate_monthly_skill_metrics: point metrics + CRPS + EM ensemble + Naive Mean baseline (21 tests). |
