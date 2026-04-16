# GitHub Issue: FD-SNOW

**Title**: `feat(forecast_dashboard): Snow visualization — configurable year start, units & labels`

**Labels**: `enhancement`, `forecast_dashboard`, `snow`, `medium-priority`

**Status**: Phase 1 implemented (P1a + P1b + P1c) — 225 tests pass; Phase 2 blocked on colleague (services)

---

## Summary

Three independent improvements to the snow data visualization in the forecast
dashboard, plus a prerequisite task for the colleague managing `sapphire/services/`:

1. **Configurable display year start date** — allow starting the x-axis at a
   month/day other than Jan 1 (e.g. Sep 1 for a hydrological year).
2. **Percentile range bands** — display P5–P95 and P25–P75 shaded areas, matching
   the discharge hydrograph pattern.
3. **Label & unit fixes** — forecast label says only "Forecast" (drop "5 day mean"),
   and snow height (HS) is shown in cm instead of m.

## Motivation

- Central Asian hydromet services define their hydrological year starting Sep 1.
  The current Jan 1–Dec 31 window cuts off the autumn snow accumulation context.
- Percentile bands give operational hydrologists an immediate sense of whether
  current snow conditions are within normal range — the same visual language they
  already read on the discharge hydrograph.
- The "5 day mean" qualifier on the snow forecast label is confusing for snow
  data and adds no useful information.
- HS values from SnowMapper are in meters, but field hydrologists think in
  centimeters.

---

## Code review findings (2026-04-16)

A detailed review of the actual codebase against the original plan uncovered
several issues. All corrections are incorporated below. The key findings:

1. **Year-filtering in vizualization.py**: `current_year = station_data[station_data['year'] == date_picker.year]`
   (line 2217) would silently discard data from the previous calendar year when
   using a hydrological-year window (e.g. Sep–Aug). This is the critical change
   in P1a — not just widening the API fetch window but also replacing the
   calendar-year filter with a date-range filter.

2. **Vlines hardcoded to single calendar year**: `create_cached_vlines_hs_special_case`
   (line 1546) generates vlines only for `datetime.now().year`. Rather than
   adapting them to span two years, we remove vlines from snow plots entirely
   (user decision — they add clutter on snow plots and the cross-year fix would
   be disproportionately complex).

3. **`current_period` must stay**: The original plan said to remove the entire
   `current_period`/`forecast_period` block (lines 2244–2249). But `current_period`
   IS used at line 2255 in `current_year_text` — the legend label for the
   current-year observation curve. Removing it would cause a `NameError`.

4. **db.py threading avoided**: Instead of threading config through 6 function
   signatures (`_get_snow_single` → `get_snow_data` → `get_data` →
   `_get_data_monthly` → `DataManager.load_station` → callers), we widen the
   fetch window unconditionally to 2 years (`PREVIOUS_YEAR` to `CURRENT_YEAR`).
   The visualization layer filters to the configured display window. ~730 rows
   of daily snow data is trivial overhead.

5. **No tests will break**: Zero existing tests cover any of the four files being
   modified. `test_lr_only_fixes.py` does import `src.vizualization` at the top
   level, but our changes are inside function bodies, not at module level.

---

## Implementation review (2026-04-16)

An automated review verified every line number, variable name, function signature,
and data flow claim in the plan against the actual codebase. All references are
accurate. Two amendments are required before implementation:

### Amendment 1: Config parsing must validate month/day bounds

The proposed `try/except` catches non-numeric input but not out-of-range values
(e.g. `99-99`, `02-29`). These pass parsing but crash `_snow_display_window()`
at runtime — a **new failure mode** not present in the current code.

**Fix** — validate with a date construction in a non-leap year:
```python
try:
    month, day = int(snow_start[:2]), int(snow_start[3:])
    date(2001, month, day)  # non-leap year validates range + rejects Feb 29
except (ValueError, IndexError):
    month, day = 1, 1
```

### Amendment 2: Parse env var in `init_dashboard()`, not in `display_weather_and_snow_data()`

`display_weather_and_snow_data()` returns a 2-tuple destructured at line 40.
Changing it to a 4-tuple modifies an existing function contract unnecessarily.

**Fix** — parse the env var directly in `init_dashboard()` (3 lines) before the
`DashboardConfig(...)` constructor call. Leave `display_weather_and_snow_data()`
unchanged.

### Additional observations (no action needed)

- `historical_data` (line 2226) is dead code — computed, never used. Unaffected.
- `forecast_period` unused after P1b is safe — `apps/forecast_dashboard/` is
  excluded from ruff linting.
- `doy` column with cross-year data is not a problem — x-axis uses `date`.
- `end_date` in `db.py` stays at `CURRENT_YEAR-12-31` — correct as-is.

---

## Phase 1 — Dashboard-only changes (no services dependency)

All three sub-tasks are independent and can be implemented in parallel.

### P1a: Configurable year start date

**Goal**: Allow the snow x-axis to start at any month/day, configured via
environment variable. Default: `01-01` (calendar year, preserves current
behaviour).

**Files to modify**:
| File | Change |
|------|--------|
| `apps/forecast_dashboard/dashboard/config.py` | Add `snow_display_start_month: int` and `snow_display_start_day: int` to `DashboardConfig` (line 23). Read from env var `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` (format `"MM-DD"`, default `"01-01"`). |
| `apps/forecast_dashboard/src/db.py` | In `_get_snow_single()` (line 245): change `start_date` from `f"{CURRENT_YEAR}-01-01"` to `f"{PREVIOUS_YEAR}-01-01"`. No signature change. |
| `apps/forecast_dashboard/src/vizualization.py` | Add `snow_display_start_month=1, snow_display_start_day=1` parameters to `plot_daily_snow_data()` (line 2173). Replace calendar-year filter (line 2217) with date-range filter. Remove vlines (lines 2269–2272, 2307, 2309). Update xlim (line 2322). |
| `apps/forecast_dashboard/dashboard/plot_manager.py` | Pass `self._cfg.snow_display_start_month` and `self._cfg.snow_display_start_day` to the `viz.plot_daily_snow_data()` call (line 323). |

#### config.py changes

Add two fields to `DashboardConfig` (after line 23):
```python
snow_display_start_month: int
snow_display_start_day: int
```

Parse the env var directly in `init_dashboard()`, before the `DashboardConfig(...)`
constructor call (line 42). Do NOT modify `display_weather_and_snow_data()` — its
2-tuple return signature must remain unchanged (see Amendment 2):
```python
from datetime import date
snow_start = os.getenv('ieasyhydroforecast_SNOW_DISPLAY_START_MMDD', '01-01')
try:
    snow_month, snow_day = int(snow_start[:2]), int(snow_start[3:])
    date(2001, snow_month, snow_day)  # validate range, reject Feb 29
except (ValueError, IndexError):
    snow_month, snow_day = 1, 1
```

Pass the new fields into the `DashboardConfig(...)` constructor in
`init_dashboard()` (line 42).

#### db.py change (1-line change, no signature change)

```python
# BEFORE (line 245)
"start_date": f"{CURRENT_YEAR}-01-01",

# AFTER
"start_date": f"{PREVIOUS_YEAR}-01-01",
```

`PREVIOUS_YEAR` already exists at line 20 (`PREVIOUS_YEAR = CURRENT_YEAR - 1`).
This fetches ~2 years of snow data so the visualization layer always has enough
data regardless of the configured display start. For daily snow data this is
~730 rows — trivial.

**Why not thread the config through to db.py?** That would require modifying 6
function signatures: `_get_snow_single` → `get_snow_data` → `get_data` →
`_get_data_monthly` → `DataManager.load_station` → 3 callers. The wider fetch
is simpler and risk-free.

#### vizualization.py changes

**1. Add parameters to `plot_daily_snow_data`** (line 2173):

```python
# BEFORE
def plot_daily_snow_data(_, wm, snow_data, variable, station, date_picker, linreg_predictor):

# AFTER
def plot_daily_snow_data(_, wm, snow_data, variable, station, date_picker,
                         linreg_predictor, snow_display_start_month=1,
                         snow_display_start_day=1):
```

Default values of `1, 1` preserve current behaviour if called without the new args.

**2. Add date-window helper** (inside the function, or as a module-level helper):

```python
from datetime import date, timedelta

def _snow_display_window(start_month: int, start_day: int,
                         ref_date: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return (begin, end) Timestamps for the snow display window.

    If start is Jan 1 → Jan 1 – Dec 31 of ref_date's year (unchanged).
    Otherwise, spans two calendar years:
      - If ref_date >= start date this year → this year's start to next year's (start - 1 day).
      - If ref_date < start date this year → previous year's start to this year's (start - 1 day).
    """
    if start_month == 1 and start_day == 1:
        return (pd.Timestamp(ref_date.year, 1, 1),
                pd.Timestamp(ref_date.year, 12, 31))

    year_start = date(ref_date.year, start_month, start_day)
    if ref_date >= year_start:
        begin = year_start
    else:
        begin = date(ref_date.year - 1, start_month, start_day)
    end = date(begin.year + 1, start_month, start_day) - timedelta(days=1)
    return pd.Timestamp(begin), pd.Timestamp(end)
```

**3. Replace calendar-year filter** (line 2217):

```python
# BEFORE
current_year = station_data[station_data['year'] == date_picker.year].copy()

# AFTER
display_begin, display_end = _snow_display_window(
    snow_display_start_month, snow_display_start_day,
    date_picker.date() if hasattr(date_picker, 'date') else date_picker)
current_year = station_data[
    (station_data['date'] >= display_begin) &
    (station_data['date'] <= display_end)
].copy()
```

This is the critical fix. The old code assumed all relevant data falls within
a single calendar year. A hydrological year (e.g. Sep 2025 – Aug 2026)
spans two calendar years — the old filter would silently discard Sep–Dec data.

**4. Remove vlines** (lines 2269–2272 and 2307, 2309):

```python
# DELETE lines 2269-2272:
#    if variable == 'HS':
#        vlines = create_cached_vlines_hs_special_case(_, horizon, for_dates=True)
#    else:
#        vlines = create_cached_vlines(_, horizon, for_dates=True, y_text=y_min * 1.05)

# REPLACE figure composition (lines 2307, 2309):
# BEFORE
figure = vlines * hv_norm * hv_current_year * hv_forecast   # line 2307
figure = vlines * hv_norm * hv_current_year                 # line 2309

# AFTER
figure = hv_norm * hv_current_year * hv_forecast
figure = hv_norm * hv_current_year
```

Vlines were hardcoded to a single calendar year (`datetime.now().year`) via
`create_cached_vlines_hs_special_case`. Adapting them to span two years would
require modifying the cache key, the vline generation range, and the
year-source. Removing them is simpler and the snow plots are readable without
period boundary markers.

**5. Update xlim** (line 2322):

```python
# BEFORE
xlim=(min(norm_snow['date']), max(norm_snow['date'])),

# AFTER
xlim=(display_begin, display_end),
```

Using the computed window directly rather than deriving from data. This ensures
the x-axis always shows the full configured window even if data is sparse.

#### plot_manager.py change

```python
# BEFORE (lines 323-325)
self.snow_plots[var].object = viz.plot_daily_snow_data(
    self._, wm, dm.snow_data, var, wm.station_selector.value,
    wm.date_picker.value, dm.linreg_predictor,
)

# AFTER
self.snow_plots[var].object = viz.plot_daily_snow_data(
    self._, wm, dm.snow_data, var, wm.station_selector.value,
    wm.date_picker.value, dm.linreg_predictor,
    snow_display_start_month=self._cfg.snow_display_start_month,
    snow_display_start_day=self._cfg.snow_display_start_day,
)
```

**Acceptance criteria**:
- With `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD=09-01`, the snow plot shows
  Sep 1 of the previous year through Aug 31 of the current year (when viewed
  between Jan 1 and Aug 31), or Sep 1 of the current year through Aug 31 of
  next year (when viewed between Sep 1 and Dec 31).
- With default (`01-01`) or unset, behaviour is identical to today.
- Norm, current-year, and forecast traces all align to the new date window.
- No vertical period-boundary lines on snow plots.

---

### P1b: Simplify forecast label

**Goal**: Change the snow forecast legend entry from
`"Forecast, 5 day mean: 0.42 m"` to `"Forecast: 0.42 cm"` (or just
`"Forecast"` when no value is available).

**File**: `apps/forecast_dashboard/src/vizualization.py`

**Change at line 2259 only**:

```python
# BEFORE
forecast_text = (
    f"{_('Forecast')}, {forecast_period}: "
    f"{forecast_mean:.{decimals}f} {config['unit']}"
    if not pd.isna(forecast_mean)
    else _('Forecast')
)

# AFTER
forecast_text = (
    f"{_('Forecast')}: {forecast_mean:.{decimals}f} {config['unit']}"
    if not pd.isna(forecast_mean)
    else _('Forecast')
)
```

**IMPORTANT**: Do NOT remove the `current_period` / `forecast_period` block
(lines 2244–2249). `current_period` is used at line 2255 in the current-year
curve legend label:
```python
current_year_text = f"{_('Current year')}, {current_period}: {mean_value:.{decimals}f} {config['unit']}" ...
```
Removing the block would cause a `NameError` at runtime.

The `forecast_period` variable (lines 2246, 2249) becomes unused after this
change. It can be removed in a cleanup pass, but leaving it is harmless and
lower risk.

**Acceptance criteria**: Snow forecast legend reads `"Forecast: <value> <unit>"`
with no period qualifier. Current-year legend is unchanged.

---

### P1c: Snow height in centimetres

**Goal**: Display HS values in cm instead of m.

**File**: `apps/forecast_dashboard/src/vizualization.py`

**Changes**:

1. Update the HS config dict (line 2189):
   ```python
   # BEFORE
   'HS': {'label': _('Snow Height'), 'unit': 'm',
          'ylabel': _('Snow Height (m)'), 'decimals': 2},
   # AFTER
   'HS': {'label': _('Snow Height'), 'unit': 'cm',
          'ylabel': _('Snow Height (cm)'), 'decimals': 1},
   ```

2. After line 2214 (`station_data['doy'] = ...`), before line 2217
   (`current_year = ...`), add:
   ```python
   if variable == 'HS':
       station_data[variable] = station_data[variable] * 100
       station_data['norm'] = station_data['norm'] * 100
   ```

   **Why this position matters**: Line 2217 does `current_year = station_data[...].copy()`.
   The conversion must happen BEFORE that `.copy()` so the scaled values
   propagate into `current_year`, and from there into `norm_snow` (line 2219,
   derived from `current_year['norm']`), `forecasts` (line 2223), and
   `predictor_snow` (line 2240). Placing it after the copy would miss
   `current_year` entirely.

3. Once percentile columns exist (Phase 4), the same ×100 conversion must
   also apply to `q05`, `q25`, `q75`, `q95`, `min`, `max`.

**Acceptance criteria**: HS y-axis shows "Snow Height (cm)", values are 100×
the raw API values, displayed with 1 decimal.

---

## Phase 2 — Colleague task: Add percentile columns to Snow table

> **Owner**: colleague managing `sapphire/services/`
> **Depends on**: nothing (can start in parallel with Phase 1)

The `Snow` DB model (`sapphire/services/preprocessing/app/models.py`, lines
117–148) currently has `value`, `norm`, and `value1`–`value14`. It has **no**
percentile fields.

The `Hydrograph` model (same file, lines 41–79) already has:
```
q05, q25, q50, q75, q95  (all Float, nullable)
```
plus `min`, `max`, `mean`, `std`.

### Requested changes

**2a. DB model** — add to the `Snow` class in `models.py`:
```python
# Statistical / percentile fields (matching Hydrograph conventions)
q05 = Column(Float)
q25 = Column(Float)
q75 = Column(Float)
q95 = Column(Float)
min = Column(Float)
max = Column(Float)
```

Create an Alembic migration for the new columns.

**2b. Schemas** — add the same fields as `Optional[float]` to `SnowBase` in
`schemas.py` (line 100):
```python
q05: Optional[float] = None
q25: Optional[float] = None
q75: Optional[float] = None
q95: Optional[float] = None
min: Optional[float] = None
max: Optional[float] = None
```

**2c. API** — no route changes needed; the existing `GET /snow/` and
`POST /snow/` endpoints already serialize/deserialize from the schema, so the
new fields will appear automatically once the schema is updated.

**Acceptance criteria**:
- `GET /preprocessing/snow/?snow_type=HS&code=...` returns `q05`, `q25`,
  `q75`, `q95`, `min`, `max` fields (initially null for existing rows).
- `POST /preprocessing/snow/` accepts and persists the new fields.
- Existing data is not lost during migration.

---

## Phase 3 — Compute and write snow percentiles (apps-side)

> **Depends on**: Phase 2

**Files to modify**:
| File | Change |
|------|--------|
| `apps/preprocessing_gateway/dg_utils.py` | In `calculate_snow_norms()` (lines 304–372): in addition to `.mean()`, compute `.quantile(0.05)`, `.quantile(0.25)`, `.quantile(0.75)`, `.quantile(0.95)`, `.min()`, `.max()` grouped by `(code, dayofyear)`. |
| `apps/preprocessing_gateway/dg_utils.py` | In `write_snow_to_api()` (lines 420–623): include `q05`, `q25`, `q75`, `q95`, `min`, `max` in the API payload. |

**Acceptance criteria**:
- After a norm recalculation run, `GET /preprocessing/snow/?snow_type=HS&code=...`
  returns non-null percentile values for dates with sufficient historical data.

---

## Phase 4 — Display percentile bands in snow plot

> **Depends on**: Phase 1a (date window), Phase 3 (percentile data available)

**Files to modify**:
| File | Change |
|------|--------|
| `apps/forecast_dashboard/src/db.py` | In `_get_snow_single()`: stop dropping percentile columns. Keep `q05`, `q25`, `q75`, `q95`, `min`, `max` alongside `value` and `norm`. |
| `apps/forecast_dashboard/src/vizualization.py` | In `plot_daily_snow_data()`: add three `plot_runoff_range_area()` calls following the pattern from `plot_daily_hydrograph_data()` (lines 1803–1813). |

**Plotting pattern** (from the existing discharge hydrograph):
```python
full_range_area = plot_runoff_range_area(
    norm_snow, 'date', 'min', 'max',
    _("Full range legend entry"), runoff_full_range_color)

area_05_95 = plot_runoff_range_area(
    norm_snow, 'date', 'q05', 'q95',
    _("90-percentile range legend entry"), runoff_90percentile_range_color)

area_25_75 = plot_runoff_range_area(
    norm_snow, 'date', 'q25', 'q75',
    _("50-percentile range legend entry"), runoff_50percentile_range_color)

figure = full_range_area * area_05_95 * area_25_75 * hv_norm * hv_current_year
```

The same color constants are reused (`runoff_full_range_color`,
`runoff_90percentile_range_color`, `runoff_50percentile_range_color` — lines
91–93 in `vizualization.py`).

If HS, the ×100 conversion from P1c must also apply to the percentile columns.
Add them to the conversion block:
```python
if variable == 'HS':
    station_data[variable] = station_data[variable] * 100
    station_data['norm'] = station_data['norm'] * 100
    for col in ['q05', 'q25', 'q75', 'q95', 'min', 'max']:
        if col in station_data.columns:
            station_data[col] = station_data[col] * 100
```

**y-axis limits** should expand to include the full range:
```python
all_values = pd.concat([
    norm_snow[['min', 'max']].values.flatten(),
    current_year[variable],
    norm_snow[variable]
]).dropna()
```

**Acceptance criteria**:
- Snow plot shows three nested shaded bands (full range, P5–P95, P25–P75)
  behind the norm and current-year curves — visually matching the discharge
  hydrograph.
- Hover on band edges shows the percentile values.

---

## Dependency graph

```
P1a (config year start)  ─────────────────────────┐
P1b (forecast label)     [independent]             │
P1c (HS cm units)        [independent]             │
P2  (colleague: DB cols) ──► P3 (compute q-tiles) ─┼──► P4 (display bands)
```

```json
{
  "phases": {
    "P1a": { "depends_on": [], "parallel_agents": 1 },
    "P1b": { "depends_on": [], "parallel_agents": 1 },
    "P1c": { "depends_on": [], "parallel_agents": 1 },
    "P2":  { "depends_on": [], "owner": "colleague", "parallel_agents": 0 },
    "P3":  { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4":  { "depends_on": ["P1a", "P3"], "parallel_agents": 1 }
  }
}
```
