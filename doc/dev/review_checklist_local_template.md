# Local Pipeline Review Checklist — TEMPLATE

Reusable template for manual verification of a SAPPHIRE daily pipeline run.
Copy this file, fill in Section 0.1, and work through each phase in order.

---

## Usage

1. Copy this file:
   ```bash
   cp doc/dev/review_checklist_local_template.md \
      doc/dev/review_checklist_local_YYYY-MM-DD.md
   ```
2. Fill in the environment variable values in **Section 0.1**.
3. Run the pipeline using one of two approaches:
   a. **Per-module approach** (recommended for verification): Run each module
      individually using the run commands in Sections 2–7, verifying data tables
      after each one.
   b. **Full pipeline approach**: Run `bash apps/run_locally.sh daily` for the
      full pipeline, then use Section 10 for verification.
4. Work through each section, running the curl commands and recording results in
   the `<!-- RESULT: -->` placeholders.
5. Keep the filled checklist **local only** — it contains operational data.
   The `.gitignore` pattern `doc/dev/review_checklist_local_20*.md` must exclude
   it from commits. This template (no date suffix) is safe to commit.

---

## API Key Notes

**ML forecasts are stored with `horizon=day`** in the postprocessing API
regardless of the horizon_type context they were run in (pentad or decad).
To query ML model forecasts (TFT, TiDE, TSMixer), always use `horizon=day`
with a `model` filter.

**LR forecasts have a separate endpoint**: `/api/postprocessing/lr-forecast/`
(not `/api/postprocessing/forecast/`).

**Combined forecasts** (EM, NE) are at `/api/postprocessing/forecast/` with
`horizon=pentad` or `horizon=decade`.

---

## 0. Prerequisites

### 0.1 Environment variables

Fill in these values before running any curl commands or the pipeline. All
subsequent commands reference these variables — do not hardcode values inline.

```bash
export BASE_URL="http://localhost:8000"
export S1="<station_code_1>"          # e.g. primary monitoring station
export S2="<station_code_2>"          # e.g. secondary monitoring station
export TODAY="YYYY-MM-DD"             # date of this pipeline run
export RECENT_START="YYYY-MM-DD"      # ~10 days before TODAY
export RECENT_END="YYYY-MM-DD"        # day before TODAY (TODAY minus 1)
export TODAY_MINUS_5="YYYY-MM-DD"     # TODAY minus 5 days (for recent daily data window)
export TODAY_MINUS_30="YYYY-MM-DD"    # TODAY minus 30 days (for maintenance coverage checks)
export FORECAST_END="YYYY-MM-DD"      # TODAY plus 15 days (for checking forecast-period meteo/snow)
export PREV_PENTAD="YYYY-MM-DD"       # most recent pentad issue day ≤ RECENT_END (5/10/15/20/25/EOM)
export PREV_DECAD="YYYY-MM-DD"        # most recent decad issue day ≤ RECENT_END (10/20/EOM)
export MONTH_START="YYYY-MM-01"       # first day of current month (for long-term queries)
export MONTH_END="YYYY-MM-31"         # last day of current month (use 28/29/30/31 as appropriate)
```

> Note: `RECENT_END` is typically `TODAY - 1 day`. It is a separate variable
> so that all baseline queries use a consistent window and do not accidentally
> include the post-run state.

> `PREV_PENTAD` is the most recent pentad issue day on or before `RECENT_END`.
> Pentad days: 5, 10, 15, 20, 25, last day of month. `PREV_DECAD` is similar
> for decad days (10, 20, last day of month). These target the specific dates
> that hindcast/maintenance should have filled.

### 0.1a Helper functions

Define this function in your shell session before running any checks. All
queries below pipe their JSON output through `table` for full tabular display.

```bash
# Reusable table formatter — pipe any JSON array into this
table() {
  python3 -c "
import sys, json
d = json.load(sys.stdin)
if not d: print('(no records)'); sys.exit()
keys = list(d[0].keys())
rows = [[str(r.get(k, '') or '') for k in keys] for r in d]
widths = [max(len(k), max((len(row[i]) for row in rows), default=0)) for i, k in enumerate(keys)]
fmt = '  '.join(f'{:<{w}}' for w in widths)
print(fmt.format(*keys))
print(fmt.format(*['-'*w for w in widths]))
for row in rows: print(fmt.format(*row))
print(f'\n({len(d)} records)')
"
}
```

### 0.2 Service health checks

- [ ] API gateway is up:
  ```bash
  curl -s $BASE_URL/health | python3 -m json.tool
  ```
  <!-- RESULT: -->

- [ ] All downstream services are ready:
  ```bash
  curl -s $BASE_URL/health/ready | python3 -m json.tool
  ```
  <!-- RESULT: -->

- [ ] Individual service status:
  ```bash
  curl -s $BASE_URL/health/services | python3 -m json.tool
  ```
  <!-- RESULT: -->

**Expected**: all services report `"status": "healthy"` or `"ok"`. If any
service is down, do not proceed — the pipeline will write to a dead endpoint
and silently drop data.

### 0.3 Confirm Docker containers are running

```bash
cd sapphire && docker-compose ps
```

<!-- RESULT: (list containers and their status) -->

All containers (`preprocessing-api`, `postprocessing-api`, `api-gateway`, etc.)
should show `Up`.

### 0.4 Automated pre-run validation (baseline snapshot)

Run `validate_pipeline.py` in pre-run mode to snapshot current record counts.
This replaces the manual baseline counting in Sections 1.1–1.4 for automated
checks (ML flag distribution, snow dates, EM/NE parity, data freshness).

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh validate --phase pre --baseline /tmp/vp_baseline.json
```

<!-- RESULT: (paste last line: "VALIDATION SUMMARY: N passed, 0 failed, ...") -->

The baseline JSON is written to `/tmp/vp_baseline.json`. The automated checks
cover the following and produce JSON output in `counts` and `max_date` fields:

| Automated check | Replaces / supplements |
|----------------|------------------------|
| ML flag distribution | Section 5.1 manual `flag_dist=` inspection |
| Snow operational values | Section 1.3 year-2000 date note |
| EM/NE parity (pentad/decade) | Section 7.1 manual count comparison |
| Data freshness (`max_date`) | Sections 1.1–1.4 `max_date=` recording |

Manual sections 1.1–1.4 remain useful for per-station spot-checks and
discharge value inspection; the automated checks provide a quick pass/fail.

---

## 1. Before Run: Baseline Snapshot

Capture existing data for both stations **before** running the pipeline. This
establishes what existed in the `RECENT_START` to `RECENT_END` window so you
can confirm new records appear after the run.

### 1.1 Preprocessing — Runoff

- [ ] $S1 — recent daily runoff (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — recent daily runoff (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

### 1.2 Preprocessing — Meteo

- [ ] $S1 temperature (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 temperature — forecast period (ERA5 extension check):
  ```bash
  # If forecast-period rows exist, ERA5 forecast extension is working.
  # Empty result means only reanalysis data is available.
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 precipitation (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 precipitation — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 temperature (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 temperature — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 precipitation (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 precipitation — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

### 1.3 Preprocessing — Snow

- [ ] $S1 SWE (most recent records):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 SWE — forecast period:
  ```bash
  # Check for forecast-period snow values
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 SWE (most recent records):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 SWE — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**Note**: Historical SWE norm records use year-2000 dates as a day-of-year
index. Operational SWE records written by `preprocessing_gateway` should have
current-year dates. If **all** snow dates are year-2000, the operational
update window was likely missed (see PREPG-003). The automated
`check_snow_operational_values` check in Section 0.4 detects this condition
and emits WARN when only year-2000 dates are present.

### 1.4 Postprocessing — Short-term baseline counts

**Automated alternative available**: Section 0.4 (`--phase pre`) records
baseline counts for all check targets automatically. The manual queries below
remain useful for per-station and per-model inspection.

Record counts here; compare after run to confirm new records were written.

- [ ] $S1 ML TFT forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 ML TFT forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 EM pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 LR pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 LR pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

---

## 2. Operational — preprocessing_runoff

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh preprocessing_runoff
```

### What this module writes

- `preprocessing_runoff` writes today's discharge observations to the
  preprocessing API (`horizon=day`).

### 2.1 Verify: Data freshness — new runoff record for today

**Automated alternative available**: The `check_data_freshness` check (Section
0.4) compares `max_date` for each dataset against `forecast_date` and emits
WARN for any dataset more than 3 days stale (configurable via
`FRESHNESS_THRESHOLD_DAYS`). The manual queries below provide per-station
discharge values and flag details not captured by the automated check.

- [ ] $S1 — today + past 5 days discharge:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — today + past 5 days discharge:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: A record with `"date": "$TODAY"` and a non-null
`"discharge"` value. If the iEasyHydro source has not provided today's
observation yet, `count=0` is acceptable — note this as a data availability
issue, not a code bug.

**Red flags**:
- `"discharge": null` — data received but value is missing.
- HTTP 4xx/5xx from the API endpoint — service is down or endpoint changed.

---

## 3. Operational — preprocessing_gateway

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh preprocessing_gateway
```

### What this module writes

- `preprocessing_gateway` extends ERA5 meteo and snow data through today.

### 3.1 Verify: New meteo data for today

**Automated alternative available**: `check_data_freshness` (Section 0.4)
covers meteo `max_date` freshness automatically. The manual queries below
give actual temperature and precipitation values for sanity-range checking
(-30 to +40 °C, ≥ 0 mm).

- [ ] $S1 temperature (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (sanity: values typically -30 to +40 °C) -->

- [ ] $S1 temperature — forecast period (ERA5 extension check):
  ```bash
  # If forecast-period rows exist, ERA5 forecast extension is working.
  # Empty result means only reanalysis data is available.
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 precipitation (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (sanity: ≥ 0, typically < 100 mm/day) -->

- [ ] $S1 precipitation — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 temperature (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 temperature — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 precipitation (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 precipitation — forecast period:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**Red flags**:
- Empty arrays — ERA5 extension did not run or failed silently.
- Values identical to previous day for multiple stations — possible ERA5
  stale data.

### 3.2 Verify: Snow data

- [ ] $S1 — most recent SWE records:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — forecast-period snow values:
  ```bash
  # Check for forecast-period snow values
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — most recent SWE records:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — forecast-period snow values:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

---

## 4. Operational — linear_regression

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_PREDICTION_MODE=BOTH \
  bash apps/run_locally.sh linear_regression
```

> **Note**: `SAPPHIRE_PREDICTION_MODE=BOTH` runs both pentad and decad
> forecasts in a single invocation.

### What this module writes

- `linear_regression` writes LR pentad and decad forecasts to `/lr-forecast/`.

### 4.1 Verify: LR forecasts written

Pentad issue days: 5, 10, 15, 20, 25, and last day of the month.
Decad issue days: 10, 20, and last day of the month.

Determine whether TODAY is a boundary day before checking:

```
TODAY day-of-month: ____
Is a pentad issue day? [ ] YES  [ ] NO
Is a decad issue day?  [ ] YES  [ ] NO
```

- [ ] $S1 — LR pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — LR pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

> **LR-008 check**: On pentad issue days (5,10,15,20,25,EOM), `horizon_in_year` must equal the **target** pentad (issue pentad + 1, wrapping to 1 after pentad 72). E.g., on day 25 of month 3 (issue pentad 17): `horizon_in_year=18`, `horizon_value=6`. If you see the issue pentad (e.g., `horizon_in_year=17`, `horizon_value=5`), the LR-008 metadata override is not active.

- [ ] $S1 — LR decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — LR decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

> **LR-008 check**: On decad issue days (10, 20, EOM), `horizon_in_year` must equal the **target** decad (issue decad + 1, wrapping to 1 after decad 36). E.g., on day 20 of month 3 (issue decad 8): `horizon_in_year=9`, `horizon_value=3`. If you see the issue decad (e.g., `horizon_in_year=8`, `horizon_value=2`), the LR-008 metadata override is not active.

**Red flags**:
- No records at all — LR module has not written any forecasts recently.
- Negative forecast values (`-1.0`) — sentinel value leaking through.

---

## 5. Operational — machine_learning

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh machine_learning
```

> **Note**: `ML_MODE` defaults to `DECAD`. Set `ML_MODE=BOTH` if ML should
> run for all prediction modes.

### What this module writes

- `machine_learning` writes TFT, TiDE, TSMixer forecasts stored with
  `horizon=day` at the postprocessing API.

### 5.1 Verify: ML daily forecasts written

**Automated alternative available**: `check_ml_flag_distribution` (Section
0.4) detects stuck-flag conditions (all records with the same flag value) and
populates `counts` in the JSON output. The manual queries below show per-model,
per-station breakdowns and exact forecast values.

- [ ] $S1 TFT — today's forecasts (all target dates):
  ```bash
  echo "=== S1 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TiDE — today's forecasts:
  ```bash
  echo "=== S1 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TSMixer — today's forecasts:
  ```bash
  echo "=== S1 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TFT — recent history (TODAY_MINUS_5 to TODAY):
  ```bash
  echo "=== S1 TFT (recent history) ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=200" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TFT — today's forecasts:
  ```bash
  echo "=== S2 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TiDE — today's forecasts:
  ```bash
  echo "=== S2 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TSMixer — today's forecasts:
  ```bash
  echo "=== S2 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] Cross-module date consistency — ML forecast issue_date must equal TODAY:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json, os
  d = json.load(sys.stdin)
  today = os.environ.get('TODAY','?')
  for r in d[:3]:
      issue = r.get('issue_date') or r.get('date','?')
      print(f'  issue_date={issue}  expected={today}  match={issue==today}')
  "
  ```
  <!-- RESULT: issue_date=  match= (all should be True) -->

**What to look for**: At least one record per model per station with today's
date and non-null `"forecasted_discharge"` value.

**Red flags**:
- Empty arrays for all three models — ML module crashed before writing.
- Records present for one station but not the other — org-scoping or station
  filter issue.
- `"forecasted_discharge": null` — model ran but produced NaN output.
- `flag_dist` contains only one flag value across many records — flag logic
  may be stuck.

---

## 6. Operational — long_term_forecasting

### Gate logic

The long-term forecasting phase runs only when TODAY falls within ±5 days of
a monthly issue day (10th or 25th of the month).

```
Gate: LT runs if |TODAY - nearest_issue_day| ≤ 5
Issue days: 10th and 25th of each month

TODAY day-of-month: ____
Nearest issue day:  ____  (10 or 25)
Delta:              ____  days
Gate:               [ ] OPEN (run expected)   [ ] CLOSED (run not expected)
```

If the gate is CLOSED and you still want to verify LT behaviour, override the
date:

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  LT_FORECAST_TODAY=<YYYY-MM-10 or YYYY-MM-25> \
  bash apps/run_locally.sh long-term
```

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh long_term_forecasting
```

### What this module writes

- `long_term_forecasting` writes monthly forecasts and updates monthly skill
  metrics when the gate is open.

### 6.1 Verify: Long-term forecasts written (only if gate OPEN or forced)

- [ ] $S1 — monthly forecasts (current month window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — monthly forecasts (current month window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**Red flags**:
- Empty arrays after a forced run — `long_term_forecasting` module crashed or
  no models were eligible.
- Records with null `forecast` values — model ran but output NaN.

### 6.2 Verify: Long-term skill metrics updated (only if gate OPEN or forced)

- [ ] $S1 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

---

## 7. Operational — postprocessing_forecasts

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_PREDICTION_MODE=BOTH \
  bash apps/run_locally.sh postprocessing_forecasts
```

> **Note**: `SAPPHIRE_PREDICTION_MODE=BOTH` runs both pentad and decad
> combined forecasts in a single invocation.

### What this module writes

- `postprocessing_forecasts` reads individual model forecasts, computes
  ensemble mean (EM) and norm-error (NE) combined forecasts, and writes them
  to `/forecast/` with `horizon=pentad` or `horizon=decade`.

### 7.1 Verify: Combined forecasts (EM, NE) written

- [ ] $S1 — EM pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — NE pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — NE pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — EM decad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM decad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: EM rows should have non-null `q05`, `q25`, `q75`, `q95`
fields if ML models ran successfully. NE rows represent norm-error ensembles.

**Red flags**:
- EM/NE arrays empty while individual ML model arrays are populated —
  postprocessing ensemble step failed.
- EM `q05`/`q25`/`q75`/`q95` all null — quantiles not being written.
- `q05 > q25` or `q75 > q95` — quantile ordering violation (see 7.2).

### 7.2 Quantile ordering spot-check

- [ ] $S1 — EM pentad raw data (full table):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — EM pentad quantile ordering validation:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "
  import sys, json
  rows = json.load(sys.stdin)
  for r in rows:
      q = [r.get('q05'), r.get('q25'), r.get('q75'), r.get('q95')]
      if all(x is not None for x in q):
          ok = q[0] <= q[1] <= q[2] <= q[3]
          print(r.get('date'), [round(x,3) for x in q], 'OK' if ok else 'FAIL')
      else:
          print(r.get('date'), q, 'NULL_QUANTILES')
  "
  ```
  <!-- RESULT: all OK? -->

- [ ] $S2 — EM pentad raw data (full table):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM pentad quantile ordering validation:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "
  import sys, json
  rows = json.load(sys.stdin)
  for r in rows:
      q = [r.get('q05'), r.get('q25'), r.get('q75'), r.get('q95')]
      if all(x is not None for x in q):
          ok = q[0] <= q[1] <= q[2] <= q[3]
          print(r.get('date'), [round(x,3) for x in q], 'OK' if ok else 'FAIL')
      else:
          print(r.get('date'), q, 'NULL_QUANTILES')
  "
  ```
  <!-- RESULT: all OK? -->

**Expected**: All rows print `OK`. Collapsed quantiles (equal values at
low-variance stations) are acceptable but must still be in non-decreasing
order.

---

## 7a. Post-Operational Log Scan

Quick scan after all operational modules. Full log analysis in Section 8a.

```bash
# Scan for errors in the most recent log
grep -E "ERROR|CRITICAL|Traceback" apps/logs/run_locally_*.log 2>/dev/null | tail -20
```

<!-- RESULT: (paste any ERROR/CRITICAL/Traceback lines, or "clean") -->

---

## 8. Maintenance Runs

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh maintenance
```

> **Individual maintenance targets** (if you want to run them separately):
> - `maintenance:preprocessing_runoff` — Runoff gap-filling (30-day lookback)
> - `maintenance:preprocessing_gateway` — Extend ERA5 reanalysis data
> - `maintenance:linear_regression` — Linear regression hindcast
> - `maintenance:machine_learning` — ML NaN recalc + gap-fill + new stations
> - `maintenance:postprocessing_forecasts` — Fill missing ensemble forecasts
> - `maintenance:postprocessing_long_term` — Fill missing monthly ensemble forecasts

### 8.1 Verify: Preprocessing gap-fill

#### 30-day runoff coverage

Uses `$TODAY_MINUS_30` set in Section 0.1 for the 30-day lookback window.

- [ ] $S1 — discharge over last 30 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: (paste table output) (expected ~30 rows if no gaps) -->

- [ ] $S2 — discharge over last 30 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: Count should equal the number of days in the window
(up to 30) if data is complete. Fewer records indicate remaining gaps
(acceptable if source data is unavailable for those dates).

**Red flags**:
- Count is 0 — neither operational nor maintenance preprocessing wrote any data.
- Maintenance run logs show errors for these station codes.

#### ERA5 meteo backfill (30-day T coverage)

- [ ] $S1 T — 30-day meteo:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 T — 30-day meteo:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] Review window ($S1 T and P, RECENT_START to TODAY):
  ```bash
  echo "=== S1 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  echo "=== S1 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (should match Section 3 result) -->

### 8.2 Verify: ML gap-fill

- [ ] $S1 TFT — past 14 days (compare row count vs Section 1.4 baseline):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TFT — past 14 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: If gaps existed before the run, counts should be higher
after maintenance.

### 8.2a Diagnose: ML hindcast failure triage (if 8.2 shows no improvement)

Skip this section if Section 8.2 shows expected count increases. Use these
queries when ML maintenance appears to have failed silently — counts unchanged,
or logs show hindcast subprocess errors. These checks map to ML-002 failure
vectors (see `doc/plans/issues/high_prio_gi_draft_ml_hindcast_subprocess_root_cause.md`).

#### Input data availability (ML-002 Vectors 2, 6)

The hindcast needs historical discharge and ERA5 meteo data. If the API
returns empty results for these, the hindcast script crashes before producing
output (no try/except around `read_meteo_data_combined()` or
`fl.read_daily_discharge_data()`).

- [ ] Discharge data depth — does historical data exist for the hindcast
  training window? The hindcast typically needs data back to
  `ieasyhydroforecast_START_DATE` (often 2000-01-01). Check if at least some
  records exist in a recent year:
  ```bash
  echo "=== S1 2023 discharge ==="
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  echo "=== S2 2023 discharge ==="
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (expect ~5 rows shown; if (no records) then data absent) -->

- [ ] ERA5 meteo data depth — does T and P data exist for the hindcast
  training window? The script crashes at line 267 if `era5_data_transformed`
  is empty (`.min()` on empty series raises TypeError):
  ```bash
  echo "=== S1 T 2023 ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  echo "=== S1 P 2023 ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (expect ~5 rows shown; if (no records) then data absent) -->

**Red flags**:
- `count=0` for discharge or meteo — the hindcast will crash on empty data.
  Check whether the data was migrated to the DB (see Section 11 remediation).
- Low counts (e.g., < 100 for a full year) — may indicate incomplete migration
  or pagination limits masking the true count.

> **Pagination note**: The API default limit is 100. The queries above use
> `limit=5` intentionally — we only need to confirm records *exist*, not
> fetch them all. If `count=5`, data exists (at least 5 records). If
> `count=0`, data is genuinely absent.

#### Per-model hindcast output (ML-002 Vector 8 — silent data loss)

When the hindcast subprocess runs but a model's `predictor.hindcast()` throws
an exception, it is silently caught (`print(e)`, no logger) and returns an
empty DataFrame. The result is a CSV with headers but no data rows. Check
each model separately to identify which model failed:

- [ ] $S1 TFT — 30-day forecast coverage:
  ```bash
  echo "=== S1 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TiDE — 30-day forecast coverage:
  ```bash
  echo "=== S1 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TSMixer — 30-day forecast coverage:
  ```bash
  echo "=== S1 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TFT — 30-day forecast coverage:
  ```bash
  echo "=== S2 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TiDE — 30-day forecast coverage:
  ```bash
  echo "=== S2 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TSMixer — 30-day forecast coverage:
  ```bash
  echo "=== S2 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: All three models should have similar `unique_dates`
counts (~30). Large discrepancies between models indicate model-specific
failures (e.g., missing `.pt` file, scaler incompatibility).

**Red flags**:
- One model has 0 records while others have ~30 — that model's hindcast
  crashed (check model file exists on disk, scaler CSVs present).
- All models have records but high `null_fc` count — hindcast ran but
  produced NaN values (flag-related, see ML-006).
- All models have 0 records — hindcast subprocess crashed before any model
  ran (env var issue, API unreachable, or working directory problem).

#### Flag distribution analysis (ML-002 Vector 8)

The API does not expose a `flag` filter, so fetch recent forecasts and
count flags client-side. Flag semantics: 0 = good forecast, 1 = NaN
(gap to fill), 2 = NaN (unfillable), 4 = hindcast-produced.

- [ ] $S1 TFT flag distribution (30-day window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: After successful maintenance, most records should have
`flag=0` (operational) or `flag=4` (hindcast-produced). A high count of
`flag=1` or `flag=2` indicates the hindcast did not fill these gaps.

**Red flags**:
- All records have `flag=1` — maintenance ran but hindcast produced no data.
  Cross-reference with per-model coverage above to identify which model failed.
- `flag=None` — flag was not written at all (API write bug, see ML-004).

#### Filesystem checks (requires server access)

These checks cannot be done via the API. Run them on the server or in the
Docker container when the above API checks indicate a failure.

```bash
# Check model files exist (Vector 3 — IndexError on missing .pt)
ls -la $MODELS_AND_SCALERS_PATH/TFT/*.pt
ls -la $MODELS_AND_SCALERS_PATH/TiDE/*.pt
ls -la $MODELS_AND_SCALERS_PATH/TSMixer/*.pt

# Check scaler CSVs exist (Vector 7 — unguarded pd.read_csv)
for model_dir in TFT TiDE TSMixer; do
  echo "--- $model_dir ---"
  ls $MODELS_AND_SCALERS_PATH/$model_dir/scaler_stats_*.csv 2>/dev/null || echo "MISSING scaler CSVs"
done

# Check static features file exists (Vector 5 — index type mismatch)
ls -la $MODELS_AND_SCALERS_PATH/static_features/ML_basin_attributes_v2.csv

# Check hindcast logs for swallowed exceptions (Vector 8)
# The predictor catches all exceptions with print() — grep for error output
grep -i "error in hindcasting" apps/logs/*.log 2>/dev/null | tail -10
```

<!-- RESULT: (paste output or "all files present") -->

> **Cross-reference**: If API checks show one specific model missing while
> others are fine, check that model's `.pt` and scaler files first. If ALL
> models are missing, check API reachability and env var setup (see ML-002
> issue file for the full failure vector list).

### 8.3 Verify: LR hindcast

#### 30-day coverage

- [ ] $S1 — LR pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (expect ~5-6 pentad issue days in 30 days) -->

- [ ] $S2 — LR pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — LR decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (expect ~3 decad issue days in 30 days) -->

**What to look for**: 5 or 6 pentad issue days within 30 days. If fewer
records appear, LR hindcast may not have written for these stations.

#### 8.3a Verify: LR hindcast — previous pentad/decad spot-check

Targeted check for the most recent pentad issue day. Uses `$PREV_PENTAD` and
`$PREV_DECAD` set in Section 0.1.

- [ ] $S1 — LR pentad at PREV_PENTAD (single-date check):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (expect ≥1 row; WARN if empty — hindcast may not have filled this gap) -->

- [ ] $S2 — LR pentad at PREV_PENTAD (regression check for LR fix):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (S2 must have ≥1 record after LR fix) -->

- [ ] $S1 — LR decad at PREV_DECAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$PREV_DECAD&end_date=$PREV_DECAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (expect ≥1 row; WARN if empty) -->

> **S2 regression check**: S2 previously returned 0 records from the
> postprocessing API because the code queried `/forecast/?model=LR` instead
> of `/lr-forecast/`. After the fix, S2 must have records here. A FAIL on
> S2 indicates the fix has regressed.

### 8.4 Verify: Postprocessing maintenance

#### EM gap-fill coverage

- [ ] $S1 — EM pentad 30-day records (row count should ≈ LR pentad count from 8.3):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — NE pentad 30-day records (row count should ≈ EM pentad count):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — NE pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — EM decad 30-day records (row count should ≈ LR decad count from 8.3):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — NE decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — NE decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**What to look for**: EM and NE pentad counts should roughly match LR pentad
count (from 8.3). EM and NE decad counts should roughly match LR decad count.
Gaps in EM/NE where LR exists indicate the maintenance gap-fill did not run
or ran before LR hindcast wrote its data. See 8.4a for targeted date-level checks.

#### 8.4a Verify: Postprocessing maintenance — EM/NE at previous pentad

Only relevant if 8.3a showed LR records exist at PREV_PENTAD.

- [ ] $S1 — EM pentad at PREV_PENTAD (value-level check):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) (WARN if empty — maintenance may not have run yet) -->

- [ ] $S2 — EM pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — NE pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — NE pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: (paste table output) -->

> **Dependency note**: Postprocessing maintenance only fills EM/NE gaps when
> individual-model rows (LR, TFT, etc.) already exist for that date. If LR
> records exist (8.3a passed) but EM/NE are absent, maintenance may have run
> before LR hindcast wrote its data. Re-run maintenance standalone:
> ```bash
> ieasyhydroforecast_env_file_path=<path> \
>   SAPPHIRE_PREDICTION_MODE=BOTH \
>   POSTPROCESSING_GAPFILL_MAX_MONTHS=1 \
>   python apps/postprocessing_forecasts/postprocessing_maintenance.py
> ```

---

## 8a. Post-Maintenance Log Scan

Scan pipeline logs for errors before proceeding to the final verification.

```bash
# Scan for errors in the most recent run_locally log
grep -E "ERROR|CRITICAL|Traceback" apps/logs/run_locally_*.log 2>/dev/null | tail -20

# If log path differs, find the most recent log:
ls -t apps/logs/ | head -5
```

<!-- RESULT: (paste any ERROR/CRITICAL/Traceback lines, or "clean") -->

```bash
# Count warnings by module for a quick health summary
grep -oE "\[(preprocessing|machine_learning|linear_regression|postprocessing)[^\]]*\]" \
  apps/logs/run_locally_*.log 2>/dev/null | sort | uniq -c | sort -rn | head -20
```

<!-- RESULT: -->

---

## 9. Recalculate — Skill Metrics and Norms

### 9.1 Before: Skill Metrics Snapshot

Capture current state before recalculation so you can confirm values changed.

- [ ] $S1 — pentad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — pentad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — decad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — decad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — monthly skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — monthly skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

### 9.2 Run: recalculate_skill_metrics

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_PREDICTION_MODE=ALL \
  bash apps/run_locally.sh recalculate_skill_metrics
```

> **Note**: This is a slow operation (can take hours for large datasets).
> `SAPPHIRE_PREDICTION_MODE=ALL` recalculates pentad + decad + monthly +
> quarterly + seasonal + daily skill metrics. Use
> `SAPPHIRE_PREDICTION_MODE=BOTH` for pentad + decad only.

<!-- RESULT: (paste completion message or duration) -->

### 9.3 Verify: Skill Metrics Updated

Same queries as 9.1 but labeled AFTER. Compare `n_pairs` (should be >=
BEFORE value) and `nse` (may change as new forecast-observation pairs are
included). If `n_pairs` decreased, data may have been lost during
recalculation — investigate.

- [ ] $S1 — pentad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — pentad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — decad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — decad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — monthly skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — monthly skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**Red flags**:
- `n_pairs` = 0 or 1 after recalculation — insufficient historical data
  (acceptable for new stations, investigate for established ones).
- `n_pairs` decreased vs BEFORE — possible data loss during recalculation.
- Skill metrics completely absent — recalculation crashed, check logs.
- `nse` significantly worse than BEFORE — may indicate data quality issues
  in newly added pairs.

### 9.4 Run: recalculate_snow_norms

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh recalculate_snow_norms
```

<!-- RESULT: (paste completion message or duration) -->

### 9.5 Verify: Snow Norms Updated

Check that snow norm records exist for SWE and HS. Snow norms use year-2000
dates as day-of-year indices. After recalculation, `norm_dates` count should
be ~365. If `norm_dates=0`, the recalculation did not write norm records.

- [ ] $S1 — SWE norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) (norm records have year-2000 dates; operational records have current-year dates) -->

- [ ] $S1 — SWE forecast-period values:
  ```bash
  # Check for forecast-period snow values
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — SWE norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — SWE forecast-period values:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — HS norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=HS&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — HS norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=HS&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

---

## 9a. Automated Post-Run Validation (delta report)

Run `validate_pipeline.py` in post-run mode to compare current record counts
against the pre-run baseline and report any decreases (WARN) or increases
(INFO). This catches silent regressions that manual spot-checks might miss.

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh validate --phase post --baseline /tmp/vp_baseline.json
```

<!-- RESULT: (paste VALIDATION SUMMARY and any DELTA WARN lines) -->

**What to look for**:
- `DELTA WARN` lines indicate record counts decreased — investigate before
  signing off.
- `DELTA INFO` lines indicate counts increased — expected for new pipeline
  runs that add data.
- No delta lines means nothing changed (expected if run did not write new data,
  e.g. non-forecast day).

To also save the full JSON output for archival:

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh validate \
    --phase post \
    --baseline /tmp/vp_baseline.json \
    --output-json /tmp/vp_$(date +%F).json
```

<!-- RESULT: JSON file written to /tmp/vp_YYYY-MM-DD.json -->

---

## 10. Post-Run: Full Verification

Run after the entire pipeline completes. Confirm all expected data exists for
both stations.

### 10.1 Preprocessing completeness

- [ ] $S1 — runoff today + past 5 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — runoff today + past 5 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — meteo T (today + past 5 days):
  ```bash
  echo "=== S1 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — meteo P (today + past 5 days):
  ```bash
  echo "=== S1 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — meteo T (today + past 5 days):
  ```bash
  echo "=== S2 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — meteo P (today + past 5 days):
  ```bash
  echo "=== S2 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

### 10.2 Short-term forecast completeness

- [ ] $S1 TFT — today's forecasts:
  ```bash
  echo "=== S1 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TiDE — today's forecasts:
  ```bash
  echo "=== S1 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 TSMixer — today's forecasts:
  ```bash
  echo "=== S1 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TFT — today's forecasts:
  ```bash
  echo "=== S2 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TiDE — today's forecasts:
  ```bash
  echo "=== S2 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 TSMixer — today's forecasts:
  ```bash
  echo "=== S2 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — EM pentad records (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — EM pentad records (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

### 10.3 Skill metrics check

- [ ] $S1 — pentad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — pentad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S1 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

- [ ] $S2 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: (paste table output) -->

**Red flags**:
- `n_pairs` is 0 or 1 — recalculation ran but insufficient historical pairs
  (acceptable for new stations, flag for established ones).
- Skill metrics completely absent — recalculation step was skipped or crashed.

### 10.4 Summary table

Fill in after completing all verification steps above. Record actual values,
not just PASS/FAIL where a threshold applies.

| Check | $S1 result | $S2 result | Threshold | Status |
|-------|-----------|-----------|-----------|--------|
| Runoff today (discharge) | | | non-null | |
| Meteo T today | | | -30 to +40 °C | |
| Meteo P today | | | ≥ 0 mm | |
| Snow SWE (recent) | | | non-null | |
| ML TFT count today | | | ≥ 1 | |
| ML TiDE count today | | | ≥ 1 | |
| ML TSMixer count today | | | ≥ 1 | |
| ML null forecasts | | | 0 | |
| ML forecast issue_date = TODAY | | | TRUE | |
| LR pentad (recent issue day) | | | non-null fc; `horizon_in_year` = issue pentad + 1 (LR-008) | |
| LR decad (recent issue day) | | | non-null fc; `horizon_in_year` = issue decad + 1 (LR-008) | |
| LR S2 at PREV_PENTAD (regression) | | | ≥ 1 record (was 0 before fix) | |
| EM pentad (recent issue day) | | | non-null q | |
| EM quantile ordering | | | all OK | |
| NE pentad (recent issue day) | | | ≥ 1 record | |
| Long-term forecast count (Section 6) | | | ≥ 1 if gate open | |
| Monthly skill n_pairs (Section 6/9) | | | > 1 | |
| Pentad skill n_pairs | | | > 1 | |
| Pentad skill nse | | | numeric | |
| Decad skill n_pairs | | | > 1 | |
| Skill metrics delta vs before (Section 9) | n/a | n/a | n_pairs >= BEFORE | |
| Snow norms count (Section 9) | | | norm_dates ~365 | |
| Log scan errors | n/a | n/a | 0 ERROR/CRITICAL | |

---

## 11. Common Failure Patterns and Remediation

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| All preprocessing empty | Service not running or iEasyHydro API unreachable | Check `docker-compose ps`; check iEasyHydro API connectivity |
| Runoff null but meteo present | iEasyHydro source returned no obs for today | Check source API; not a code bug if data availability is the constraint |
| ML forecasts missing, LR present | ML module crashed (date format bug, shape mismatch) | Check pipeline logs for tracebacks in `machine_learning` phase |
| EM missing, individual ML present | Postprocessing ensemble step crashed | Check logs for `postprocessing_forecasts` phase errors |
| EM forecast value diverges from LR | Stale EM record computed before LR fix; or LR wrote after EM was computed | Re-run `postprocessing_maintenance.py` to recompute EM from updated LR values; compare LR fc from 8.3a with EM fc from 8.4a |
| EM `q05` all null | Quantile fields not being written | Check postprocessing_forecasts version; run `run_tests.sh postprocessing_forecasts` |
| `q05 > q25` quantile inversion | Quantile regression ordering not enforced | Check postprocessing quantile sort step |
| LR returns `-1.0` values | Sentinel value not converted to NaN | Check `linear_regression` module for sentinel guard |
| ML issue_date != TODAY | Clock skew or date override still active | Verify `$TODAY` env var; check for stale `LT_FORECAST_TODAY` override |
| Flag distribution stuck (all same flag) | Flag logic crash; all records assigned default flag | Check ML flag assignment code; review flag distribution in Section 5.1 |
| Skill metrics absent | Recalculation step skipped or API error on write | Check logs for `skill` keyword; verify postprocessing API write permissions |
| Skill `n_pairs` = 1 | Recalculation ran with insufficient history | Acceptable for new stations; investigate for established stations |
| Long-term forecasts absent | Gate condition not met (expected today) or module crash | Check gate logic in Section 6; use `LT_FORECAST_TODAY` override to force run |
| Log scan shows Traceback | Unhandled exception in a module | Read full traceback; identify module and fix before next run |
