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
3. Run the pipeline:
   ```bash
   ieasyhydroforecast_env_file_path=<path-to-your-.env> bash apps/run_locally.sh daily
   ```
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
```

> Note: `RECENT_END` is typically `TODAY - 1 day`. It is a separate variable
> so that all baseline queries use a consistent window and do not accidentally
> include the post-run state.

### 0.2 Service health checks

- [ ] API gateway is up:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s $BASE_URL/health | python3 -m json.tool
  ```
  <!-- RESULT: -->

- [ ] All downstream services are ready:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s $BASE_URL/health/ready | python3 -m json.tool
  ```
  <!-- RESULT: -->

- [ ] Individual service status:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s $BASE_URL/health/services | python3 -m json.tool
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

---

## 1. Before Run: Baseline Snapshot

Capture existing data for both stations **before** running the pipeline. This
establishes what existed in the `RECENT_START` to `RECENT_END` window so you
can confirm new records appear after the run.

### 1.1 Preprocessing — Runoff

- [ ] $S1 — recent daily runoff (count + max date):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}')
  [print(f'  date={r.get(\"date\")}  discharge={r.get(\"discharge\")}  flag={r.get(\"flag\")}') for r in d[-3:]]
  "
  ```
  <!-- RESULT: count=  max_date= -->

- [ ] $S2 — recent daily runoff (count + max date):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}')
  [print(f'  date={r.get(\"date\")}  discharge={r.get(\"discharge\")}  flag={r.get(\"flag\")}') for r in d[-3:]]
  "
  ```
  <!-- RESULT: count=  max_date= -->

### 1.2 Preprocessing — Meteo

- [ ] $S1 temperature (recent):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  vals = [r.get('value') for r in d if r.get('value') is not None]
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}  range=[{min(vals):.2f},{max(vals):.2f}]' if vals else f'count={len(d)} no values')
  "
  ```
  <!-- RESULT: count=  max_date=  range= -->

- [ ] $S1 precipitation (recent):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  vals = [r.get('value') for r in d if r.get('value') is not None]
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}  range=[{min(vals):.2f},{max(vals):.2f}]' if vals else f'count={len(d)} no values')
  "
  ```
  <!-- RESULT: count=  max_date=  range= -->

- [ ] $S2 temperature (recent):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  vals = [r.get('value') for r in d if r.get('value') is not None]
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}  range=[{min(vals):.2f},{max(vals):.2f}]' if vals else f'count={len(d)} no values')
  "
  ```
  <!-- RESULT: count=  max_date=  range= -->

- [ ] $S2 precipitation (recent):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  vals = [r.get('value') for r in d if r.get('value') is not None]
  print(f'count={len(d)}  max_date={dates[-1] if dates else \"none\"}  range=[{min(vals):.2f},{max(vals):.2f}]' if vals else f'count={len(d)} no values')
  "
  ```
  <!-- RESULT: count=  max_date=  range= -->

### 1.3 Preprocessing — Snow

- [ ] $S1 SWE (most recent records):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  value={r.get(\"value\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  values= -->

- [ ] $S2 SWE (most recent records):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  value={r.get(\"value\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  values= -->

**Note**: SWE records use year-2000 dates as a day-of-year index for
climatological norms. This is expected.

### 1.4 Postprocessing — Short-term baseline counts

Record counts here; compare after run to confirm new records were written.

- [ ] $S1 ML TFT forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=500" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= (baseline) -->

- [ ] $S2 ML TFT forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=500" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= (baseline) -->

- [ ] $S1 EM pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= (baseline) -->

- [ ] $S1 LR pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= (baseline) -->

---

## 2. Phase 1: Preprocessing (runs once)

### What this phase writes

- `preprocessing_runoff` writes today's discharge observations to the
  preprocessing API (`horizon=day`).
- `preprocessing_gateway` extends ERA5 meteo and snow data through today.

### 2.1 Verify: Data freshness — new runoff record for today

- [ ] $S1 — today's discharge record:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  discharge={r.get(\"discharge\")}  flag={r.get(\"flag\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  discharge=  flag= -->

- [ ] $S2 — today's discharge record:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  discharge={r.get(\"discharge\")}  flag={r.get(\"flag\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  discharge=  flag= -->

- [ ] Window count delta (RECENT_START to TODAY, vs baseline in 1.1):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S2 count={len(d)}')"
  ```
  <!-- RESULT: S1 count=  S2 count=  (delta vs baseline: +0 if iEasyHydro has no today obs yet) -->

**What to look for**: A record with `"date": "$TODAY"` and a non-null
`"discharge"` value. If the iEasyHydro source has not provided today's
observation yet, `count=0` is acceptable — note this as a data availability
issue, not a code bug.

**Red flags**:
- `"discharge": null` — data received but value is missing.
- HTTP 4xx/5xx from the API endpoint — service is down or endpoint changed.

### 2.2 Verify: New meteo data for today

- [ ] $S1 temperature today (date + value):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  [print(f'  date={r.get(\"date\")}  T={r.get(\"value\")}') for r in d] if d else print('count=0')
  "
  ```
  <!-- RESULT: date=  T= (sanity: typically -30 to +40 °C) -->

- [ ] $S1 precipitation today (date + value):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  [print(f'  date={r.get(\"date\")}  P={r.get(\"value\")}') for r in d] if d else print('count=0')
  "
  ```
  <!-- RESULT: date=  P= (sanity: ≥ 0, typically < 100 mm/day) -->

- [ ] $S2 temperature today:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  [print(f'  date={r.get(\"date\")}  T={r.get(\"value\")}') for r in d] if d else print('count=0')
  "
  ```
  <!-- RESULT: date=  T= -->

- [ ] $S2 precipitation today:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  [print(f'  date={r.get(\"date\")}  P={r.get(\"value\")}') for r in d] if d else print('count=0')
  "
  ```
  <!-- RESULT: date=  P= -->

- [ ] Window count delta ($S1 T and P, RECENT_START to TODAY):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 T count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 P count={len(d)}')"
  ```
  <!-- RESULT: S1 T count=  S1 P count=  (expect +1 vs baseline) -->

**Red flags**:
- Empty arrays — ERA5 extension did not run or failed silently.
- Values identical to previous day for multiple stations — possible ERA5
  stale data.

### 2.3 Verify: Snow data

- [ ] $S1 — most recent SWE records:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  value={r.get(\"value\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  values= -->

- [ ] $S2 — most recent SWE records:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  value={r.get(\"value\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  values= -->

---

## 3. Phase 2: Maintenance Preprocessing (runs once)

### What this phase writes

- `preprocessing_runoff --maintenance` backfills any gaps in the past ~30 days
  of discharge data via upserts.
- `preprocessing_gateway` extends ERA5 reanalysis data.

### 3.1 Verify: Gap-fill — 30-day runoff coverage

Note the 30-day start date (TODAY minus 30 days) and set it manually below.

- [ ] $S1 — discharge count and date range over last 30 days:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=60" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  range={dates[0] if dates else \"none\"} to {dates[-1] if dates else \"none\"}')
  "
  ```
  <!-- RESULT: count=  range= to  (expected ~30 if no gaps) -->

- [ ] $S2 — discharge count and date range over last 30 days:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=60" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  range={dates[0] if dates else \"none\"} to {dates[-1] if dates else \"none\"}')
  "
  ```
  <!-- RESULT: count=  range= to  -->

**What to look for**: Count should equal the number of days in the window
(up to 30) if data is complete. Fewer records indicate remaining gaps
(acceptable if source data is unavailable for those dates).

**Red flags**:
- Count is 0 — neither operational nor maintenance preprocessing wrote any data.
- Maintenance run logs show errors for these station codes.

### 3.2 Verify: ERA5 meteo backfill (30-day T coverage)

- [ ] $S1 T — 30-day count and range:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=60" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  range={dates[0] if dates else \"none\"} to {dates[-1] if dates else \"none\"}')
  "
  ```
  <!-- RESULT: count=  range= to  -->

- [ ] $S2 T — 30-day count and range:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=60" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(r.get('date','') for r in d)
  print(f'count={len(d)}  range={dates[0] if dates else \"none\"} to {dates[-1] if dates else \"none\"}')
  "
  ```
  <!-- RESULT: count=  range= to  -->

- [ ] Review window unchanged from Phase 1 (RECENT_START to TODAY):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 T count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 P count={len(d)}')"
  ```
  <!-- RESULT: S1 T count=  S1 P count=  (should match Phase 1 result) -->

---

## 4. Phase 3: Forecasting + Postprocessing (PENTAD then DECAD)

### What this phase writes

- `machine_learning` writes TFT, TiDE, TSMixer forecasts stored with
  `horizon=day` at the postprocessing API.
- `linear_regression` writes LR pentad and decad forecasts to
  `/lr-forecast/`.
- `postprocessing_forecasts` reads individual model forecasts, computes
  ensemble mean (EM) and norm-error (NE) combined forecasts, and writes them
  to `/forecast/` with `horizon=pentad` or `horizon=decade`.

### 4.1 Verify: ML daily forecasts written

- [ ] All models, $S1 — today's forecasts (issue_date=$TODAY):
  ```bash
  for model in TFT TiDE TSMixer; do
    echo "S1 $model:"
    curl -w "  Time: %{time_total}s\n" -s \
      "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=50" \
      | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  vals = [r.get('forecasted_discharge') for r in d]
  flags = [r.get('flag') for r in d]
  nulls = sum(1 for v in vals if v is None)
  flag_dist = {f: flags.count(f) for f in set(flags)}
  print(f'  count={len(d)}  null_fc={nulls}  flag_dist={flag_dist}')
  if d: print(f'  fc_range=[{min(v for v in vals if v):.3f}, {max(v for v in vals if v):.3f}]')
  "
  done
  ```
  <!-- RESULT: TFT count=  TiDE count=  TSMixer count=  null_fc=  flag_dist= -->

- [ ] All models, $S2 — today's forecasts:
  ```bash
  for model in TFT TiDE TSMixer; do
    echo "S2 $model:"
    curl -s \
      "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=50" \
      | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  vals = [r.get('forecasted_discharge') for r in d]
  flags = [r.get('flag') for r in d]
  nulls = sum(1 for v in vals if v is None)
  flag_dist = {f: flags.count(f) for f in set(flags)}
  print(f'  count={len(d)}  null_fc={nulls}  flag_dist={flag_dist}')
  if d: print(f'  fc_range=[{min(v for v in vals if v):.3f}, {max(v for v in vals if v):.3f}]')
  "
  done
  ```
  <!-- RESULT: TFT count=  TiDE count=  TSMixer count=  null_fc=  flag_dist= -->

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

- [ ] Window count delta ($S1 TFT, RECENT_START to TODAY vs baseline in 1.4):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=500" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count=  (delta vs baseline 1.4: should be +N_targets per run) -->

**What to look for**: At least one record per model per station with today's
date and non-null `"forecasted_discharge"` value.

**Red flags**:
- Empty arrays for all three models — ML module crashed before writing.
- Records present for one station but not the other — org-scoping or station
  filter issue.
- `"forecasted_discharge": null` — model ran but produced NaN output.
- `flag_dist` contains only one flag value across many records — flag logic
  may be stuck.

### 4.2 Verify: LR forecasts written

Pentad issue days: 1, 6, 11, 16, 21, 26 of the month.
Decad issue days: 1, 11, 21 of the month.

Determine whether TODAY is a boundary day before checking:

```
TODAY day-of-month: ____
Is a pentad issue day? [ ] YES  [ ] NO
Is a decad issue day?  [ ] YES  [ ] NO
```

- [ ] $S1 — LR pentad forecasts (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  fc_values= -->

- [ ] $S2 — LR pentad forecasts (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  fc_values= -->

- [ ] $S1 — LR decad forecasts (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  fc_values= -->

- [ ] $S2 — LR decad forecasts (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}') for r in d]
  "
  ```
  <!-- RESULT: count=  dates=  fc_values= -->

- [ ] Confirm today's LR count (0 expected if not a boundary day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$TODAY&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 pentad today count={len(d)}')"
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$TODAY&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'S1 decad today count={len(d)}')"
  ```
  <!-- RESULT: pentad today count=  decad today count= -->

**Red flags**:
- No records at all — LR module has not written any forecasts recently.
- Negative forecast values (`-1.0`) — sentinel value leaking through.

### 4.3 Verify: Combined forecasts (EM, NE) written

- [ ] $S1 — EM pentad (recent window, count + quantile sample):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  for r in d[-2:]:
      print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}  q05={r.get(\"q05\")}  q25={r.get(\"q25\")}  q75={r.get(\"q75\")}  q95={r.get(\"q95\")}')
  "
  ```
  <!-- RESULT: count=  q05=  q25=  q75=  q95= -->

- [ ] $S2 — EM pentad (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  for r in d[-2:]:
      print(f'  date={r.get(\"date\")}  fc={r.get(\"forecasted_discharge\")}  q05={r.get(\"q05\")}  q95={r.get(\"q95\")}')
  "
  ```
  <!-- RESULT: count=  q05=  q95= -->

- [ ] $S1 — NE pentad (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= -->

- [ ] $S2 — NE pentad (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= -->

- [ ] $S1 — EM decad (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= -->

- [ ] $S2 — EM decad (recent window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= -->

**What to look for**: EM rows should have non-null `q05`, `q25`, `q75`, `q95`
fields if ML models ran successfully. NE rows represent norm-error ensembles.

**Red flags**:
- EM/NE arrays empty while individual ML model arrays are populated —
  postprocessing ensemble step failed.
- EM `q05`/`q25`/`q75`/`q95` all null — quantiles not being written.
- `q05 > q25` or `q75 > q95` — quantile ordering violation (see 4.4).

### 4.4 Quantile ordering spot-check

- [ ] $S1 — EM pentad quantile ordering:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" \
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

- [ ] $S2 — EM pentad quantile ordering:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" \
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

## 5. Phase 4: Maintenance (PENTAD then DECAD)

### What this phase writes

- ML maintenance: recalculates NaN forecasts, fills ML gaps, handles new
  stations.
- LR hindcast: backfills historical LR forecasts.
- Postprocessing maintenance: fills missing EM/NE ensembles where individual
  model rows exist but ensemble was not computed.

### 5.1 Verify: ML gap-fill — count delta vs baseline

- [ ] $S1 TFT — past 14 days count (compare vs Section 1.4 baseline):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count=  delta vs 1.4 baseline= -->

- [ ] $S2 TFT — past 14 days count:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count=  delta= -->

**What to look for**: If gaps existed before the run, counts should be higher
after maintenance.

### 5.2 Verify: LR hindcast — 30-day coverage

- [ ] $S1 — LR pentad 30-day record count and dates:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(set(r.get('date','') for r in d))
  print(f'count={len(d)}  dates={dates}')
  "
  ```
  <!-- RESULT: count=  dates= (expect ~5-6 pentad issue days in 30 days) -->

- [ ] $S2 — LR pentad 30-day count:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
  <!-- RESULT: count= -->

- [ ] $S1 — LR decad 30-day count:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=50" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  dates = sorted(set(r.get('date','') for r in d))
  print(f'count={len(d)}  dates={dates}')
  "
  ```
  <!-- RESULT: count=  dates= (expect ~3 decad issue days in 30 days) -->

**What to look for**: 5 or 6 pentad issue days within 30 days. If fewer
records appear, LR hindcast may not have written for these stations.

### 5.3 Verify: Postprocessing maintenance — EM gap-fill coverage

- [ ] $S1 — EM pentad 30-day count (should match LR pentad count):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'EM pentad records')"
  ```
  <!-- RESULT: count= (should ≈ LR pentad count from 5.2) -->

- [ ] $S2 — EM pentad 30-day count:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=<TODAY_MINUS_30>&end_date=$TODAY&limit=50" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'EM pentad records')"
  ```
  <!-- RESULT: count= -->

**What to look for**: EM count should roughly match LR count. Gaps in EM
where LR exists indicate the maintenance gap-fill did not run.

---

## 5a. Post-Run Log Scan

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

## 6. Phase 5: Long-Term Forecasting (gated)

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

### 6.1 Verify: Long-term forecasts written (only if gate OPEN or forced)

- [ ] $S1 — monthly forecasts (current month window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&start_date=<YYYY-MM-01>&end_date=<YYYY-MM-31>&limit=20" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecast\")}  model={r.get(\"model\")}') for r in d[:5]]
  "
  ```
  <!-- RESULT: count=  fc_values=  models= -->

- [ ] $S2 — monthly forecasts (current month window):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&start_date=<YYYY-MM-01>&end_date=<YYYY-MM-31>&limit=20" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  date={r.get(\"date\")}  fc={r.get(\"forecast\")}  model={r.get(\"model\")}') for r in d[:5]]
  "
  ```
  <!-- RESULT: count=  fc_values=  models= -->

**Red flags**:
- Empty arrays after a forced run — `long_term_forecasting` module crashed or
  no models were eligible.
- Records with null `forecast` values — model ran but output NaN.

### 6.2 Verify: Long-term skill metrics updated (only if gate OPEN or forced)

- [ ] $S1 — monthly skill metrics (model + n_pairs + nse):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}  mae={r.get(\"mae\")}') for r in d[:5]]
  "
  ```
  <!-- RESULT: count=  n_pairs=  nse=  mae= -->

- [ ] $S2 — monthly skill metrics:
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=10" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print(f'count={len(d)}')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}  mae={r.get(\"mae\")}') for r in d[:5]]
  "
  ```
  <!-- RESULT: count=  n_pairs=  nse=  mae= -->

---

## 7. Post-Run: Full Verification

Run after the entire pipeline completes. Confirm all expected data exists for
both stations.

### 7.1 Preprocessing completeness

- [ ] $S1 — runoff record today:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); r=d[0] if d else {}; print('PASS discharge=' + str(r.get('discharge')) if d else 'WARN no obs (check data availability)')"
  ```
  <!-- RESULT: -->

- [ ] $S2 — runoff record today:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); r=d[0] if d else {}; print('PASS discharge=' + str(r.get('discharge')) if d else 'WARN no obs (check data availability)')"
  ```
  <!-- RESULT: -->

- [ ] $S1 — meteo T and P today:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('T: PASS val=' + str(d[0].get('value')) if d else 'T: FAIL')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('P: PASS val=' + str(d[0].get('value')) if d else 'P: FAIL')"
  ```
  <!-- RESULT: T=  P= -->

- [ ] $S2 — meteo T and P today:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('T: PASS val=' + str(d[0].get('value')) if d else 'T: FAIL')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('P: PASS val=' + str(d[0].get('value')) if d else 'P: FAIL')"
  ```
  <!-- RESULT: T=  P= -->

### 7.2 Short-term forecast completeness

- [ ] $S1 — at least one ML model wrote a forecast today:
  ```bash
  for model in TFT TiDE TSMixer; do
    curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=5" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print('$model: PASS count=' + str(len(d)) if len(d)>0 else '$model: FAIL')"
  done
  ```
  <!-- RESULT: TFT=  TiDE=  TSMixer= -->

- [ ] $S2 — at least one ML model wrote a forecast today:
  ```bash
  for model in TFT TiDE TSMixer; do
    curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=5" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print('$model: PASS count=' + str(len(d)) if len(d)>0 else '$model: FAIL')"
  done
  ```
  <!-- RESULT: TFT=  TiDE=  TSMixer= -->

- [ ] $S1 — EM record exists (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('EM pentad: PASS count=' + str(len(d)) if d else 'EM pentad: FAIL')"
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM record exists (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('EM pentad: PASS count=' + str(len(d)) if d else 'EM pentad: FAIL')"
  ```
  <!-- RESULT: -->

### 7.3 Skill metrics check

- [ ] $S1 — pentad skill metrics (model + n_pairs + nse):
  ```bash
  curl -w "\nTime: %{time_total}s\n" -s \
    "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print('PASS' if d else 'FAIL - no records')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}  mae={r.get(\"mae\")}') for r in d[:3]]
  "
  ```
  <!-- RESULT: n_pairs=  nse=  mae= -->

- [ ] $S2 — pentad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print('PASS' if d else 'FAIL - no records')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}  mae={r.get(\"mae\")}') for r in d[:3]]
  "
  ```
  <!-- RESULT: n_pairs=  nse=  mae= -->

- [ ] $S1 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print('PASS' if d else 'FAIL - no records')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}') for r in d[:3]]
  "
  ```
  <!-- RESULT: n_pairs=  nse= -->

- [ ] $S2 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=5" \
    | python3 -c "
  import sys, json
  d = json.load(sys.stdin)
  print('PASS' if d else 'FAIL - no records')
  [print(f'  model={r.get(\"model\")}  n_pairs={r.get(\"n_pairs\")}  nse={r.get(\"nse\")}') for r in d[:3]]
  "
  ```
  <!-- RESULT: n_pairs=  nse= -->

**Red flags**:
- `n_pairs` is 0 or 1 — recalculation ran but insufficient historical pairs
  (acceptable for new stations, flag for established ones).
- Skill metrics completely absent — recalculation step was skipped or crashed.

### 7.4 Summary table

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
| LR pentad (recent issue day) | | | non-null fc | |
| LR decad (recent issue day) | | | non-null fc | |
| EM pentad (recent issue day) | | | non-null q | |
| EM quantile ordering | | | all OK | |
| NE pentad (recent issue day) | | | ≥ 1 record | |
| Pentad skill n_pairs | | | > 1 | |
| Pentad skill nse | | | numeric | |
| Decad skill n_pairs | | | > 1 | |
| Log scan errors | n/a | n/a | 0 ERROR/CRITICAL | |

---

## 8. Common Failure Patterns and Remediation

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| All preprocessing empty | Service not running or iEasyHydro API unreachable | Check `docker-compose ps`; check iEasyHydro API connectivity |
| Runoff null but meteo present | iEasyHydro source returned no obs for today | Check source API; not a code bug if data availability is the constraint |
| ML forecasts missing, LR present | ML module crashed (date format bug, shape mismatch) | Check pipeline logs for tracebacks in `machine_learning` phase |
| EM missing, individual ML present | Postprocessing ensemble step crashed | Check logs for `postprocessing_forecasts` phase errors |
| EM `q05` all null | Quantile fields not being written | Check postprocessing_forecasts version; run `run_tests.sh postprocessing_forecasts` |
| `q05 > q25` quantile inversion | Quantile regression ordering not enforced | Check postprocessing quantile sort step |
| LR returns `-1.0` values | Sentinel value not converted to NaN | Check `linear_regression` module for sentinel guard |
| ML issue_date != TODAY | Clock skew or date override still active | Verify `$TODAY` env var; check for stale `LT_FORECAST_TODAY` override |
| Flag distribution stuck (all same flag) | Flag logic crash; all records assigned default flag | Check ML flag assignment code; review flag distribution in Section 4.1 |
| Skill metrics absent | Recalculation step skipped or API error on write | Check logs for `skill` keyword; verify postprocessing API write permissions |
| Skill `n_pairs` = 1 | Recalculation ran with insufficient history | Acceptable for new stations; investigate for established stations |
| Long-term forecasts absent | Gate condition not met (expected today) or module crash | Check gate logic in Section 6; use `LT_FORECAST_TODAY` override to force run |
| Log scan shows Traceback | Unhandled exception in a module | Read full traceback; identify module and fix before next run |
