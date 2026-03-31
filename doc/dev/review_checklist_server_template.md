# Server Pipeline Review Checklist — TEMPLATE

Reusable template for manual verification of a SAPPHIRE pipeline run on a
production or test server (AWS EC2). All verification is done via `curl`
commands against the SAPPHIRE API and Docker commands on the server itself.

---

## Usage

1. SSH into the server:
   ```bash
   ssh ubuntu@<server-ip>
   ```
2. Copy this file to the server or keep it open locally — all `curl` commands
   run on the server (or from any machine that can reach the API).
3. Fill in the environment variable values in **Section 0.1**.
4. Work through each section, running the curl commands and pasting the output
   into the `<!-- RESULT: -->` placeholders.
5. **Result format**: Paste the **complete, unabridged** output of each curl
   command (piped through `table`) into the RESULT comment. **NEVER** summarize,
   shorten, or use `...` to elide rows. The full raw table with every row is the
   record — it must be reviewable without re-running the query.
   ```
   <!-- RESULT:
   horizon_type  code   date        discharge  predictor  horizon_value  ...
   ------------  -----  ----------  ---------  ---------  -------------  ...
   day           12345  2026-01-01  5.2                   1              ...
   day           12345  2026-01-02  5.3                   2              ...

   (N records)
   -->
   ```
6. Keep the filled checklist **local only** — it contains operational data.
   This template (no date suffix) is safe to commit.

---

## API Key Notes

**Raw short-term ML forecasts are stored with `horizon=day`** in the
postprocessing API regardless of the horizon_type context they were run in
(pentad or decad). To query ML model forecasts (TFT, TiDE, TSMixer), always
use `horizon=day` with a `model` filter.

**Raw long-term ML forecasts are stored with `horizon=month`,
`horizon=quarter`, or `horizon=season`** in the postprocessing API. To query
long-term ML model forecasts (TFT, TiDE, TSMixer), use `horizon=month`,
`horizon=quarter`, or `horizon=season`, depending on the horizon_type.

**LR forecasts have a separate endpoint**: `/api/postprocessing/lr-forecast/`
(not `/api/postprocessing/forecast/`).

**Combined forecasts** (EM, NE, and most other models) are at
`/api/postprocessing/forecast/` with `horizon=pentad`, `horizon=decade`,
`horizon=month`, `horizon=quarter`, or `horizon=season`.

---

## 0. Prerequisites

### 0.1 Environment variables

Fill in these values before running any curl commands. All subsequent commands
reference these variables — do not hardcode values inline.

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

### Server-specific variables

```bash
export REPO_DIR="/data/SAPPHIRE_Forecast_Tools"
export ENV_FILE="/data/<data_folder>/config/<env_file>"
export LOG_DIR="/home/ubuntu/logs"
```

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
rows = [['' if r.get(k) is None else str(r[k]) for k in keys] for r in d]
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

#### SAPPHIRE services (API stack)

```bash
cd /data/SAPPHIRE_Forecast_Tools/sapphire && docker compose ps
```

<!-- RESULT: -->

All containers (`preprocessing-api`, `postprocessing-api`, `api-gateway`, etc.)
should show `Up`.

#### Luigi daemon

```bash
cd /data/SAPPHIRE_Forecast_Tools && docker compose -f bin/docker-compose-luigi.yml ps
```

<!-- RESULT: -->

The `luigi-daemon` container should show `Up`. If not:

```bash
docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
```

#### Luigi web UI

```bash
curl -s http://localhost:8082/ | head -5
```

<!-- RESULT: (should return HTML, confirming Luigi daemon is reachable) -->

### 0.4 Check crontab is installed

```bash
crontab -l
```

<!-- RESULT: -->

Verify the crontab matches the expected schedule (see `doc/deployment.md`).

### 0.5 Check recent pipeline logs

```bash
ls -lt $LOG_DIR/sapphire_*.log | head -10
```

<!-- RESULT: -->

If the pipeline has already run today, recent log files should be present.

---

## 1. Preprocessing Verification

### 1.1 Runoff data

- [ ] $S1 — recent daily runoff (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — recent daily runoff (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

**What to look for**: A record with `"date": "$TODAY"` and a non-null
`"discharge"` value. If the iEasyHydro source has not provided today's
observation yet, `count=0` is acceptable — note this as a data availability
issue, not a code bug.

### 1.2 Meteo data

- [ ] $S1 temperature (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 precipitation (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 temperature (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 precipitation (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

### 1.3 Snow data

- [ ] $S1 SWE (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 SWE (past 5 days + forecast period):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&start_date=$TODAY_MINUS_5&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

**Note**: Historical SWE norm records use year-2000 dates as a day-of-year
index. Operational SWE records written by `preprocessing_gateway` should have
current-year dates. If **all** snow dates are year-2000, the operational
update window was likely missed.

---

## 2. Short-Term Forecasts — Linear Regression

Pentad issue days: 5, 10, 15, 20, 25, and last day of the month.
Decad issue days: 10, 20, and last day of the month.

```
TODAY day-of-month: ____
Is a pentad issue day? [ ] YES  [ ] NO
Is a decad issue day?  [ ] YES  [ ] NO
```

### 2.1 LR pentad forecasts

- [ ] $S1 — LR pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — LR pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

> **LR-008 check**: On pentad issue days (5,10,15,20,25,EOM), `horizon_in_year`
> must equal the **target** pentad (issue pentad + 1, wrapping to 1 after
> pentad 72). E.g., on day 25 of month 3 (issue pentad 17):
> `horizon_in_year=18`, `horizon_value=6`.

### 2.2 LR decad forecasts

- [ ] $S1 — LR decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — LR decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

**Red flags**:
- No records at all — LR module has not written any forecasts recently.
- Negative forecast values (`-1.0`) — sentinel value leaking through.

---

## 3. Short-Term Forecasts — Machine Learning

### 3.1 ML daily forecasts

- [ ] $S1 TFT — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 TiDE — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 TSMixer — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TFT — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TiDE — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TSMixer — forecasts (TODAY to FORECAST_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$FORECAST_END&limit=200" | table
  ```
  <!-- RESULT: -->

### 3.2 ML issue_date consistency

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

**Red flags**:
- Empty arrays for all three models — ML module crashed before writing.
- `"forecasted_discharge": null` — model ran but produced NaN output.

---

## 4. Short-Term Forecasts — Combined (EM, NE)

### 4.1 Combined pentad forecasts

- [ ] $S1 — EM pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — NE pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — NE pentad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 4.2 Combined decad forecasts

- [ ] $S1 — EM decad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM decad (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 4.3 Quantile ordering spot-check

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

**Red flags**:
- EM/NE arrays empty while individual ML model arrays are populated —
  postprocessing ensemble step failed.
- EM `q05`/`q25`/`q75`/`q95` all null — quantiles not being written.
- `q05 > q25` or `q75 > q95` — quantile ordering violation.

---

## 5. Long-Term Forecasts

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

If the gate is CLOSED, skip this section (long-term forecasts are not expected).

### 5.1 Long-term forecast records

Query each horizon separately. `horizon_value` maps to month_N (0=current
month, 1=next month, etc.).

- [ ] $S1 — month_0 (horizon_value=0):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&horizon_value=0&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — month_1 (horizon_value=1):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&horizon_value=1&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — month_2 (horizon_value=2):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&horizon_value=2&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — month_3 (horizon_value=3):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&horizon_value=3&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — month_1 (horizon_value=1):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&horizon_value=1&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — month_2 (horizon_value=2):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&horizon_value=2&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — month_3 (horizon_value=3):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&horizon_value=3&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — quarterly forecasts:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=QUARTER&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — quarterly forecasts:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=QUARTER&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — seasonal forecasts:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=SEASON&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — seasonal forecasts:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=SEASON&start_date=$MONTH_START&end_date=$MONTH_END&limit=50" | table
  ```
  <!-- RESULT: -->

### 5.2 Long-term skill metrics

- [ ] $S1 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

**Red flags**:
- Empty arrays after the gate was OPEN — `long_term_forecasting` module
  crashed or no models were eligible.
- Records with null `forecast` values — model ran but output NaN.

---

## 6. Maintenance Coverage

### 6.1 Runoff 30-day coverage

- [ ] $S1 — discharge over last 30 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — discharge over last 30 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

**What to look for**: Count should be close to 30 if data is complete.

### 6.2 ERA5 meteo 30-day coverage

- [ ] $S1 T — 30-day meteo:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 T — 30-day meteo:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

### 6.3 ML 30-day forecast coverage

- [ ] $S1 TFT — past 30 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TFT — past 30 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

### 6.4 LR 30-day coverage

- [ ] $S1 — LR pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: (expect ~5-6 pentad issue days in 30 days) -->

- [ ] $S2 — LR pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — LR decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 6.5 EM/NE 30-day coverage

- [ ] $S1 — EM pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — EM decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

**What to look for**: EM and NE pentad counts should roughly match LR pentad
count. Gaps in EM/NE where LR exists indicate maintenance gap-fill did not
run or ran before LR hindcast wrote its data.

---

## 7. Skill Metrics

### 7.1 Pentad skill metrics

- [ ] $S1 — pentad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — pentad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: -->

### 7.2 Decad skill metrics

- [ ] $S1 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — decad skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

**Red flags**:
- `n_pairs` is 0 or 1 — insufficient historical data (acceptable for new
  stations, investigate for established ones).
- Skill metrics completely absent — recalculation step was skipped or crashed.

---

## 8. Server-Side Checks

These checks require shell access on the server and are not available via
the API.

### 8.1 Pipeline log scan

```bash
# Most recent pipeline logs
ls -lt $LOG_DIR/sapphire_*.log | head -10
```

<!-- RESULT: -->

```bash
# Scan for errors in today's logs
grep -E "ERROR|CRITICAL|Traceback" $LOG_DIR/sapphire_*$(date +%Y%m%d).log 2>/dev/null | tail -30
```

<!-- RESULT: -->

```bash
# Scan for warnings
grep -c "WARNING" $LOG_DIR/sapphire_*$(date +%Y%m%d).log 2>/dev/null
```

<!-- RESULT: -->

### 8.2 Docker container health

```bash
# All containers with status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | sort
```

<!-- RESULT: -->

```bash
# Check for OOMKilled or restarting containers
docker ps -a --filter "status=exited" --format "table {{.Names}}\t{{.Status}}" | head -20
```

<!-- RESULT: -->

### 8.3 Disk space

```bash
df -h /data
```

<!-- RESULT: -->

**Red flag**: If disk usage is above 85%, plan cleanup before next pipeline run.

### 8.4 Luigi task history

```bash
# Check recent Luigi task completions/failures via API
curl -s "http://localhost:8082/api/task_list" | python3 -c "
import sys, json
tasks = json.load(sys.stdin)
for name, info in sorted(tasks.items()):
    status = info.get('status', '?')
    if status in ('DONE', 'FAILED', 'RUNNING'):
        print(f'  {status:8s} {name}')
" 2>/dev/null | head -30
```

<!-- RESULT: -->

### 8.5 Model files check (if ML forecasts are missing)

Only run this section if Section 3 showed missing ML forecasts.

```bash
# Check model files exist inside the ML container
docker run --rm \
  -v /data/<data_folder>/models_and_scalers:/app/models_and_scalers \
  mabesa/sapphire-ml:$(grep ieasyhydroforecast_backend_docker_image_tag $ENV_FILE | cut -d= -f2) \
  sh -c "
echo '=== TFT ==='
ls -la /app/models_and_scalers/TFT/*.pt 2>/dev/null || echo 'MISSING .pt files'
echo '=== TiDE ==='
ls -la /app/models_and_scalers/TiDE/*.pt 2>/dev/null || echo 'MISSING .pt files'
echo '=== TSMixer ==='
ls -la /app/models_and_scalers/TSMixer/*.pt 2>/dev/null || echo 'MISSING .pt files'
"
```

<!-- RESULT: -->

---

## 9. Manual Pipeline Trigger (if needed)

Use these commands to manually trigger pipeline steps on the server. Each
command runs the same Docker workflow that cron would trigger.

### 9.1 Gateway preprocessing

```bash
cd $REPO_DIR && bash bin/run_preprocessing_gateway.sh $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_gateway_manual_$(date +%Y%m%d).log
```

### 9.2 Pentadal forecasts

```bash
cd $REPO_DIR && bash bin/run_pentadal_forecasts.sh $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_pentadal_manual_$(date +%Y%m%d).log
```

### 9.3 Decadal forecasts

```bash
cd $REPO_DIR && bash bin/run_decadal_forecasts.sh $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_decadal_manual_$(date +%Y%m%d).log
```

### 9.4 Long-term forecasts

```bash
cd $REPO_DIR && bash bin/run_long_term_forecasts.sh $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_longterm_manual_$(date +%Y%m%d).log
```

### 9.5 Long-term forecasts (dry run)

```bash
cd $REPO_DIR && bash bin/run_long_term_forecasts.sh --dry-run $ENV_FILE
```

### 9.6 Daily maintenance

```bash
cd $REPO_DIR && bash bin/run_daily_maintenance.sh $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_maintenance_manual_$(date +%Y%m%d).log
```

### 9.7 Periodic maintenance

```bash
# Long-term postprocessing gap-fill
cd $REPO_DIR && bash bin/run_periodic_maintenance.sh long_term $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_periodic_longterm_manual_$(date +%Y%m%d).log

# Skill recalculation
cd $REPO_DIR && bash bin/run_periodic_maintenance.sh skill_recalc $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_periodic_skillrecalc_manual_$(date +%Y%m%d).log

# Snow norm recalculation
cd $REPO_DIR && bash bin/run_periodic_maintenance.sh snow_norms $ENV_FILE 2>&1 | tee $LOG_DIR/sapphire_periodic_snownorms_manual_$(date +%Y%m%d).log
```

---

## 10. Summary Table

Fill in after completing all verification steps above. Record actual values,
not just PASS/FAIL where a threshold applies.

| Check | $S1 result | $S2 result | Threshold | Status |
|-------|-----------|-----------|-----------|--------|
| API health | | | all healthy | |
| Docker containers | | | all Up | |
| Crontab installed | n/a | n/a | matches doc/deployment.md | |
| Runoff today (discharge) | | | non-null | |
| Meteo T today | | | -30 to +40 °C | |
| Meteo P today | | | ≥ 0 mm | |
| Snow SWE (recent) | | | non-null | |
| ML TFT count today | | | ≥ 1 | |
| ML TiDE count today | | | ≥ 1 | |
| ML TSMixer count today | | | ≥ 1 | |
| ML forecast issue_date = TODAY | | | TRUE | |
| LR pentad (recent issue day) | | | non-null fc | |
| LR decad (recent issue day) | | | non-null fc | |
| EM pentad (recent issue day) | | | non-null q | |
| EM quantile ordering | | | all OK | |
| NE pentad (recent issue day) | | | ≥ 1 record | |
| Long-term forecast count (Section 5) | | | ≥ 1 if gate open | |
| Monthly skill n_pairs (Section 5/7) | | | > 1 | |
| Pentad skill n_pairs | | | > 1 | |
| Decad skill n_pairs | | | > 1 | |
| 30-day runoff coverage | | | ~30 records | |
| 30-day EM pentad coverage | | | ~5-6 records | |
| Log scan errors | n/a | n/a | 0 ERROR/CRITICAL | |
| Disk space | n/a | n/a | < 85% | |
| Luigi tasks | n/a | n/a | no FAILED tasks | |

---

## 11. Common Failure Patterns and Remediation

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| All preprocessing empty | Service not running or iEasyHydro API unreachable | Check `docker compose ps`; check iEasyHydro API connectivity |
| Runoff null but meteo present | iEasyHydro source returned no obs for today | Check source API; not a code bug if data availability is the constraint |
| ML forecasts missing, LR present | ML module crashed (date format bug, shape mismatch) | Check pipeline logs; check model `.pt` files (Section 8.5) |
| EM missing, individual ML present | Postprocessing ensemble step crashed | Check logs for `postprocessing_forecasts` phase errors |
| EM `q05` all null | Quantile fields not being written | Check postprocessing_forecasts version |
| `q05 > q25` quantile inversion | Quantile regression ordering not enforced | Check postprocessing quantile sort step |
| LR returns `-1.0` values | Sentinel value not converted to NaN | Check `linear_regression` module for sentinel guard |
| Long-term forecasts absent (gate OPEN) | Module crash or schedule query failure | Check `sapphire_longterm_*.log`; run dry-run (Section 9.5) |
| Luigi daemon not reachable | Container stopped or port conflict | `docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon` |
| OOMKilled containers | Insufficient memory for ML models | Check `docker ps -a`; consider `t3.xlarge` instance |
| Disk full | Log accumulation or Docker images | Clean logs older than 7 days; `docker system prune` |
| Skill `n_pairs` = 1 | Recalculation ran with insufficient history | Acceptable for new stations; investigate for established ones |
| Log scan shows Traceback | Unhandled exception in a module | Read full traceback; identify module and fix before next run |
