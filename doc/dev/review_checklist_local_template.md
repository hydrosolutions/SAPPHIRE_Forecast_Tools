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
5. **Result format**: Paste the **complete, unabridged** output of each curl
   command (piped through `table`) into the RESULT comment. **NEVER** summarize,
   shorten, or use `...` to elide rows. The full raw table with every row is the
   record — it must be reviewable without re-running the query. Even for large
   result sets (100+ rows), paste the entire output. Use multi-line comments:
   ```
   <!-- RESULT:
   horizon_type  code   date        discharge  predictor  horizon_value  ...
   ------------  -----  ----------  ---------  ---------  -------------  ...
   day           12345  2026-01-01  5.2                   1              ...
   day           12345  2026-01-02  5.3                   2              ...

   (N records)
   -->
   ```
7. Keep the filled checklist **local only** — it contains operational data.
   The `.gitignore` pattern `doc/dev/review_checklist_local_20*.md` must exclude
   it from commits. This template (no date suffix) is safe to commit.

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
rows = [['' if r.get(k) is None else str(r[k]) for k in keys] for r in d]
widths = [max(len(k), max((len(row[i]) for row in rows), default=0)) for i, k in enumerate(keys)]
fmt = '  '.join(f'{:<{w}}' for w in widths)
print(fmt.format(*keys))
print(fmt.format(*['-'*w for w in widths]))
for row in rows: print(fmt.format(*row))
print(f'\n({len(d)} records)')
"
}

# Skill-metric summariser — groups rows by (model, horizon_value) and shows n_pairs.
# Use for Section 9.6: /skill-metric/ has NO horizon_value query param, so per-lead
# stratification must be inspected client-side.
skillsum() {
  python3 -c "
import sys, json
from collections import defaultdict
rows = json.load(sys.stdin)
if not rows: print('(no records)'); sys.exit()
g = defaultdict(list)
for r in rows:
    g[(r.get('model_type'), r.get('horizon_value'))].append(r)
print(f'{\"model\":<16}{\"hv\":>4}{\"rows\":>6}{\"n_pairs(min/max)\":>20}{\"nse(min/max)\":>24}')
print('-' * 70)
for (m, hv), rs in sorted(g.items(), key=lambda kv: (str(kv[0][0]), -1 if kv[0][1] is None else kv[0][1])):
    npv = [r['n_pairs'] for r in rs if r.get('n_pairs') is not None]
    nse = [r['nse'] for r in rs if r.get('nse') is not None]
    np_s = f'{min(npv):g}/{max(npv):g}' if npv else '-'
    ns_s = f'{min(nse):.3g}/{max(nse):.3g}' if nse else 'ALL NULL'
    print(f'{str(m):<16}{str(hv):>4}{len(rs):>6}{np_s:>20}{ns_s:>24}')
print(f'\n({len(rows)} records; {len(g)} (model,hv) groups)')
"
}
```

> **Pagination trap (PREPQ-011 class).** Every endpoint defaults to `limit=100`.
> Once skill rows are stratified per lead, a single station × 5 horizons × ~10
> models × 4 leads easily exceeds 100 — and the API **silently truncates**, which
> reads as "these leads were never written". All Section 9.6 queries therefore
> pass an explicit large `limit`. If a result is exactly the limit you passed,
> **raise the limit and re-run** before drawing any conclusion.

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

<!-- RESULT: -->

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

<!-- RESULT: -->

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

### 0.5 Feature-flag preflight

Recent work landed **flag-gated, default OFF**. Merging changes nothing until a
deployment opts in — so a review run that forgets the flag silently tests the
*old* behaviour and reports it as current. Establish flag state **before** any
run, and record it.

#### 0.5a Flags that must be decided per deployment

| Flag | Default | Effect when ON | Set for this run? |
|------|---------|----------------|-------------------|
| `SAPPHIRE_SKILL_LEAD_AWARE` | **OFF** | Long-term skill metrics and EM / Naive Mean / Skilled Mean ensembles are computed and stored **per operational lead** (`horizon_value`) instead of collapsed; operational "latest" readers and the monthly maintenance gap detector select only the configured operational issuance; the dashboard monthly panel resolves its lead from config instead of a hard-coded `horizon_value=1`. | <!-- ON / OFF --> |

<!-- RESULT: flag state chosen for this run = -->

The following flags gate `forecast_skill_eval` only — an **offline analysis
tool**, not the operational pipeline. Leave them unset unless this review also
runs the evaluator: `SAPPHIRE_SKILL_PROB`, `SAPPHIRE_SKILL_VALUE`,
`SAPPHIRE_SKILL_LT_LEAD`, `SAPPHIRE_SKILL_LR_REPAIR`,
`SAPPHIRE_SKILL_FORECAST_ONLY`.

These carry working defaults and need **no** `.env` entry — record them only if
this deployment deliberately overrides one:
`ieasyhydroforecast_nse_threshold_long_term` (LT Skilled-Mean pool, NSE>0),
`ieasyhydroforecast_efficiency_threshold_long_term`,
`ieasyhydroforecast_accuracy_threshold_long_term`,
`ieasyhydroforecast_min_pairs_long_term{,_quarter,_season}` (min-n floor,
defaults 4/5/5), `SAPPHIRE_MONTHLY_FROM_DECADAL` (defaults **True**).

#### 0.5b Hard prerequisite — do not enable the flag until this passes

Under `SAPPHIRE_SKILL_LEAD_AWARE=true` the write path **raises and aborts** if a
long-term config lacks `operational_issue_day` (by design — it must never
silently score the wrong rows).

```bash
for f in "$ieasyhydroforecast_configuration_path/$ieasyhydroforecast_ml_long_term_configuration"/*.json; do
  [ -f "$f" ] || continue
  python3 -c "
import json,sys
d=json.load(open('$f'))
print('%-22s lead=%s issue_day=%s' % ('$(basename "$f" .json)',
      d.get('operational_month_lead_time'), d.get('operational_issue_day')))
"
done
```

<!-- RESULT: -->

**Every row must show a non-`None` `lead` AND a non-`None` `issue_day`.** Any
`issue_day=None` ⇒ **NOT READY**; do not enable the flag for this deployment.

Record the resolved lead map — Section 9.6 checks the stored rows against it:

| Mode | Configured lead (`horizon_value`) | Issue day |
|------|-----------------------------------|-----------|
| month_0 | | |
| month_1 | | |
| month_2 | | |
| month_3 | | |
| quarter | | |
| seasonal_january | | |
| seasonal_february | | |
| seasonal_march | | |
| seasonal_april | | |

> `month_N` does **not** universally mean lead N — it is per-deployment. Read the
> lead from the config, never from the mode name.

#### 0.5c Confirm the flag actually resolves ON in the run environment

The helper fails loudly on a typo'd value rather than silently resolving to OFF,
so this also validates the token you set:

```bash
SAPPHIRE_SKILL_LEAD_AWARE=true python3 -c "
import sys; sys.path.insert(0, 'apps')
from iEasyHydroForecast.skill_lead_aware_flag import skill_lead_aware_enabled
print('SAPPHIRE_SKILL_LEAD_AWARE resolves to:', skill_lead_aware_enabled())
"
```

<!-- RESULT: (expect True) -->

#### 0.5d Enabling requires a full-history recalc

Existing stored rows were written **single-lead**. Turning the flag on without
recalculating leaves the DB a mix of single-lead and per-lead rows until the
next natural recalc — Section 9.6 will then show a confusing half-migrated
state that is neither a bug nor a pass. **Section 9.2 (recalc) is mandatory, not
optional, on the run that first enables this flag**, and must be run with the
flag set.

> If this local DB holds **more than one organisation** (the dev
> `postprocessing_db` commonly holds both kyg and taj codes), the recalc is
> **per-org**: run it once per deployment `.env`. Recalculating one org does not
> migrate the other org's rows.

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
  <!-- RESULT: -->

- [ ] $S2 — recent daily runoff (today + past 5 days):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 1.2 Preprocessing — Meteo

Each query covers recent history through the forecast period in a single call.
If forecast-period rows (dates > TODAY) are present, ERA5 forecast extension
is working. If only rows up to TODAY appear, only reanalysis data is available.

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

### 1.3 Preprocessing — Snow

Each query covers recent history through the forecast period in a single call.

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
  <!-- RESULT: -->

- [ ] $S2 ML TFT forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 EM pentad forecasts (RECENT_START to RECENT_END):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 LR pentad forecasts (PREV_PENTAD to TODAY):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$PREV_PENTAD&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 LR pentad forecasts (PREV_PENTAD to TODAY):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$PREV_PENTAD&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 LR decad forecasts (PREV_DECAD to TODAY):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$PREV_DECAD&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 LR decad forecasts (PREV_DECAD to TODAY):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=decade&start_date=$PREV_DECAD&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — today + past 5 days discharge:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

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

Each query covers recent history through the forecast period in a single call.
If forecast-period rows (dates > TODAY) are present, ERA5 forecast extension
is working.

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

### 3.2 Verify: Snow data

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
  <!-- RESULT: -->

- [ ] $S2 — LR pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

> **LR-008 check**: On pentad issue days (5,10,15,20,25,EOM), `horizon_in_year` must equal the **target** pentad (issue pentad + 1, wrapping to 1 after pentad 72). E.g., on day 25 of month 3 (issue pentad 17): `horizon_in_year=18`, `horizon_value=6`. If you see the issue pentad (e.g., `horizon_in_year=17`, `horizon_value=5`), the LR-008 metadata override is not active.

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

> **⚠️ ML rejects `SAPPHIRE_PREDICTION_MODE=BOTH`.** Unlike `linear_regression`
> and `postprocessing_forecasts` (which accept `BOTH`, and `ALL`, natively),
> `machine_learning/make_forecast.py` raises `ValueError` on anything other than
> `PENTAD` or `DECAD`. Run this module **once per mode**:
> ```bash
> for M in PENTAD DECAD; do
>   ieasyhydroforecast_env_file_path=<path-to-your-.env> \
>     SAPPHIRE_PREDICTION_MODE=$M ML_MODE=BOTH \
>     bash apps/run_locally.sh machine_learning
> done
> ```

> **Ordering note.** In the real pipeline (`run_short_term_pipeline`)
> preprocessing runs **once**, then for each mode (PENTAD, then DECAD) the order
> is **machine_learning → linear_regression → postprocessing_forecasts**. This
> checklist presents LR (§4) before ML (§5) for readability. If you are
> reproducing production behaviour rather than spot-checking one module, follow
> the runner's order, and note that with the default `ML_MODE=DECAD` the ML step
> is **intentionally skipped for PENTAD** — record that as PASS (no-op), not as
> a missing write.

### What this module writes

- `machine_learning` writes TFT, TiDE, TSMixer forecasts stored with
  `horizon=day` at the postprocessing API.

### 5.1 Verify: ML daily forecasts written

**Automated alternative available**: `check_ml_flag_distribution` (Section
0.4) detects stuck-flag conditions (all records with the same flag value) and
populates `counts` in the JSON output. The manual queries below show per-model,
per-station breakdowns and exact forecast values.

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

> **⚠️ Use `long-term-operational` — NOT `long-term` or `long_term_forecasting`.**
> Both of those run the **simulate** path: `run_long_term_forecasting()` is
> labelled `(simulate)` in `apps/run_locally.sh` and invokes
> `dev_code/simulate_forecasts.py`, which defaults to **historical year 2024**
> and `month_0` (`LT_SIMULATE_YEARS`, `LT_SIMULATE_MODES`). It does **not**
> consult the operational day-of-month gate. Verifying today's operational
> long-term forecast with either target produces a **false PASS** — you will be
> looking at a 2024 simulation. Only `long-term-operational` runs
> `run_forecast.py` for the config-resolved active modes and then the real
> long-term postprocessor (`postprocessing_operational_long_term.py`).

Operational long-term run (this is the one to use):

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_SKILL_LEAD_AWARE=<flag-from-0.5> \
  bash apps/run_locally.sh long-term-operational
```

If the gate is CLOSED and you still want to exercise the operational path,
override the date (do **not** substitute a simulate target):

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_SKILL_LEAD_AWARE=<flag-from-0.5> \
  LT_FORECAST_TODAY=<YYYY-MM-10 or YYYY-MM-25> \
  bash apps/run_locally.sh long-term-operational
```

Record which modes the schedule query actually selected — coverage depends on
it, and a fallback selects a different (wider) mode set than the config-aware
path:

```bash
grep -iE "active mode|schedule|fallback|skill type" apps/logs/run_locally_*.log | tail -20
```

<!-- RESULT: active modes = ; fallback? = -->

> **`postprocessing_forecasts` is the SHORT-TERM postprocessor.** The single
> module target runs `postprocessing_operational.py` only. Long-term ensembles
> come from `postprocessing_operational_long_term.py`, which runs *inside*
> `long-term-operational`. Running §7 alone never produces long-term EM /
> Naive Mean / Skilled Mean rows.

### What this module writes

- `long_term_forecasting` writes monthly forecasts and updates monthly skill
  metrics when the gate is open.

### 6.1 Verify: Long-term forecasts written (only if gate OPEN or forced)

Query each horizon separately. `horizon_value` maps to month_N (0=current month,
1=next month, etc.). Only query horizons that the active modes should have written.

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

**Red flags**:
- Empty arrays after a forced run — `long_term_forecasting` module crashed or
  no models were eligible.
- Records with null `forecast` values — model ran but output NaN.

### 6.2 Verify: Long-term skill metrics updated (only if gate OPEN or forced)

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
  <!-- RESULT: -->

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
  <!-- RESULT: -->

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

<!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — discharge over last 30 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 T — 30-day meteo:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=60" | table
  ```
  <!-- RESULT: -->

- [ ] Review window ($S1 T and P, RECENT_START to TODAY):
  ```bash
  echo "=== S1 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  echo "=== S1 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 8.2 Verify: ML gap-fill

- [ ] $S1 TFT — past 14 days (compare row count vs Section 1.4 baseline):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TFT — past 14 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$TODAY&limit=200" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] ERA5 meteo data depth — does T and P data exist for the hindcast
  training window? The script crashes at line 267 if `era5_data_transformed`
  is empty (`.min()` on empty series raises TypeError):
  ```bash
  echo "=== S1 T 2023 ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  echo "=== S1 P 2023 ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=2023-01-01&end_date=2023-12-31&limit=5" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S1 TiDE — 30-day forecast coverage:
  ```bash
  echo "=== S1 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 TSMixer — 30-day forecast coverage:
  ```bash
  echo "=== S1 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TFT — 30-day forecast coverage:
  ```bash
  echo "=== S2 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TiDE — 30-day forecast coverage:
  ```bash
  echo "=== S2 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TSMixer — 30-day forecast coverage:
  ```bash
  echo "=== S2 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

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

<!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S1 — LR decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

**What to look for**: 5 or 6 pentad issue days within 30 days. If fewer
records appear, LR hindcast may not have written for these stations.

#### 8.3a Verify: LR hindcast — previous pentad/decad spot-check

Targeted check for the most recent pentad issue day. Uses `$PREV_PENTAD` and
`$PREV_DECAD` set in Section 0.1.

- [ ] $S1 — LR pentad at PREV_PENTAD (single-date check):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — LR pentad at PREV_PENTAD (regression check for LR fix):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — LR decad at PREV_DECAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$PREV_DECAD&end_date=$PREV_DECAD&limit=5" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — EM pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — NE pentad 30-day records (row count should ≈ EM pentad count):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — NE pentad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — EM decad 30-day records (row count should ≈ LR decad count from 8.3):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — NE decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — NE decad 30-day records:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=NE&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — EM pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — NE pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — NE pentad at PREV_PENTAD:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$PREV_PENTAD&end_date=$PREV_PENTAD&limit=5" | table
  ```
  <!-- RESULT: -->

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

<!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — pentad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — decad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — decad skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — monthly skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — monthly skill metrics (BEFORE):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

### 9.2 Run: recalculate_skill_metrics

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  SAPPHIRE_SKILL_LEAD_AWARE=<ON-value-from-0.5a> \
  SAPPHIRE_PREDICTION_MODE=ALL \
  bash apps/run_locally.sh recalculate_skill_metrics
```

> **Note**: This is a slow operation (can take hours for large datasets).
> `SAPPHIRE_PREDICTION_MODE=ALL` recalculates pentad + decad + monthly +
> quarterly + seasonal + daily skill metrics. Use
> `SAPPHIRE_PREDICTION_MODE=BOTH` for pentad + decad only.

> **Carry the flag from Section 0.5.** This recalc is what writes the per-lead
> rows Section 9.6 verifies. Omitting `SAPPHIRE_SKILL_LEAD_AWARE` here — even
> when the rest of the run had it set — rewrites the rows single-lead and
> silently undoes the migration.

<!-- RESULT: -->

### 9.3 Verify: Skill Metrics Updated

Same queries as 9.1 but labeled AFTER. Compare `n_pairs` (should be >=
BEFORE value) and `nse` (may change as new forecast-observation pairs are
included). If `n_pairs` decreased, data may have been lost during
recalculation — investigate.

- [ ] $S1 — pentad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — pentad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — decad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — decad skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — monthly skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — monthly skill metrics (AFTER):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=50" | table
  ```
  <!-- RESULT: -->

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

<!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — SWE norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — SWE forecast-period values:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&start_date=$TODAY&end_date=$FORECAST_END&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — HS norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=HS&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — HS norms (all rows):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=HS&limit=50" | table
  ```
  <!-- RESULT: -->

---

## 9.6 Skill Metrics and Ensembles — All Horizons

Sections 9.1/9.3 cover pentad, decad and month only, and none of the earlier
sections check long-term **ensembles** at all. This section closes both gaps
across all five horizons and verifies the per-lead stratification.

Run **after** the Section 9.2 recalc. Uses `skillsum` and `table` from 0.1a and
the lead map recorded in 0.5b.

### 9.6.1 Skill metrics — per horizon, per model, per lead

`/skill-metric/` exposes `horizon`, `code`, `model`, `start_date`, `end_date`,
`skip`, `limit` — **there is no `horizon_value` filter**, so lead stratification
is inspected client-side via `skillsum`. Note `limit=2000` throughout (see the
pagination trap in 0.1a).

- [ ] $S1 — skill by (model, lead), all five horizons:
  ```bash
  for H in pentad decade month quarter season; do
    echo "=== S1 $H ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=$H&limit=2000" | skillsum
  done
  ```
  <!-- RESULT: -->

- [ ] $S2 — skill by (model, lead), all five horizons:
  ```bash
  for H in pentad decade month quarter season; do
    echo "=== S2 $H ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=$H&limit=2000" | skillsum
  done
  ```
  <!-- RESULT: -->

- [ ] Full unabridged rows for the long-term horizons (values, not just the summary):
  ```bash
  for H in month quarter season; do
    echo "=== S1 $H (full) ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=$H&limit=2000" | table
  done
  ```
  <!-- RESULT: -->

**Expected — flag ON:** every long-term horizon shows rows at **each lead in the
0.5b map** for that mode family (month: one `hv` per configured `month_*`
config; quarter: the single configured quarter lead; season: one `hv` per
configured `seasonal_*` config). Short-term (pentad/decad) is **not** lead-aware
— `horizon_value` is the sentinel `0` there, and that is correct.

**Expected — flag OFF:** long-term skill is collapsed; expect a single lead
group (or the `0` sentinel) per model. This is the pre-feature behaviour, not a
defect.

**Red flags**:
- A configured lead from 0.5b has **no** skill rows at all — the recalc did not
  emit that lead, or the config's `operational_issue_day` does not match any
  stored issuance.
- Rows appear at a `horizon_value` that is **not** in the 0.5b map, and is not
  `0` — stray/stale aggregate rows (the PP-043 class: aggregates scored as raw
  model rows at `hv = horizon_in_year`). These are what blank out dashboard
  skill tiles. **Check `horizon_in_year` before filing**: if the stray `hv`
  equals it, it is the PP-043 aggregate class; if not, see the quarter caveat
  below.

> **⚠️ Known exception — do NOT file quarter stray leads as a new defect.**
> `QUARTER.horizon_value` is **overloaded**: the service stores it with no
> defined meaning, and the pre-existing operational quarter rows were migrated
> from a config set that has since changed — so quarter legitimately shows leads
> beyond the single one today's `quarter.json` configures. This is the **open
> convention question tracked as MIG-008**
> (`doc/prod/longforecast_quarter_season_hv_convention.md`), awaiting a decision
> from the postprocessing-service owner + long-term modeller. Record what you
> observe as evidence for that decision; do not "fix" it and do not re-file it.
> The same caveat applies to `SEASON` on deployments whose seasonal configs
> post-date the stored rows. Month is **not** subject to this — month is
> config-per-lead and a stray month lead is a genuine finding.
- Result count equals the `limit` you passed — truncated, re-run with a higher
  limit before concluding anything.
- Short-term `horizon_value` is non-zero — short-term must stay sentinel `0`.

### 9.6.2 Min-n floor (no small-sample noise)

Long-term skill has a configurable minimum-pair floor (defaults MONTH=4,
QUARTER=5, SEASON=5). Rows below the floor must be **absent or tombstoned**,
never published with a real NSE — an unfloored 2-pair NSE has been observed as
low as −13.6 million.

- [ ] $S1 — long-term rows violating the floor:
  ```bash
  for H in month quarter season; do
    K=4; [ "$H" != "month" ] && K=5
    echo "=== S1 $H (floor K=$K) ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=$H&limit=2000" \
      | K=$K python3 -c "
import sys, json, os
K = int(os.environ['K']); rows = json.load(sys.stdin)
bad = [r for r in rows
       if r.get('n_pairs') is not None and 0 < r['n_pairs'] < K and r.get('nse') is not None]
print(f'{len(bad)} VIOLATION(S)' if bad else 'OK - no row with 0 < n_pairs < K carries metrics')
for r in bad[:20]:
    print('  ', r.get('model_type'), 'hv=', r.get('horizon_value'),
          'n_pairs=', r.get('n_pairs'), 'nse=', r.get('nse'))
"
  done
  ```
  <!-- RESULT: -->

- [ ] $S2 — same check:
  ```bash
  for H in month quarter season; do
    K=4; [ "$H" != "month" ] && K=5
    echo "=== S2 $H (floor K=$K) ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=$H&limit=2000" \
      | K=$K python3 -c "
import sys, json, os
K = int(os.environ['K']); rows = json.load(sys.stdin)
bad = [r for r in rows
       if r.get('n_pairs') is not None and 0 < r['n_pairs'] < K and r.get('nse') is not None]
print(f'{len(bad)} VIOLATION(S)' if bad else 'OK')
for r in bad[:20]:
    print('  ', r.get('model_type'), 'hv=', r.get('horizon_value'),
          'n_pairs=', r.get('n_pairs'), 'nse=', r.get('nse'))
"
  done
  ```
  <!-- RESULT: -->

**Expected**: `OK` for every horizon. Any violation means the min-n gate is not
being applied on this path.

### 9.6.3 Tombstones are expected, not failures

Skill writes are upsert-only. When a recalc stops emitting a key (floored out,
or an aggregate discarded), a **tombstone** is written — `n_pairs = 0` with NULL
metrics — so the stale row cannot keep showing on the dashboard.

- [ ] $S1 — tombstone census across long-term horizons:
  ```bash
  for H in month quarter season; do
    echo "=== S1 $H ==="
    curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=$H&limit=2000" \
      | python3 -c "
import sys, json
rows = json.load(sys.stdin)
tomb = [r for r in rows if r.get('n_pairs') == 0]
bad  = [r for r in tomb if r.get('nse') is not None or r.get('mae') is not None]
print(f'{len(rows)} rows, {len(tomb)} tombstones (n_pairs=0)')
print(f'  MALFORMED: {len(bad)} tombstone(s) still carry metrics' if bad
      else '  OK - all tombstones have NULL metrics')
for r in tomb[:10]:
    print('   tomb:', r.get('model_type'), 'hv=', r.get('horizon_value'))
"
  done
  ```
  <!-- RESULT: -->

**Expected**: tombstones may be present (normal); every tombstone must have NULL
`nse`/`mae`. A tombstone carrying live metrics is a real defect. A *rising*
tombstone count across successive recalcs of unchanged data is also suspicious —
it means keys are being emitted inconsistently between runs.

### 9.6.4 Long-term ensembles — EM / Naive Mean / Skilled Mean

The long-term ensembles live on `/long-forecast/` (not `/forecast/`), which
**does** support a `horizon_value` filter. DB model names: `EM`,
`Naive Mean`, `Skilled Mean`.

- [ ] $S1 — ensemble presence per horizon and model:
  ```bash
  for H in month quarter season; do
    for M in "EM" "Naive%20Mean" "Skilled%20Mean"; do
      echo "=== S1 $H $M ==="
      curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=$H&model=$M&start_date=$MONTH_START&end_date=$MONTH_END&limit=500" | table
    done
  done
  ```
  <!-- RESULT: -->

- [ ] $S2 — ensemble presence per horizon and model:
  ```bash
  for H in month quarter season; do
    for M in "EM" "Naive%20Mean" "Skilled%20Mean"; do
      echo "=== S2 $H $M ==="
      curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=$H&model=$M&start_date=$MONTH_START&end_date=$MONTH_END&limit=500" | table
    done
  done
  ```
  <!-- RESULT: -->

- [ ] $S1 — monthly ensembles stratified by lead (flag ON only):
  ```bash
  for HV in 0 1 2 3; do
    echo "=== S1 month hv=$HV ==="
    curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&horizon_value=$HV&start_date=$MONTH_START&end_date=$MONTH_END&limit=500" | table
  done
  ```
  <!-- RESULT: -->

**Expected**: ensemble rows exist at each lead the 0.5b map configures, and
**not** at leads it does not. A lead with per-model forecasts (Section 6) but no
ensemble row means the ensemble step skipped that lead.

**Red flags**:
- `Skilled Mean` absent everywhere while `EM` is present — the skilled pool was
  starved. Cross-check the LT NSE>0 pool threshold; verify at least one model
  has a long-term skill row with `nse > 0` in 9.6.1.
- Ensemble rows at `horizon_value` = calendar month (e.g. 7 in July) — the
  stale-aggregate sentinel bug (`hv` should be a **lead**, never a month).

### 9.6.5 Quarter/season EM parity — EM = mean(LR_Base, LR_SM)

For quarter and season, `EM` is defined as the plain mean of `LR_Base` and
`LR_SM` (de-skill-gated, two-model). This is a closed-form check.

- [ ] $S1 — quarter and season EM parity:
  ```bash
  for H in quarter season; do
    echo "=== S1 $H EM parity ==="
    python3 -c "
import json, subprocess, os
base = os.environ['BASE_URL']; code = os.environ['S1']
def get(model):
    url = (f\"{base}/api/postprocessing/long-forecast/?code={code}\"
           f\"&horizon_type=$H&model={model}&limit=2000\")
    return json.loads(subprocess.run(['curl','-s',url],capture_output=True,text=True).stdout)
key = lambda r: (r.get('date'), r.get('horizon_value'), r.get('valid_from'))
em   = {key(r): r for r in get('EM')}
base_= {key(r): r for r in get('LR_Base')}
sm   = {key(r): r for r in get('LR_SM')}
common = set(em) & set(base_) & set(sm)
print(f'EM={len(em)} LR_Base={len(base_)} LR_SM={len(sm)} comparable={len(common)}')
bad = 0
for k in sorted(common):
    a, b = base_[k].get('forecasted_discharge'), sm[k].get('forecasted_discharge')
    e = em[k].get('forecasted_discharge')
    if None in (a, b, e): continue
    exp = (a + b) / 2
    if abs(exp - e) > max(1e-6, 5e-4 * max(abs(exp), 1)):
        bad += 1
        if bad <= 10: print('  MISMATCH', k, f'EM={e:g} expected={exp:g}')
print('  OK - all comparable rows match' if bad == 0 else f'  {bad} MISMATCH(ES)')
"
  done
  ```
  <!-- RESULT: -->

**Expected**: `comparable` > 0 and zero mismatches. `comparable=0` is **not** a
pass — it means EM and its two inputs share no key, which is itself a finding
(check whether `horizon_value` differs between the EM row and its inputs).

### 9.6.6 Short-term ensembles — EM / NE at pentad and decad

Section 7.1 checks the operational window; this widens to the full recalc window
and adds the per-model comparison that reveals stranded period rows.

- [ ] $S1/$S2 — EM and NE counts vs contributing models (pentad + decad):
  ```bash
  for H in pentad decade; do
    for C in $S1 $S2; do
      echo "=== $C $H ==="
      for M in EM NE LR TFT TiDE TSMixer; do
        n=$(curl -s "$BASE_URL/api/postprocessing/forecast/?code=$C&horizon=$H&model=$M&start_date=$TODAY_MINUS_30&end_date=$TODAY&limit=500" \
            | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
        printf '  %-10s %s\n' "$M" "$n"
      done
    done
  done
  ```
  <!-- RESULT: -->

**Expected**: `EM` and `NE` counts roughly track the per-model counts over the
same window.

**Red flags**:
- Per-model period rows present but `EM`/`NE` at 0 for a boundary date — the
  PP-045 class: short-term per-model PENTAD/DECADE rows are written **only** by
  the operational path on boundary days, and maintenance cannot heal them. Use
  `apps/postprocessing_forecasts/backfill_period_forecasts.py` (one calendar year
  per pass) rather than expecting maintenance to fill the gap.

---

## 9a. Automated Post-Run Validation (delta report)

Run `validate_pipeline.py` in post-run mode to compare current record counts
against the pre-run baseline and report any decreases (WARN) or increases
(INFO). This catches silent regressions that manual spot-checks might miss.

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> \
  bash apps/run_locally.sh validate --phase post --baseline /tmp/vp_baseline.json
```

<!-- RESULT: -->

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
  <!-- RESULT: -->

- [ ] $S2 — runoff today + past 5 days:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — meteo T (today + past 5 days):
  ```bash
  echo "=== S1 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — meteo P (today + past 5 days):
  ```bash
  echo "=== S1 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — meteo T (today + past 5 days):
  ```bash
  echo "=== S2 T ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — meteo P (today + past 5 days):
  ```bash
  echo "=== S2 P ==="
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY_MINUS_5&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 10.2 Short-term forecast completeness

- [ ] $S1 TFT — today's forecasts:
  ```bash
  echo "=== S1 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 TiDE — today's forecasts:
  ```bash
  echo "=== S1 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 TSMixer — today's forecasts:
  ```bash
  echo "=== S1 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TFT — today's forecasts:
  ```bash
  echo "=== S2 TFT ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TiDE — today's forecasts:
  ```bash
  echo "=== S2 TiDE ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TiDE&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 TSMixer — today's forecasts:
  ```bash
  echo "=== S2 TSMixer ==="
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TSMixer&start_date=$TODAY&end_date=$TODAY&limit=100" | table
  ```
  <!-- RESULT: -->

- [ ] $S1 — EM pentad records (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

- [ ] $S2 — EM pentad records (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=50" | table
  ```
  <!-- RESULT: -->

### 10.3 Skill metrics check

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
| **Flag state (0.5a)** | n/a | n/a | `SAPPHIRE_SKILL_LEAD_AWARE` recorded, resolves as intended | |
| **LT config prerequisite (0.5b)** | n/a | n/a | every config has lead AND issue_day | |
| **Recalc carried the flag (9.2)** | n/a | n/a | TRUE | |
| Month skill leads present (9.6.1) | | | matches 0.5b lead map | |
| Quarter skill leads present (9.6.1) | | | matches 0.5b lead map | |
| Season skill leads present (9.6.1) | | | matches 0.5b lead map | |
| Stray leads (not in map, not 0) (9.6.1) | | | none | |
| Short-term `horizon_value` sentinel (9.6.1) | | | all 0 | |
| Truncation check (result count ≠ limit) | | | TRUE for every query | |
| Min-n floor violations (9.6.2) | | | 0 | |
| Malformed tombstones (9.6.3) | | | 0 | |
| LT EM present, all 3 horizons (9.6.4) | | | ≥ 1 row each | |
| LT Naive Mean present (9.6.4) | | | ≥ 1 row each | |
| LT Skilled Mean present (9.6.4) | | | ≥ 1 row each | |
| Quarter EM = mean(LR_Base,LR_SM) (9.6.5) | | | comparable > 0, 0 mismatch | |
| Season EM = mean(LR_Base,LR_SM) (9.6.5) | | | comparable > 0, 0 mismatch | |
| Short-term EM/NE vs per-model (9.6.6) | | | counts track | |

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
| Long-term skill collapsed to one lead | `SAPPHIRE_SKILL_LEAD_AWARE` OFF, or the recalc ran without it | Re-check 0.5c; re-run Section 9.2 **with the flag set** |
| Only *some* leads per-lead, others collapsed | Flag enabled but no full-history recalc — DB is half-migrated | Run the Section 9.2 full recalc; if the DB holds several orgs, once per org `.env` |
| Recalc aborts with a config error | A long-term config lacks `operational_issue_day` — fail-loud by design | Fix the config (0.5b), do not disable the flag to work around it |
| Skill rows at `horizon_value` = calendar month | Stale aggregate rows scored as raw model rows (PP-043 class) | Re-run the recalc so tombstones invalidate them; confirm none remain in 9.6.1 |
| Skill row count == the `limit` passed | Silent server-side truncation at the default `limit=100` | Re-run with a larger `limit`; never conclude "absent" from a truncated page |
| `Skilled Mean` missing while `EM` present | Skilled pool starved — no model cleared the long-term NSE>0 gate | Confirm in 9.6.1 that some model has a long-term row with `nse > 0` |
| Long-term row with tiny `n_pairs` and a wild NSE | Min-n floor not applied on this path | Check the `ieasyhydroforecast_min_pairs_long_term*` values in effect (defaults 4/5/5) |
| Tombstone (`n_pairs=0`) carrying metrics | Tombstone written malformed | Real defect — capture the row and file it |
| EM parity `comparable=0` at quarter/season | EM and LR_Base/LR_SM share no key — usually a `horizon_value` mismatch | Compare `horizon_value` on the EM row vs its inputs before assuming EM is wrong |
| Per-model period rows exist but EM/NE absent | Missed operational boundary day; maintenance cannot heal per-model period rows | Run `apps/postprocessing_forecasts/backfill_period_forecasts.py`, one calendar year per pass (PP-045) |
