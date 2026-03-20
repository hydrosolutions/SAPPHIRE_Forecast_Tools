# Local Pipeline Review Checklist — 2026-03-20

Manual verification plan for running the SAPPHIRE daily pipeline locally and
confirming that API data is written correctly at each phase for stations
**15189** and **16059**.

Run the full daily pipeline with:

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> bash apps/run_locally.sh daily
```

---

## API Key Notes

**ML forecasts are stored with `horizon=day`** in the postprocessing API regardless
of the horizon_type context they were run in (pentad or decad). To query ML model
forecasts (TFT, TiDE, TSMixer), always use `horizon=day` with a `model` filter.

**LR forecasts have a separate endpoint**: `/api/postprocessing/lr-forecast/`
(not `/api/postprocessing/forecast/`).

**Combined forecasts** (EM, NE) are at `/api/postprocessing/forecast/` with
`horizon=pentad` or `horizon=decade`.

---

## 0. Prerequisites

### 0.1 Environment variables

Set the following before running any curl commands or the pipeline:

```bash
export BASE_URL="http://localhost:8000"
export S1="15189"
export S2="16059"
export TODAY="2026-03-20"
export RECENT_START="2026-03-10"
export RECENT_END="2026-03-19"
```

### 0.2 Service health checks

- [ ] Check API gateway is up:
  ```bash
  curl -s $BASE_URL/health | python3 -m json.tool
  ```
- [ ] Check all downstream services are ready:
  ```bash
  curl -s $BASE_URL/health/ready | python3 -m json.tool
  ```
- [ ] Check individual service status:
  ```bash
  curl -s $BASE_URL/health/services | python3 -m json.tool
  ```

**Expected**: all services report `"status": "healthy"` or `"ok"`. If any service
is down, do not proceed — the pipeline will write to a dead endpoint and silently
drop data.

### 0.3 Confirm Docker containers are running

```bash
cd sapphire && docker-compose ps
```

All containers (`preprocessing-api`, `postprocessing-api`, `api-gateway`, etc.)
should show `Up`.

---

## 1. Before Run: Baseline Snapshot

Capture existing data for both stations before running the pipeline. This
establishes what existed on 2026-03-10 to 2026-03-18 so you can confirm new
records appear after the run.

### 1.1 Preprocessing — Runoff

- [ ] Station 15189 daily runoff (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```
- [ ] Station 16059 daily runoff (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```
- [ ] Record count for each station:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))"
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))"
  ```

Note the most recent date in each response — after the pipeline run you expect
a new record dated `2026-03-20`.

### 1.2 Preprocessing — Meteo (temperature and precipitation)

- [ ] Station 15189 temperature (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```
- [ ] Station 15189 precipitation (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```
- [ ] Station 16059 temperature (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```
- [ ] Station 16059 precipitation (recent):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$RECENT_START&end_date=$RECENT_END&limit=50" | python3 -m json.tool
  ```

### 1.3 Preprocessing — Snow

- [ ] Station 15189 SWE (most recent records):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 SWE (most recent records):
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=10" | python3 -m json.tool
  ```

### 1.4 Postprocessing — Short-term forecasts baseline

- [ ] LR pentad forecasts (recent):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  ```
- [ ] ML daily forecasts — TFT (recent):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  ```
- [ ] EM pentad forecasts (recent):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$RECENT_END&limit=20" | python3 -m json.tool
  ```

---

## 2. Phase 1: Preprocessing (runs once)

### What this phase writes

- `preprocessing_runoff` writes today's discharge observations to the
  preprocessing API (`horizon=day`).
- `preprocessing_gateway` extends ERA5 meteo and snow data through today.

### 2.1 Verify: New runoff record for today

**Result**: No runoff record for 2026-03-20 — iEasyHydro source has not provided
today's observation yet. Data available through 2026-03-19. Window count = 10
(RECENT_START to TODAY) for both stations. Not a code bug — data availability issue.

- [x] Station 15189 — today's discharge record: **0 records** (no today obs)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Station 16059 — today's discharge record: **0 records** (no today obs)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Window count (RECENT_START to TODAY): **S1=10, S2=10** (up to 2026-03-19)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```

**What to look for**: A record with `"date": "2026-03-20"` and a non-null `"discharge"` value.

**Red flags**:
- Empty array `[]` — preprocessing_runoff did not write or the iEasyHydro API returned no data.
- `"discharge": null` — data was received but value is missing.
- HTTP 4xx/5xx from the API endpoint — service is down or endpoint changed.

### 2.2 Verify: New meteo data for today

- [x] Station 15189 temperature today: **1 record, T=-9.71** (updated from -9.24 by reanalysis)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Station 15189 precipitation today: **1 record, P=0.15** (updated from 0.49 by reanalysis)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Station 16059 temperature today: **1 record, T=-6.01** (updated from -5.94 by reanalysis)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Station 16059 precipitation today: **1 record, P=1.24** (updated from 0.87 by reanalysis)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -m json.tool
  ```
- [x] Window counts: **S1 T=11, S1 P=11** (was 9, now 11 = RECENT_START to TODAY inclusive)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```

**What to look for**: Records with `"date": "2026-03-20"` and non-null `"value"`.

**Red flags**:
- Empty arrays — ERA5 extension did not run or failed silently.
- Values identical to previous day for multiple stations — possible ERA5 stale data.

### 2.3 Verify: Snow data updated

- [x] Station 15189 — most recent SWE record: **5 records, year-2000 dates (norms), values 71.88–78.72**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=5" | python3 -m json.tool
  ```
- [x] Station 16059 — most recent SWE record: **5 records, year-2000 dates (norms), values 50.90–53.16**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S2&snow_type=SWE&limit=5" | python3 -m json.tool
  ```

**Note**: SWE records use year-2000 dates as a day-of-year index for climatological norms. This is expected.

---

## 3. Phase 2: Maintenance Preprocessing (runs once)

### What this phase writes

- `preprocessing_runoff --maintenance` backfills any gaps in the past 30 days
  of discharge data. It upserts records that were missing or null.
- `preprocessing_gateway` extends ERA5 reanalysis (same as Phase 1 but for
  reanalysis product).

### 3.1 Verify: Gap-fill — check for any newly filled records

Compare the 30-day window before and after. If no gaps existed, counts will be
unchanged.

- [x] Station 15189 — discharge count over last 30 days: **31 records, 2026-02-17 to 2026-03-19**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); dates=[r.get('date','') for r in d]; print(f'range: {min(dates)} to {max(dates)}')"
  ```
- [x] Station 16059 — discharge count over last 30 days: **31 records, 2026-02-17 to 2026-03-19**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); dates=[r.get('date','') for r in d]; print(f'range: {min(dates)} to {max(dates)}')"
  ```

**What to look for**: Count should equal the number of days in the window (up to
30) if data is complete. Fewer records indicate remaining gaps (acceptable if
source data is unavailable).

**Red flags**:
- Count is 0 — neither operational nor maintenance preprocessing wrote any data.
- Maintenance run logs show errors for these station codes.

### 3.2 Verify: ERA5 meteo backfill (maintenance:preprocessing_gateway)

- [x] Meteo T 30-day coverage: **S1=32 (2026-02-17 to 2026-03-20), S2=32 (2026-02-17 to 2026-03-20)**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); dates=[r.get('date','') for r in d]; print(f'range: {min(dates)} to {max(dates)}')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); dates=[r.get('date','') for r in d]; print(f'range: {min(dates)} to {max(dates)}')"
  ```
- [x] Review window counts unchanged: **S1 T=11, S1 P=11** (consistent with operational run)
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
- [x] Snow SWE unchanged (still norms only): **confirmed, year-2000 dates**
  ```bash
  curl -s "$BASE_URL/api/preprocessing/snow/?code=$S1&snow_type=SWE&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  date={r.get(\"date\",\"?\")} value={r.get(\"value\",\"?\")}') for r in d]"
  ```

---

## 4. Phase 3: Forecasting + Postprocessing (PENTAD then DECAD)

### What this phase writes

- `machine_learning` writes TFT, TiDE, TSMixer forecasts. These are stored with
  `horizon=day` at the postprocessing API.
- `linear_regression` writes LR pentad and decad forecasts to the
  `/lr-forecast/` endpoint.
- `postprocessing_forecasts` reads individual model forecasts, computes ensemble
  mean (EM) and norm-error (NE) combined forecasts, and writes them to
  `/forecast/` with `horizon=pentad` or `horizon=decade`.

### 4.1 Verify: ML daily forecasts written

- [x] All models, both stations — today's forecasts (run on 2026-03-20): **11 records each, targets 2026-03-21 to 2026-03-31**
  ```bash
  for model in TFT TiDE TSMixer; do
    echo "S1 $model:"; curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  count={len(d)}'); [print(f'  target={r.get(\"target\")} fc={r.get(\"forecasted_discharge\")}') for r in d[:3]]"
    echo "S2 $model:"; curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  count={len(d)}'); [print(f'  target={r.get(\"target\")} fc={r.get(\"forecasted_discharge\")}') for r in d[:3]]"
  done
  ```

- [x] Window counts (RECENT_START to TODAY): **TFT=121, TiDE=106, TSMixer=111 per station**
  ```bash
  for model in TFT TiDE TSMixer; do
    echo "S1 $model:"; curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$RECENT_START&end_date=$TODAY&limit=500" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  count={len(d)}')"
    echo "S2 $model:"; curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=$model&start_date=$RECENT_START&end_date=$TODAY&limit=500" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  count={len(d)}')"
  done
  ```

- [x] Null discharge check: **all valid, 0 nulls** (TFT=11, TiDE=11, TSMixer=11)
  ```bash
  for model in TFT TiDE TSMixer; do
    echo "$model:"; curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); null=[r for r in d if r.get('forecasted_discharge') is None]; print(f'  valid={len(d)-len(null)}, null={len(null)}')"
  done
  ```

**What to look for**: At least one record per model per station with
today's date and non-null `"forecasted_discharge"` value.

**Red flags**:
- Empty arrays for all three models — ML module crashed before writing.
- Records present for one station but not the other — org-scoping or station
  filter issue.
- `"forecasted_discharge": null` — model ran but produced NaN output.

### 4.2 Verify: LR pentad forecasts written

March 20 is day 20 of the month — this is NOT a standard pentad issue day (1, 6,
11, 16, 21, 26). LR may not produce a new pentad forecast today. Check logs first.
If no new record, that is expected.

- [x] Station 15189 — LR pentad forecasts (recent window): **2 records (2026-03-10, 2026-03-15)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [x] Station 16059 — LR pentad forecasts (recent window): **2 records (2026-03-10, 2026-03-15)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [x] Station 15189 — LR decad forecasts (recent window): **1 record (2026-03-10, fc=1.866)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 — LR decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [x] Today's LR forecasts (should be 0 — Mar 20 is not a boundary day): **confirmed 0 pentad, 0 decad**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$TODAY&end_date=$TODAY&limit=10" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$TODAY&end_date=$TODAY&limit=10" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```

**What to look for**: If today is an issue day, a new record dated `2026-03-20`.
If not an issue day, confirm recent records exist from the previous issue day.

**Red flags**:
- No records at all (empty array) — LR module has not written any forecasts
  recently.
- Negative forecast values (`-1.0`) — sentinel value leaking through; should be
  NaN instead.

### 4.3 Verify: Combined forecasts (EM, NE) written

- [ ] Station 15189 — EM pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 — EM pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 15189 — NE pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 — NE pentad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=NE&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 15189 — EM decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 — EM decad forecasts (recent window):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=decade&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=10" | python3 -m json.tool
  ```

**What to look for**: EM rows should have non-null `q05`, `q25`, `q75`, `q95`
fields if ML models ran successfully. NE rows represent norm-error ensembles.

**Red flags**:
- EM/NE arrays empty while ML individual model arrays are populated — postprocessing
  ensemble step failed.
- EM `q05`/`q25`/`q75`/`q95` all null — quantiles not being written (PP-019 regression).
- `q05 > q25` or `q75 > q95` — quantile ordering violation.

### 4.4 Quantile ordering spot-check

- [ ] Station 15189 — check EM quantile ordering for the most recent EM pentad record:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" | python3 -c "
import sys, json
rows = json.load(sys.stdin)
for r in rows:
    q = [r.get('q05'), r.get('q25'), r.get('q75'), r.get('q95')]
    ok = all(x is not None for x in q) and q[0] <= q[1] <= q[2] <= q[3]
    print(r.get('date'), q, 'OK' if ok else 'FAIL')
"
  ```
- [ ] Station 16059 — same check:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" | python3 -c "
import sys, json
rows = json.load(sys.stdin)
for r in rows:
    q = [r.get('q05'), r.get('q25'), r.get('q75'), r.get('q95')]
    ok = all(x is not None for x in q) and q[0] <= q[1] <= q[2] <= q[3]
    print(r.get('date'), q, 'OK' if ok else 'FAIL')
"
  ```

**Expected**: All rows print `OK`. Collapsed quantiles (equal values at low-variance
stations) are acceptable but must still be in non-decreasing order.

---

## 5. Phase 4: Maintenance (PENTAD then DECAD)

### What this phase writes

- ML maintenance: recalculates NaN forecasts, fills ML gaps, handles new stations.
- LR hindcast: backfills historical LR forecasts.
- Postprocessing maintenance: fills missing EM/NE ensembles where individual
  model rows exist but ensemble was not computed.

### 5.1 Verify: ML gap-fill — check for any newly filled historical records

- [x] Station 15189 — TFT forecasts for the past 14 days: **30 records**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=TFT&start_date=2026-03-05&end_date=$TODAY&limit=30" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'records')"
  ```
- [x] Station 16059 — TFT forecasts for the past 14 days: **30 records**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=TFT&start_date=2026-03-05&end_date=$TODAY&limit=30" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'records')"
  ```

**What to look for**: If gaps existed before the run, counts should be higher
after maintenance. Compare with baseline count from Section 1.4.

### 5.2 Verify: LR hindcast — recent LR records updated

- [x] Station 15189 — LR pentad record count (review window): **2 records (2026-03-10, 2026-03-15)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); [print(f'  date={r.get(\"date\",\"?\")}') for r in d]"
  ```
- [x] Station 16059 — LR pentad (review window): **2 records (2026-03-10, 2026-03-15)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S2&horizon=pentad&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}')"
  ```
- [x] Station 15189 — LR decade (review window): **1 record (2026-03-10)**
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=$RECENT_START&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'count={len(d)}'); [print(f'  date={r.get(\"date\",\"?\")}') for r in d]"
  ```
- [x] Wider window check (Feb 1 - Mar 20): **pentad: 9 records (Feb 5–Mar 15), decad: 4 records (Feb 10–Mar 10)** — full hindcast coverage
  ```bash
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=pentad&start_date=2026-02-01&end_date=$TODAY&limit=100" | python3 -c "import sys,json; d=json.load(sys.stdin); dates=sorted(set(r.get('date','') for r in d)); print(f'count={len(d)} dates={dates}')"
  curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=$S1&horizon=decade&start_date=2026-02-01&end_date=$TODAY&limit=100" | python3 -c "import sys,json; d=json.load(sys.stdin); dates=sorted(set(r.get('date','') for r in d)); print(f'count={len(d)} dates={dates}')"
  ```

**What to look for**: 5 or 6 pentad issue days within 30 days. If fewer records
appear, LR hindcast may not have written for these stations.

### 5.3 Verify: Postprocessing maintenance gap-fill — EM coverage

- [ ] Station 15189 — EM pentad record count over past 30 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'EM pentad records')"
  ```
- [ ] Station 16059 — EM pentad record count over past 30 days:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=2026-02-17&end_date=$TODAY&limit=50" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'EM pentad records')"
  ```

**What to look for**: EM count should roughly match LR count (one EM per issue day).
Gaps in EM where LR exists indicate the maintenance gap-fill did not run.

---

## 6. Phase 5: Long-Term Forecasting (gated)

March 20 is 10 days from March 10 and 5 days before March 25. The gate is
`±5 days from 10th/25th`. March 20 falls outside the March-10 window
(|20-10| = 10 > 5) and just within the March-25 window (|25-20| = 5 ≤ 5).

**The long-term phase MAY run today** (March 20 is within the ±5 day gate for March 25). Skip Sections
6.1 to 6.3. If you want to force a long-term run, use the `LT_FORECAST_TODAY`
override (see note below).

If you override the date to force the run:

```bash
ieasyhydroforecast_env_file_path=<path-to-your-.env> LT_FORECAST_TODAY=2026-03-10 bash apps/run_locally.sh long-term
```

### 6.1 Verify: Long-term forecasts written (only if forced)

- [ ] Station 15189 — monthly forecasts (recent):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&start_date=2026-03-01&end_date=2026-03-31&limit=20" | python3 -m json.tool
  ```
- [ ] Station 16059 — monthly forecasts (recent):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&start_date=2026-03-01&end_date=2026-03-31&limit=20" | python3 -m json.tool
  ```
- [ ] Record counts:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S1&horizon_type=month&start_date=2026-03-01&end_date=2026-03-31&limit=20" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'long-term records')"
  curl -s "$BASE_URL/api/postprocessing/long-forecast/?code=$S2&horizon_type=month&start_date=2026-03-01&end_date=2026-03-31&limit=20" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d), 'long-term records')"
  ```

**Red flags**:
- Empty arrays after a forced run — `long_term_forecasting` module crashed or
  no models were eligible.
- Records with null `forecast` values — model ran but output NaN.

### 6.2 Verify: Long-term skill metrics updated (only if forced)

- [ ] Station 15189 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=month&limit=10" | python3 -m json.tool
  ```
- [ ] Station 16059 — monthly skill metrics:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=month&limit=10" | python3 -m json.tool
  ```

---

## 7. Post-Run: Full Verification

Run after the entire pipeline completes. Confirm all expected data exists for
both stations.

### 7.1 Preprocessing completeness

- [ ] Station 15189 — runoff record for today present:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S1&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('PASS' if len(d) > 0 else 'FAIL - no runoff record')"
  ```
- [ ] Station 16059 — runoff record for today present:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/runoff/?code=$S2&horizon=day&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('PASS' if len(d) > 0 else 'FAIL - no runoff record')"
  ```
- [ ] Station 15189 — meteo (T and P) for today present:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('T: PASS' if len(d) > 0 else 'T: FAIL')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S1&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('P: PASS' if len(d) > 0 else 'P: FAIL')"
  ```
- [ ] Station 16059 — meteo (T and P) for today present:
  ```bash
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=T&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('T: PASS' if len(d) > 0 else 'T: FAIL')"
  curl -s "$BASE_URL/api/preprocessing/meteo/?code=$S2&meteo_type=P&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('P: PASS' if len(d) > 0 else 'P: FAIL')"
  ```

### 7.2 Short-term forecast completeness

- [ ] Station 15189 — at least one ML model wrote a forecast today:
  ```bash
  for model in TFT TiDE TSMixer; do curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('$model:', 'PASS' if len(d) > 0 else 'FAIL')"; done
  ```
- [ ] Station 16059 — at least one ML model wrote a forecast today:
  ```bash
  for model in TFT TiDE TSMixer; do curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=day&model=$model&start_date=$TODAY&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('$model:', 'PASS' if len(d) > 0 else 'FAIL')"; done
  ```
- [ ] Station 15189 — EM record exists (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S1&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('EM pentad: PASS' if len(d) > 0 else 'EM pentad: FAIL')"
  ```
- [ ] Station 16059 — EM record exists (recent issue day):
  ```bash
  curl -s "$BASE_URL/api/postprocessing/forecast/?code=$S2&horizon=pentad&model=EM&start_date=$RECENT_START&end_date=$TODAY&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('EM pentad: PASS' if len(d) > 0 else 'EM pentad: FAIL')"
  ```

### 7.3 Skill metrics check

- [ ] Station 15189 — pentad skill metrics exist:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=pentad&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('pentad skill: PASS' if len(d) > 0 else 'pentad skill: FAIL - no records')"
  ```
- [ ] Station 16059 — pentad skill metrics exist:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=pentad&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('pentad skill: PASS' if len(d) > 0 else 'pentad skill: FAIL - no records')"
  ```
- [ ] Station 15189 — decad skill metrics exist:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S1&horizon=decade&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('decad skill: PASS' if len(d) > 0 else 'decad skill: FAIL - no records')"
  ```
- [ ] Station 16059 — decad skill metrics exist:
  ```bash
  curl -s "$BASE_URL/api/postprocessing/skill-metric/?code=$S2&horizon=decade&limit=5" | python3 -c "import sys,json; d=json.load(sys.stdin); print('decad skill: PASS' if len(d) > 0 else 'decad skill: FAIL - no records')"
  ```

**Red flags**:
- Skill metric `n_pairs` is 0 or 1 — recalculation ran but insufficient historical
  pairs (acceptable for new stations, flag for established ones).
- Skill metrics completely absent — recalculation step was skipped or crashed.

### 7.4 Summary pass/fail table

Fill in after completing all verification steps above:

| Check | Station 15189 | Station 16059 |
|-------|--------------|--------------|
| Runoff today | | |
| Meteo T today | | |
| Meteo P today | | |
| Snow (recent) | | |
| ML TFT forecast today | | |
| ML TiDE forecast today | | |
| ML TSMixer forecast today | | |
| LR forecast (recent issue day) | | |
| EM pentad (recent issue day) | | |
| EM quantile ordering | | |
| NE pentad (recent issue day) | | |
| Pentad skill metric | | |
| Decad skill metric | | |

---

## 8. Common Failure Patterns and Remediation

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| All preprocessing empty | Service not running or iEasyHydro API unreachable | Check `docker-compose ps`; check iEasyHydro API connectivity |
| ML forecasts missing, LR present | ML module crashed (date format bug, shape mismatch) | Check pipeline logs for tracebacks in `machine_learning` phase |
| EM missing, individual ML present | Postprocessing ensemble step crashed | Check logs for `postprocessing_forecasts` phase errors |
| EM `q05` all null | PP-019 quantile regression; API writer not sending quantile fields | Check postprocessing_forecasts version; run `run_tests.sh postprocessing_forecasts` |
| LR returns `-1.0` values | Sentinel value not converted to NaN | Check `linear_regression` module for sentinel guard |
| Skill metrics absent | Recalculation step skipped or API error on write | Check logs for `skill` keyword; verify postprocessing API write permissions |
| Long-term forecasts absent | Gate condition not met (expected today) or module crash | Check `LT_FORECAST_TODAY`; review `long_term_forecasting` logs |
