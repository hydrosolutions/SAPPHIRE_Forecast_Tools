# Runoff long-horizon hydrograph P3 implementation prompt — local end-to-end verification

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the verification agent. P3 actually runs the P1+P2 writer against the
> user's local stack (with the iEH HF SSH tunnel up) and captures evidence
> that monthly and seasonal triad fields are populated through
> `/hydrograph/`. Plan at commit `ec03c44`; P2 writer at commit `785528a`.

--- BEGIN PROMPT ---

You are a verification agent on the SAPPHIRE forecast tools project.
Your role is **Phase 3 only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
This is a **live end-to-end verification** phase: you invoke the
P1+P2 writer against the operator's local stack, probe the resulting
records via the preprocessing API, and produce a single evidence
artifact. You do NOT write production code.

## What you are doing

**Goal**: Produce
`doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`
recording:

1. Stack health (local API + iEH HF SSH tunnel both reachable).
2. The writer's run output for `--target-year 2026` (exit code,
   record counts, any warnings).
3. API probes confirming written records:
   - Monthly: `GET /preprocessing/hydrograph/?horizon=month&...` returns
     12 records per station with at least some non-null `previous` and
     `current` per the per-month D-Q6 threshold.
   - Seasonal: `GET /preprocessing/hydrograph/?horizon=season&...` returns
     1 record per station with non-null `norm` and `previous` where
     P2's strict-completeness rule allows it. `current` is expected to
     be `None` for target year 2026 (June 2026 is the in-progress
     month per D2, so the season's strict-completeness rule fires).
4. Full test suite regression (`SAPPHIRE_TEST_ENV=True bash
   run_tests.sh`) — zero unexpected new failures or skips.
5. A `DISPATCH:` line: `PROCEED` (Phase 4 + Phase 5 can dispatch) or
   `BLOCKED — <reason>`.

**Files you may modify (exhaustive)**

- `doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`
  (CREATE)

You may NOT modify any other file. No production code, no tests, no
edits to the plan document, the decisions artifact, prior evidence
files, the env file, the writer, or anywhere else. P3 is verification;
fixes belong in a follow-up phase.

## Inputs the operator provides

- **Env file**: `$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm`
  — already in place. Source it via the standard pattern (set
  `ieasyhydroforecast_env_file_path` and let
  `setup_library.load_environment()` do the rest). NEVER echo
  the env file's contents into the evidence artifact.
- **Local SAPPHIRE API**: `http://localhost:8000/api/` (assumed
  reachable; sanity-check at the top of the probe).
- **iEH HF SSH tunnel**: assumed up on port 5555. Sanity-check by
  attempting an `IEasyHydroHFSDK()` construction; if it raises a
  connection error, write `DISPATCH: BLOCKED — tunnel not up` and
  stop.

## Procedure

### Step 1 — Sanity-check the stack

Confirm the local API is responsive:

```python
import urllib.request, sys
try:
    r = urllib.request.urlopen('http://localhost:8000/health', timeout=5)
    print('API HTTP', r.status)
except Exception as e:
    print('API FAIL:', type(e).__name__, e)
    sys.exit(1)
```

Confirm the iEH HF SDK can be constructed (this hits the SSH tunnel
on port 5555):

```bash
# From inside apps/preprocessing_runoff/, with the env loaded:
uv run python -c "
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK
sdk = IEasyHydroHFSDK()
sites = sdk.get_discharge_sites()
print(f'SDK OK, {len(sites)} discharge sites available')
"
```

If either check fails, write evidence with the failing line and
`DISPATCH: BLOCKED — <stack component> not reachable` and stop.

### Step 2 — Invoke the writer for target year 2026

From `apps/preprocessing_runoff/`:

```bash
cd apps/preprocessing_runoff

# Source env via setup_library pattern, the same way sync_monthly_norms.py
# does. The exact incantation depends on local conventions; the writer's
# main() calls sl.load_environment() which reads the env file path from
# the ieasyhydroforecast_env_file_path environment variable.
export ieasyhydroforecast_env_file_path="$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm"

# Optional first-pass dry-run to confirm the station set resolves:
uv run python sync_long_horizon_hydrograph.py --target-year 2026 --dry-run

# Live write:
uv run python sync_long_horizon_hydrograph.py --target-year 2026
```

Capture stdout + stderr, exit code, and the total record count
logged by the writer's "Long-horizon monthly hydrograph ingestion
wrote N records" line. Record these in the evidence artifact (no
station codes).

If the writer exits non-zero, capture the exit code + the last 20
lines of output in the evidence file and write `DISPATCH: BLOCKED
— writer exit code <N>`. Do NOT debug the writer; that's a P1/P2
follow-up.

### Step 3 — Probe the API for written records

For each of a SMALL representative sample of stations (3-5
stations, aliased), query the preprocessing API:

```python
import urllib.request, json
API_BASE = "http://localhost:8000/api/preprocessing"

def fetch(code, horizon):
    url = f"{API_BASE}/hydrograph/?horizon={horizon}&code={code}&limit=100"
    return json.loads(urllib.request.urlopen(url, timeout=30).read())
```

For each station, fetch both `horizon=month` and `horizon=season`.
Record per-station-per-horizon:
- Total records returned (expected: 12 for month; ≥1 for season).
- Count of records with non-null `norm`.
- Count of records with non-null `previous`.
- Count of records with non-null `current`.
- For seasonal: the actual stored `norm`, `previous`, `current`
  values (rounded to 2 decimals), so you can sanity-check them
  against the formula "mean of 6 monthly values". Do NOT include
  raw runoff values from other endpoints — only the hydrograph
  fields.

### Step 4 — Run the regression test suite

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff 2>&1 | tail -10
```

Confirm: at least 319 passed (the P1+P2 baseline), 2 pre-existing
skips in `test_src.py`, zero new failures, zero new skips. Paste
the tail-10 output verbatim in the evidence artifact (no station
codes in test names — there should be none, but check).

### Step 5 — Build the evidence artifact

Create
`doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`
with this structure:

```markdown
# Runoff Long-Horizon Hydrograph — Local End-to-End Evidence

**Date produced**: YYYY-MM-DD
**Plan reference**: doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md (commit ec03c44)
**Writer commit**: apps/preprocessing_runoff/sync_long_horizon_hydrograph.py (commit 785528a)
**Target year**: 2026

## 1. Stack health

- Local API health: HTTP 200 on http://localhost:8000/health.
  (or: failure summary if not reachable.)
- iEH HF SDK readiness: <N> discharge sites returned from
  get_discharge_sites().

## 2. Writer run

- Command: `uv run python sync_long_horizon_hydrograph.py --target-year 2026`
- Exit code: 0 (or N)
- Wall time: <seconds>
- Stations processed: <count>
- Records written: <count>  (expected: 12 monthly + 1 seasonal per station = 13 × stations)
- Last 10 log lines (no station codes; replace with aliases):

```
<paste>
```

If exit was non-zero, paste the full final error and stop here
with `DISPATCH: BLOCKED — writer exit code <N>`.

## 3. API probe results

Sample of 3-5 stations (aliased as `<station-1>`...`<station-5>`).

### Monthly records

| Station | Records returned | Non-null norm | Non-null previous | Non-null current |
|---|---|---|---|---|
| `<station-1>` | 12 | 12 | 11 | 5 |
| ... | ... | ... | ... | ... |

(Counts depend on the data; the exact numbers reflect D-Q6 +
D2 + Q-4 outcomes. As long as at least one station has at
least one non-null `previous` and at least one non-null
`current`, the writer is doing useful work. Months without
populated values are the per-month threshold or in-progress
months working correctly, not failures.)

### Seasonal records

| Station | Records returned | norm | previous | current |
|---|---|---|---|---|
| `<station-1>` | 1 | <value or null> | <value or null> | null (in-progress year) |
| ... | ... | ... | ... | ... |

Sanity check: for at least one station, manually verify the
seasonal `previous` equals approximately the mean of the six
monthly `previous` values (months 4-9). Cite the calculation
in the evidence file:

> `<station-K>`: monthly previous for Apr-Sep = (10, 12, 15, 13, 11, 9);
> mean = 11.67; API-returned seasonal previous = 11.67. Match.

Expected: seasonal `current` is `None` for target year 2026
because June 2026 is in-progress (D2 propagates through D1's
strict-completeness rule). If `current` is unexpectedly
non-None, treat as a finding and document it.

## 4. Regression test suite

Tail of `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`:

```
<paste tail-10>
```

Result: <N> passed, <M> skipped. Expected: ≥319 passed, 2 skipped.

## 5. DECISION

`DISPATCH: PROCEED`

(or `DISPATCH: BLOCKED — <one-line reason>` if any of: stack
unhealthy; writer exit non-zero; API probes show zero non-null
previous/current across all sample stations; regression test
suite has new failures or new skips.)

## 6. Notes for Phase 4

(Optional: one-paragraph observations for the Phase 4 operator
wrapper. E.g. "writer ran in ~<seconds>s for <N> stations; the
yearly wrapper should expect this runtime"; or "writer encountered
no warnings", etc. Keep concise; no real station codes.)
```

### Step 6 — Self-review

- `git status --short` should show only the new evidence file
  added; no other files touched.
- The artifact contains no real station code matching the
  project pattern. Grep your output for any 4-5 digit string
  that looks like a station code that isn't `<station-N>`,
  `19999`, a year, a port, a record count, or a coordinate.
- The `DISPATCH:` line is exactly one of the two allowed forms.
- The writer was invoked exactly once for target year 2026; no
  accidental writes for other years.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the evidence artifact path
   above.** No production code, no tests, no plan edits.
2. **Do NOT include real station codes in the evidence file.**
   Aliases (`<station-1>`, …) or the `19999` sentinel only.
3. **Do NOT echo the env file's contents into the artifact.**
   Record the variable name(s) you read from and the station
   count; nothing else from the env file.
4. **Do NOT debug the writer.** If it fails, capture evidence
   and `DISPATCH: BLOCKED`. Fixes are a follow-up phase.
5. **Do NOT invoke the writer for any year other than 2026.**
   The plan scopes P3 to the current target year. Backfill
   runs are an operator concern.
6. **Do NOT commit, push, branch, stage, or stash.** The
   orchestrator commits after deliberation.
7. **Do NOT use curl.** Use Python urllib or `requests`.
8. **Do NOT retry indefinitely.** If the API or the SSH tunnel
   is unreachable, write `DISPATCH: BLOCKED — <reason>` and
   stop.

## Deliverable format

Return a single Markdown report to the orchestrator (under ~150
lines):

1. **Summary** — 3-4 sentences: stack reachable? writer exit
   code? probe result? regression test result? final dispatch
   decision?
2. **Files created** — single path:
   `doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`.
3. **Writer outcome** — exit code; stations processed; records
   written; wall time.
4. **Probe outcome** — sample count; at least one station with
   non-null `previous`/`current` in monthly; at least one
   station with seasonal `previous` populated; seasonal
   `current` is `None` for target year 2026 (expected).
5. **Regression test** — pass/fail/skip counts; compare to
   baseline 319/0/2.
6. **DISPATCH decision** — quote the exact line verbatim.
7. **Scope check** — confirm only the evidence file was
   modified; no production code, no plan edits, no env file
   edits.
8. **Sensitive-data check** — confirm no real station codes
   appear in the evidence file or the orchestrator report.
9. **Coordination items** (optional) — anything the
   orchestrator should know (e.g. an unexpectedly slow run, a
   surprising data shape, etc.).

## What success looks like

- One new evidence file at the specified path.
- The artifact records writer-run output + API probe results
  + regression test result.
- At least one sample station shows non-null monthly
  `previous` AND non-null monthly `current` (subject to D-Q6
  + D2).
- At least one sample station shows non-null seasonal
  `previous`.
- Seasonal `current` is `None` for target year 2026 (expected).
- Regression test suite: ≥319 passed, 2 skipped (baseline
  preserved).
- The final `DISPATCH:` line is either `PROCEED` or
  `BLOCKED — <reason>`.
- No real station codes anywhere.
- Phase 4 and Phase 5 dispatch decision is operationally
  enforceable from the artifact's last line.

If you write `DISPATCH: BLOCKED`, the orchestrator will read the
reason, possibly run remediation, and re-dispatch P3. **Do not
patch the gap in this phase.** P3 is a verification gate, not a
fix.

--- END PROMPT ---
