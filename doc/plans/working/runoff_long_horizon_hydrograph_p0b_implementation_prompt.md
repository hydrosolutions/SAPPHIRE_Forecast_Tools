# Runoff long-horizon hydrograph P0b implementation prompt — coverage & audit probe

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the implementation agent. This is the **gating phase**: its
> `DISPATCH:` artifact decides whether P1 can proceed. Work continues
> on `develop_dashboard_snow_display` (plan at `355e276`, P0a
> decisions artifact at `28ba979`).

--- BEGIN PROMPT ---

You are a verification agent on the SAPPHIRE forecast tools project.
Your role is **Phase 0b only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
P0a (commit `28ba979`) committed the design decisions; P0b is the
**gating phase** — it produces a single evidence artifact whose
`DISPATCH:` line decides whether Phase 1 can dispatch.

You do **not** write production code in this phase. You read-only
probe the live API and the codebase and produce one Markdown
artifact.

## What you are doing

**Goal**: Produce
`doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md`
containing two gates:

1. **Daily runoff coverage** for the target year `Y` and prior year
   `Y-1` across the configured station set, against the plan-pinned
   **≥80%** threshold.
2. **Grep audit** of all app-side writers that produce hydrograph
   rows with `horizon_type` in `{month, season}`.

The artifact must end with exactly one `DISPATCH:` line. Phase 1
must NOT dispatch unless that line reads `DISPATCH: PROCEED`.

**Files you may modify (exhaustive)**

- `doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md`
  (create)

You may NOT modify any other file. No production code, no tests,
no edits to the plan document, the decisions artifact, the env
file, or anywhere else.

## Inputs the operator provides

- **Env file**: `$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm`
  — already in place. Use only to read the configured station
  set if the API doesn't directly expose it; do NOT echo the
  file's contents into the evidence artifact.
- **Local SAPPHIRE API**: `http://localhost:8000/api/` (assumed
  reachable; sanity-check at the top of the probe).
- **iEH HF SSH tunnel**: already up per the operator. The
  preprocessing_runoff module ran successfully ~30 minutes
  before this probe (see commit history), so the upstream data
  is current.

## Procedure

### Step 1 — Sanity-check the stack

Confirm `http://localhost:8000/health` returns `200`. **Use Python
urllib, not curl** — sandboxes commonly block curl with
false-positive "mutating HTTP method" errors on plain GETs.
Python stdlib goes through reliably:

```bash
python3 -c "
import urllib.request, sys
try:
    r = urllib.request.urlopen('http://localhost:8000/health', timeout=5)
    print('HTTP', r.status)
except Exception as e:
    print('FAIL:', type(e).__name__, e)
    sys.exit(1)
"
```

If non-200, write evidence with `DISPATCH: BLOCKED — local stack
not reachable` and stop.

### Step 2 — Determine the station set

The "planned station set" is the set of station codes the
preprocessing_runoff module operates on for runoff. Determine
it from one of these sources, in priority order:

1. The kghm env file at
   `$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm`
   — look for `ieasyhydroforecast_HRU_*` or similar configured
   station lists. Read the env file, identify the relevant var,
   record the **count** of stations (not the values).
2. If the env file doesn't expose a single list, query the API
   for distinct codes:
   `GET /runoff/?horizon=day&start_date=2026-01-01&end_date=2026-01-02&limit=10000`
   and extract unique `code` values.
3. If neither works, fall back to a documented sample of 5-10
   stations (state the sample explicitly in the evidence file).

In the evidence file, record the **count and source** of the
station set, but NEVER the raw codes — alias them as `<station-1>`,
`<station-2>`, …, or use the `19999` sentinel. This matches
[[feedback-no-real-station-codes]] and the plan's MINOR-2 fix.

### Step 3 — Coverage probe

For each station in the planned set and each year in `{Y, Y-1}`
where `Y = 2026`:

Query the preprocessing API endpoint:

```
GET http://localhost:8000/api/preprocessing/runoff/?horizon=day&code={station}&start_date={year}-01-01&end_date={year}-12-31&limit=10000
```

Compute the denominator **per the plan's threshold definition** — full
year for complete years, days-elapsed for the in-progress year:

```python
import calendar, datetime
today = datetime.date.today()
if year < today.year:
    denominator = 366 if calendar.isleap(year) else 365  # complete year
elif year == today.year:
    denominator = today.timetuple().tm_yday  # in-progress year
else:
    raise AssertionError("future year — should not be in {Y, Y-1}")
```

Then:
- `rows_returned` = `len(response_json)`.
- `non_null_value_count` = count of rows where the value field is
  not `None` and is finite. (Per round-1 evidence, the runoff
  endpoint surfaces the value as `discharge`, not `value`; honor
  whatever the live API returns and document the field name in
  the evidence artifact.)
- `coverage_pct` = `non_null_value_count / denominator * 100`.
- `passes_threshold` = `coverage_pct >= 80`.

This denominator rule is plan-pinned. The probe initially BLOCKED
under the old fixed-365 denominator in commit `6d5c81a` because
2026 (today day 153) was rated at ~42% by definition; the
elapsed-days rule corrects that.

Record per (station, year) in a small Python script that builds
a table. **Use Python urllib or `requests` (already available);
do NOT use curl.**

Implementation sketch (adapt as needed):

```python
import urllib.request, json, calendar, datetime
API_BASE = "http://localhost:8000/api/preprocessing"
today = datetime.date.today()
Y = today.year  # 2026 on 2026-06-02
stations = [...]  # from Step 2; aliases in the artifact
VALUE_FIELD = "discharge"  # confirm against the live API per Step 3 note
results = []
for station_alias, station_code in zip(aliases, stations):
    for year in [Y, Y - 1]:
        url = (
            f"{API_BASE}/runoff/?horizon=day&code={station_code}"
            f"&start_date={year}-01-01&end_date={year}-12-31&limit=10000"
        )
        rows = json.loads(urllib.request.urlopen(url, timeout=30).read())
        if year < today.year:
            denom = 366 if calendar.isleap(year) else 365
        else:  # in-progress year
            denom = today.timetuple().tm_yday
        non_null = sum(
            1 for r in rows
            if r.get(VALUE_FIELD) is not None
            and r[VALUE_FIELD] == r[VALUE_FIELD]  # NaN-safe
        )
        pct = round(non_null / denom * 100, 1)
        passes = pct >= 80.0
        results.append(
            (station_alias, year, len(rows), non_null, denom, pct, passes)
        )
```

**Do not include real station codes in the evidence file.** The
script uses real codes internally to call the API; only the
aliases appear in the rendered artifact.

### Step 4 — Grep audit

Run the plan-pinned command from the project root:

```bash
rg -nP '"(month|season)"|horizon_type\s*=\s*["\x27]?(month|season)|write_hydrograph\b' apps/
```

(The `\x27` is a regex-safe way to express a single quote inside
the alternation; if `rg` rejects it, use the literal `'` and
escape the outer quoting appropriately in your shell.)

For each match, manually triage in the evidence file:
- **Expected match**: `apps/preprocessing_runoff/sync_monthly_norms.py`
  (the old runoff path to be retired in Phase 4).
- **Expected absent**: the new
  `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
  (Phase 1 hasn't built it yet).
- **Any other match**: triage individually. False positives
  (e.g. dashboard read paths, doc strings, test fixtures) must be
  justified inline in the evidence file. Genuine third-party
  writers cause `DISPATCH: BLOCKED — additional writer surfaced`.

### Step 5 — DISPATCH decision

Compute the final `DISPATCH:` line:

- `DISPATCH: PROCEED` requires **all** of:
  - Coverage gate: every `(station, year)` pair in `{Y, Y-1}`
    passes the ≥80% threshold.
  - Audit gate: no untriaged matches and no third-party writers
    beyond `sync_monthly_norms.py`.
- `DISPATCH: BLOCKED — <one-line reason>` otherwise. If both gates
  fail, prefer the coverage-failure reason and enumerate failing
  pairs in the evidence file (in alias form, not real codes).

### Step 6 — Write the evidence file

Create
`doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md`
with this exact structure:

```markdown
# Runoff Long-Horizon Hydrograph — Coverage & Audit Probe

**Date produced**: YYYY-MM-DD
**Plan reference**: doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md (commit 355e276)
**Decisions artifact**: doc/plans/working/runoff_long_horizon_hydrograph_decisions.md (commit 28ba979)
**Target year (Y)**: 2026 (Y-1 = 2025)

## 1. Stack health

Python urllib GET on http://localhost:8000/health → HTTP 200.
(or: failure reason if not reachable.)

## 2. Station set

- Source: <env file variable, or API query, or documented fallback>
- Count: <N>
- Aliasing: stations are aliased as `<station-1>` ... `<station-N>` in this artifact; real codes never appear.

## 3. Coverage results (≥80% threshold per plan)

Threshold: non-null daily runoff covers ≥80% of expected days per (station, year). Denominator: full year (365/366) for complete years; days-elapsed-so-far (`today.timetuple().tm_yday`) for the in-progress year. On 2026-06-02 the in-progress denominator is 153.

| Station | Year | Rows | Non-null | Denominator (kind) | % | Pass? |
|---|---|---|---|---|---|---|
| `<station-1>` | 2026 | … | … | 153 (elapsed) | …% | ✓/✗ |
| `<station-1>` | 2025 | … | … | 365 (full) | …% | ✓/✗ |
| ... | ... | ... | ... | ... | ... | ... |

**Coverage gate**: PASS / FAIL. If FAIL, failing (station, year) pairs enumerated above.

## 4. Audit results (grep over apps/)

Command run:

```
rg -nP '"(month|season)"|horizon_type\s*=\s*["\x27]?(month|season)|write_hydrograph\b' apps/
```

| File | Line | Match | Triage |
|---|---|---|---|
| apps/preprocessing_runoff/sync_monthly_norms.py | … | … | Expected — old runoff path, to be retired in Phase 4 |
| ... | ... | ... | ... |

**Audit gate**: PASS / FAIL. If FAIL, untriaged or third-party writer enumerated above.

## 5. DECISION

`DISPATCH: PROCEED`

(or)

`DISPATCH: BLOCKED — <one-line reason>`

## 6. Notes for Phase 1

(Optional: any observations the Phase 1 implementer should know
about the coverage data — e.g. which subset of stations is best
represented, whether `Y-1` is denser than `Y`, etc. Keep concise;
no real codes.)
```

### Step 7 — Self-review

- `git diff --stat` — only the evidence file added; no other
  files modified.
- The artifact contains no real station code matching the
  project pattern. Grep your output for any 4-5 digit string
  that looks like a station code that isn't `<station-N>` or
  `19999`. The probe target URLs in any sample code shown in the
  artifact must also use `<station-N>` / `19999`, not real codes.
- The `DISPATCH:` line is exactly one of the two allowed forms.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the evidence artifact path
   above.** No production code, no tests, no plan edits.
2. **Do NOT include real station codes in the evidence file.**
   Aliases (`<station-1>`, …) or the `19999` sentinel only.
3. **Do NOT echo the env file's contents into the artifact.**
   Record the variable name(s) you read from and the station
   count; nothing else from the env file.
4. **Do NOT relax the ≥80% threshold** if the coverage check
   fails. Write `DISPATCH: BLOCKED` and stop. The threshold is
   plan-pinned.
5. **Do NOT narrow the grep pattern silently.** Every match must
   be triaged in the artifact; any narrowing or false-positive
   dismissal needs an inline justification.
6. **Do NOT commit, push, branch, stage, or stash.** The
   orchestrator commits after deliberation.
7. **Do NOT use curl.** Use Python urllib or `requests`.
8. **Do NOT retry indefinitely.** If the API or the SSH tunnel is
   unreachable, write `DISPATCH: BLOCKED — <reason>` and stop.
   This phase produces a decision artifact, not a fix.

## Deliverable format

Return a single Markdown report to the orchestrator (under ~150
lines):

1. **Summary** — 2-3 sentences: stack reachable? coverage
   result? audit result? final dispatch decision?
2. **Files created** — single path:
   `doc/plans/working/runoff_long_horizon_hydrograph_coverage_probe.md`.
3. **Coverage gate** — pass count / fail count out of total
   (station, year) pairs.
4. **Audit gate** — number of matches found, count of expected
   vs untriaged vs third-party.
5. **DISPATCH decision** — quote the exact line verbatim.
6. **Scope check** — confirm only the evidence file was
   modified; no production code, no plan edits, no env file
   edits.
7. **Sensitive-data check** — confirm no real station codes
   appear in the evidence file or the orchestrator report.
8. **Coordination items** (optional) — anything the orchestrator
   should know (e.g. unexpected third-party writer surfaced).

## What success looks like

- One evidence file at the specified path.
- The artifact records the live probe + audit results.
- The final `DISPATCH:` line is either `PROCEED` or
  `BLOCKED — <reason>`.
- No real station codes anywhere in the artifact or the
  orchestrator report.
- No production code, no tests, no plan edits.
- Phase 1 dispatch decision is operationally enforceable from
  the artifact's last line.

If you write `DISPATCH: BLOCKED`, the orchestrator will read the
reason, possibly run remediation (e.g. operator backfills daily
runoff), and re-dispatch P0b. **Do not patch the gap in this
phase.** P0b is a verification gate, not a fix.

--- END PROMPT ---
