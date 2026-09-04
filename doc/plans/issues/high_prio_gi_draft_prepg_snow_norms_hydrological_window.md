# PREPG-022 — Snow norm/stat records are calendar-year shaped, so the dashboard's hydrological display window loses its bands after 31 December

**Module:** `preprocessing_gateway`
**Priority:** High
**Status:** Draft
**Found:** 2026-09-03/04, kghm local stack (`localhost:8000`, `sapphire-preprocessing-db`)
**Blocked by:** P-061 (yearly snow-norm task runs the wrong image)
**Related:** PREPG-007 (superseded Change A), PREPG-020 (preservation-read abort), PREPG-021
(upstream HS value hole), FD snow display window (`apps/forecast_dashboard/src/snow_window.py`)

---

## Problem

On a deployment with a hydrological snow display window
(`ieasyhydroforecast_SNOW_DISPLAY_START_MMDD=09-01`, kghm), the Predictors tab
draws SWE / Snow height / Snowmelt-runoff with **`mean`, `norm`, `previous`,
`min`/`max` and the `q05…q95` percentile bands present only from 1 September to
31 December**, then blank for the remaining eight months of the plotted season.

This is not a dashboard defect. `plot_daily_snow_data` and `_get_snow_single`
both key on `snow_display_window(9, 1, ref)` and fetch exactly the window they
plot; they render faithfully what the API returns. The rows are not there.

### Observed (local kghm DB, 2026-09-04)

Display window for `ref_date = 2026-09-04` is **2026-09-01 … 2027-08-31**.

```
snow table, all three snow_types:
  mean / norm / min / max / q05..q95 / previous : non-null only 2026-01-01 … 2026-12-31
  overall max(date)                              : 2026-12-31   (zero rows in 2027)
```

Per-month for one representative station, `snow_type='SWE'`, spanning two seasons
(station code omitted — codes are operationally sensitive, see CLAUDE.md):

| months | rows | `value` | `mean` / `previous` / `norm` |
|---|---|---|---|
| 2025-09 … 2025-12 | 104 | 104 | **0** |
| 2026-01 … 2026-08 | 243 | 226 | 243 |
| 2026-09 … 2026-12 | 122 | **0** | 122 |
| 2027-01 … 2027-08 | **0** | 0 | 0 |

The same defect, mirrored: last season the bands were missing for the *first*
four months of the window, this season for the *last* eight.

---

## Root cause

`recalculate_snow_norms.py` writes one **calendar year** of records:

```python
# apps/preprocessing_gateway/recalculate_snow_norms.py:159-161
is_leap = dg_utils.is_leap_year(year)
n_days = 366 if is_leap else 365
date_range = pd.date_range(start=f"{year}-01-01", periods=n_days, freq="D")
```

`year` defaults to `date.today().year` (`recalculate_snow_norms.py:405`), and the
job is scheduled once a year on **31 August** (`doc/deployment.md:1008`,
`0 2 31 8 *`; rationale recorded in `bin/yearly_snow_norm_recalculation.sh:7-10`
— end of the snow year, before the new accumulation season, owner decision
2026-08-19).

The cron line invokes `bin/run_periodic_maintenance.sh snow_norms`, which routes
through Luigi to `YearlySnowNormRecalculation` (`apps/pipeline/pipeline_docker.py:2011-2039`).
`bin/yearly_snow_norm_recalculation.sh` is the **legacy** wrapper (`bin/README.md:52`,
`doc/prod/update_deployment_checklist.md:860`) and is not what the schedule runs.

A hydrological display window always straddles two calendar years. The 31-Aug-N
run writes `N-01-01 … N-12-31`. The window that opens the next day runs
`N-09-01 … N+1-08-31`. The intersection is `N-09-01 … N-12-31` — four months.
The `N+1-01-01 … N+1-08-31` half is written only by the 31-Aug-**N+1** run,
i.e. *after the season it belongs to has already ended*.

So under the current cadence **the Jan→Aug portion of every hydrological season
is permanently bandless**, recurring every 1 January.

`doc/plans/snow_visualization_population_design.md` predicted this
("recurring every January"). Its Change A — move the job to 1 January — was
superseded on 2026-08-19 because it would shift the climatology mid-season.
That decision was correct and stands; the gap was simply left in place, because
**when the climatology is computed** and **which dates receive records** were
treated as one knob. They are separable: the 31-August run can keep computing
climatology at the end of the snow year and additionally emit the records for
the season that starts the next day.

### Blocking dependency — the scheduled path cannot currently reach this script

`YearlySnowNormRecalculation` runs `command=["uv", "run", "recalculate_snow_norms.py"]`
in `image_name="sapphire-pipeline"` (`apps/pipeline/pipeline_docker.py:2032-2037`).
That image is built from `apps/pipeline/Dockerfile`, which copies only
`apps/iEasyHydroForecast` (`:20`) and `apps/pipeline` (`:23`) — **not**
`apps/preprocessing_gateway`, where `recalculate_snow_norms.py` lives — and
`_standard_maintenance_volumes()` (`pipeline_docker.py:1668`) mounts config and
intermediate data only, no source. The script is therefore not present in the
container.

This is a pre-existing defect already recorded under P-007
(`doc/plans/module_issues.md:76`, "needs its own one-line correction"),
re-verified against current source on 2026-09-04. It is **not** caused by this
issue, but it gates it: P1–P4 can be implemented, tested and verified locally
(`apps/run_locally.sh:1076` runs the script directly via `run_in_venv`, bypassing
Docker and Luigi) and still not reach a server through the documented 31-August
schedule. Split out as **P-061** and this issue is blocked on it — see below.

### Two secondary observations (not this issue's scope)

- **`value`/`current` empty for the whole window.** The local snow sync last ran
  2026-08-21 (`apps/preprocessing_gateway/logs/log.2026-08-21`: 630 records =
  63 codes × Aug 20–29, including SnowMapper's forward projection), so the last
  non-null `value` is 2026-08-29 — before the window opens. That is the
  PREPG-007 self-heal path (maintenance mode, already widened to 365 d in
  `dg_utils.py`), not this bug. Note that even on a healthy daily server, a
  window that opened 3 days ago yields 3 points on a 365-day axis.
- **`previous` blank 2026-09-07 … 2026-09-24.** `previous` is calendar-date
  aligned to year−1 and the 2025 SWE/RoF series is missing those 18 dates
  entirely (`SWE` and `ROF`: 18 absent dates in 2025; `HS`: 61, the PREPG-021
  hole). Upstream data gap, not a recalc defect. **Distinct from PREPG-021**,
  which is *rows present with NULL `value`*; here the rows are absent altogether.

---

## Fix

Extend the written date range from *one calendar year* to *the calendar year
plus the remainder of the hydrological season that starts in it*, and make the
`previous` lookup per-date rather than one fixed prior year.

Chosen shape (**Option C**, superset — see Alternatives):

```
start = {year}-01-01
end   = {year}-12-31                              when SNOW_DISPLAY_START_MMDD == 01-01
      = ({year}+1)-<MM>-<DD> minus one day        otherwise
```

For `09-01` and `year=2026`: **2026-01-01 … 2027-08-31** (608 days), instead of
2026-01-01 … 2026-12-31 (365 days).

Why a superset rather than the hydrological span alone: the range stays
Jan-1-anchored, so it remains a strict extension of today's behaviour. Nothing
that is currently written stops being written, the tail of the *outgoing* window
(Jan–Aug N) is still covered on a first-ever run, and the change cannot regress
a deployment that has never run the job before.

### Correctness constraints the implementation must respect

1. **The preservation read must cover the full extended range.** Today the
   "existing target-year records" read is hardcoded to `{year}-01-01 …
   {year}-12-31` (`recalculate_snow_norms.py:197-198`). If the written range
   grows but the read does not, every date past 31 December is written with
   `value`, `current` and `value1..value14` **nulled** — silently destroying the
   daily sync's data on a re-run. This is the single highest-risk line in the
   change. It must move to the same `start`/`end` bounds as `date_range`, and
   the PREPG-020 abort-on-read-failure behaviour must be preserved verbatim.

2. **`previous` must align to *the date's own* prior year, not to `year - 1`.**
   `prior_year = year - 1` (`:225`) and `_date(year, dt.month, dt.day)` (`:281`)
   both assume every written date lies in `year`. With the extended range, a
   date in `year+1` must look up `year`. The prior-year read therefore becomes
   two reads (`year-1` and `year`) or one read over `{year-1}-01-01 …
   {year}-12-31`, and the lookup key must be derived from `dt.year - 1`. The
   existing Feb-29 `ValueError` guard must survive.

3. **The range stops at the season end; it does not run to 31 Dec `year+1`.**
   For `09-01`/`year=2026` the last written date is 2027-08-31 — Sep–Dec 2027
   is *not* written by this run and must not be. Those dates are written a year
   later by the `year=2027` run, whose own `{year}-01-01` anchor covers them and
   whose `previous` source (Sep–Dec 2026) exists by then. Do not pre-write
   placeholder rows past the season end to "reserve" them.

4. **Leap-year day-of-year drift is pre-existing and out of scope.** `doy` 60 is
   29 Feb in a leap year and 1 Mar otherwise, so norm/stat lookups shift a day
   across a leap boundary. The extended range makes this visible across *two*
   years instead of one but does not introduce it. Do not "fix" it here — note
   it and open a separate draft if it matters.

5. **Idempotency.** The write is an upsert; re-running must be safe and must not
   change `value`/`current`/bands. The following year's run intentionally
   rewrites the overlapping Sep–Dec rows with a recomputed climatology.

### Alternatives considered

| Option | Range written | Verdict |
|---|---|---|
| A | Always two calendar years (`N` and `N+1`) | Works, but writes ~120 days nobody displays and needs the same read/`previous` fixes anyway. No simpler than C. |
| B | Hydrological span only (`N-09-01 … N+1-08-31`) | Smallest write, but on a first-ever run leaves Jan–Aug `N` — the still-displayed tail of the outgoing window — unwritten. Regression risk on fresh deployments. |
| **C** | `N-01-01 … end of season starting in N` | **Chosen.** Strict superset of current behaviour, one run covers whichever window the dashboard asks for. |
| D | Move cron to 1 January | Rejected 2026-08-19 (shifts climatology mid-season). Not revisited. |

---

## Phases

### P1 — Extend the written range and fix the `previous` alignment

**Goal:** `recalculate_snow_norms.py` writes the full hydrological span and
computes `previous` correctly for dates outside `year`.

**Files (only these):**
- `apps/preprocessing_gateway/recalculate_snow_norms.py`

**Depends on:** —
**Agents:** 1 (worktree isolation — this touches a write path that can null a
year of operational data)

**Scope:**
- Add a module-level helper that resolves the write range, e.g.
  `_snow_record_range(year: int, start_month: int, start_day: int) -> tuple[pd.Timestamp, pd.Timestamp]`,
  implementing the table under *Fix*. Return `({year}-01-01, {year}-12-31)` when
  `(start_month, start_day) == (1, 1)`.
- Read `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD` **in `main()` only**, parse
  it with the same tolerant `MM-DD` semantics and `(1, 1)` fallback the
  dashboard uses (`apps/forecast_dashboard/dashboard/config.py:45-51` — invalid
  or absent ⇒ `01-01`, and 02-29 must be rejected), and pass the parsed
  month/day into `recalculate_norms()` as new keyword arguments defaulting to
  `1, 1`. Do **not** read env vars inside `_recalculate_norms_impl`.
- Replace the `is_leap_year`/`periods=n_days` construction at `:159-161` with
  `pd.date_range(start, end, freq="D")` from the helper.
- Move the target-year preservation read (`:197-198`) onto the same bounds.
- Replace the single `prior_year = year - 1` read (`:225-227`) with a read
  covering `{start.year - 1}-01-01 … {end.year - 1}-12-31`, and key the
  `previous` lookup off `dt.year - 1` (`:281-283`), keeping the Feb-29
  `ValueError` guard.
- Log the resolved range once per run, and log how many records were written
  with a null `previous`.

**Do NOT** change any existing function signature other than adding the two new
defaulted keyword arguments — which are permitted on **both** `recalculate_norms()`
and `_recalculate_norms_impl()`, since the impl builds the range. Do not change
control flow, the PREPG-020
`SnowPreservationReadError` raise sites and messages, the per-station
write-failure isolation, `_json_safe`, or `_parse_snow_vars`.

**Acceptance criteria:**
- With `SNOW_DISPLAY_START_MMDD=09-01` and `year=2026`, the record set spans
  2026-01-01 … 2027-08-31 inclusive (608 records per code/type).
- With the var unset or `01-01`, the record set is byte-identical to today's for
  the same inputs (365/366 records, same fields).
- For a date in 2027, `previous` equals the 2026 same-calendar-date `value`.
- For a date in 2026, `previous` equals the 2025 same-calendar-date `value`
  (unchanged from today).
- A `read_snow` failure on either the target-range or the prior-range read still
  raises `SnowPreservationReadError` and writes nothing for that code/type.
- Stored `value`/`current`/`value1..14` are preserved across the **whole**
  extended range, including dates past 31 December.

### P2 — Tests

**Goal:** the contract above is pinned, including the null-out regression.

**Files (only these):**
- `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py`

**Depends on:** P1
**Agents:** 1

**Scope — add, do not rewrite existing tests:**
- `_snow_record_range` unit tests: `(1,1)` ⇒ calendar year; `(9,1)` ⇒
  `N-01-01 … N+1-08-31`; `(10,1)` ⇒ `N-01-01 … N+1-09-30`; leap `year` and leap
  `year+1` both produce the right day count; invalid/absent `MM-DD` ⇒ `(1,1)`.
- **Regression (the important one):** target-range preservation read must cover
  dates past 31 December. Stub `read_snow` to return stored `value`/`current`/
  `value3` for `2027-03-15`; assert the written record for that date keeps them.
  A mutation that leaves the read at `{year}-12-31` must fail this test.
- `previous` for a `year+1` date resolves against `year`, not `year-1`.
- `previous` is `None`, not an exception, for `2027-02-29`-shaped misses.
- The written set **ends** at the season boundary: for `(9,1)`/`year=2026` no
  record exists for `2027-09-01` or later.
- Backwards compatibility: existing calendar-year tests
  (`test_happy_path_norms_written_to_api`, `test_leap_year_includes_day_366`,
  `test_record_builder_previous_uses_calendar_date_alignment`, …) pass
  unmodified — they call `recalculate_norms` without the new kwargs.
- PREPG-020: read failure on the widened prior-range read still aborts.

**Acceptance criteria:**
`SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` — zero fail,
zero unexpected skip. Then the full `SAPPHIRE_TEST_ENV=True bash run_tests.sh`.

### P3 — Runner + documentation

**Goal:** an operator can tell what the job now writes and how to heal a season.

**Files (only these):**
- `apps/pipeline/pipeline_docker.py` — `YearlySnowNormRecalculation` docstring
  only (this is the task the schedule actually runs). **No** behaviour change.
- `bin/yearly_snow_norm_recalculation.sh` (legacy wrapper; docstring/banner only
  — no cadence change; 31 August stands)
- `bin/README.md`
- `doc/deployment.md` (the cron block at :1008 — comment only)
- `doc/prod/update_deployment_checklist.md`
- `doc/plans/snow_visualization_population_design.md` (append a note that the
  superseded Change A's *symptom* is addressed here without a cadence change)
- `doc/plans/module_issues.md` (register PREPG-022)

**Depends on:** P1
**Agents:** 1 (may run in parallel with P2)

**Scope:** state that the 31-August run writes from 1 January of the target year
through the end of the hydrological season starting that year; that the range
depends on `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD`; and document the
one-time heal for an existing deployment whose next season is already open:

```bash
ieasyhydroforecast_SNOW_RECALC_YEAR=<current year> \
  bash bin/run_periodic_maintenance.sh snow_norms <env_file>
```

**Do NOT** change the cron schedule, the `run_periodic_maintenance.sh` task
list, or any Docker/image reference.

### P4 — Verify on the local stack

**Goal:** evidence, not a claim.

**Depends on:** P1, P2
**Agents:** 0 (orchestrator runs it)

1. `ieasyhydroforecast_SNOW_RECALC_YEAR=2026` recalc against `localhost:8000`.
2. Re-run the per-month coverage query for a representative station, `snow_type='SWE'`
   over `2026-09-01 … 2027-08-31`; every month must show non-null `mean`,
   `norm`, `min`, `max`, `q05..q95`.
3. Confirm `value`/`current` for `2026-01-01 … 2026-08-29` are **unchanged**
   (row-level diff against a pre-run snapshot) — the null-out regression check
   against real data.
4. Open the dashboard Predictors tab and confirm the bands span the full axis.

---

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P1", "P2"], "parallel_agents": 0 }
  }
}
```

---

## Blocked on P-061 (owner decision 2026-09-04: split)

The image defect above is filed separately as **P-061**
(`high_prio_gi_draft_p_snow_norm_task_wrong_image.md`) — a one-line swap of
`YearlySnowNormRecalculation` from `sapphire-pipeline` to `sapphire-prepgateway`.
It is a different bug: the yearly job is unreachable on the servers today
regardless of the date-range defect, so it stands on its own merits and is
reviewed on its own.

This issue's P1–P3 may be implemented in parallel with P-061 — they touch
disjoint files — but **P-022 is not deployable until P-061 lands**. P4 verifies
the logic locally only and must not be reported as "verified in production"
before then.

## Out of scope

- The cron cadence (31 August stands, owner decision 2026-08-19).
- `value`/`current` population — PREPG-007's maintenance self-heal.
- The 2025 absent-date holes (SWE/RoF 2025-09-07…09-24; HS 61 dates) — upstream,
  adjacent to PREPG-021 but a different failure mode (absent rows vs NULL value).
- Leap-year day-of-year drift in the norm/stat lookup (pre-existing).
- Any `sapphire/services/` change — the schema and `/snow/` endpoint already
  expose every field.

## Follow-up worth opening separately

Nothing currently detects "the dashboard's display window has no band rows". A
`validate_pipeline` check that asserts non-null `mean`/`norm` coverage over
`snow_display_window(...)` would have caught this on 1 January rather than in
September. Draft as its own issue if wanted.
