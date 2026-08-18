# PP-045 — Missed boundary-day operational run leaves short-term per-model period gap that maintenance cannot heal

**Status**: Review — Option B **merged to trunk** via PR #425 (`cd97db57`,
2026-07-23); commits `cce5922a` + `62bbba65`. Secondary decade-EM anomaly filed
as PP-048. See "Verification", "Decision & Workplan", and the 2026-08-17
re-assessment at the end of this file for what still blocks Complete.
*(Header corrected 2026-08-17: the previous text said "shipped on branch
`fix_postprocessing_boundary_gap`, NOT pushed" — stale; the branch is gone and
`git merge-base --is-ancestor` confirms both commits are on `maxat_sapphire_2`.)*
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `postprocessing`, `data-integrity`, `operational`

---

## Summary

Short-term per-model period forecasts (PENTAD/DECADE rows in the `forecasts`
table for models like TSMIXER/TIDE/TFT) are the only short-term product written on
the **daily/boundary-day cadence** by the operational code path alone, and only on
hydrological boundary days. If a boundary-day operational run is missed, nothing on
that cadence recreates those period rows: operational on a non-boundary day is gated
out, and maintenance cannot discover a date that has no `combined` rows at all.

**Corrected 2026-08-17 — operational is not the only writer, nor the only scheduled
one.** At least three entrypoints write per-model period rows.
`recalculate_skill_metrics.py` re-saves them as a side effect of a skill
recalculation, and it **is itself scheduled** — yearly, 01:00 UTC on 31 December
(`bin/run_periodic_maintenance.sh skill_recalc`, dispatched to
`YearlySkillRecalculation`) — so the distinction is cadence, not "scheduled versus
manual". `backfill_period_forecasts.py` is the recovery tool this issue delivered.
Consequently *"a period row exists for date D, therefore operational ran on D"* is
never a safe inference. What each entrypoint can heal, and what stops it, is a
per-entrypoint question — see "Net effect" below, §A6 and §H. Superseded claims are
recorded in the Corrections log.

## Context

The `postprocessing_forecasts` app has **six top-level production scripts** with a
`__main__` block: short- and long-term operational, short- and long-term
maintenance, `recalculate_skill_metrics.py`, and `backfill_period_forecasts.py`.
The two that drive the routine short-term cycle, and the two this issue is
principally about, are:

- **Operational** (`postprocessing_operational.py`) — reads DAY forecasts,
  aggregates them into pentad/decad period rows, creates ensembles, writes to
  the API. Runs from cron on operational days.
- **Maintenance** (`postprocessing_maintenance.py`) — a gap-filler intended to
  backfill *ensemble* rows (EM/NE) that operational missed. Runs from cron
  frequently.

**Do not read "entry points" as "writers of period rows"** — they are different
sets, and conflating them is what produced this issue's original wrong premise.
§A6 has the writer inventory.

DAY→period aggregation lives in `_normalize_ml_forecasts`
(`src/data_reader.py:2356-2504`) and is intact (verified; PP-023/PP-031 added
the period-aware and boundary filters). The write path is
`file_writer.save_forecast_data` → `api_writer._write_combined_forecast_to_api`.

Observed on a local dev DB (org = Tajik, placeholder code `19999`): per-model
PENTAD rows frozen at target 2026-07-05, DECADE at 2026-06-30, while DAY
forecasts are current (issued ~2026-07-17). The DAY inputs needed to build the
missing 2026-07-10 / 2026-07-15 pentad and 2026-07-10 decad periods exist.
Re-running in maintenance mode (`SAPPHIRE_PREDICTION_MODE=BOTH`) exits cleanly
but advances nothing.

## Problem

Two independent boundary gates, both keyed on the same predicate
(pentad boundary = day ∈ {5,10,15,20,25,last-of-month}; decad boundary =
day ∈ {10,20,last-of-month}):

1. **Entry gate** — `postprocessing_operational.py:214-232`: on a non-boundary
   day, `_run_short_term_postprocessing` is never called for that horizon, so
   nothing is written. Predicates: `is_pentad_boundary`/`is_decad_boundary`
   at `postprocessing_operational.py:54-63`.

2. **Maintenance date universe** — `postprocessing_maintenance.py:181-197`
   builds its gap-detection universe **only from existing `combined` rows**
   (`gap_detector.detect_missing_ensembles`, universe from `recent_combined`
   at `src/gap_detector.py:88-100`). `detect_missing_ensembles` *accepts* a
   `modelled_forecasts` parameter designed to expand that universe
   (`src/gap_detector.py:21,36-39,92-95`), **but maintenance never passes it**
   (`postprocessing_maintenance.py:192-197`). A boundary date with zero
   `combined` rows is therefore never discovered and never filled. Maintenance
   *does* re-aggregate (scoped read `postprocessing_maintenance.py:263`; NE
   creation `:276-277`) and can refresh stale individual rows (`:300-322`) and
   create NE-gap rows (`:323-346`), but only for dates already present in the
   combined universe (`:256`). Crucially, its write set (`refresh_parts`,
   `:294-371`) is built **only** from stale-matched individual rows, NE gaps, and
   EM output — it never emits fresh individual per-model rows for a
   newly-discovered date. So maintenance cannot introduce a brand-new
   zero-combined boundary date, and could not recreate the missing per-model
   rows even if that date were made discoverable.

**Net effect (verified, and corrected vs. the original diagnosis):**

- Between two boundary-day operational runs, **nothing on the daily/boundary-day
  cadence heals a missed period** (operational on a non-boundary day = entry gate
  skip; maintenance any
  day = universe excludes the missing date). **Corrected 2026-08-17** — the earlier
  absolute is superseded (see the Corrections log). Off that cadence, a
  `recalculate_skill_metrics.py` run (itself scheduled yearly on 31 December), a
  manual `SAPPHIRE_FORECAST_DATE` operational run, and the PP-045 backfill CLI can
  each emit a fresh per-model row. What each can and cannot reach differs;
  **maintenance alone cannot emit a fresh per-model row at all.** The precondition
  and the per-entrypoint matrix are in §H.
- The gap is **self-healing on the next same-horizon boundary-day operational
  run within the same calendar year.** Operational reads the *whole current
  year* (`postprocessing_operational.py:123-128`; DAY fetch spans
  `{year}-01-01..{year}-12-31`, `src/data_reader.py:2191-2196`), re-aggregates
  all boundary issue dates present, and the pre-write dedup keeps **one row per
  `(code, period_in_year, model_short)`** — not a single global latest
  (`src/file_writer.py:120-122`). So the 2026-07-20 boundary run would re-write
  the missed 07-10/07-15 pentad and 07-10 decad periods, **provided** (a) the DAY
  archive still holds those issue dates, and (b) ML reading is enabled/configured
  (`ieasyhydroforecast_run_ML_models=true` and a non-empty
  `ieasyhydroforecast_available_ML_models`, `src/data_reader.py:2646-2662`) and
  the API/DAY source is reachable (`:2172-2179`). If DAY retention is shorter than
  the gap, or ML is disabled/unavailable, the within-year self-heal does not
  fire either. **Proviso (a) is the one to check first** — **§C's** C1–C6 table
  enumerates the ways the merged archive fails to yield a usable input row.
  **INFERRED, not verified:** the observed field cases *look like* exactly that
  (§H), but §B is REPORTED and no live-data check has been run — that is §E's
  probe.
- The gap is **permanent across a calendar-year boundary for the *operational*
  path**: operational passes `start_year=end_year=today.year`, so it never
  re-reads a prior year, and maintenance cannot seed a zero-combined date.
  **Corrected 2026-08-17 — this is not unconditional, and operational's year
  bound is not the only mechanism.** `recalculate_skill_metrics.py` *does* read
  every year, but its saved payload then passes the yearless dedup and two-year
  filter (PP-046), so a `period_in_year` also present in a later year collapses
  to the later row; a prior-year period *absent* from later years can survive and
  be written. Cross-year recovery therefore has two distinct limiters, not one —
  see §H.

**Current recovery options (all manual / out-of-band).** Updated 2026-08-17: the
supported tool is now the first entry; the original two are retained as fallbacks.
Their caveats are unchanged **except** the raw-SQL script's EM claim, corrected
below and logged.

- **`apps/postprocessing_forecasts/backfill_period_forecasts.py` (PP-045, merged
  PR #425)** — re-aggregates through the existing operational aggregation + save
  path, one calendar year per internal call, API-only by default. **Only the
  *years* of `--start-date`/`--end-date` are used:** every selected year is
  reprocessed in full for every configured station, so a narrow date range does not
  narrow the work. This is the supported recovery route. **It can only re-aggregate
  inputs that already exist**: if the merged archive yields nothing for the
  affected issue dates the run exits 0 having written nothing new, and the fix is
  upstream (see §H).
- A manual operational run with `SAPPHIRE_FORECAST_DATE=<missed boundary date>`
  (override honored at `postprocessing_operational.py:200-212`). **Side effect:**
  this run rewrites that year's `simulated` and `simulated_latest` combined CSVs
  from scratch (`src/file_writer.py:193-218`) — it does not merge with current
  CSV state. Warn any operator/consumer that relies on those CSV artifacts.
- The un-wired standalone script
  `apps/machine_learning/reaggregate_day_to_periods.py` — but it bypasses the
  app entirely (direct `docker exec psql` + raw `INSERT … ON CONFLICT`,
  `:277-299`), writes individual + NEURAL_ENSEMBLE rows, sets `horizon_value=0`,
  and skips the `api_writer` null-drop / dedup guards. It derives no EM of its own,
  but its SQL does not exclude source EM rows, so it cannot be relied on never to
  write EM (§H). *Corrected 2026-08-17 — see the Corrections log.* Not a like-for-like substitute; no **executable**
  wiring anywhere in the repo (referenced only in plan/archive docs).

## Desired Outcome

A missed boundary day does not silently strand short-term per-model period rows.
**Corrected 2026-08-17** — the superseded wording is in the Corrections log. That
decision is closed — the owner chose Option B on
2026-07-17 and it shipped in PR #425 on 2026-07-23. The A/B/C analysis below is
retained as the historical record of how the choice was made, not as a live
question. "Done" means: the reproduction in Testing no longer strands the missed
periods under the shipped recovery mechanism, and the behavior is documented —
the documentation half is still outstanding, see §G.

---

## Technical Analysis

### Key files

- `apps/postprocessing_forecasts/postprocessing_operational.py`
  - `:54-63` — boundary predicates (`calendar.monthrange` last-of-month; no
    off-by-one).
  - `:123-128` — whole-current-year read (`start_year=end_year=today.year`).
  - `:142-148` — EM creation skipped when skill metrics empty.
  - `:200-212` — `SAPPHIRE_FORECAST_DATE` override parsed into `today`.
  - `:214-232` — entry gate; only boundary days call short-term postprocessing.
- `apps/postprocessing_forecasts/postprocessing_maintenance.py`
  - `:181-197` — combined-only universe; `modelled_forecasts` **not passed**.
  - `:256,263` — scoped modelled read for `affected_dates` only.
  - `:348-353` — EM skipped when skill metrics empty; individual/NE refreshed.
- `apps/postprocessing_forecasts/src/gap_detector.py:21,36-39,88-100` —
  `detect_missing_ensembles`; universe from combined, optional
  `modelled_forecasts` expansion arg.
- `apps/postprocessing_forecasts/src/data_reader.py`
  - `:2191-2196` — DAY fetch year span.
  - `:2356-2504` — `_normalize_ml_forecasts` (boundary drop `:2386-2404`,
    period filter `:2407-2432`, aggregation `:2441-2463`).
  - `:432-457` — `read_skill_metrics` returns empty df (not error) when API+CSV
    both lack rows.
- `apps/postprocessing_forecasts/src/file_writer.py:120-129` — per-period
  `get_latest_forecasts` dedup (the within-year backfill enabler).
- `apps/postprocessing_forecasts/src/api_writer.py:373-397` — null-drop
  (`dropna(subset=["forecasted_discharge"])`) + cardinality dedup.
- `apps/machine_learning/reaggregate_day_to_periods.py` — un-wired DB-wide
  reaggregation script (direct SQL; emits individual **and** NE rows; bypasses app
  guards). *Corrected 2026-08-17 — see the Corrections log.*
- Prior art for an app-integrated backfill entrypoint:
  `apps/preprocessing_runoff/backfill_discharge_aggregation.py`.

### Latent defect found while mapping — FILED as PP-046

**Filed 2026-07-23 as PP-046**
(`doc/plans/issues/mid_prio_gi_draft_pp_get_latest_forecasts_yearless_key.md`);
no longer an open action here. Mechanism retained below because it is the
clearest statement of the defect anywhere, and §H depends on it.

`get_latest_forecasts` (`src/file_writer.py:118-129`) dedups on a **yearless**
key `(code, period_in_year, model_short)` and applies the `>= latest_year-1`
year filter *after* dedup. The "keep last two years" comment is therefore
misleading: the **same period-in-year across two years collapses to the later
row**. Not the primary cause of this gap, but relevant to any "keep two years of
per-period rows" assumption. **Update 2026-08-17:** PP-046 frames this as a
risk "for any future multi-year caller" — but `recalculate_skill_metrics.py` is an
existing one, so the defect manifests today rather than latently (§H).

---

## Implementation Plan

> **HISTORICAL RECORD — NOT A LIVE DECISION.** Everything from here to the end of
> "Dependency graph" records how the remediation was chosen and built. The owner
> selected **Option B** on 2026-07-17; it shipped as
> `backfill_period_forecasts.py` in **PR #425** on 2026-07-23 and is on trunk.
> The "OPEN DECISION" and "Recommendation (for the owner to weigh)" headings
> below are preserved verbatim as the audit trail for a merged PR — **they are
> not open questions.** For what remains, see §G.

### Approach — OPEN DECISION FOR THE HUMAN OWNER

The three options differ in how aggressively the system self-recovers. They are
**not** mutually exclusive (A is a safe floor under B or C). No option requires
`sapphire/services/` changes; all are apps-side.

**Option A — Ops/docs only (smallest).**
Document that a missed short-term boundary needs a manual operational re-run with
`SAPPHIRE_FORECAST_DATE=<boundary>`; no code change. Add a runbook entry and,
optionally, an operational alert when the latest per-model period lags the DAY
horizon by more than one boundary.
- Pros: zero code risk; matches the fact that the gap self-heals at the next
  boundary within-year.
- Cons: does **not** fix the genuinely-permanent cross-year gap; relies on an
  operator noticing and acting.

**Option B — Give maintenance (or a new backfill entrypoint) the ability to
catch up missed per-model aggregation.**
NOTE — this is **larger than it first appears.** Passing `modelled_forecasts`
into `detect_missing_ensembles` is **necessary but not sufficient**: even with
the zero-combined date made discoverable, maintenance's write set
(`refresh_parts`, `postprocessing_maintenance.py:294-371`) only emits
stale-matched individual rows, NE gaps, and EM — it **never writes fresh
individual per-model rows** for a newly-discovered date (`:300-346`). So Option B
must additionally introduce a write path that emits the individual per-model
rows for discovered gap dates (writing all of `modelled_filtered` for those
dates, while *not* double-writing already-present non-stale rows — the very case
`:294-296` currently guards against), and it must relax maintenance's
`combined.empty` early return (`:185-187`) so a fully-empty-combined date range
still reaches detection. The cleaner alternative is a **bounded, app-integrated
backfill entrypoint** (modelled on
`preprocessing_runoff/backfill_discharge_aggregation.py`) that reaggregates a
date window straight through the operational aggregation + `file_writer` /
`api_writer` path (not the raw-SQL script), reusing the code that already emits
per-model rows.
- Pros: heals the gap without waiting for the next boundary; closes the cross-
  year permanent case if the window can reach prior years.
- Cons: substantially larger than "pass an arg"; must not alter existing
  maintenance ensemble behavior or double-write non-stale rows; needs careful
  test coverage; expands maintenance's write responsibilities (touches
  PP-007/PP-024 territory — coordinate). A separate backfill entrypoint avoids
  perturbing the maintenance write set but adds a new operator-run tool.

**Option C — Make the operational run self-healing.**
On each operational run, detect recent boundaries since the last per-model period
row for each horizon and backfill any missed ones (bounded lookback), in addition
to today's boundary.
- Pros: no operator action; heals within-year gaps automatically on the very
  next run of any kind that hits the operational path.
- Cons: changes operational control flow (highest-risk path); needs a bound to
  avoid re-aggregating the whole year every run; still bounded by the current-
  year read unless combined with a wider read.

### Recommendation (for the owner to weigh, not a decision)

Given that the within-year gap already self-heals at the next boundary run, the
*marginal* value of B/C over A is: (1) closing the cross-year permanent gap, and
(2) removing the multi-day window where per-model periods lag. If the cross-year
gap is the real operational pain, **B** (backfill entrypoint) targets it most
directly with the least control-flow risk. If operators frequently miss
consecutive boundaries, **C** adds resilience but touches the riskiest path.
**A** alone is defensible only if missed boundaries are rare and always caught
within the same year.

### Files to Modify

See "Decision & Workplan (Option B)" below. No `sapphire/services/` files.

---

## Decision & Workplan (Option B — backfill entrypoint)

**Decision (2026-07-17):** Build a bounded, app-integrated backfill CLI that
re-aggregates a date range through the **existing** operational aggregation +
`file_writer`/`api_writer` path — so it emits per-model **and** EM/NE rows with
the existing null-drop/dedup guards, with no change to operational/maintenance
*runtime* control flow.

### Design — reuse the operational seam

`_run_short_term_postprocessing(config, today, errors, timing_stats_)`
(`postprocessing_operational.py:110-174`) already: reads a whole year of DAY data
(`:123-128`), re-aggregates **all** boundary issue dates via `_normalize_ml_forecasts`,
creates ensembles, and writes per-model + EM/NE rows through
`file_writer.save_forecast_data` (`:167`). `today` is used **only** to set
`start_year=end_year=today.year` in the read (`:126-127`) — it is not used
anywhere else in the function (verified). So the seam generalizes to an arbitrary
year with two additive optional params.

### CRITICAL constraint — process ONE YEAR AT A TIME

`get_latest_forecasts` dedups on the **yearless** key
`(code, period_in_year, model_short)` *before* the `year >= latest_year - 1`
filter (`src/file_writer.py:120-129`). A single multi-year read+save would
therefore collapse the same period-in-year across years to the **latest** year:
for any period present in the latest year, all prior years' rows for that period
are dropped and never reach the API. (Prior-year rows for periods *absent* from
the latest year can survive the `>= latest_year-1` filter — so the collapse is
not total — but the periods we most need to heal are exactly the ones that recur
every year, so relying on that is unsafe.) **The backfill MUST iterate per year**
(`start_year == end_year` on each call), ascending, so each year's
`get_latest_forecasts` yields that year's period rows and the API upsert (unique
key includes `date`) persists them. This per-year iteration is the mechanism that
heals the cross-year permanent gap.

### CLI granularity & idempotency notes

- **Whole-year granularity.** Aggregation re-does the **whole year** regardless of
  sub-year dates (it reads the full year and re-aggregates every boundary period).
  The CLI accepts `--start-date`/`--end-date` for operator convenience, but the
  effective granularity is **whole years spanned by the range**: every period in
  each touched year is recomputed and re-upserted. Document this.
- **Issue-date, not target-date.** The year read is bounded on **issue date**
  (`src/data_reader.py:2191-2196`), while a period's target = issue date + 1
  (`src/postprocessing_tools.py`). A period whose target starts on Jan 1 of year
  Y is produced by the **Dec 31 of year Y-1** issue date, which falls in year
  Y-1's read. So to heal a Jan-1-boundary period, the range must include the
  prior year. Document; the per-year loop naturally covers it when the range
  spans the year edge.
- **Idempotency is not uniform.** Per-model and NE rows recompute to the **same**
  values from the same DAY archive (safe to re-upsert). **EM is not byte-idempotent**:
  it is recomputed against the *current* skill metrics (read without year scope,
  `src/data_reader.py:401`; joined on period/code/model,
  `src/ensemble_calculator.py:115-128`), so re-upserting a historical EM reflects
  today's skill weights — consistent with operational behavior (operational also
  uses current skill), but not a byte-for-byte replay. State this in help text.
- **API-only write (avoids CSV staleness).** `save_forecast_data` overwrites the
  operational combined CSV, which normally holds only the current year. A backfill
  of a *prior* year would otherwise leave the CSV holding historical-only data —
  stale for any CSV-fallback consumer when the API is down. **The backfill writes
  API-only by default** (skips the combined-CSV overwrite); the DB/API is the
  source of truth and the CSV is regenerated by the next operational run. Provide
  an opt-in `--write-csv` if an operator explicitly wants the CSV refreshed.
- **Surface write failures.** `save_forecast_data` returns `None` even when the
  API write was swallowed (e.g. `SAPPHIRE_API_FAILURE_MODE=warn`,
  `apps/iEasyHydroForecast/forecast_library.py:88-110`, or
  `_write_combined_forecast_to_api(...)==False`). A backfill must **not** report
  silent success: run with fail-loud API mode and log a per-year written-row count
  from the actual API path so an operator can confirm rows landed. (A full
  post-write read-back verification, as in
  `preprocessing_runoff/backfill_discharge_aggregation.py`, is a nice-to-have;
  note it as a future enhancement if not built now.)
- **Run with the API reachable.** Observation CSV fallback ignores the year bounds
  (`src/data_reader.py:2553` area), so an API-disabled run could read unscoped CSV
  observations. The backfill is a DB-write tool; require/assume the API is up.

### WP1 — Locked tests (TDD, write first, expect red)

- **Goal**: encode the contract as failing tests before implementation.
- **Files**: `apps/postprocessing_forecasts/tests/test_backfill_period_forecasts.py` (new).
- **Depends on**: none. **Agents**: 1.
- **Tests** (all use placeholder code `"19999"`, fixed dates — no `date.today()`):
  - **T1 (aggregation regression)**: `_normalize_ml_forecasts` on a whole-year DAY
    frame that includes a boundary issue date "missed" by operational produces the
    per-model period row for that period (proves aggregation recreates the missed
    period given DAY data). Reuses the `data_reader._normalize_ml_forecasts` seam
    already covered by `tests/test_data_reader_ml_aggregation.py`.
  - **T2 (entrypoint year-range, additive param)**: with `data_reader.read_observed_and_modelled_data`
    and `file_writer.save_forecast_data` mocked,
    `_run_short_term_postprocessing(config, today, errors, ts, start_year=2025, end_year=2025)`
    calls the reader with `start_year=2025, end_year=2025` and calls
    `save_forecast_data`; and the **default** call (no `start_year`/`end_year`)
    still uses `today.year` (back-compat lock).
  - **T3 (dry-run)**: `dry_run=True` → `save_forecast_data` NOT called; `dry_run=False` → called.
  - **T4 (per-year iteration + ascending)**: backfill `main()` with
    `--start-date 2024-03-01 --end-date 2026-07-10 --horizon both` invokes
    `_run_short_term_postprocessing` once per (horizon, year) with
    `start_year == end_year == Y` for `Y in [2024, 2025, 2026]` ascending. Locks
    the yearless-key protection.
  - **T5 (CLI validation)**: end-date < start-date, or malformed date → non-zero
    exit / explicit error.
  - **T6 (horizon selection)**: `--horizon pentad` runs only PENTAD; `decad` only
    DECAD; `both` runs both.
  - **T7 (end-to-end persistence — do NOT over-mock)**: with a fake in-memory
    `api_writer._write_combined_forecast_to_api` (capturing client) but the
    **real** `file_writer.save_forecast_data` and real aggregation, drive a whole
    year whose DAY frame contains a "missed" boundary issue date and assert the
    corresponding per-model period row is actually present in the payload handed to
    the API. This is the true regression lock (T1–T4 mock the persistence path).
  - **T8 (write-csv default off)**: backfill run does NOT overwrite the combined
    CSV by default (assert `atomic_write_csv` not called / CSV unchanged);
    `--write-csv` re-enables it.
  - **T9 (write-failure surfaced)**: when the API write reports failure
    (fake returns `False` / raises under fail-loud), the backfill exits non-zero
    and does not log silent success.
- **Acceptance**: tests present, run **red** (symbols/params not yet present).

### WP2 — Implement

- **Goal**: make WP1 green with the minimal additive change.
- **Files**:
  - `apps/postprocessing_forecasts/src/file_writer.py` — **additive only**: append
    `write_csv: bool = True` to `save_forecast_data`; when `False`, skip the two
    `atomic_write_csv` calls (`:196`, `:209`) but keep the `get_latest_forecasts`
    + API write path. Default `True` preserves current behavior. **Do NOT change**
    the return value or error-handling semantics (operational relies on them). The
    backfill surfaces API-write failures instead by setting
    `SAPPHIRE_API_FAILURE_MODE=fail` (`forecast_library.py:110-113` re-raises), so
    write exceptions propagate to the backfill's try/except → non-zero exit. The
    non-exception `_write_combined_forecast_to_api(...)==False` swallow is a
    pre-existing app-wide limitation → note as residual risk, do not re-plumb.
  - `apps/postprocessing_forecasts/postprocessing_operational.py` — **additive
    only**: append optional kwargs `start_year=None, end_year=None, dry_run=False,
    write_csv=True` to `_run_short_term_postprocessing`; derive `start_year/end_year`
    from `today.year` when `None`; pass years + `write_csv` through; when
    `dry_run`, skip the save and log the period/model coverage that *would* be
    written. **No change** to the two existing call sites' behavior (`:222`,
    `:232`) or to any control flow.
  - `apps/postprocessing_forecasts/backfill_period_forecasts.py` — **new** CLI
    (`--start-date`, `--end-date`, `--horizon {pentad,decad,both}`, `--dry-run`,
    `--write-csv` [default off]). Loads env via `sl.load_environment()`, validates
    dates, iterates `Y in range(start.year, end.year+1)` ascending, calls
    `_run_short_term_postprocessing(config, dt.date(Y,1,1), errors, timing_stats, start_year=Y, end_year=Y, dry_run=..., write_csv=<flag>)`
    per selected horizon; runs with fail-loud API mode; logs per-year written-row
    count; aggregates errors; **non-zero exit on any error or swallowed API-write
    failure**. Model the argparse/env/exit-code shape on
    `apps/preprocessing_runoff/backfill_discharge_aggregation.py`.
- **Depends on**: WP1. **Agents**: 1 (already in a dedicated worktree).
- **Acceptance**: WP1 green; full `postprocessing_forecasts` suite green.
- **Escalation flag for the owner**: the API-only-by-default write (`--write-csv`
  opt-in) is a behavioral choice about the combined-CSV artifact, made to avoid
  cross-year CSV staleness. Called out here and in the final report so the owner
  can override to CSV+API if preferred.

### WP3 — Verify + review (maps to top-level Phase 6)

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero fail, zero
  unexpected skip.
- `verify` skill: drive the backfill on the local Tajik DB to heal the reproduced
  07-10/07-15 pentad + 07-10 decad gap end-to-end; confirm per-model + EM/NE rows
  land.
- Adversarial diff review (codex) of the final patch.
- Scrub sensitive data (placeholder `19999` only).
- Record kyg end-to-end as DEFERRED/blocked-on-infra.

### Dependency graph

```json
{
  "phases": {
    "WP1": { "depends_on": [], "parallel_agents": 1 },
    "WP2": { "depends_on": ["WP1"], "parallel_agents": 1 },
    "WP3": { "depends_on": ["WP2"], "parallel_agents": 1 }
  }
}
```

---

## Verification (2026-07-23) — implemented & reviewed

**Commits** (branch `fix_postprocessing_boundary_gap`, not pushed):
`cce5922a` (WP1 tests + WP2 impl) → `62bbba65` (review-round fixes).

**Delivered (apps-side only; no `sapphire/services/` change):**
- `src/file_writer.py`: `save_forecast_data(..., write_csv=True, require_api=False)`
  — additive; default path byte-identical. `write_csv=False` = API-only;
  `require_api=True` raises on API-unavailable or falsy API-write return.
- `postprocessing_operational.py`: `_run_short_term_postprocessing(...,
  start_year=None, end_year=None, dry_run=False, write_csv=True, require_api=False)`
  — additive; existing call sites unchanged; `dry_run` skips save AND the
  log-file write.
- `backfill_period_forecasts.py` (new): `main(argv)->int`; `--start-date/--end-date/
  --horizon/--dry-run/--write-csv`; per-year ascending; API-only by default;
  `SAPPHIRE_API_FAILURE_MODE=fail` + `require_api=True` so any API failure exits
  non-zero.

**Tests:** `test_backfill_period_forecasts.py` (23 tests incl. the yearless-collapse
regression G15, the three `require_api` failure-mode tests, and a COMPOSED
end-to-end test: real `_run_short_term_postprocessing` → real aggregation → real
`save_forecast_data` → captured API payload). Full suite: **1754 passed, 1 xfailed
(pre-existing), 0 failed, 0 unexpected skips**.

**Review:** independent codex adversarial diff review (4 Important findings, all
fixed) + codex WP1 test-quality review (gaps closed) + a confirm-fixes codex pass
(F1–F4 all CONFIRMED-FIXED) + a final holistic independent codex cross-check
(verdict: safe-to-merge-with-fixes; its required fixes — doc accuracy below +
the added `require_api`/composed tests — are applied).

**End-to-end (live Tajik dev DB, non-destructive `--dry-run`, exit 0):**
- PENTAD: **1746 pre-save candidate rows** — 39 distinct pentad_in_year × 6
  candidate models.
- DECAD: **744 pre-save candidate rows** — 19 distinct decad_in_year × 6
  candidate models.
- IMPORTANT (corrected by the final review): these counts are what the aggregation
  hands to `save_forecast_data` **before** `api_writer` processing — the 6 models
  are `LR + TFT + TiDE + TSMixer + NE + EM`, but **`api_writer` drops `LR` before
  the API write** (`src/api_writer.py:190`), and null-drop + unique-key dedup
  (`:373`) run after, so the **actual API payload is smaller (~5 models)**. The
  dry-run therefore confirms the backfill *reaches and re-aggregates* the missed
  July periods for both horizons (39 pentads to mid-July, 19 decads).

**End-to-end REAL WRITE + read-back (live Tajik dev DB, 2026-07-23) — persistence PROVEN:**
- BEFORE (frozen gap, matches symptom): PENTAD per-model max `2026-07-05` (all 5
  DB models TFT/TIDE/TSMIXER/ENSEMBLE_MEAN/NEURAL_ENSEMBLE); DECADE per-model max
  `2026-06-30` (DECADE `ENSEMBLE_MEAN` stuck at `2026-04-10`). Rows: PENTAD 86 587,
  DECADE 45 361.
- Ran the real backfill (`--horizon both`, API-only, `require_api=True`), exit 0.
- AFTER (read back from the DB): PENTAD per-model max `2026-07-15` — the missed
  **07-10 and 07-15** boundaries now present for **all 5 models**; DECADE per-model
  max `2026-07-10` — missed **07-10** present for all 5 models (DECADE
  `ENSEMBLE_MEAN` also advanced `04-10`→`07-10`, so decade skill was present this
  run). Rows: PENTAD 86 933 (+346), DECADE 45 534 (+173). (07-20 correctly absent
  — its boundary DAY targets aren't complete at 07-23.)
- IDEMPOTENCY: a second identical run exited 0 with **unchanged** counts
  (86 933 / 45 534) — confirms upsert, safe to re-run.
- This is the DB-confirmed proof the dry-run could not give: the LR-drop /
  null-drop / dedup happen inside `api_writer` on the real path and the correct
  per-model + ensemble rows land and read back. (The `api_writer` zero/partial-
  count honesty gap remains a latent concern → follow-up ticket, but did not
  manifest here — every expected period persisted.)

**Open items:**
- **DEFERRED / blocked-on-infra:** full Kyrgyz (`15xxx/16xxx`) short-term pipeline
  end-to-end verification (kyg server down). Fix NOT considered fully verified
  until kyg is exercised.
- **Owner decision flag:** API-only-by-default write (`--write-csv` opt-in) — a
  behavioral choice on the combined-CSV artifact; owner may override to CSV+API.
- **Residual risk (range edge):** an operator specifying a range starting exactly
  on Jan 1 will miss that period unless the prior calendar year is included
  (issue-date semantics, documented in the CLI docstring/help).
- **Residual risk (persistence proof):** `api_writer._write_combined_forecast_to_api`
  returns `True` even on a zero/partial server write (`src/api_writer.py:405` area),
  so `require_api=True` catches API-unavailable / exception / explicit-`False`
  failures but does **not** prove the server actually persisted the rows. Fully
  closing this needs a write-count check or post-write read-back — a shared-code
  change beyond this patch; **candidate follow-up ticket** (see below).
- **Separate tickets to file:** (1) secondary decade-EM skill-empty anomaly;
  (2) yearless-key `get_latest_forecasts` collapse (latent); (3) `api_writer`
  reports success on zero/partial writes (weakens any `require_api` guarantee).

---

## Testing

### Reproduction (deterministic, no live DB required)

The cleanest seam is `gap_detector.detect_missing_ensembles` (pure function, no
clock):

1. **Maintenance-cannot-heal proof** — Arrange `combined` with per-model + EM
   **and NE** rows for code `"19999"` up to a valid pentad boundary (e.g.
   `2026-07-05`) and **no** rows for `2026-07-10`. (Include NE in the existing
   rows: with `ensemble_models={"EM","NE"}` the detector reports EM *and* NE gaps
   independently per date — `src/gap_detector.py:112-129` — so omitting NE would
   surface spurious NE gaps for the already-present dates and muddy the
   assertion.) Act: call `detect_missing_ensembles(combined,
   ensemble_models={"EM","NE"}, horizon_type="pentad")` **without**
   `modelled_forecasts`. Assert: no result row has `date == 2026-07-10` (the
   missing boundary is invisible). Complement: call again **with**
   `modelled_forecasts` containing `(2026-07-10, "19999")` → `2026-07-10` now
   appears as an EM+NE gap — proving the date becomes *discoverable*, though (see
   Option B) that alone does not make maintenance write the per-model rows.

2. **Entry-gate skip** — Assert `is_pentad_boundary(date(2026,7,17)) is False`
   and `is_decad_boundary(date(2026,7,17)) is False`, so a run on 07-17 writes
   no short-term periods; the missed pentad boundaries between 07-05 and 07-17
   are 07-10 and 07-15, the missed decad boundary is 07-10.

3. **Self-heal (within-year)** — Assert `get_latest_forecasts` keeps one row per
   `(code, period_in_year, model_short)` so a whole-year re-aggregation writes
   every distinct period, not just today's (guards the claim that a later
   boundary run backfills earlier missed periods).

The chosen option's fix adds a locked regression test proving the missed period
is recovered by the chosen mechanism.

### Testing Commands

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh   # full affected scope
```

### Manual Verification (Tajik, local)

Reproduce the original gap, apply the fix, and drive the real flow (`verify`
skill) to confirm the missed 07-10/07-15 pentad and 07-10 decad periods are
recovered end-to-end.

---

## Documentation Impact

> **FROZEN historical snapshot as of 2026-07-23. Do not tick these boxes.** The
> single live checklist is **§G**; these items are tracked there, with the
> operator-facing runbook identified as the largest remaining gap.

- [ ] `apps/postprocessing_forecasts/README.md` — recovery procedure / entrypoint.
- [ ] `doc/data_flow_short_term.md` — if the maintenance/operational boundary
      behavior changes.
- [ ] `doc/prod/` runbook — the manual recovery command (Option A minimum).
- [ ] Claude memory — the self-heal-within-year vs permanent-across-year nuance.

---

## Secondary anomaly — FILED as PP-048

**Filed 2026-07-23 as PP-048**
(`doc/plans/issues/low_prio_gi_draft_pp_decade_ensemble_mean_freeze.md`); no
longer an open action here. Triage analysis retained as the historical record.

Tajik DECADE `ENSEMBLE_MEAN` sits far behind its own per-model decade
(04-10 vs 06-30). Mechanism (verified in code, contingent on a data state):
EM creation is gated on **non-empty skill metrics** in both paths
(`postprocessing_maintenance.py:348-353`, `postprocessing_operational.py:142-148`);
`read_skill_metrics` returns empty (not an error) when API+CSV both lack rows
(`src/data_reader.py:432-457`). If decad skill metrics are empty (consistent with
the `.bak_corrupted` decad skill CSVs noted in project memory) while pentad skill
is present, EM is skipped while individual/NE decad rows still advance — but the
advance comes from the **operational** boundary run, which saves individual/NE
even when skill is empty (`postprocessing_operational.py:135-168`), *not* from
maintenance (if only EM is missing and skill is empty, maintenance builds no
`refresh_parts` and writes nothing, `:348-369`). Net: decade EM freezes at the
last date skill metrics were present while per-model decade keeps advancing via
operational. This is a **different root cause**
(skill-metrics-empty gate, arguably intended) from the boundary gap and depends
on a data state not confirmable read-only. **Recommendation:** file as a separate
`gi_draft` after confirming decad `read_skill_metrics` returns empty on the dev
DB; do not bundle its fix here. Escalating the in-scope-vs-separate call to the
owner.

---

## Out of Scope

- Any `sapphire/services/` change (API contract). If the chosen option appears to
  need one, STOP and escalate.
- The yearless-key `get_latest_forecasts` collapse (latent; separate ticket).
- The secondary decade-EM anomaly (separate ticket, above).

## Dependencies

- Touches PP-007 (maintenance reads from API) / PP-024 (maintenance API writes)
  territory if Option B expands maintenance writes — coordinate.

## Acceptance Criteria

> **FROZEN historical snapshot as of 2026-07-23. Do not tick these boxes.** The
> single live checklist is **§G**. The one unchecked item below (the kyg
> end-to-end deferral) has been **migrated into §G**, which is where it is now
> tracked and resolved.

- [x] Reproduction test(s) above committed and passing.
- [x] Under the chosen option, the missed 07-10/07-15 pentad and 07-10 decad
      periods are recovered (unit/integration + manual end-to-end dry-run on
      Tajik: 39 pentads × 6 models, 19 decads × 6 models would be written).
- [x] No change to existing maintenance ensemble behavior or operational
      boundary-day behavior beyond the scoped fix (regression tests green;
      default `save_forecast_data`/`_run_short_term_postprocessing` paths
      byte-identical, confirmed by codex confirm-fixes pass).
- [x] `postprocessing_forecasts` suite green — 1749 passed, 1 xfailed
      (pre-existing), 0 failed, 0 unexpected skips. (Change is apps-side within
      one module; full-repo `run_tests.sh` not re-run — no cross-module impact.)
- [x] No real station codes or discharge values in code, tests, or docs
      (placeholder `19999` only; scanned each changed file).
- [ ] **DEFERRED — blocked on infra (Kyrgyz server down):** once the kyg server
      is back, run the full kyg (`15xxx/16xxx`) short-term pipeline end-to-end
      (preprocessing_runoff → linear_regression → gateway → machine_learning →
      postprocessing_forecasts) and confirm the fix heals kyg pentad/decad the
      same way. The fix is **not** considered fully verified until kyg is
      exercised.

---

## Confirmed on trunk 2026-08-17 (tjhm)

Re-assessment against `maxat_sapphire_2` @ `8e3fc1bc`. Every claim is tagged
**PROVEN** (direct code read and/or a green test in this repo), **INFERRED**
(consistent with the evidence, not established), or **REPORTED** (supplied to
this session, not re-verified here). This section was itself put through an
out-of-loop adversarial review (`codex exec`, read-only, fresh context) before
being committed; that reviewer refuted several claims in the first draft, and the
corrections are folded in below — including one that corrects this issue's own
original premise (§A6).

### A. Code re-verification — PROVEN

1. **The fix is on trunk.** `apps/postprocessing_forecasts/backfill_period_forecasts.py`
   and `tests/test_backfill_period_forecasts.py` exist at `8e3fc1bc`;
   `git merge-base --is-ancestor` succeeds for both `cce5922a` and `62bbba65`;
   the file first appears on the first-parent history at `cd97db57` "Merge pull
   request #425". The branch is deleted — treat as merged.

2. **The yearless-dedup rationale for one-year-at-a-time is still accurate.**
   `file_writer.get_latest_forecasts` sorts by `date` descending and calls
   `drop_duplicates(subset=["code", <period_col>, "model_short"], keep="first")`
   (`src/file_writer.py:118`) **before** applying `year >= latest_year - 1`
   (`:124`). The key carries no year, so the same period-in-year across two years
   collapses to the later row. The load-bearing detail: `save_forecast_data`
   hands **`simulated_latest`** — the deduped frame — to
   `api_writer._write_combined_forecast_to_api` (`:244`), so the collapse reaches
   the API write and is not merely a CSV artifact. Locked by
   `TestGetLatestForecastsCollapsesAcrossYears::test_same_period_two_years_collapses_to_later_year`.
   The CLI docstring's "ONE YEAR AT A TIME" section is correct as written. (The
   underlying defect remains open as PP-046.)

3. **Maintenance still cannot discover a zero-`combined` date.**
   `postprocessing_maintenance.py:185` early-returns on empty combined; `:192`
   calls `gap_detector.detect_missing_ensembles(...)` **without**
   `modelled_forecasts`, which the detector still accepts and still uses to widen
   its universe (`src/gap_detector.py:88`). Unchanged from the original diagnosis.

4. **Test suite green on trunk** — *this item is REPORTED, not PROVEN: the run
   happened in the 2026-08-17 session and no durable log is attached, so it is not
   reproducible from this file. Re-run to confirm.*
   `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` →
   **1832 passed, 1 xfailed, 0 failed, 0 skipped** (30.7 s).
   **PROVEN (static):** `test_backfill_period_forecasts.py` contains **23** test
   methods (an earlier draft of this section said 26 — a miscount that included
   `class` lines; 23 matches the 2026-07-23 record above).

5. **`validate_pipeline` structurally cannot see a stranded historical boundary
   day.** `run_tier1_short_term` computes
   `boundary = most_recent_pentad_boundary(forecast_date)`
   (`apps/validate_pipeline/validate_pipeline.py:389`) and queries every
   configured short-term model over `[boundary, forecast_date]` only (`:459`). On
   2026-08-17 that window is `[2026-08-15, 2026-08-17]`. **PROVEN:** the window
   never looks further back than the most recent boundary, so any earlier stranded
   day is outside it by construction. **INFERRED (not verified here):** that the
   validator therefore actually returned PASS on the reported tjhm data — that
   depends on DB contents this session did not query. Same family as INFRA-024 (a
   check that cannot fail) and INFRA-026 (expectations that do not match the
   product's schedule); neither currently names the "historical boundary day
   silently empty" case, so it should be added to one of them.

6. **CORRECTION to this issue's own premise: per-model PERIOD rows are written by
   at least three entrypoints, not "only the operational code path".** The
   Summary and Problem sections above say per-model period rows "are created
   **only** by the operational code path". That is wrong on trunk:
   - `postprocessing_operational.py` — the boundary-gated path (as described).
   - `recalculate_skill_metrics.py::_run_short_term_recalc` — calls
     `file_writer.save_forecast_data(config, modelled)` (`:233`) after
     re-aggregating, i.e. it writes per-model PERIOD rows as a side effect of a
     skill recalculation.
   - `backfill_period_forecasts.py` — PP-045's own CLI.
   Plus `apps/machine_learning/reaggregate_day_to_periods.py`, which upserts
   individual/NE period rows by raw SQL (un-wired, noted in the original analysis).

   Two consequences worth carrying:
   - Observing a PERIOD row for a date does **not** identify which entrypoint
     produced it. Any diagnosis that reasons backwards from "a period row exists,
     therefore an operational run happened" is unsound (see §C).
   - `_run_short_term_recalc` calls `read_observed_and_modelled_data(config.name,
     codes=codes)` with **no** `start_year`/`end_year`
     (`recalculate_skill_metrics.py:191`), and those parameters default to `None`
     = unbounded (`src/data_reader.py:2746-2751`). So the ST skill recalc reads
     **every** year at once and then saves through the same yearless-key dedup as
     everything else (`src/file_writer.py:118`) — precisely the multi-year collapse
     that PP-046 describes and that PP-045's CLI iterates per year to avoid. Stated
     precisely: the unbounded input is collapsed to **one row per
     `(code, period_in_year, model_short)`**, then filtered to the last two years.
     It is therefore *not* true that the recalc writes rows across all years, and
     *not* true that only the latest year survives — a period-in-year absent from
     the latest year can retain its prior-year row. The net effect is a silent
     *under-write* (at most one row per period-in-year reaches the API, regardless
     of how many years were read), not data loss, since rows are upserted rather
     than deleted. **Not fixed here** — record and file as a new ticket per the
     repo's found-while-mapping rule; it is the strongest concrete argument that
     PP-046 is worth fixing.

### B. Field evidence — REPORTED, not re-run here

This session was read-only and did not query the shared DBs (a concurrent module
review held the tunnels). Recorded verbatim as context:

- LR raw forecasts (`/lr-forecast/`, pentad) present for **all** issue days
  including 2026-07-25, 07-31, 08-05, 08-10 (78 rows each).
- Per-model PERIOD rows (`/forecast/?horizon=pentad`) present **only** for
  07-05, 07-10, 07-15, 07-20 and 08-15 — the four days above are absent.
- `maintenance:postprocessing_forecasts` ran (26 s, PASS, ~12.8k records,
  13-month window) and created none of them; it only enriched dates that already
  had `combined` rows.
- EM **and** NE both absent for the four days.

A similar signature was independently recorded on kghm on 2026-08-14
(`doc/dev/review_checklist_local_2026-08-14_kyg.md`: "TFT pentad present only for
08-15, nothing for 08-05/08-10") — but that record explicitly labels its own
PP-045 attribution as underdetermined (`:2158`) and also records an LR **hindcast**
run producing fourteen write batches (`:2122`). So kghm is a second *observation*,
not a second confirmation, and it must be probed on its own data and config
(§E) rather than assumed to share tjhm's cause. That checklist additionally
states PP-045's fix is "not on trunk" — stale and wrong; PR #425 merged
2026-07-23, three weeks earlier. What it actually observed is trunk behaviour
*with* the fix present, because the fix is a manual tool that nobody ran.

### C. What is established, and what decides the diagnosis — READ THIS FIRST

**Established (PROVEN, code-only):** maintenance cannot heal a zero-`combined`
boundary date (§A3), and the validator's window cannot see one (§A5). Those two
facts are independent of any DB state.

**REPORTED, not proven here:** that the condition is currently live on tjhm and
kghm (§B). This session did not query the DBs; the kghm record self-labels as
underdetermined. Do not cite §B as proof of a live defect until §E runs.

**Two earlier inferences are withdrawn:**

- ~~"An operational pentad run executed on 2026-08-15, because only operational
  writes per-model PERIOD rows."~~ **Refuted by §A6** — at least three entrypoints
  write those rows. The producer of the 08-15 rows is unknown.
- ~~"LR rows on all four days indicate the pipeline was invoked on those days."~~
  **Refuted** — `linear_regression.py` has a `--hindcast` mode that iterates
  historical forecast dates (`:698`, `:813`), and the kghm checklist records
  exactly such a hindcast. LR presence carries no information about when, or
  through which mode, those rows were created.

**The remaining question, which decides everything:** for each (code, model) and
each of the four dates, did a **usable row survive merged-archive normalization**
at the time the aggregation ran? Note this is *not* the same as "does a DAY row
exist": `_read_ml_forecasts_pp_api` fetches both `horizon="day"` and the migrated
period archive and merges them (`src/data_reader.py:2194`), retaining period rows
for dates before each (code, model)'s first DAY issue date
(`_merge_archives_by_day_cutover`, `:2087`, `:2148`). With no DAY rows at all it
returns the period archive unchanged. LR presence in `/lr-forecast/` is
independent of both (`:2630`), and LR is dropped before the combined write —
returning `False` if the frame is LR-only (`src/api_writer.py:230`, `:242`).

**Decision tree — the first draft's H1/H2 split was not exhaustive.** At least
six distinct causes produce the observed pattern, and they need different fixes:

| # | Cause | How to detect it | Who fixes it |
|---|---|---|---|
| C1 | No usable row for those issue dates in either archive | **Derived-frame probe (P2), not a log line.** `No <model> forecasts from API` (`src/data_reader.py:2661`) fires only when the *entire* scoped read is empty, so it cannot single out four missing dates when other dates exist. | `machine_learning` (see caveat below) |
| C2 | Rows present, `target` outside the following period | `Filtered %d/%d daily targets outside …` (`src/data_reader.py:2418`) — an **aggregate count**, not per-date; a non-zero count says "look here", P2 says which dates | ML output / aggregation filter |
| C3 | Rows present, `forecasted_discharge` NULL | `Dropped %d null-discharge forecast records …` (`src/api_writer.py:413`), again aggregate | ML output. `recalculate_nan_forecasts.py` is the usual tool but is **not** a general remedy — it selects only flag `1`/`2` rows, not every null-discharge row |
| C4 | Rows exist **now** but were absent/unreadable **during** the historical run — the reader turns exceptions into `None` (`src/data_reader.py:2183`, `:2198`) | historical operational log only; no query of current state can see this | transient; needs log evidence |
| C5 | Codes or models excluded by the operational station-selection or `ieasyhydroforecast_available_ML_models` config (station scoping invoked at `postprocessing_operational.py:146`; the model gate and config parsing follow `src/data_reader.py:2646`) | config diff, not DB | configuration |
| C6 | Rows usable and written, but the write silently under-persisted (PP-047: `_write_combined_forecast_to_api` receives a server `count` but returns `True` without comparing it to the submitted record count, `src/api_writer.py:445`, `:463`) | server-side count vs submitted | PP-047 |

Only **C6**, and a hypothetical seventh case where everything above is clean and
the rows still never appeared, would represent a genuine *new* defect in PP-045's
territory. C1–C5 mean the four days are **not** a PP-045 reproduction: PP-045's
original "self-heals at the next within-year boundary run" claim survives (its
stated proviso (a) — "the DAY archive still holds those issue dates" — simply
failed), and `backfill_period_forecasts.py` cannot recover them, because it
re-runs the same aggregation over the same inputs.

**Caveat on the C1 remedy.** The obvious upstream tool is
`machine_learning/fill_ml_gaps.py` (wired into `maintenance:machine_learning` and
the default ML `RUN_MODE`; the orchestrator already sequences ML maintenance
before postprocessing maintenance, `apps/pipeline/pipeline_docker.py:1860`). But
its gap detection only examines gaps *between consecutive dates for codes already
present* (`fill_ml_gaps.py:265`, `:278`, `:304`) — it cannot discover an empty
model/code archive, a leading gap, or a trailing gap. If the first DAY date is
around 08-13/08-15, the four earlier dates may be invisible to it as well, which
would explain why routine maintenance did not close the gap. Verify before
recommending it as the remedy.

Also note the mechanism that keeps the *existing* period rows alive:
`_merge_archives_by_day_cutover` retains period-archive rows for dates before each
(code, model)'s first DAY issue date, so 07-05..07-20 can be re-emitted unchanged
from the period archive with no DAY data behind them. A DAY archive that begins
around 08-13 would reproduce the entire observed pattern on its own.

### D. Two defects in the shipped artefact's stated contract — PROVEN

1. **Inaccurate parenthetical.** `backfill_period_forecasts.py:3-8` says
   maintenance "recalculates skill metrics, not period forecasts". Both halves are
   wrong. Maintenance *reads* skill metrics, it does not recalculate them (that is
   `recalculate_skill_metrics.py`), and it *does* write period forecasts —
   refreshed individual, NE and EM rows (`postprocessing_maintenance.py:291`,
   `:348`, `:380`). What it cannot do is discover a date with zero `combined`
   rows, and its write set never emits fresh per-model rows for a newly-discovered
   date. The rest of this issue states it correctly; only the CLI docstring is
   wrong, and an operator reading only the CLI would draw the wrong conclusion
   about what maintenance did.

2. **Understated precondition.** Neither the docstring nor `--help` states what
   the backfill actually requires: **a usable row must survive merged-archive
   normalization for that issue date** — a DAY row with an in-period `target` and
   a non-null discharge, *or* a retained pre-cutover period-archive row. The first
   draft of this section wrote "ML DAY rows must exist", which is too strong: with
   no DAY rows the reader falls back to the period archive
   (`src/data_reader.py:2103`, `:2148`, `:2194`). The docstring should state the
   real precondition, name the three filters that can silently empty a date
   (boundary drop, in-period target filter, null-discharge drop), and point at the
   upstream tools (`fill_ml_gaps.py`, `recalculate_nan_forecasts.py`) **with** the
   caveat in §C that `fill_ml_gaps.py` cannot see leading/trailing gaps.

Both are documentation-only fixes to a shipped file; neither changes behaviour.
They are **blocking**, not cosmetic — this issue's own Desired Outcome states
that "done" includes "the behavior is documented".

### E. Verification design — DESIGNED, NOT RUN

Not run this session: the shared DBs and SSH tunnels were in use by a concurrent
module review, and the constraint was read-only. **P0–P3 are read-only and safe to
run alone. P4 writes and needs owner go-ahead plus an idle tunnel.** Run the whole
sequence **independently on tjhm and on kghm** — §B establishes two observations,
not one shared cause.

- **P0 (config, read-only).** Record, per org: `ieasyhydroforecast_run_ML_models`,
  `ieasyhydroforecast_available_ML_models`, and the station-selection file's
  `stationsID`. A code or model absent here is cause **C5** and explains the gap
  with no DB work at all.
- **P1 (logs, read-only, no DB access).** Preserve the historical operational and
  maintenance logs covering 2026-07-20 → 2026-08-17 **before any write**, then grep
  them for: `No <model> forecasts from API` (**C1**),
  `Filtered %d/%d daily targets outside` (**C2**),
  `Dropped %d null-discharge forecast records` (**C3**),
  `No non-LR forecast records to write`, and any reader exception around the ML
  fetch (**C4**). Read these as *signals, not verdicts*. The two filter lines
  (`Filtered …`, `Dropped …`) carry an aggregate count for the whole run and never
  the specific dates or codes, so a non-zero count says "C2/C3 is in play" and P2
  says which dates. The other three carry no count at all: they are presence/absence
  markers for the whole scoped read. What makes
  this step non-optional is **C4** — an input that was absent or unreadable *then*
  and is present *now* leaves no trace in current DB state, so the log is the only
  evidence that will ever exist for it. Run it first, and preserve the logs before
  anything writes.
- **P2 (derived frame, read-only) — the primary discriminator.** Do **not** stop at
  counting raw rows: that inspects the wrong frame and cannot separate C1 from
  C2/C3. Call the production reader on the real config and print exact coverage:
  `data_reader.read_observed_and_modelled_data("pentad", codes=<selection>,
  start_year=2026, end_year=2026)`, then print
  `(date, code, model_short, forecasted_discharge)` for the four dates plus the
  controls. This exercises the archive merge, boundary drop, in-period target
  filter, aggregation and station scoping. **It is not the final write payload** —
  it still contains LR, may contain null-discharge rows, and precedes virtual
  stations, NE, EM, the yearless-key dedup and the writer's LR/null/dedup filters.
  What it establishes is the necessary condition: if the four dates are absent
  *here*, no backfill can write them, and the cause is C1–C5. Reading is
  side-effect-free; nothing is saved.
- **P3 (controls + cutover).** Same query for 2026-08-15 (a date that does have
  period rows) and 2026-07-20 (present, but possibly served from the retained
  period archive rather than DAY). Separately — the derived frame has discarded
  archive origin, so this cannot come from P2 — query the raw archives directly for
  each (code, model)'s **first DAY issue date**:
  `client.read_short_term_forecasts(horizon="day", model=<M>, code=<code>)` and
  take the minimum `date`. That is the cutover boundary; a DAY archive beginning
  ~08-13 explains the whole pattern.
- **P3b (branch).** Map the result onto the C1–C6 table in §C and route
  accordingly. C1–C5 ⇒ **not a PP-045 reproduction**; file the real cause against
  the owning module and do not attribute these days to PP-045. C6 or "all clean
  and still absent" ⇒ re-open the within-year self-heal analysis in this issue
  **before** running any backfill.
- **P4 (only if P3b lands on C6/unexplained; writes; owner go-ahead required).**
  - Snapshot the affected rows (counts + values) before touching anything.
  - **`--horizon pentad` only.** The reported anomaly is pentad-specific, and the
    CLI expands each touched year to a whole-year read and save
    (`backfill_period_forecasts.py:203`, `:209`); `--horizon both` would needlessly
    rewrite the entire 2026 decad population and its EM rows.
  - `--dry-run` first, but do **not** treat its output as coverage proof: the
    dry-run logs only total rows, distinct period count and distinct model count
    (`postprocessing_operational.py:197`) — it never lists dates or codes and
    cannot show whether the four dates survive. P2 is the coverage evidence.
  - Then the real run (API-only, `require_api=True`), then an **exact post-write
    read-back** of the four dates for all five DB models. The read-back is
    mandatory, not a nicety: PP-047 means `require_api=True` catches
    API-unavailable / exception / explicit-`False` but **not** a `True` returned
    over a zero or partial server write.
  - Then re-run once and confirm counts are unchanged (idempotence), mirroring the
    2026-07-23 verification recorded above.

Caveat that applies to P4 under any cause: EM is recomputed against **current**
skill metrics (`postprocessing_operational.py:170`, `:187`;
`src/ensemble_calculator.py:115`, `:165`), so a backfill of historical dates does
not replay the original ensemble values.

### F. Manual vs automatic — ANSWER AND RECOMMENDATION

**Recommendation: keep the *repair* manual; open a separate small ticket for
*detection and reporting*. Do not make PP-045 self-repairing.**

Three honest arguments, with the overreach of the first draft removed:

- **A postprocessing-side automatic repair is not sufficient on its own.** Under
  causes C1–C5 (§C) there is no usable input to aggregate, so a PP-side healer
  would run, find nothing, and — absent care — report success. This does **not**
  mean automatic repair is impossible: the orchestrator already sequences ML
  maintenance before postprocessing maintenance
  (`apps/pipeline/pipeline_docker.py:1860`), so a coordinated upstream-plus-PP
  automation is conceivable. But it would need cross-module contracts that do not
  exist today (notably a `fill_ml_gaps.py` that can see leading/trailing gaps, and
  PP-047's write-count check), which is a substantially larger design than PP-045.
- **Repair rewrites history.** EM is recomputed against current skill metrics
  (confirmed, §E caveat), so a backfill of historical dates does not replay the
  original ensemble values whenever the inputs or skill membership have changed.
  It is *not* true that every invocation changes them — unchanged inputs are
  deterministic — and it is *not* true that this feeds back into the short-term
  skill store: `recalculate_skill_metrics.py` recomputes skill from raw
  observed/modelled data and explicitly excludes EM re-derivation (`:191`, `:219`,
  `:224`). The exposure is to downstream consumers of historical EM —
  `forecast_skill_eval`, dashboards, bulletins. A tool an operator invokes
  deliberately, having read that caveat, is a different risk from a cron job doing
  it unattended.
- **The blast radius is a whole calendar year.** The CLI's granularity is a full
  year per pass by construction (whole-year read plus PP-046's yearless-key
  constraint). Acceptable for a deliberate recovery; poor as automatic behaviour.

The case for *some* automation is nonetheless strong, and this session's evidence
is why: a routine three-week dev-machine staleness produced four apparently
stranded issue days, and **no layer reported it** — maintenance ran green, and the
validator's window (§A5) never looks further back than the most recent boundary.
Silence here is indistinguishable from health.

**Recommendation: detect-and-report, never auto-fix.**

- **Where:** `maintenance:postprocessing_forecasts` — it runs frequently from cron
  and is where an operator already looks. Emit a WARN listing boundary dates in the
  lookback window with zero per-model period rows and, because that is the
  operator's actual next decision, which of the C1–C6 causes the evidence points
  at. **No writes, no exit-code change** (the exit contract is PP-051/PP-055
  territory; do not entangle them).
- **Cost — corrected, and not as cheap as the first draft claimed.** Maintenance
  does *not* already own this. `read_combined_forecasts` reads the combined archive
  **without date bounds** (`src/data_reader.py:844`); the 13-month cutoff lives in
  `gap_detector` and is relative to the maximum observed date (`:72`, `:80`); and
  maintenance never enumerates *expected* boundary dates at all — it only ever
  reasons about dates that already exist. A zero-row detector therefore needs three
  things that do not exist yet: an expected-boundary calendar, an active
  code/model history (so retired stations do not alarm), and DAY/archive
  availability logic (so C1 is reported as an upstream gap rather than a PP gap).
  Small, but a real ticket — not a two-line WARN.
- **Why not `validate_pipeline`:** that is where such a check morally belongs, but
  its expectation model is itself under repair (INFRA-024 / INFRA-026). Adding a
  boundary-history sweep before those land risks re-opening INFRA-026 from the
  other side — a check that fires on healthy deployments. Lift it there afterwards.
- **Honest counter-argument:** `postprocessing_maintenance.py` already carries the
  most contested write/exit semantics in the module (PP-007, PP-024, PP-051,
  PP-055), and a WARN nobody reads is not detection. Mitigation: the change touches
  no write path, and the post-run log scan in the local review checklists
  (`doc/dev/review_checklist_local_*.md` §8a) already greps these logs.

**File as a new ticket, do not bundle into PP-045.** PP-045's scope was "provide a
recovery mechanism", and that is delivered. "Make the condition visible" is a
different contract with a different risk profile. **Do not assume PP-056 is free:**
it is unused on trunk `8e3fc1bc` but claimed by an uncommitted entry (quarter
skill-metric `horizon_value=0`) in a parallel session's working copy of
`doc/plans/module_issues.md`. Allocate against the *current* index, not trunk's.

### G. Remaining checklist to move Review → Complete

**Blocking — evidence:**

- [ ] Run P0–P3b (§E, read-only) **on tjhm** and record the outcome here.
- [ ] Run P0–P3b **on kghm** independently — §B is two observations, not one
      confirmed cause, and the kghm record self-labels as underdetermined.
- [ ] Preserve the 2026-07-20 → 2026-08-17 operational/maintenance logs for both
      orgs **before** any write. Cause C4 (input absent *then*, present *now*) is
      invisible to any query of current DB state; the logs are the only evidence.
- [ ] **Resolve the deferred kyg criterion — migrated here 2026-08-17 from the now
      frozen Acceptance Criteria, and this is its only live home.** As written it
      demands the full **kyg** (`15xxx/16xxx`) short-term pipeline end-to-end
      (preprocessing_runoff → linear_regression → gateway → machine_learning →
      postprocessing_forecasts). The 2026-08-14 kghm local review reproduced the
      *condition* on kyg data but never exercised the *fix*, and running only the
      backfill against local kghm data is **not** equivalent pipeline evidence —
      it still needs a configured API/database plus write authorisation. Owner
      decision: run the full pipeline when kyg is available, or formally
      waive/downgrade the criterion with a written rationale.

**Blocking — the tool is invisible to operators, and "documented" is in this
issue's own Desired Outcome:**

- [ ] `doc/prod/` runbook entry for `backfill_period_forecasts.py`. Currently
      referenced only in this issue, `doc/plans/module_issues.md`, and the
      `doc/dev/review_checklist_local_*.md` diagnostics tables. For a
      manual-recovery tool the operator-facing runbook *is* the deliverable.
      It must require an **exact post-write read-back** (PP-047: the writer can
      return `True` over a zero/partial persist) and recommend `--horizon` scoped
      to the affected horizon rather than `both`.
- [ ] CLI docstring fix 1 — the maintenance parenthetical (§D1), including that
      maintenance does *not* recalculate skill metrics.
- [ ] CLI docstring/`--help` fix 2 — the real precondition and the three silent
      filters (§D2), with the `fill_ml_gaps.py` leading/trailing-gap caveat.
- [ ] `apps/postprocessing_forecasts/README.md` — recovery procedure (no backfill
      section exists at all).
- [ ] `doc/data_flow_short_term.md` — the backfill as the recovery path for
      stranded period rows.

**Non-blocking corrections (factual, safe to apply):**

- [x] **DONE 2026-08-17 (plan phase P1).** This issue's Summary/Problem sections
      said per-model period rows are created "only" by the operational path. The
      narrative sections above are now reconciled with §A6: the writer inventory is
      corrected, the absolute "no re-run heals it"/"never recreated across a year
      boundary" claims are replaced by the precondition and per-entrypoint matrix in
      **§H**, the historical A/B/C decision sections are banner-disarmed, the two
      superseded checklists are frozen, and every in-place correction is recorded in
      the **Corrections log**.
- [ ] `doc/dev/review_checklist_local_2026-08-14_kyg.md` — three errors, not one:
      the stale "not on trunk" claim; the "period rows are written **only**
      operationally" claim (`:4924`); and the diagnostics-table row that reads
      "per-model period rows present but EM/NE absent ⇒ PP-045" (`:5179`), when
      PP-045 concerns *missing per-model rows* and EM has a separate skill gate.
      Dated review record — the owner should choose correct-in-place vs annotate.
- [ ] `doc/plans/module_issues.md` PP-045 row — Status cell still reads "Option B
      implemented (branch `fix_postprocessing_boundary_gap`)"; should read merged
      via PR #425. **Deliberately not edited on 2026-08-17**: a concurrent session
      had uncommitted changes to that file, and a one-row edit on a separate branch
      would have handed them a needless conflict.
- [ ] Claude memory — the self-heal-within-year vs permanent-across-year nuance,
      the real backfill precondition, and the three-writers correction (§A6).

**New tickets to file (found while mapping; per repo rule, not fixed here):**

- [ ] **ST skill recalc reads all years and saves through the yearless-key dedup**
      (§A6). `recalculate_skill_metrics.py:191` passes no year bounds; the save
      path collapses period-in-year across years. Silent under-write, not data
      loss. Strongest concrete instance of PP-046.
- [ ] **Detect-and-report for stranded boundary days** (§F). Allocate a free ID —
      allocate against the *current* `module_issues.md`, not trunk's (see §F —
      PP-056 is free on trunk but claimed in a parallel session's working copy).

**Owner decisions still open (carried forward, unchanged):**

- [ ] API-only-by-default write (`--write-csv` opt-in) — behavioural choice on the
      combined-CSV artifact; owner may override to CSV+API.

**Explicitly NOT blocking Complete:** PP-046 (yearless key — worked around by the
per-year loop), PP-048 (decade EM freeze). **PP-047 is not a code blocker either,
but it does change the runbook**: without a post-write read-back, the manual
recovery can report success after zero/partial persistence.

---

## H. What actually heals a missed period (2026-08-17)

Replaces the absolutes this issue originally carried in "Net effect". Analysed
2026-08-17 from code; reviewed by four independent out-of-loop `codex exec` passes,
which refuted two successive drafts before this one.

### The two-part rule

> **Precondition (universal for the app path).** No entrypoint that goes through the
> normal reader can emit a period row unless the merged archive still yields a usable
> input row for that issue date — one surviving the boundary drop and the in-period
> `target` filter, with a non-null discharge at the API write. **A gap whose inputs
> were never produced is unhealable by any of them.** The raw-SQL reaggregator is
> outside this precondition: it builds its own aggregation from DAY records and
> upserts directly, which is precisely why it is not a like-for-like substitute.
>
> **Ability (entrypoint-specific).** Meeting the precondition is not sufficient.
> Each entrypoint has its own limit, and **maintenance cannot write a fresh
> per-model row for a missed date at all.**

Note the distinction between *reach of the read* and *reach of what actually lands*.

| Entrypoint | Emits a *fresh* per-model row? | Reach of what actually lands | Its own limiter |
|---|---|---|---|
| `postprocessing_operational.py` (boundary day) | **Yes** | current year | Year-scoped read ⇒ never touches prior years |
| `postprocessing_maintenance.py` | **No** | Mixed, not uniformly bounded: gap and stale-quantile detection use `gap_detector`'s lookback (default 13 months from the max `combined` date), but the **stale-EM scan is unbounded** — it filters `combined` directly | Universe built solely from existing `combined` rows; early-returns on empty `combined`; `refresh_parts` never emits fresh non-stale individual rows (its individual writes need an existing stale key; genuinely new rows are NE/EM only). Writes direct to the API, bypassing `get_latest_forecasts` |
| `recalculate_skill_metrics.py` | **Yes** | **only latest year and latest-1** — the read reach is *not* the emission reach | Start-year filter drops pre-`SAPPHIRE_SKILL_METRICS_START_YEAR` rows; then the yearless dedup + two-year filter (PP-046) collapses any `period_in_year` also present in a later year. Early-returns if observed **or** modelled is empty |
| `backfill_period_forecasts.py` (PP-045) | **Yes** | any year in range | Per-year iteration is exactly what defeats PP-046 |
| operational with `SAPPHIRE_FORECAST_DATE` | **Yes** | one chosen year | **The chosen date must itself be a boundary date** or the entry gate skips the run; also rewrites that year's combined CSVs from scratch |
| `reaggregate_day_to_periods.py` (raw SQL, un-wired) | **Yes — individual *and* NE** | any | Outside the app path: ignores the source `target` (recomputes `date + 1`), skips the `api_writer` LR-drop/null-drop/dedup, sets `horizon_value=0`. It emits no EM of its own, but its SQL does not exclude source EM rows, so "never writes EM" is **not** code-guaranteed |

### The old-heals / recent-does-not asymmetry

**Mechanism — PROVEN from code.** For dates before each (code, model)'s first DAY
issue date, `_merge_archives_by_day_cutover` retains whatever **period-archive rows
already exist** — it retains, it does not synthesise, so a pre-cutover date with no
archived row stays empty. Retained rows carry `target = date + 1` (set by both the
migrator and the normal writer), so they pass the in-period filter and re-emit
unchanged. Dates in the DAY era require a real DAY row.

**Attribution to the observed cases — INFERRED, not verified.** That the specific
gaps operators have seen fall on the far sides of that cutover is corroborated by
the 20-day run gap in the local logs (`log_operational.2026-07-24` → `.2026-08-13`)
and by the owner's operational experience (2026-08-17: every recent gap observed was
on a day the pipeline did not run). **No live-data check has been run** — first-DAY
dates per (code, model) are still unqueried; that is §E's probe.

### Hypothesis tested and REFUTED — do not reintroduce it

That the recalc path is *observation-gated*: that because skill metrics need measured
discharge, a period which has not completed gets skipped, producing the asymmetry.
**The code does not do this.** Recorded so it is not re-derived:

- The observation merge — `pd.merge(simulated, observed[["code","date","discharge_avg",
  "delta"]], on=["code","date"])`, no `how=` ⇒ inner — produces `skill_metrics_df`,
  which feeds **only the skill statistics**. It never filters the frame returned for
  saving.
- `recalculate_skill_metrics.py` passes `exclude_models=["EM"]` (PP-030), taking the
  "Skipping EM ensemble derivation (excluded)" branch where
  **`joint_forecasts = simulated.copy()`**. This is recalc-specific: without EM
  exclusion the joined frame does influence generated EM rows.
- The only row-removing operation on `simulated` inside that function is the
  start-year filter (`SAPPHIRE_SKILL_METRICS_START_YEAR`, default `today.year - 20`),
  which cuts off *very old* data — the opposite direction from the asymmetry.

**Wording caution.** The recalc frame is unfiltered *by observations*. It is **not**
"the unfiltered saved frame": what reaches the database is `simulated_latest`, after
the yearless dedup and two-year filter, then the `api_writer` LR-drop, null-drop and
dedup.

### What this means for this issue's own tool

**The backfill CLI does not address the cause seen in the field cases to date.**
When the pipeline did not run there is nothing to aggregate. (How *common* that
cause is across deployments is unmeasured — the supporting evidence is one local log
gap plus owner experience, both INFERRED above. Do not upgrade this to a frequency
claim without data.) Its genuine
value is narrower: (1) inputs exist but the boundary-day postprocessing was missed;
(2) cross-year recovery where its per-year iteration avoids the PP-046 collapse — the
most controlled cross-year option, though not the only one (see the table). If inputs
are absent the next step is upstream — `fill_ml_gaps.py` / `hindcast_ML_models.py`,
noting that `fill_ml_gaps.py` sees only gaps *between* existing dates and may miss a
leading or trailing one.

---

## Corrections log

Required by the plan's contract C1: narrative sections (`## Summary`, `## Context`,
`## Problem`, `## Desired Outcome`) are corrected **in place** because they are the
reader's entry point, so each superseded claim is preserved here instead of in the
body. Sections from `## Implementation Plan` to `### Dependency graph`, and
`## Verification (2026-07-23)`, are preserved verbatim and are **not** logged here.

| Date | Section | Superseded claim | Why it was wrong |
|---|---|---|---|
| 2026-08-17 | `## Summary` | Period rows "are created **only** by the operational code path" | False. At least three entrypoints write them; `recalculate_skill_metrics.py` re-saves them as a side effect of a skill recalculation. §A6, §H. |
| 2026-08-17 | `## Summary` | An interim correction said operational is the only writer "on a schedule" | Also false — `recalculate_skill_metrics.py` is scheduled yearly (01:00 UTC, 31 December, `bin/run_periodic_maintenance.sh skill_recalc` → `YearlySkillRecalculation`). The real distinction is cadence: daily/boundary-day versus yearly. |
| 2026-08-17 | `## Problem`, `## Technical Analysis` | `reaggregate_day_to_periods.py` writes "individual + NEURAL_ENSEMBLE rows only (**never ENSEMBLE_MEAN**)" / "NE-only" | It derives no EM of its own, but its SQL selects every DAY `model_type` and does not exclude source EM rows, so "never writes EM" is not code-guaranteed. §H. |
| 2026-08-17 | `## Summary` | A boundary missed across a calendar-year edge "is never recreated at all" | Too strong, and it named the wrong mechanism. See the `## Problem` entry below. |
| 2026-08-17 | `## Context` | "The `postprocessing_forecasts` app has two entry points" | The module has six top-level production scripts with a `__main__` block. The count was also being read as "two writers of period rows", which is a different and wrong claim. |
| 2026-08-17 | `## Problem` | "no re-run heals a missed period" | Too strong. A recalc run, a manual `SAPPHIRE_FORECAST_DATE` run and the PP-045 backfill can each emit a fresh per-model row; maintenance cannot at all. The surviving claim is about the **daily/boundary-day cadence**, not about "routine" versus "manual" — the recalc is itself scheduled (yearly, 31 December). §H. |
| 2026-08-17 | `## Problem` | Cross-year permanence attributed solely to operational's `start_year=end_year=today.year` | Incomplete and not unconditional. Recalc reads every year but is then limited by PP-046's yearless dedup; a prior-year period absent from later years can survive and be written. §H. |
| 2026-08-17 | `## Problem` | "Current recovery options (**both** manual / out-of-band)" listing only the `SAPPHIRE_FORECAST_DATE` re-run and the raw-SQL script | Omitted `backfill_period_forecasts.py`, the tool this issue delivered, merged in PR #425 three weeks before. An operator following it reached for the wrong tool. |
| 2026-08-17 | `## Desired Outcome` | "The remediation depth is the open decision below (A/B/C)" | The decision was taken 2026-07-17 and shipped 2026-07-23. |

---

## References

- Investigation + independent (codex) verification: this session (2026-07-17).
- 2026-08-17 status re-assessment (trunk `8e3fc1bc`, tjhm evidence): sections
  "Confirmed on trunk 2026-08-17" above.
- Related: PP-007, PP-024, PP-023, PP-031; prior art
  `apps/preprocessing_runoff/backfill_discharge_aggregation.py`.
