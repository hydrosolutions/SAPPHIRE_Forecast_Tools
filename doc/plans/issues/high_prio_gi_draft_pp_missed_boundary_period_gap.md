# PP-045 — Missed boundary-day operational run leaves short-term per-model period gap that maintenance cannot heal

**Status**: Draft — **awaiting human decision on approach (A/B/C, see Implementation Plan)**
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `postprocessing`, `data-integrity`, `operational`

---

## Summary

Short-term per-model period forecasts (PENTAD/DECADE rows in the `forecasts`
table for models like TSMIXER/TIDE/TFT) are created **only** by the operational
code path, and only on hydrological boundary days. If a boundary-day operational
run is missed, no routine re-run (operational on a non-boundary day, or
maintenance on any day) recreates those period rows until the *next*
boundary-day operational run — and a boundary missed across a calendar-year
edge is never recreated at all.

## Context

The `postprocessing_forecasts` app has two entry points:

- **Operational** (`postprocessing_operational.py`) — reads DAY forecasts,
  aggregates them into pentad/decad period rows, creates ensembles, writes to
  the API. Runs from cron on operational days.
- **Maintenance** (`postprocessing_maintenance.py`) — a gap-filler intended to
  backfill *ensemble* rows (EM/NE) that operational missed. Runs from cron
  frequently.

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

- Between two boundary-day operational runs, **no re-run heals a missed period**
  (operational on a non-boundary day = entry gate skip; maintenance any day =
  universe excludes the missing date).
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
  fire either.
- The gap is **genuinely permanent across a calendar-year boundary**: a boundary
  missed in a prior year is never re-read (operational passes
  `start_year=end_year=today.year`), and maintenance cannot seed a
  zero-combined date. That period is lost until a manual recovery.

**Current recovery options (both manual / out-of-band):**

- A manual operational run with `SAPPHIRE_FORECAST_DATE=<missed boundary date>`
  (override honored at `postprocessing_operational.py:200-212`). **Side effect:**
  this run rewrites that year's `simulated` and `simulated_latest` combined CSVs
  from scratch (`src/file_writer.py:193-218`) — it does not merge with current
  CSV state. Warn any operator/consumer that relies on those CSV artifacts.
- The un-wired standalone script
  `apps/machine_learning/reaggregate_day_to_periods.py` — but it bypasses the
  app entirely (direct `docker exec psql` + raw `INSERT … ON CONFLICT`,
  `:277-299`), writes individual + NEURAL_ENSEMBLE rows only (**never
  ENSEMBLE_MEAN**), sets `horizon_value=0`, and skips the `api_writer`
  null-drop / dedup guards. Not a like-for-like substitute; no **executable**
  wiring anywhere in the repo (referenced only in plan/archive docs).

## Desired Outcome

A missed boundary day does not silently strand short-term per-model period rows.
The remediation depth is the open decision below (A/B/C). Whatever is chosen,
"done" means: the reproduction in Testing no longer strands the missed periods
under the chosen recovery mechanism, and the behavior is documented.

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
  reaggregation script (direct SQL; NE-only; bypasses app guards).
- Prior art for an app-integrated backfill entrypoint:
  `apps/preprocessing_runoff/backfill_discharge_aggregation.py`.

### Latent defect found while mapping (record, do not necessarily fix here)

`get_latest_forecasts` (`src/file_writer.py:118-129`) dedups on a **yearless**
key `(code, period_in_year, model_short)` and applies the `>= latest_year-1`
year filter *after* dedup. The "keep last two years" comment is therefore
misleading: the **same period-in-year across two years collapses to the later
row**. Not the primary cause of this gap, but relevant to any "keep two years of
per-period rows" assumption and worth a separate ticket.

---

## Implementation Plan

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

Depends on the chosen option; to be specified in the follow-on workplan after the
decision. No `sapphire/services/` files in any option.

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

- [ ] `apps/postprocessing_forecasts/README.md` — recovery procedure / entrypoint.
- [ ] `doc/data_flow_short_term.md` — if the maintenance/operational boundary
      behavior changes.
- [ ] `doc/prod/` runbook — the manual recovery command (Option A minimum).
- [ ] Claude memory — the self-heal-within-year vs permanent-across-year nuance.

---

## Secondary anomaly (triage — recommend SEPARATE ticket)

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

- [ ] Reproduction test(s) above committed and passing.
- [ ] Under the chosen option, the missed 07-10/07-15 pentad and 07-10 decad
      periods are recovered (unit/integration + manual end-to-end on Tajik).
- [ ] No change to existing maintenance ensemble behavior or operational
      boundary-day behavior beyond the scoped fix (regression tests green).
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero fail, zero
      unexpected skip.
- [ ] No real station codes or discharge values in code, tests, or docs
      (placeholder `19999` only).
- [ ] **DEFERRED — blocked on infra (Kyrgyz server down):** once the kyg server
      is back, run the full kyg (`15xxx/16xxx`) short-term pipeline end-to-end
      (preprocessing_runoff → linear_regression → gateway → machine_learning →
      postprocessing_forecasts) and confirm the fix heals kyg pentad/decad the
      same way. The fix is **not** considered fully verified until kyg is
      exercised.

---

## References

- Investigation + independent (codex) verification: this session (2026-07-17).
- Related: PP-007, PP-024, PP-023, PP-031; prior art
  `apps/preprocessing_runoff/backfill_discharge_aggregation.py`.
