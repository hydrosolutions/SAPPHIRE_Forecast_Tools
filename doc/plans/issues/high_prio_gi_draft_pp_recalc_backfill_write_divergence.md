# PP-060 — `recalculate_skill_metrics` and the operational/backfill path write short-term period rows through the same sink with divergent semantics

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High (owner to confirm — see "Priority rationale")
**Labels**: `postprocessing`, `data-integrity`, `consistency`, `dashboard`

---

## Summary

Two code paths write short-term per-model PENTAD/DECADE rows through the same
`file_writer.save_forecast_data` sink, from the same aggregation, and disagree on
**eight** axes — including whether `ENSEMBLE_MEAN` is written, whether the operational
combined CSVs are rewritten, and whether a failed API write is reported.

The sharpest consequence is not the yearly cron. It is that **the forecast dashboard
runs a station-scoped skill recalculation after an interactive save**, and that
recalculation inherits `write_csv=True` — so a single-station action can rewrite the
shared combined CSVs.

## Context

`postprocessing_operational._run_short_term_postprocessing` aggregates DAY → period,
creates ensembles, and saves. `backfill_period_forecasts.py` (PP-045, PR #425) reuses
it with `require_api=True`, one calendar year per call, and `write_csv` taken from
`--write-csv`, which **defaults to false**.

`recalculate_skill_metrics.py::_run_short_term_recalc` recalculates skill metrics and,
as a side effect, re-aggregates and calls `file_writer.save_forecast_data(config,
modelled)` on the same table — with **no** `write_csv` or `require_api` argument, so it
inherits the defaults.

**How the recalc actually gets invoked.** Three call sites; their relative frequency is
**not** established by the repository, so the order below is presentational only:

1. **From the forecast dashboard, station-scoped, on an interactive save.**
   `apps/forecast_dashboard/src/vizualization.py` (~`:4027-4046`) launches the
   `skill_recalc` task with `SAPPHIRE_RECALC_STATION_CODE=<station_code>` after a user
   edits and saves. Failures are swallowed as non-fatal — and note the container runner
   itself catches and prints every exception, so the caller's own `logger.warning` will
   generally not fire; the failure surfaces only in printed/container output.
2. **From deployment initialisation** — `apps/pipeline/pipeline_docker.py` (~`:2562`).
3. **From a documented yearly cron.** `doc/deployment.md` and
   `doc/prod/update_deployment_checklist.md` both specify 01:00 UTC on 31 December,
   routed to `YearlySkillRecalculation`. **This is the documented schedule, not a
   verified one** — repository contents cannot prove a deployed crontab, and
   `apps/pipeline/README` contradictorily says 1 January. Confirm with `crontab -l`
   before relying on it.

## Problem

For the same inputs, the two paths differ as follows. Rows marked **conditional** are
the ones an earlier draft of this issue got wrong by stating them absolutely.

| Axis | Operational / backfill | Recalc |
|---|---|---|
| **ENSEMBLE_MEAN** | Created via `create_ensemble_forecasts` — but **conditionally**: skipped when skill metrics are empty, and the calculator emits none when fewer than two models qualify | **Never** — `exclude_models=["EM"]` (PP-030) routes to a branch returning the frame unchanged |
| **CSV** | `write_csv` is a parameter; the backfill passes `--write-csv`, which **defaults to false**, *specifically so it does not clobber the operational combined CSVs by default* — an operator can opt in | Inherits **`write_csv=True`** with no way to opt out — rewrites both combined CSVs as a side effect, including on a **station-scoped dashboard run** |
| **API failure** | Backfill passes `require_api=True` and forces `SAPPHIRE_API_FAILURE_MODE=fail`, so failures surface and the CLI exits non-zero | Inherits **`require_api=False`** and the ambient failure mode — a falsy write result is ignored, exceptions default to warn-and-continue |
| **Year scope** | Backfill saves **one calendar year per call**, deliberately, to defeat the yearless dedup (PP-046) | Reads **unbounded**, then `calculate_skill_metrics` applies `SAPPHIRE_SKILL_METRICS_START_YEAR` (default `today.year - 20`), then the sink's yearless dedup narrows the API payload again |
| **What each artefact receives** | Same asymmetry as any caller | The **full combined CSV** gets the broader start-year-filtered frame; only `_latest.csv` **and the API** get the yearless-dedup result. These scopes are not the same. |
| **Station scope + virtual stations** | Reads configured codes and synthesises virtual stations whenever the modelled frame is non-empty | Under `SAPPHIRE_RECALC_STATION_CODE` it processes **one station** and **skips** virtual-station synthesis outright — two divergences, not one |
| **Empty-observation behaviour** | Ignores the observed result and can still save modelled data | **Early-returns** when observed *or* modelled is empty — writes nothing |
| **Operator controls** | Backfill has `--dry-run` and per-(year, horizon) error isolation | Neither |

Two of these contradict decisions taken elsewhere:

- **The CSV rewrite.** PP-045 made API-only the backfill default and put CSV writing
  behind an opt-in flag, on the stated grounds that rewriting the operational combined
  CSVs by default is unsafe — *"never clobbers the operational combined CSVs (which the daily
  operational run owns)"*. The recalc does exactly that whenever it reaches the save.
  On the dashboard's scoped path this is worse than inconsistent: **the shared combined
  CSVs can be rewritten from a single station's history.**
- **The swallowed write failure.** This is the PP-051 family — the same operation
  reports loudly through one entrypoint and silently through the other. Note the
  backfill's guarantee is itself partial: **PP-047** means `_write_combined_forecast_to_api`
  can return `True` over a zero or partial server write, so `require_api=True` catches
  unavailable/exception/explicit-false, not every persistence failure.

## Impact

**Observed** on the local dev DB, 2026-08-18, by direct read-only query
(`horizon_type='PENTAD'`). External observation — recorded here with provenance, not
derivable from the repository:

| date | distinct codes | ENSEMBLE_MEAN | NEURAL_ENSEMBLE |
|---|---|---|---|
| 2026-07-05 | 67 | 40 | 67 |
| 2026-07-10 | 15 | 12 | 15 |
| 2026-07-20 | 52 | 34 | 52 |
| **2026-07-25** | **15** | **0** | **15** |
| **2026-07-31** | **15** | **0** | **15** |
| **2026-08-05** | **15** | **0** | **15** |
| **2026-08-10** | **15** | **0** | **15** |
| 2026-08-15 | 66 | 24 | 66 |

DAY inputs exist for all four dates (426 rows each for TSMIXER/TIDE/TFT, 71 distinct
codes), so this is not an input gap.

**Candidate explanations — plural, and this issue does not pick one.** An earlier draft
called the recalc the "leading explanation" on the grounds that only it writes
per-model + NE without EM. That reasoning is **wrong**: the operational path also saves
without EM whenever skill metrics are empty or fewer than two models qualify. So the
observed state is consistent with at least:

- a recalc write (EM excluded by design);
- an operational or backfill write that hit the skill/eligibility gate;
- an operational write whose EM rows were never created for an unrelated reason —
  upserts do not delete existing rows, so an absent EM is an EM never written, not one
  removed.

**Rows cannot be attributed to a writer from the table alone.** `Forecast` carries no
`created_at`, `updated_at` or writer column, and the upsert updates in place. External
evidence (logs) may still discriminate; the table will not.

**The diagnostic cost is nonetheless real.** This state was investigated for weeks as
PP-045's "stranded boundary day, inputs never produced". It is neither — the inputs and
the rows both exist. The divergence is what makes the state ambiguous.

## Desired Outcome

> **DECIDED 2026-08-18 — option (a).** The recalc stops writing period forecasts.
> Recorded together with a repo-wide decision that **CSV output is being deprecated**
> now the database services are deployed, which retires **two** of the eight axes below —
> CSV writing and artefact-frame scope — rather than fixing them. **Six survive**: the EM
> row set, `require_api`, year scope, station scope plus virtual stations,
> empty-observation behaviour, and operator controls.
>
> **Precondition to verify, not to assume.** An earlier draft claimed option (a) breaks
> new-site seeding because the recalc's API forecast write seeds a fresh deployment. The
> pipeline shape is confirmed — `bin/initialize_site_backfill.sh` has three phases, ends
> with the recalc, and nothing after it writes forecasts — but the seeding claim is **not
> established**: the init flow regenerates LR only, the recalc *reads* ML rows from a
> `forecasts` table a purged site does not have, and `api_writer` drops LR before the
> combined write and returns false when nothing non-LR remains. Establish what a fresh
> site actually ends up with **before** building any seeding step. See the plan's §T2
> consequences.
>
> Note (b) is not being pursued, so **PP-030's `exclude_models=["EM"]` stays as-is** —
> under (a) the recalc writes no forecast rows at all.
>
> **Option (a) does not retire the dashboard CSV problem.** It removes the recalc's
> `save_forecast_data` call, but the recalc still calls `save_skill_metrics`, whose CSV
> write is unconditional — so a station-scoped recalc can still overwrite the shared
> skill-metric CSV until the deprecation removes that write too.

The two contracts that were on the table, retained for the record — **(a) was chosen**:

- **(a) The recalc should not write period forecasts.** It is a skill-metrics job; drop
  the `save_forecast_data` call. Smallest change, and it removes the divergence. It
  removes the *combined-CSV* clobber path but **not** the dashboard problem entirely —
  `save_skill_metrics` still writes its CSV unconditionally.
- **(b) The recalc legitimately refreshes period forecasts.** Then it must write the
  same row set as the operational path and share its CSV and failure-reporting
  semantics — and the scoped dashboard invocation needs its own answer, because a
  one-station recalc must not rewrite shared artefacts.

**Independently of the contract, the dashboard's scoped recalc should not rewrite shared
artefacts.** Note what (a) does and does not do here: it removes the `save_forecast_data`
call, so the inherited `write_csv=True` on *that* path goes away — but `save_skill_metrics`
has **no `write_csv` parameter at all**, so its shared-CSV write survives (a) and needs
either a new parameter or the CSV deprecation. Separate, smaller, higher-urgency fix.

---

## Technical Analysis

### Key files

- `apps/postprocessing_forecasts/recalculate_skill_metrics.py` — `_run_short_term_recalc`:
  the unbounded read, the empty-observation early return, the scoped-code branch and its
  virtual-station skip, `exclude_models=["EM"]`, and the bare `save_forecast_data` call.
- `apps/postprocessing_forecasts/postprocessing_operational.py` —
  `_run_short_term_postprocessing`, and the skill-empty branch that skips ensemble
  creation while still saving.
- `apps/postprocessing_forecasts/src/ensemble_calculator.py` — the ≥2-qualifying-models
  gate.
- `apps/postprocessing_forecasts/src/skill_metrics.py` — `calculate_skill_metrics`, its
  `SAPPHIRE_SKILL_METRICS_START_YEAR` filter and the `exclude_models` branch.
- `apps/postprocessing_forecasts/src/file_writer.py` — `save_forecast_data` and its
  defaults; `get_latest_forecasts`; and the point where the combined CSV and the
  `_latest`/API payloads diverge in scope.
- `apps/forecast_dashboard/src/vizualization.py` — the station-scoped recalc launch.
- `apps/pipeline/pipeline_docker.py` — `YearlySkillRecalculation` and the
  initialisation-time invocation.
- `sapphire/services/postprocessing/app/models.py`, `app/crud.py` — no provenance
  columns; upsert updates in place. **Colleague-managed: read only.**

### Why the defaults are the trap

`save_forecast_data`'s defaults (`write_csv=True`, `require_api=False`) were chosen to
preserve pre-PP-045 behaviour when those parameters were introduced. The backfill
overrides both. The recalc predates them and overrides neither, so it inherited a
contract nobody chose for it.

---

## Open questions for the owner

1. ~~**(a) or (b)**~~ — **DECIDED 2026-08-18: option (a)**, see Desired Outcome.
2. **Should the dashboard's scoped recalc write anything shared at all?** After decision
   (a) this is a **scheduling** question about the surviving **skill-metric** CSV, not a
   design question about the combined CSVs: fix it now, or let the CSV deprecation remove
   that write. See the plan's T2.2 / T3.1.
3. **Should `forecasts` carry provenance?** It would have made this diagnosable. That is
   a `sapphire/services/postprocessing` schema change — **colleague-managed, discuss
   first**, and likely out of scope here.
4. **The 15-vs-71 code gap is unexplained.** Station selection is 62; DAY holds 71;
   these dates aggregated to 15. The scoped-recalc and virtual-station divergences are
   candidates but nothing is established. Resolve here or split out.

## Testing

Rewritten for **option (a)**. Under (a) the recalc writes no forecast rows, so the
original checklist's "both paths produce the same set" and "both report forecast-API
failure identically" are not meaningful — they are retained at the bottom, struck out, as
the record of what option (b) would have required.

- [ ] Unit: the recalc performs **no** forecast write — `save_forecast_data` is not
      called from `_run_short_term_recalc`.
- [ ] Unit: the recalc's **skill-metric** write and its failure return are unchanged.
- [ ] Unit: a station-scoped recalc does not rewrite the shared **skill-metric** CSV
      (this one survives option (a) — `save_skill_metrics` has no `write_csv` parameter).
- [ ] Unit: an **unscoped** recalc's behaviour is unchanged by that suppression.
- [ ] Unit: the suppression also disables the CSV-backed consistency check — exercise it
      with `SAPPHIRE_CONSISTENCY_CHECK=true`, or an API-only run verifies a scoped frame
      against stale or absent shared CSV state and reports a spurious mismatch.
- [ ] Update, narrowly: the existing tests asserting two forecast-save calls, four recalc
      output CSVs, saved forecast contents, and forecast-save failure propagation. These
      are intentional expectation changes; do not weaken unrelated coverage.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

Option-(b) checklist, retained as record only — **not** current work:

- [ ] ~~Unit: for one fixed input frame, assert both paths produce the same set of
      `model_short` values under equal skill conditions.~~
- [ ] ~~Unit: assert a failed forecast-API write is reported identically by both paths.~~
- [ ] ~~Regression: do not simply delete `exclude_models=["EM"]` — PP-030 introduced it
      because boundary-date misalignment produced EM rows with `n_pairs` of 1-2.~~
      *(Moot under (a): the recalc writes no forecast rows, so the exclusion stays
      untouched. This constraint returns only if the contract is revisited.)*

## Documentation Impact

- [ ] `apps/postprocessing_forecasts/README.md` — the recalc's effect on `forecasts`.
- [ ] `doc/prod/backfill_period_forecasts_runbook.md` §1 — note that per-model rows with
      EM absent has several possible causes, not just a stranded boundary.
- [ ] `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` §H — its
      per-entrypoint matrix predicted this divergence; add the confirmation.

## Out of Scope

- Fixing PP-046, PP-047 or PP-051. This issue records that the recalc manifests them;
  the `require_api` divergence is a sibling of PP-051, not a duplicate.
- Any `sapphire/services/` schema change (open question 3).

## Dependencies

- **PP-030** — owns `exclude_models=["EM"]`. **Not a prerequisite under option (a)**: the
  recalc writes no forecast rows, so the exclusion is untouched. It becomes a prerequisite
  again only if the contract is revisited toward (b).
- **PP-045** — the field case this divergence made ambiguous; §H carries the matrix.
- **PP-046, PP-047, PP-051** — manifested or partially manifested here.

## Priority rationale (owner to confirm)

Proposed **High**, resting mainly on the **dashboard's station-scoped recalc inheriting
`write_csv=True`**: a routine single-station user action can rewrite shared combined
CSVs, and its failures are swallowed as non-fatal. The other grounds are weaker than an
earlier draft claimed — no data loss is proven, the CSVs regenerate on the next
operational run, absent EM is a missing row rather than a wrong value, and the
attribution of the observed DB state is genuinely uncertain. The comparison with PP-051
is imperfect: PP-051's High rests partly on API-only long-term data loss, which does not
apply to a CSV-backed short-term write. **Medium is defensible** if the dashboard path
is fixed separately and quickly.

## References

- Found 2026-08-18 during PP-045 phase P4's database probe.
- Related: PP-030, PP-045, PP-046, PP-047, PP-051.
- **Renumbered 2026-08-18: this issue was filed as PP-059 and is now PP-060.** A
  parallel session allocated PP-059 to a different issue ("Remove monthly EM"); this one
  yielded the id. Any earlier reference to PP-059 for the write divergence — including
  PR #445's title and description — means this issue.
- **Index row still pending** — `doc/plans/module_issues.md` has uncommitted changes in
  that parallel session. `PP-060` was free at the time of renaming; re-verify before
  adding the row.
