# PP-059 — `recalculate_skill_metrics` and the operational/backfill path write short-term period rows through the same sink with divergent semantics

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

The owner picks one contract and the code matches it:

- **(a) The recalc should not write period forecasts.** It is a skill-metrics job; drop
  the `save_forecast_data` call. Smallest change, removes the divergence, and removes
  the dashboard CSV-clobber path entirely.
- **(b) The recalc legitimately refreshes period forecasts.** Then it must write the
  same row set as the operational path and share its CSV and failure-reporting
  semantics — and the scoped dashboard invocation needs its own answer, because a
  one-station recalc must not rewrite shared artefacts.

**Independently of (a)/(b), the dashboard's scoped recalc should not inherit
`write_csv=True`.** That is arguably a separate, smaller, higher-urgency fix.

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

1. **(a) or (b)** — a design decision that shapes all the work.
2. **Should the dashboard's scoped recalc write anything shared at all?** It currently
   can rewrite the combined CSVs from one station.
3. **Should `forecasts` carry provenance?** It would have made this diagnosable. That is
   a `sapphire/services/postprocessing` schema change — **colleague-managed, discuss
   first**, and likely out of scope here.
4. **The 15-vs-71 code gap is unexplained.** Station selection is 62; DAY holds 71;
   these dates aggregated to 15. The scoped-recalc and virtual-station divergences are
   candidates but nothing is established. Resolve here or split out.

## Testing

- [ ] Unit: for one fixed input frame, assert both paths produce the same set of
      `model_short` values **under equal skill conditions** — the test that would have
      caught the EM divergence without being defeated by the skill gate.
- [ ] Unit: assert a station-scoped recalc does not rewrite shared combined CSVs.
- [ ] Unit: assert a failed API write is reported identically by both paths.
- [ ] Regression: **do not simply delete `exclude_models=["EM"]`.** PP-030 introduced it
      because boundary-date misalignment produced EM rows with `n_pairs` of 1-2;
      removing it restores that defect unless the alignment is fixed first.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

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

- **PP-030** — owns `exclude_models=["EM"]`; its rationale must be understood first.
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
- **Index row pending** — not added to `doc/plans/module_issues.md` because that file had
  uncommitted changes in a parallel session. `PP-059` is the next free id against that
  working copy (trunk is at PP-055); re-check before publishing.
