# PP-075 — Short-term EM skill metrics, one shared delta, dashboard range options (umbrella)

**Status:** In Progress. Backend A–D, including the member-write fix, is implemented, independently reviewed and verified. The owner explicitly accepted the 12 existing skips and two expected failures on 2026-11-14 for this publication. Frontend E is deferred; deployment is pending.
**Modules:** `apps/postprocessing_forecasts` (A–D), `apps/forecast_dashboard` (E, deferred: plan and discuss with Maxat).
**Related:** PP-072 (EM rule, done), PP-074 (delta, folded into chunk A), PP-073 (deferred, not on this path),
FD-023 / FD-004 (folded into chunk E). Local verification below does not establish deployment or completion of the full repository gate.

**Authority:** PP-072 governs EM membership and arithmetic; this issue governs the remaining minimal fix.
The [8 November reconstruction vision](../../../planning/visions/2026-11-08-boundary-issued-short-term-em-and-hindcast-skills.md)
is deprecated in full. Do not restore its different thresholds, LR-band rule, daily reconstruction or broader publication machinery.

## Overall goal (owner, 2026-11-13)

1. Short-term (pentad/decad) EM is calculated like the long-term Skilled Mean, **without weighting** (done: PP-072).
2. EM has **skill metrics where enough forecasts exist**; different model combinations are grouped together for the
   EM skill calculation.
3. The **limitations** of the EM skill metrics are documented.
4. Frontend range choice: **native** (what the model computes; for EM what we compute; for LR native = delta),
   **delta** (hydromet default, the same LR delta for all models), **manual** (± %), **mixed** (delta + manual).
5. Constraints: maintenance mode, minimal changes, no refactoring.

## Evidence and limitations

Read-only inspection found stored period forecasts from LR and ML members, with
uneven coverage across stations and periods. Stored EM history was shorter than member
history, and existing EM skills included insufficient or invalid samples associated
with the earlier boundary-alignment problem. Some stations lacked enough member models
to form an EM. Quantile completeness also varied.

Reconstruction skill was not established by that inspection. Historical member rows mix
hindcasts and operational forecasts without a stored distinction; this is an accepted
limitation, not evidence of independent validation. Operational measurements and station
identifiers are intentionally excluded from this publication.

## Baseline code facts (before PP-075 implementation)

- No path computes short-term EM skill today. `read_observed_and_modelled_data` -> `read_individual_model_forecasts`
  reads LR + the ML models only; recalc adds virtual stations and NE; `calculate_skill_metrics` scores whatever model
  names are in the frame; `save_skill_metrics` writes them. `exclude_models=["EM"]` has no effect since PP-072 P2.
- Dashboard "delta" range = the row's own skill `delta` (merged per model, `forecast_dashboard/src/db.py` skill merge);
  EM has no skill row, so it gets NaN and a zero-width band. The summary table never reads Q25/Q75; the hydrograph
  uses Q25/Q75 with a delta fallback (so figure "delta" is already native-with-fallback).
- Skill `delta` per model is the first valid per-date LR delta in that model's own row order
  (`forecast_accuracy_hydromet`, `skill_metrics.py:~484-495`, warns "Delta values vary within group"); models with
  different date coverage therefore differ (PP-074).
- Composition of an EM row = sorted set of members passing the gate with finite values on that date; it varies by date.
  The skill table's unique key has no composition column, so only one EM skill row per (code, period, date) can be stored.

## History: why EM skill is not produced today (verified in git)

- Short-term EM *forecasts* are calculated and written (operational + maintenance, `create_ensemble_forecasts`).
- EM *skill* was derived in `calculate_skill_metrics` (the "legacy EM block"). PP-030
  set `exclude_models=["EM"]` in `recalculate_skill_metrics.py` after recalculation
  produced invalid boundary-period skills: LR and ML normalization gave different issue
  dates, so grouping on `[period, date, code]` did not form the intended multi-model groups.
  That was a deliberate stop-gap (Option C of the archived PP-030,
  `archive/review_gi_draft_pp_em_recalc_boundary_fix.md`); Option B (score actual EM records)
  was never implemented. PP-072 P2 deleted the already-disabled block. Existing legacy
  EM skill rows therefore do not establish valid reconstruction skills.
- Trap for chunk B: any EM hindcast must group members on one issue-date convention, or PP-030 repeats.

## Owner decisions (2026-11-13)

- Chunks A, B, D accepted. **B: the EM hindcast is written as forecast rows AND scored** (like other models' hindcasts).
- **C: minimum 4 distinct matched target years** for each station, horizon and calendar-period bucket. This is
  a skill reporting floor, not a requirement for four member models. Duplicate rows cannot inflate the count.
  Undefined metrics remain unavailable even when four years exist. The member gate remains PP-072; no leakage-free
  check is added and the **in-sample** limitation is documented.
- Recalculation may **overwrite old EM forecasts and skill values**. A newly insufficient-history or otherwise
  unavailable result must not leave an old valid-looking EM skill behind: use the existing write/invalidation
  mechanism for the affected EM skill keys. This is narrow replacement, not PP-073 cleanup or blanket deletion.
- Select one latest applicable finite, non-negative **LR/observation delta** per station, horizon and period,
  independently of each model's forecast coverage, and broadcast it to the output skill rows. Historical accuracy
  continues to use each matched historical date's delta. Replacing the reported delta after scoring does not silence
  the existing variation warning; remove that promise rather than altering historical accuracy to satisfy it.
- Pre-2025 forecasts are **mixed** hindcast/operational (all pre-2024 are hindcasts, 2024–25 mixed). This is a design
  property of the system and is **not to be changed**; document as a limitation/assumption.

## Rule for every EM (forecast and hindcast) row — owner, 2026-11-13

EM is built only from member forecasts for the **same station, horizon, target period and boundary issue date**
(e.g. LR and ML for the same pentad 4 target). No daily forecasts and no in-between-period issue dates enter an EM row. Forecast and hindcast rows are
the same thing in storage. Historical period rows can include non-boundary issue dates,
so the hindcast must filter to boundary issue dates
(`gap_detector._plan_is_valid_boundary`); acceptance test in chunk B.

For recalculation, both member scoring and EM scoring interpret `SAPPHIRE_SKILL_METRICS_START_YEAR` by
**target year**: a 31 December issue for the first period of the next year remains in that next year's sample.
Use a narrow optional scoring-window parameter; other callers retain their existing default behavior.
Persist generated EM forecasts for all eligible stored boundary rows; restrict metric evaluation to the configured
scoring window. This does not add daily reconstruction or change historical accuracy's per-date tolerance.

## Work chunks (ordered, minimal)

Paths below are repository-relative. Each implementation phase uses one implementation agent; the orchestrator
reviews its diff and verification before the dependent phase. Add focused regressions in
`apps/postprocessing_forecasts/tests/`; no new framework, schema, feature flag or general refactor.

| Phase | Goal and allowed files | Acceptance | Depends |
|---|---|---|---|
| A | **Shared reported delta** (PP-074): select the latest applicable LR/observation delta once, independently of model coverage, then broadcast it to all models. `apps/postprocessing_forecasts/src/skill_metrics.py` and focused tests | Different model coverage and shuffled input order give equal deterministic reported delta; historical accuracy still uses per-date deltas | — |
| B | **Generate and score EM hindcasts** from stored period member rows using PP-072's existing `create_ensemble_forecasts`. Write resulting EM forecast rows and score their actual series with the existing skill function, grouped by `(period, code, "EM")` without composition; label is the sorted union of actual members. `apps/postprocessing_forecasts/recalculate_skill_metrics.py`, helper in `src/skill_metrics.py`, narrow stored-period-only read option/helper in `src/data_reader.py` (existing default read path unchanged), focused tests | Pentad and decad EM rows are written; boundary periods (1, last period, month-ends and year rollover at the scoring-window start) match correctly; other-issue or other-target rows never contribute; changing composition does not split the skill sample | A |
| C | **Four-year reporting and replacement**: count distinct target years with finite matched EM/observation pairs; require at least four for reportable skills. Recalculation overwrites old values and invalidates affected unavailable EM skills using the existing mechanism. `apps/postprocessing_forecasts/src/skill_metrics.py`, `recalculate_skill_metrics.py`, existing writer/invalidation functions only where required, focused tests | Three years cannot yield reportable skill; four can yield only defined metrics; duplicates/missing observations cannot inflate years; old affected EM skill is replaced or unavailable after save/readback; unrelated models and long-term results are preserved | B |
| D | **Document limitations and actual behavior**. `apps/postprocessing_forecasts/README.md`, `doc/user_guide.md`, `doc/configuration.md`, and the `create_ensemble_forecasts` docstring | Explain in-sample selection optimism, mixed historical hindcast/operational inputs, composition-free skill, distinct-year `n_pairs`, unavailable metrics, shared reported delta versus per-date accuracy tolerance; distinguish implemented from deployed | B, C |
| E | **Frontend (deferred; Maxat)**: native/delta/manual/mixed range choice for table and hydrograph, shared LR delta and bulletin behavior | Separate plan and discussion; no dashboard implementation in A–D | A |

PP-073 gap-fill hygiene, daily/archive reconstruction, ML retraining and broader stale-data cleanup remain deferred.
Reuse stored period forecasts and the current recalculation pipeline. Do not expand into operational database work
as part of code tests; verification fixtures use synthetic data.

## Verification

Independent follow-up review cleared the narrow member-write fix and its flag-enabled
regressions. Member and EM skill rows now receive valid short-term horizon values before
lead-aware writing, preventing member rows from being dropped after concatenation.

- Final full repository suite: **8,848 passed, 12 existing skips, two existing expected
  failures, zero failures**. The owner explicitly accepted these 12 existing skips and two expected failures
  on 2026-11-14 for this publication; the global repository policy is unchanged.
- Eight non-ML Docker builds/smokes passed earlier. The final corrected postprocessing
  image build and smoke test also passed.
- Corrected both-horizon short-term skill recalculation exited 0. Final API readback
  found no duplicate full keys, finite shared-delta disagreements or mixed-null deltas
  in the current cohort. LR, NE, TFT, TiDE, TSMixer and EM agree in both horizons; the
  broader audit also found no disagreements.
- Final API readback verified no finite EM metrics below four distinct matched target
  years and populated finite metrics for eligible history. No operational measurements
  or station identifiers are included in this record.
- Earlier live `run_locally.sh all`: every module passed, including short-term validation.
  Aggregate exit status was 1 solely because final validation expects current long-term
  forecasts while the default run generated historical simulations. Independent log
  review found no other failures.
- Earlier live maintenance: all eight modules passed; aggregate exit status 0.
- Earlier separate long-term operational run: all six modules passed. Final validation
  returned 1 because its current-date expectation differs from the configured issue
  schedule; quarterly and seasonal modes were unscheduled. API readback verified monthly
  output at the configured issue date. This is not a passing aggregate-validator result.

The operational and maintenance evidence predates the final narrow recalculation fix;
those unaffected entrypoints were not broadly rerun. The corrected recalculation path,
full suite, postprocessing Docker image and API readback were verified after the fix.
Keep this umbrella issue **In Progress**: backend verification is complete and the owner
accepted the existing skips/expected failures on 2026-11-14; frontend E remains deferred
and deployment is pending.

```json
{
  "phases": {
    "A": {"depends_on": [], "parallel_agents": 1},
    "B": {"depends_on": ["A"], "parallel_agents": 1},
    "C": {"depends_on": ["B"], "parallel_agents": 1},
    "D": {"depends_on": ["B", "C"], "parallel_agents": 1},
    "E": {"depends_on": ["A"], "parallel_agents": 0, "status": "deferred"}
  }
}
```
