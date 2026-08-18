## Remove monthly EM — it is vestigial, and Skilled Mean is the intended long-term aggregate (PP-059)

**Status**: Draft (2026-08-18) — **for iterative review; not ready to implement**
**Module**: `apps/postprocessing_forecasts` (`src/ensemble_calculator.py`, `src/skill_metrics.py`)
**Priority**: **Medium** — deliberate product simplification, not a defect fix.
**Labels**: `postprocessing_forecasts`, `long-term`, `ensembles`, `removal`
**Decision**: owner, 2026-08-18 — *"we remove it"*. EM was never planned for long-term horizons;
it was renamed **Skilled Mean** there.
**Supersedes the open question in**: PP-058 (which records *why* sparse monthly EM was expected).
**Sequenced after**: **PP-057** — see § Sequencing. Fixture-level removal could technically be
built first; what PP-057 gates is the *replay acceptance evidence*, not the code.

---

## What is being removed, precisely

**Monthly EM only.** Three things share the name and must not be touched:

| Keep / remove | Aggregate | Horizon | Why |
|---|---|---|---|
| **KEEP** | EM | pentad / decad | The short-term product. Unrelated. |
| **REMOVE** | EM | **month** | Vestigial — this issue. |
| **KEEP** | EM | quarter / season | A *different* thing: fixed `LR_Base` + `LR_SM` aggregate (`AGGREGATED_EM_RAW_MODELS`), no skill gate. |
| **KEEP** | Skilled Mean | month / quarter / season | The intended long-term aggregate. |
| **KEEP** | Naive Mean | all | Unrelated. |

## Two surfaces, not one

An earlier estimate called this "one guarded block". That was **wrong** — monthly EM is derived
in **two** places, and both must be handled:

### Surface A — the forecast builder

`src/ensemble_calculator.py`, inside `create_monthly_ensemble_forecasts` (from :226):

```
:290  skill_filtered     = filter_for_highly_skilled_forecasts(...)   # STRICT gate — EM only
:296  _em_use_hv         = … skill_filtered …
:305  skill_filtered_lt  = filter_for_highly_skilled_forecasts(...)   # LONG-TERM gate — Skilled Mean
:312  for df in (joint, skill_filtered, skill_filtered_lt):           # shared normalisation
:319  skill_filtered[merge_keys].drop_duplicates()
:344-356  em_avg … model_short = "EM" … _append_to_joint
:359-361  _add_skilled_mean_monthly(…, skill_filtered_lt, …)          # KEEP
:368      _add_naive_mean_monthly(…)                                  # KEEP
```

**The trap:** removing the EM block leaves `skill_filtered` (the *strict* gate) computed and
then unused, and it is referenced inside the shared normalisation loop at `:312`. The removal
must also drop that computation and adjust the loop — a naive block deletion leaves dead code
that still costs a full `filter_for_highly_skilled_forecasts` pass.

### Surface B — the skill calculator

`src/skill_metrics.py`, `calculate_monthly_skill_metrics`, section `# --- 4. Ensemble mean (EM) ---`
at `:1561`:

```
:1576-1578  exclude existing EM / Naive Mean / Skilled Mean from ensemble input
:1585-1597  em_agg_dict … groupby(ENSEMBLE_KEY) …
:1602       em_avg["model_short"] = "EM"
:1605       discard single-model compositions
:1608       compute EM skill metrics
```

This path derives **both** the monthly EM forecast rows **and** their skill rows during recalc.
Removing Surface A alone would leave the recalc still producing monthly EM — the two must go
together or the product reappears on the next `recalculate_skill_metrics`.

## Sequencing — PP-057 first, and this is not optional

PP-057 (operational path resolves the point forecast from `q50` while models write `q`) means
monthly aggregate coverage is currently **not representative**: 0/17 tjhm stations had usable
members on the operational path. Removing EM before that lands would mean:

- the "before" measurement is taken against a broken baseline, so any coverage comparison is
  meaningless;
- a reviewer cannot distinguish "EM is gone because we removed it" from "EM was never built
  because members were unresolvable".

**Land PP-057, re-measure, then remove.** *Precision correction:* PP-057 is not a semantic
prerequisite — isolated fixture tests could implement the removal first. It is a **stacking and
evidence** dependency: the replay-based acceptance criteria below cannot produce a meaningful
result until it lands, and both changes touch the same monthly builder.

## Surface C — monthly maintenance still *expects* EM

`postprocessing_maintenance_long_term.py:115` calls the gap detector with an expected-model set of
`{"EM", "Skilled Mean", "Naive Mean"}`. If generation is removed without changing this, **every
post-cutoff month becomes a permanent, never-closing EM gap**, and maintenance repeats useless
reads on each run. This must change in the same PR.

## The stale-tombstone conflict — "leave historical rows" is not achievable by default

The recommendation below to leave stored history alone **conflicts with existing recalc
behaviour**. Monthly recalc diffs existing skill rows against newly emitted ones
(`recalculate_skill_metrics.py:339`); keys that stop being emitted are written as `n_pairs=0`,
null-metric **tombstones** (`src/stale_tombstones.py:68`). So on the next recalc, historical
monthly EM skill rows are not preserved — they are blanked.

**Pick one explicitly; there is no passive option:**

1. **Preserve** — exclude EM from the stale diff so historical rows survive untouched.
2. **Tombstone** — accept the blanking, and document it as the migration policy with its date.

## Documentation contracts that must change

- `apps/postprocessing_forecasts/README.md:30` — states EM exists at all horizons.
- `doc/data_flow_long_term.md:296` — lists monthly EM in the long-term flow.

## Consumers to check before removing (not yet verified)

- `forecast_dashboard/src/db.py` has **no** monthly-path `EM` reference — good signal, but a
  **bulletin template** or tabulator column would not appear in that grep. **Verify explicitly.**
- Any saved bulletin snapshot or published `bulletin_share` payload that already contains a
  monthly EM value.
- `validate_pipeline`'s expected-model lists, if monthly EM is named there.

## The one genuine decision: existing stored rows

Options for monthly EM rows already in `long_forecasts` / `skill_metrics`:

1. **Leave them** — the database keeps a model type nothing produces any more. A later reader
   sees a series that simply stops, with no explanation in the data.
2. **Purge them** — irreversible, and they are legitimate historical output of the previous
   contract.

**Recommendation: leave them, and record the cut-off date in this issue and in the module
README**, so the stop is explainable. Purging historical output to tidy a product surface is a
poor trade.

## Acceptance criteria

- Monthly EM forecast rows are no longer produced by the operational path, the maintenance
  gap-fill, **or** `recalculate_skill_metrics`.
- Monthly **Skilled Mean** and **Naive Mean** are **semantically equal** (sorted comparison)
  before and after, on a fixture and on a replayed real month. *"Byte-identical" is withdrawn as
  needlessly brittle.*
- **Pentad and decad EM unchanged** — pinned on output key, row count, model composition, point
  value **and** quantiles. Not merely "a fixture test".
- **Quarter and season EM unchanged on both the forecast and skill paths**, asserting the exact
  fixed `LR_Base, LR_SM` composition (`ensemble_calculator.py:668`, `skill_metrics.py:2741`).
  It shares the word but not the code.
- **Monthly maintenance**: EM is no longer an expected model, and a second maintenance pass over
  the same period is a **no-op**.
- The now-unused strict-gate `filter_for_highly_skilled_forecasts` call is removed, not left dead.
- The affected tests are updated rather than deleted. *The earlier four-file list was not a
  sufficient inventory* — monthly EM behaviour is also asserted in `test_lt_min_pairs_gate.py`,
  `test_lt_skilled_mean_nse_threshold.py`, `test_monthly_skill_stale_aggregate_regression.py`,
  the maintenance/operational and recalc tests, and golden fixtures. Re-derive the full list
  before starting.
- README and `data_flow_long_term.md` updated (see § Documentation contracts).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` green.
- A documented decision on stored history — **preserve or tombstone** (§ stale-tombstone
  conflict) — with the cut-off date recorded.

## Contract not to break

- **Do not touch short-term EM.** Its NSE ≥ 0.8 gate is correct and its removal is not in scope.
- **Do not touch quarter/season EM.** Same name, different implementation
  (`AGGREGATED_EM_RAW_MODELS`, no skill gate).
- **Skilled Mean must keep its long-term gate** (`_LONG_TERM_THRESHOLD_ENV`, NSE > 0). It uses
  `skill_filtered_lt`, which is *not* the variable being removed — confusing the two would
  silently switch Skilled Mean onto the strict gate and gut it.
- Naive Mean has **no** skill gate; it must not acquire one as a side effect.

## Open questions for review

1. Should removal be **folded into the `pp_baselines_misnomer` rename**? That work already
   touches these aggregate names and surfaces; combining avoids two rounds of test churn and one
   history decision taken twice. Standalone is fine if the rename is not imminent.
2. Should monthly EM be **removed outright, or feature-flagged off first** for one cycle so a
   deployment can revert without a code change? Flagging costs little and de-risks the bulletin
   consumers we have not yet verified.
3. Does any *external* consumer (report, export, downstream analysis) read monthly EM? The
   in-repo greps are clean, but that does not cover a hydromet's own tooling.
