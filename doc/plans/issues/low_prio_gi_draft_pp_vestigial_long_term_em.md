## Long-term EM is vestigial — Skilled Mean is the intended aggregate; decide whether monthly EM should still be produced (PP-058)

**Status**: Draft (2026-08-18)
**Module**: `apps/postprocessing_forecasts` (`src/ensemble_calculator.py`)
**Priority**: **Low** — no defect, no wrong data. This is a product-surface question.
**Labels**: `postprocessing_forecasts`, `long-term`, `ensembles`, `naming`
**Related**: PP-057 (real defect, same symptom). `pp_baselines_misnomer` (EM / Naive Mean /
Skilled Mean are ensemble aggregates, not baselines — module-wide rename pending).

---

## Question closed: "monthly EM is starved" is NOT a defect

The 2026-08-14/17 review recorded kghm monthly **EM at 22 rows against ~95 for every other
model**, survived a full-history recalc, and flagged it for triage. Two candidate explanations
were investigated and both are now settled:

1. **PP-030 excludes EM from re-derivation** — **refuted** by out-of-loop review: PP-030's
   `exclude_models=["EM"]` is wired only for pentad/decade; the monthly recalc re-derives EM
   normally (`recalculate_skill_metrics.py:320-361`).
2. **The strict skill gate suppresses it** — **true, and measured**, but not a problem:

   | Gate | kghm monthly keys `(code, month_in_year, lead)` with ≥2 qualifying models |
   |---|---|
   | current EM gate (`sdivsigma≤0.6, NSE≥0.8, accuracy≥0.8, n_pairs≥4`) | 571 / 2 592 (**22.0%**) |
   | NSE>0 gate | 2 161 / 2 592 (**83.4%**) |

**Owner resolution (2026-08-18): EM was never planned for long-term forecasts. It was renamed
to Skilled Mean for the long-term horizons.** So the intended long-term aggregate is **Skilled
Mean**, which *already* admits on NSE > 0 via the long-term override block
(`src/skill_metrics.py:~105`, shipped in PR #405):

```python
# The long-term "Skilled Mean" pool admits ALL long-term models with NSE > 0.
_LONG_TERM_NSE_EPSILON = 1e-9
_LONG_TERM_THRESHOLD_ENV = {
    "nse":       ("ieasyhydroforecast_nse_threshold_long_term", str(_LONG_TERM_NSE_EPSILON)),
    "sdivsigma": ("ieasyhydroforecast_efficiency_threshold_long_term", "False"),
    ...
}
```

**Sparse monthly EM is therefore expected**: it is a strict-gated aggregate that is not the
long-term product. No threshold change is required — an earlier revision of this file proposed
one and was wrong.

## Residual question — should long-term EM be produced at all?

Monthly EM is still computed and written, and it still appears alongside Skilled Mean. That
leaves a product surface with two aggregates where one was intended:

- **Monthly EM**: strict short-term gate, ≥2 qualifying models. Sparse by construction.
- **Monthly Skilled Mean**: long-term gate (NSE > 0), inverse-MAE weighted. The intended product.

Options, for the owner:

1. **Leave it.** Cheapest. Cost: a sparse, strict-gated aggregate sits next to the intended one,
   and anyone reading § 9.6 output will keep re-discovering "EM looks starved" — as this review
   did.
2. **Stop producing monthly EM.** Cleanest product surface. Needs a decision on existing stored
   rows and on any dashboard/bulletin consumer that reads monthly EM today.
3. **Keep producing it but document it as non-primary**, so its sparsity is not read as a fault.

**Not urgent.** Recording it so the next reviewer does not spend the same effort re-deriving
that sparse monthly EM is expected.


## Owner decision 2026-08-18 + removal cost estimate

**Decision: "Not harmful, but not very useful — can be removed if the work is small."** So the
question is cost. Scoped:

**Contained.** Monthly EM is one block inside `create_monthly_ensemble_forecasts`
(`src/ensemble_calculator.py:226`), which also produces the other two aggregates via
`_add_skilled_mean_monthly` and `_add_naive_mean_monthly`. Removing EM means deleting/guarding
the `em_avg["model_short"] = "EM"` block, **not** deleting a function — Skilled Mean and Naive
Mean must keep working from the same call.

**Two callers only**, both passing through the same builder:
```
postprocessing_operational_long_term.py:168
postprocessing_maintenance_long_term.py:195
```

**No dashboard consumer found** for monthly EM (`forecast_dashboard/src/db.py` has no `EM`
reference on the monthly path) — so the display surface is likely unaffected. **Verify before
removing**; a bulletin template or tabulator column would not appear in that grep.

**Four test files reference monthly EM** and would need updating:
`test_monthly_ensemble_creation.py`, `test_monthly_skill_metrics.py`,
`test_monthly_skill_per_lead.py`, `test_monthly_workflow_integration.py`.

**Estimate: small** — one guarded block, two call sites unchanged, four test files, plus a
decision on stored history.

### The one real decision inside it

**What happens to existing stored monthly EM rows?** Options: leave them (the database keeps a
model type nothing produces any more — confusing later), or purge them (irreversible, and they
are legitimate historical output of the old contract). Recommend **leaving them and recording
the cut-off date**, so a reader can tell why the series stops.

### Recommendation

**Do it as part of the `pp_baselines_misnomer` rename**, not standalone. That work already
touches these aggregate names and surfaces; folding the removal in avoids two separate rounds of
test churn and one migration decision made twice. Standalone is fine if the rename is far off.

> Ordering note: PP-057 should land **first**. Until the operational path can resolve `q`,
> monthly aggregate coverage is not representative, and any before/after comparison of removing
> EM would be measured against a broken baseline.

## Explicit non-scope

- **Quarter/season EM is a different thing again** — a fixed `LR_Base + LR_SM` aggregate
  (`AGGREGATED_EM_RAW_MODELS`, `src/model_names.py:14`) that bypasses the skill filter entirely.
  It is unaffected by any of the above.
- **Short-term EM is unaffected** and its NSE ≥ 0.8 gate must stay — the long-term override
  block is explicitly commented "LONG-TERM ONLY".
- **PP-057 is the real defect here** and is independent: the operational path resolves the point
  forecast from `q50` only while the models write `q`, so EM, Naive Mean *and* Skilled Mean lose
  their members entirely. Fixing that is what will actually restore long-term aggregates.
