# Forecast Skill Evaluation — Irrigation Limit-Plan Decision (Report Draft)

**Status:** draft — experiment not yet run. Results to be filled in after the
P0–P6 implementation completes.

## Purpose

Evaluate, for every forecast horizon, every model, and every station over the
entire DB archive, how often a runoff forecast would have driven the **wrong
irrigation water-distribution plan**.

The river basin organization issues a **limit plan** (farmers receive less
water) when forecast runoff falls below a configurable fraction of the
climatological norm — default **80 % of norm** (threshold editable). Otherwise
the **normal distribution plan** applies. The decision is the binary
classification `value < THRESHOLD × norm`, applied to both the forecast and the
observed runoff against the same boundary.

Error semantics (positive class = limit-plan event):

- **False positive (false alarm):** forecast below threshold → limit plan
  imposed, but observed runoff was fine → farmers needlessly under-supplied.
- **False negative (miss):** forecast at/above threshold → normal plan run, but
  observed runoff was actually scarce → over-allocation, shortage mid-season.
  Operationally the costliest error.

## > NOTE — outcome to be added

> **The outcome of the experiment — the evaluation of false positives /
> false negatives for the limit-plan irrigation water-distribution decision —
> will be added to this report once the analysis (`apps/forecast_skill_eval`)
> has been run.** Expected contents: per-station and pooled contingency tables
> (TP/FP/FN/TN), base rates, POD / FAR / POFD / CSI / frequency bias, HSS & PSS
> vs the climatology and LR/LR_Base baselines, Wilson 95 % CIs, lead-time
> stratification (long-term), and results stratified by `norm_provenance`
> {official, aggregated_from_monthly, calculated}, plus the exclusion ledger.

## Related documents

- Planner prompt / locked requirements: `forecast_skill_eval_planner_prompt.md`
- Live-DB inventory (P0 gate): `forecast_skill_eval_inventory.md` *(pending)*
