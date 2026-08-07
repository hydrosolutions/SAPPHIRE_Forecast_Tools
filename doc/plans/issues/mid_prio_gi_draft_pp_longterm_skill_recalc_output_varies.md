## INVESTIGATION: long-term skill recalc output varies across identical invocations — unordered paginated input not yet excluded (PP-049)

**Status**: Draft — **investigation, not a confirmed independent defect** (2026-07-23)
**Module**: `apps/postprocessing_forecasts` (`recalculate_skill_metrics.py`, `src/data_reader.py`, `src/skill_metrics.py`, `src/stale_tombstones.py`)
**Priority**: **Medium (investigation)** — see "Why not High (yet)".
**Blocked on / linked to**: **ML-007** (non-deterministic API pagination). Until ML-007
is excluded as the cause, this issue must not be treated as an independent defect.
**Labels**: `postprocessing`, `skill-metrics`, `long-term`, `non-determinism`, `investigation`
**Discovered**: 2026-07-23 local pipeline health review (taj + kyg, `maxat_sapphire_2` @ `16fb9a9b`), Phase E idempotency check.
**Related**: **PR #411** (min-n floor + `build_stale_tombstones`), **PP-041**, **PP-042**.

> **Framing corrected after out-of-loop review.** An earlier version of this draft
> claimed a confirmed idempotency defect with "identical inputs", called it
> "pre-existing on trunk", and named `build_stale_tombstones` as prime suspect. **All
> three were unsupported** — see "What was wrong with the first draft".

---

## Observation

Running `recalculate_skill_metrics` twice with the **same command and environment**, with
no other pipeline activity, produced different long-term skill values.

| Dataset | taj added/changed/removed | kyg added/changed/removed | Observed |
|---------|---------------------------|---------------------------|----------|
| skill_pentad | 0 / 0 / 0 | 0 / 0 / 0 | no differences in sampled passes |
| skill_decade | 0 / 0 / 0 | 0 / 0 / 0 | no differences in sampled passes |
| skill_season | 0 / 0 / 0 | 0 / 0 / 0 | no differences in sampled passes |
| **skill_month** | 0 / **169** / 0 | **1** / **873** / 0 | **differs** |
| **skill_quarter** | 0 / **10** / 0 | 0 / **102** / 0 | **differs** |

Detail (taj, actual values): **1,536 changed metric values, none within 1e-6 relative**
— i.e. not float noise. Key set stable across the compared passes (911 keys, 0 added,
0 removed). Rows flip between populated and tombstoned: `n_pairs 0.0 → 6.0`, metrics
`value → None` and `None → value` (283 `n_pairs` transitions in one pass).

Changed rows span **raw models as well as ensembles** (MC_ALD 26, Skilled Mean 21,
LR_SM_DT 21, GBT 20, LR_SM_ROF 20, SM_GBT_Norm 19, LR_Base 13, SM_GBT 10, SM_GBT_LR 9,
LR_SM 5, EM 3, Naive Mean 2).

Across three consecutive flag-ON passes (taj) the **populated count decreased**:

| Pass | tombstones | populated |
|------|-----------:|----------:|
| 1 | 534 | 377 |
| 2 | 573 | 338 |
| 3 | 638 | 273 |

Pass 1 vs pass 3 differ by 1,542 values, so the state did not return to its start. This
is an **observation from three passes** — it rules out a period-2 cycle from that
starting state; it does **not** establish monotonic continuation, another cycle, or a
causal mechanism.

## Why this is NOT yet a confirmed idempotency defect

Two independent reasons the premise "identical inputs" fails:

1. **The recalc reads unordered paginated input.** `_read_long_forecasts_api`
   (`src/data_reader.py:1446`) paginates per code in 1,000-row batches. Per-code reads
   are only safe while each filtered result is ≤1,000 rows; a multi-year, multi-model,
   multi-lead result routinely exceeds that. Per **ML-007**, the postprocessing service
   paginates with `offset(skip).limit(limit)` and **no `ORDER BY`** (0 of 6 paginated
   list readers in `postprocessing/app/crud.py` order their results, versus 4 of 4 in
   `preprocessing/app/crud.py`). Unstable input pagination alone can change which groups
   clear the min-pair floor, which would produce exactly the observed pattern.
2. **The recalc mutates the store it reads.** Monthly forecasts are read
   (`recalculate_skill_metrics.py:312`), regenerated monthly forecasts are saved
   (`:330`), and the quarterly path then reads the same long-forecast store (`:381`).
   So "no intervening pipeline activity" does **not** mean invariant inputs — the
   command changes its own input state.

### What the control did and did not prove

A control was run: two consecutive **skill-metric captures with no recalc between** →
912 keys / 912 keys, 0 added, 0 removed, 0 value differences. That proves only that the
**output measurement** is stable (the skill result set fits a single page). It does
**not** control the recalc's **input** reads, which is the gap that matters.

A parallel attempt to measure `long_forecasts` over 2000–2026 was **discarded**: its own
control (two captures, no recalc) differed by 15,553/18,068 rows, i.e. that measurement
was invalidated by ML-007. **No claim is made about `long_forecasts` row counts.**

> Key-count note: compared passes show 911 keys; the later control shows 912. One key
> was added by recalc passes run between those captures. The 911↔911 and 912↔912
> comparisons are each internally consistent.

## Candidate mechanisms (ordered; none verified)

1. **Unordered paginated forecast input (ML-007) — leading candidate.** Different input
   row sets per run → different group membership → different `n_pairs` and metrics.
   Must be excluded first.
2. **Run-order self-mutation.** Monthly writes occur before quarterly reads within a
   single invocation (see above), so pass N's writes are pass N+1's inputs.
3. **Persistence/amplification via `build_stale_tombstones` (PR #411).** *Not* an
   initiating mechanism: it runs **after** skill calculation and anti-joins existing
   against emitted keys (`src/stale_tombstones.py:220`). It cannot itself change newly
   emitted raw-model metrics or `n_pairs` — but it can **persist** a varying emission
   set as tombstones, converting transient omissions into durable-looking loss.
4. **PP-042 (display-form vs DB-form exclusion) — quarter/season only.** Quarter/season
   use display-form-only exclusion (`src/skill_metrics.py:2666`), so DB-form aggregate
   rows can pass. This is a plausible contributor to the **quarter** differences. It does
   **not** explain the monthly result, because the monthly path canonically removes
   stored EM/Naive/Skilled rows before scoring (`src/skill_metrics.py:1467`).
5. **PP-041 (stale ensemble forecast rows).** Narrow and secondary for the same reason —
   monthly canonical exclusion means surviving stale aggregates cannot explain monthly
   raw-model churn. Applicable only to paths that leave such rows in the scored input.

## Not exclusive to flag-ON

Two identical **flag-OFF** passes also differed (2,884 value differences) on revision
`16fb9a9b`. That supports **"observable with the flag disabled in this code/DB state"**.

It does **not** prove "pre-existing on trunk": the tested revision contains the
lead-aware implementation, and the database still held rows written by earlier flag-ON
passes. Establishing a trunk baseline needs a clean revision + restored snapshot, or
commit bisection. Flag-OFF populated counts moved 561 → 578 (two passes only — **no
trend claimed**, and no claim that the flag doesn't affect severity or direction).

**Practical consequence:** this issue does **not**, on current evidence, block enabling
`SAPPHIRE_SKILL_LEAD_AWARE` — but it is also not evidence that enabling it is neutral.

## Why not High (yet)

High would be justified if deterministic inputs still produced progressive loss of valid
skill rows. Today the strongest explanation is a **known** pagination defect (ML-007),
and the user-impact/compounding arguments are inferred from uncontrolled runs. Promote to
High on either: (a) a pagination-safe reproduction that still shows persistent
state-driven drift, or (b) production evidence of skill tiles actually disappearing
across recalcs.

## Required next step — controlled reproduction

The single experiment that decides this:

1. Restore a DB snapshot; capture the recalc's **input** frames (per code, with row
   counts, unique keys, duplicate and missing keys per page) — not just its output.
2. Run recalc → capture skill output.
3. **Restore the same snapshot again**, re-run with identical env, capture again.
4. Compare. If outputs now match, the variation is driven by mutated/unstable persisted
   input (ML-007 and/or self-mutation) and this issue folds into ML-007. If outputs
   still differ from byte-identical input frames, an independent non-determinism exists
   and this issue stands on its own — at that point re-scope and re-prioritise.

Pin explicitly during the experiment:

- `SAPPHIRE_RECALC_START_YEAR`, **and** `SAPPHIRE_SKILL_METRICS_START_YEAR` — the latter
  **takes precedence** (`src/skill_metrics.py:2167`), so pinning only the former can
  still yield a different start year between passes. Pin both to the same value, or
  confirm `SAPPHIRE_SKILL_METRICS_START_YEAR` is unset in the shell **and** in the
  deployment `.env`.
- The **end year**, which defaults from today (`recalculate_skill_metrics.py:302`) — a
  pass either side of a year boundary would differ for that reason alone.
- Target skill year, station configuration, and the clock/date.

## Acceptance criteria (once the cause is known)

- Both start-year variables pinned (or `SAPPHIRE_SKILL_METRICS_START_YEAR` confirmed
  unset) and the end year fixed, so the window is provably identical across passes.
- From a restored snapshot, two identical recalcs produce **zero** differences in
  `skill_metric` rows for all horizons — added, changed, removed all 0 — verified with a
  pagination-safe read (per-code within one page, or after ML-007's `ORDER BY` fix).
- N ≥ 3 identical passes leave populated/tombstoned counts constant.
- Verified for both flag states and both deployment config shapes (taj month leads
  {0,1,2}, quarter {0}; kyg month {0,1,2,3}, quarter {1}).
- Short-term (pentad/decad) shows no differences — as observed today; must not regress.
- Regression test asserting recalc idempotency on a fixture, placeholder station codes only.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` green.

## What was wrong with the first draft (recorded so it isn't repeated)

- Claimed **"identical inputs"** — false; the recalc mutates its own input store, and its
  input reads are unordered-paginated.
- Claimed the pagination control was sufficient — it validated only the **output** read.
- Named **`build_stale_tombstones` as prime suspect** — it runs after calculation and
  cannot initiate changed raw-model metrics.
- Claimed **"pre-existing on trunk"** from two flag-OFF passes — too strong.
- Cited **PP-041/PP-042** as broad explanations — monthly canonical exclusion rules both
  out for the monthly observation.
- Rated **High** on uncontrolled evidence.
