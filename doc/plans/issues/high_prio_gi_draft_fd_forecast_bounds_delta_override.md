## Deployment-level override to force forecast bounds to `forecast ± δ` (FD-023)

**Status**: Draft (2026-07-24)
**Module**: `apps/forecast_dashboard` (`src/processing.py`, `src/vizualization.py`, `dashboard/bulletin_manager.py`, `src/site.py`, `dashboard/bulletin_publish.py`)
**Priority**: **High — owner-set.** Note for transparency: the priority reflects an
explicit owner request, **not** a demonstrated production defect. This is an opt-in
feature request; no evidence is presented here that current bounds are wrong or unsafe
for any deployment. If prioritisation is later revisited, that is the basis.
**Labels**: `forecast_dashboard`, `bulletin`, `configuration`, `feature-flag`, `presentation`
**Requested**: 2026-07-24 by owner + colleague during debugging.
**Related**: **FD-018**, **FD-019** (bulletin target period / metadata — different concern,
but same bulletin write path, so shared regression surface). Prior work in the same
range-calculation branches introduced the missing-`delta` → `0` fallback discussed under
Q5; that fallback is **intentional**, not an accidental discovery.

---

## Requirement

Add a deployment-level `.env` switch controlling how a forecast's lower/upper bounds are
derived:

- **ON** — bounds are **always** `forecasted_discharge ± delta`, regardless of what the
  model produced.
- **OFF** (default) — preserve **today's behaviour**.

> **"Today's behaviour" is not uniformly "the model's bounds".** One of the four paths
> already ignores model quantiles entirely and uses `± delta`, and another has no `delta`
> fallback at all. Do not describe OFF as
> "use the model bounds" — see the table below.

## The four bound paths today (verified on `maxat_sapphire_2` @ `16fb9a9b`)

| # | Path | `fc_lower`/`fc_upper` source in the `delta` range mode | Feeds |
|---|------|--------------------------------------------------------|-------|
| 1 | `src/processing.py:1262-1266` (+ `else` at `:1288`) | **Always** `forecasted_discharge ∓ delta`; **never** reads `Q25`/`Q75` | short-term **table** → `src/site.py:210-214` (`forecast_lower_bound`/`forecast_upper_bound`) → published as **`fc_lower`/`fc_upper`** (`bulletin_publish.py:293-294`) |
| 2 | `src/vizualization.py:2452-2467`, `:2801-2816` | `Q25`/`Q75` **where present and non-NaN**, per-row fallback to `∓ delta` | short-term **plots** only |
| 3 | `dashboard/bulletin_manager.py:75-88` (`_reshape_long_forecast_for_bulletin`) | Renames **`Q25`→'Forecast lower bound'**, **`Q75`→'Forecast upper bound'** | **long-term** bulletin → `src/site.py:247-254` (`forecast_q_min`/`forecast_q_max`) → published as **`q_min`/`q_max`** (`bulletin_publish.py:283-284`) |
| 4 | `src/vizualization.py:3075-3086` in `create_forecast_summary_table` (`:3003`) | For `horizon in (month, quarter, season)`: `fc_lower`/`fc_upper` set **directly from `Q25`/`Q75`, with NO delta fallback** — missing quantiles leave them **NaN**. For short-term it **delegates to path 1**. | long-horizon **summary table**, which feeds bulletin selection |

Four consequences the implementer must know up front:

1. **Short-term and long-term use different attributes and different published fields** —
   `fc_lower`/`fc_upper` vs `q_min`/`q_max`. They are *not* one code path with one output.
2. **Overriding long-term discharge bounds also changes published VOLUME bounds.**
   `src/site.py:253-254` derives `forecast_v_min`/`forecast_v_max` as
   `q × days_in_month × 86400 / 1e6` directly from the discharge bounds. Any change to
   `q_min`/`q_max` silently propagates into the volume figures in the bulletin.
3. **The table and the plot can disagree today** — but only **where `Q25`/`Q75` exist and
   are non-NaN** for that row. It is a conditional divergence, not a guaranteed one.
4. **Path 4 has no delta fallback at all.** Unlike paths 1–2, the long-horizon summary
   table leaves bounds NaN when `Q25`/`Q75` are absent. It also uses the
   `fc_lower`/`fc_upper` names **for long-horizon data**, so field naming alone does not
   tell you which horizon a code site is serving. Because it feeds bulletin selection, it
   must be in scope, in the adapter design, and in the OFF/ON golden tests.

## Design questions for the owner (answer before implementing)

**Q0 — statistical meaning (needs domain-owner sign-off).** `delta` is **not** a
model-derived predictive interval. It is `0.674 × std` of historical discharge
(`postprocessing_forecasts/src/aggregation.py:138`, `:201`) and is used as the
**accuracy tolerance** (a forecast counts as accurate when `|error| ≤ delta`,
`src/skill_metrics.py:~394`). `0.674σ` resembles a central-50% half-width only under
strong distributional assumptions, and centring a **climatological** spread on each
model's forecast does not make it a calibrated forecast interval. So: are these limits
intended as an **operator tolerance band**, a **climatological variability band**, or a
**probabilistic uncertainty interval**? The published label must match the answer.

**Q1 — scope.** Short-term only, long-term only, or both? Paths 1–4 span both, with
different output fields (`fc_lower`/`fc_upper` vs `q_min`/`q_max`) — and note path 4 uses
the `fc_*` names for long-horizon data.

**Q2 — what does OFF mean, given paths 1–4 differ?** (a) freeze today's per-path
behaviour exactly (bug-compatible), or (b) define OFF as "model quantiles where
available, else `± delta`", which **also changes** the short-term table. (b) is a
behaviour change needing its own sign-off.

**Q3 — interaction with the range selector.** The UI offers `delta`, `Manual range,
select value below`, and `min[delta, %]` (`src/processing.py:1264-1289`). Does the switch
(i) apply only inside the `delta` branch, (ii) force that branch and hide the selector, or
(iii) override all branches? (ii)/(iii) remove operator choice — a product decision.

**Q4 — when is the override applied?** At bulletin **add** time, at **display** time, or
at **publish** time? And what happens to (a) already-persisted bulletin snapshots, and
(b) bounds a hydrologist **manually edited** in the tabulator
(`bulletin_manager.py:~898-910` handles in-cell edits)? A display-time override would
retroactively reinterpret stored bulletins and could overwrite operator edits.

**Q5 — missing delta, two distinct cases.**
- *Absent or all-NaN column*: both paths set `delta_offset = 0`
  (`processing.py:1262`, `vizualization.py:2452`) — an **intentional** prior fallback. With
  the switch ON this collapses bounds onto the point forecast (zero-width interval).
- *Partially NaN column*: `has_delta` is true, so a NaN `delta` yields NaN
  delta-fallback bounds — **but only where the corresponding quantile is also
  unavailable**. In path 2 an available `Q25`/`Q75` masks the missing `delta` entirely;
  in path 4 the quantile is used regardless. So "partially missing delta" is visible in
  different places depending on quantile coverage.
Specify both: fail loud, fall back to quantiles, or emit NULL?

**Q6 — validation and clamping.** Negative `delta`, non-finite `delta`, `Q25 > Q75`, and
`forecast − delta < 0` (low-flow stations) are all reachable. Clamp at zero, reject, or
publish as-is? Today's `delta` branch does not clamp.

**Q7 — long-term volume bounds.** Must `forecast_v_min`/`forecast_v_max` be recomputed
from the overridden discharge bounds (consequence 2 above)? Presumably yes — state it.

## Proposed shape (subject to Q0–Q7)

- **Name.** `SAPPHIRE_FORECAST_BOUNDS_FROM_DELTA`. This follows the boolean feature-flag
  style of `SAPPHIRE_SKILL_LEAD_AWARE` / `SAPPHIRE_SKILL_PROB` — offered as a
  **convention choice**, not a verified repo-wide rule (behaviour-controlling
  `ieasyhydroforecast_*` variables do exist).
- Parse with the **same explicit truthy/falsey token helper** as
  `iEasyHydroForecast/skill_lead_aware_flag.py:29-60`, so a typo raises instead of
  silently resolving to OFF.
- **Default OFF**, so merging changes nothing until a deployment opts in.
- **One schema-neutral primitive plus per-path adapters** — *not* a single function with
  the short-term signature. A resolver taking `(forecast, delta, q25, q75, mode)` and
  returning two canonical Series can serve all four paths, but each path needs its own
  adapter because long-term has no `range_type`/slider, uses different column names, and
  is renamed into gettext display columns before site hydration. **Apply it before the
  gettext rename and before the volume derivation**, or the long-term path will be
  overridden too late to affect `v_min`/`v_max`.

## Acceptance criteria

- Flag **OFF** ⇒ byte-identical output for **all four paths** (short-term table,
  short-term plots, long-term bulletin incl. `q_min`/`q_max` **and** `v_min`/`v_max`,
  and the long-horizon summary table), pinned by a golden test per path.
- Flag **ON** ⇒ `fc_lower/fc_upper` (and `q_min/q_max` if in scope) equal
  `forecasted_discharge ∓ delta` for every in-scope path, **including when `Q25`/`Q75`
  are present and non-NaN** — that is the whole point.
- Long-term volume bounds are consistent with the overridden discharge bounds.
- Q5's two missing-delta cases behave as decided, each with a test; no silent zero-width
  interval.
- Q4's decision is tested: persisted bulletin snapshots and manually edited bounds behave
  as specified on reload.
- End-to-end verification through `bulletin_publish`, not only in the table.
- A typo'd flag value raises.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` green.
- Documented in the deployment runbook with the per-path table above.
- Path 4's no-fallback behaviour is explicitly decided: with the flag ON, does a
  long-horizon row lacking `delta` produce NaN bounds or fail loud?

## Contract not to break

- Default-OFF must be a **true** no-op — this is presentation logic feeding published
  bulletins; an unintended change alters what hydrologists publish.
- Do not silently "fix" the table/plot divergence as part of this work — that is Q2(b)
  and needs separate sign-off.
- Do not change `delta`'s definition or its role in the accuracy metric; this issue only
  changes how bounds are *presented*.
