## Deferred point-resolution inventory after the merged long-term fallback fix (PP-057)

**Status**: Draft — remaining audit deferred (2026-11-26)
**Module**: `apps/postprocessing_forecasts`
**Priority**: Low — the original high-priority operational defect is resolved;
only the broader, unimplemented inventory remains open.
**Merged fix**: [PR #551](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/pull/551),
merged into `maxat_sapphire_2` on 2026-11-26 (`d614313e`).
**Related**: [PP-076 — complete](archive/review_gi_draft_pp_monthly_maintenance_mixed_issue_dates.md),
PP-045, INFRA-026.

## Completed scope

The active long-term readers/normalizers, monthly and quarter/season builders,
monthly display pivot, and maintenance point coercion now use the shared
`src/point_forecast.py` resolver. `q` remains authoritative with per-row `q50`
fallback; existing nonnull derived points and quantile columns are preserved.
The historical resolver remains available from `src/skill_metrics.py` without
changing its semantics. Quarter/season normalization retains its malformed-`q`
coercion policy. PP-076, merged in the same PR, resolves the separate monthly
maintenance issue-date selection problem; it is no longer an open dependency.

Validation included 25 focused point-fallback regressions, the final affected
suite (3,303 passed, zero skips, one existing xfail), full repository tests
(9,736 passed, 12 previously accepted skips, two existing xfails), independent
review, and Docker smoke. The full development daily run passed all 18 execution
stages, including 27 scheduled long-term model executions, monthly skills, and
operational/maintenance postprocessing. Monthly maintenance wrote two missing
ensemble rows; readback preserved existing finite counts and month_0 coverage.
Boundary-date validation still reports the separately tracked INFRA-027 LR
endpoint mismatch; this is not an entirely green data-health validation.

## Remaining scope — deferred audit only

- Reconcile the historical ten-site inventory below against current code before
  proposing any further change. It is not evidence that ten defects remain.
- Check the legacy `aggregate_monthly_fc_to_quarterly` helper and any remaining
  coercion sites for the actual frame semantics and reachability. The merged PR
  did not change this helper or the complete-quarter policy.
- Do not blanket-apply long-term point semantics to short-term frames. Any
  demonstrated residual defect needs its own minimal proposal and regression
  evidence; no further implementation is approved by this cleanup.

The historical replay goals below were not all exercised and are not claimed as
complete. No server deployment or default-image promotion is implied by the merge.

## Historical findings and proposed broader scope

The material below records the pre-fix diagnosis and earlier acceptance proposals.
Its code line numbers and present-tense descriptions are historical; the completed
scope and deferred audit above describe the current status.

## The drift, in three lines of code

**What the long-term model writes** — `apps/long_term_forecasting/lt_utils.py:357-361`:
```python
# Main model output: Q_{model_name} -> q
if q_model_col in row.index and pd.notna(row.get(q_model_col)):
    record["q"] = float(row[q_model_col])
```

**What the historical / skill path reads** — `src/skill_metrics.py:1369`:
```python
def _resolve_forecasted_discharge(df: pd.DataFrame) -> pd.Series | None:
    """Resolve the point forecast column from q or q50.
    Priority: q first (authoritative point forecast from the model), ...
```

**What the operational / maintenance ensemble path reads** —
`src/data_reader.py:1586` and `src/ensemble_calculator.py:275`:
```python
# Add forecasted_discharge from q50 if missing
if "forecasted_discharge" not in df.columns and "q50" in df.columns:
    df["forecasted_discharge"] = df["q50"].astype(float)
```

**`q` is never consulted on the operational path.** The correct resolver already exists and is
used by the historical path only — textbook implementation drift, not a missing feature.

## Live confirmation (tjhm, 2026-07-01, monthly)

```
member rows                         : 384
  q populated, q50 NULL             : 143     <-- invisible to the operational reader
  both populated                    :   0
  q NULL, q50 populated             :   0
  neither                           : 241

stations with >=2 usable members by q    (skill path sees)  : 17/17
stations with >=2 usable members by q50  (ensembles see)    :  0/17
```

**Not a single stored long-term member row on this deployment has `q50`.** The operational
ensemble builder therefore sees zero usable members at every station, while the skill path sees
a complete set at every station.

## Why it suppresses all three aggregates at once

`forecasted_discharge` is resolved **before** any aggregate-specific gate. With it NULL, every
aggregate loses its members simultaneously:

| Aggregate | Its own gate | Why it still produces nothing |
|---|---|---|
| **EM** | ≥2 models past skill thresholds + `min_pairs` | no non-null point members to admit |
| **Naive Mean** | **no skill gate at all**, ≥2 non-null members | no non-null point members |
| **Skilled Mean** | NSE>0, `min_pairs`, inverse-MAE weights | no non-null point members |

This is the diagnostic signature: an explanation based on *skill gating* cannot account for
**Naive Mean** being absent, because Naive Mean has no skill gate. A missing point column can.

**It also explains why `maintenance:postprocessing_long_term` runs clean and changes nothing** —
observed 2026-08-17: PASS in 44s, zero errors, byte-identical ensemble counts before and after.
It found members, could not resolve their point value, and had nothing to build.

## Why some ensembles exist anyway

Ensembles *do* exist for some stations/dates (tjhm 07-01: 10 of 17 stations). Those are
consistent with having been written by the **recalc / historical path**, which resolves `q`
correctly. So the database shows a mixture: dates touched by a recalc have ensembles, dates
touched only by the operational/maintenance path do not.

**This must be checked before assuming a clean fix** — a fix will change what the operational
path produces, and the two populations should be reconciled deliberately.


## Refinement 2026-08-17 — scope is 10 call sites, and it affects BOTH deployments

### The codebase already documents that `q50` is rarely populated

`_resolve_forecasted_discharge` (`src/skill_metrics.py:1369`) — the *correct* implementation —
says so in its own docstring:

```python
"""Resolve the point forecast column from q or q50.
Priority: q first (authoritative point forecast from the model),
q50 as fallback (median quantile, rarely populated).
"""
```

It is used in exactly **two** places, both on the historical/skill path
(`skill_metrics.py:1488`, `skill_metrics.py:2695`). Ten other sites coerce from `q50` only.

### Both organisations are affected, not just tjhm

| Org / horizon | member rows | with `q` | with `q50` |
|---|---|---|---|
| tjhm month (2026-07-01) | 384 | 143 | **0** |
| kghm month (Jun–Sep 2026) | 3819 | 2416 | **211** (5.5%) |
| kghm quarter (Jun–Sep 2026) | 1170 | 670 | **42** (3.6%) |
| kghm season | 0 | 0 | 0 (no members stored) |

So on kghm roughly **94–96% of stored long-term members are invisible** to the operational
ensemble path. This is not a tjhm configuration quirk — it is the normal shape of the data on
both deployments, which is what the resolver's docstring already implies.

**This also becomes the leading explanation for the kghm `2026-08-10` gap** (61 stations with
members, zero ensembles at a *configured* issuance) — previously unexplained, and not accounted
for by the asymmetric date-stamping (H2b), which explains only `07-07` and `07-23`.

### Full inventory of `q50`-only coercion sites

```
src/aggregation.py:305-306
src/ensemble_calculator.py:276-278          <- monthly builder
src/ensemble_calculator.py:637-639          <- aggregated (quarter/season) builder
src/data_reader.py:1587-1588                <- read_latest_monthly_forecasts
src/data_reader.py:1745-1746
src/data_reader.py:3348-3349
src/data_reader.py:3357-3358
src/data_reader.py:3777-3778
src/postprocessing_tools.py:261-263
postprocessing_maintenance_long_term.py:189-190
```

**Ten sites, not two.** The fix is a systematic replacement, which changes both the effort and
the risk profile from what this issue originally implied.

### Consequences for how to fix it

1. **Relocate the resolver.** It currently lives inside `skill_metrics.py`; a shared helper
   (e.g. `src/point_forecast.py`) avoids `ensemble_calculator` and `data_reader` importing from
   the skill module.
2. **Do not blanket-replace.** Some of the ten sites may be on short-term paths where `q`/`q50`
   semantics differ or `q` is absent entirely. Each site needs checking that `q` is the right
   column for the frame it handles — a global find-and-replace is the obvious wrong move here.
3. **Quarter/season confirmed in scope** — `ensemble_calculator.py:637-639` is the aggregated
   builder feeding quarter and season EM, so PP-057 is not monthly-only.
4. **Season is unverifiable on current data** — kghm has **zero** stored season members, so any
   season fix must be tested with a constructed fixture rather than replayed from the database.

### Revised acceptance criteria (supersede the originals)

- All ten sites resolve `q` first with `q50` fallback, via one shared helper.
- Each site individually justified in the PR description as handling long-term frames where `q`
  is authoritative — no blanket replacement.
- Fixture with `q` populated / `q50` NULL yields identical aggregates to a `q50`-populated one,
  at **month, quarter and season**.
- Replaying tjhm 2026-07-01: 17/17 stations gain ≥2 usable members and
  `maintenance:postprocessing_long_term` creates the missing ensembles.
- kghm month/quarter ensemble counts increase materially (94–96% of members currently unusable).
- Rows previously written by the recalc path are not duplicated or contradicted.

## What to inspect

1. `src/data_reader.py:1586` and `src/ensemble_calculator.py:275` — replace the `q50`-only
   coercion with the existing `_resolve_forecasted_discharge` semantics (`q` first, `q50`
   fallback). Confirm there are no other `q50`-only sites: grep for
   `"q50" in .*columns` across `postprocessing_forecasts`.
2. Whether quarter and season paths share the same coercion (the observed data is monthly; the
   same helper style appears in the aggregated builders).
3. Whether any consumer *depends* on `forecasted_discharge` meaning `q50` specifically —
   `q50` is the distribution median, `q` the model's authoritative point forecast. They are not
   the same quantity, and silently preferring the wrong one is how this class of bug recurs.

## Why the tests do not catch it

Existing fixtures construct forecasts with `forecasted_discharge` or `q50` already populated
(`tests/test_monthly_ensemble_creation.py:58`, `tests/test_maintenance_long_term.py:44`), so
the coercion branch is never exercised against a realistic `q`-only row.

## Acceptance criteria

- A fixture with **`q` populated and `q50` NULL** produces EM / Naive Mean / Skilled Mean
  exactly as an equivalent `q50`-populated fixture does.
- Replaying the tjhm 2026-07-01 state yields ≥2 usable members for all 17 stations, and
  `maintenance:postprocessing_long_term` then creates the missing ensembles.
- Rows already written by the recalc path are unchanged (no duplicate/conflicting aggregates).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` green.

## Contract not to break

- **Do not "fix" this by making the long-term writer populate `q50`.** `q` is the authoritative
  point forecast and `q50` is the distribution median; conflating them corrupts the meaning of
  both. The reader is what is wrong.
- The historical/skill path is already correct — leave `_resolve_forecasted_discharge` alone and
  reuse it.
