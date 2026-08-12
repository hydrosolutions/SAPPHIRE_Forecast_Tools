# DOC-005: Long-term deploy runbook's Phase 4 stop gate does not verify `skill_metrics`

**Priority**: Medium
**Module**: doc, ltf, pp
**Status**: Draft
**Created**: 2026-08-12

## Problem

`doc/prod/long_term_deploy_runbook.md` § "Phase 4 — Verify" is explicitly a
**STOP GATE for cleanup** ("do not start Phase 5 until 1–3 pass") and is also
what the runbook's `SAPPHIRE_SKILL_LEAD_AWARE` enablement section delegates its
own verification step to ("4. Verify per-lead rows exist (aggregate-only,
sentinel codes — see Phase 4)"). Its three checks are:

1. Bucket presence + date-range span in `long_forecasts` (taj
   `QUARTER hv=0`/`SEASON hv=0`; kyg `QUARTER hv=1`/`SEASON hv ∈ {3,2,1,0}`).
2. `EM = mean(LR_BASE, LR_SM)` composition join, also against `long_forecasts`.
3. Seasonal bulletin render smoke check.

All three read (or exercise) `long_forecasts` only. Confirmed by reading both
the runbook and the SQL it references in
`doc/prod/ppipe_ensemble_hv_deploy_runbook.md` § "Aggregate-Only Verification"
(lines 154-272): every query in that section targets `long_forecasts`. There
is no `skill_metrics` query anywhere in either document, and no query that
checks monthly coverage (the gate's bucket checks are scoped to
`horizon_type IN ('QUARTER', 'SEASON')` only).

## Why this matters

- A deployment can pass the Phase 4 gate — and therefore proceed to Phase 5
  cleanup — while `skill_metrics` rows for month/quarter/season still carry the
  pre-lead-aware, single-lead convention. The dashboard skill tiles read
  `skill_metrics`, not `long_forecasts`, so an operator who has "ticked every
  box" in the runbook can still ship a deployment whose skill display blends
  old- and new-convention rows.
- This gap is separate from, and would not be caught by, **PP-051** (a sibling
  draft filed alongside this one) — even if the recalc's silent-failure defect
  there is fixed, Phase 4 as written still wouldn't verify what actually landed
  in `skill_metrics`, only that the process reported success and that
  `long_forecasts` looks right.
- The monthly horizon is not checked at all by Phase 4 (only QUARTER/SEASON
  buckets are in the expected-buckets query), so a partial or failed monthly
  skill recalc is invisible to this gate even for `long_forecasts`, let alone
  `skill_metrics`.

## What a sufficient check would need to establish

Documented here as requirements, not as committed SQL — see "Ownership note"
below.

1. **Per-lead `horizon_value` presence on `skill_metrics` rows for the
   configured leads.** For each deployment's configured
   `operational_month_lead_time` values (per the runbook's own per-deployment
   parameter table — e.g. kyg quarter `hv=1`, season `hv ∈ {3,2,1,0}`; taj
   quarter `hv=0`, season `hv=0`), confirm `skill_metrics` rows exist at those
   exact `horizon_value`s, not just `long_forecasts` rows.
2. **Monthly coverage across the full recalc window**, mirroring the
   existing `long_forecasts` bucket check's `MIN(date) <=
   <SAPPHIRE_RECALC_START_YEAR>-01-01` / `MAX(date) >= <latest operational
   issue date>` acceptance criterion, applied to `skill_metrics` and including
   `horizon_type='MONTH'` (currently excluded from every Phase 4 query).
3. **Old-convention rows are either converted or tombstoned, not merely
   ignored.** The gate should be able to distinguish "no old-convention rows
   remain" from "old-convention rows exist alongside new ones and nothing
   flags the mix" — today there is no query that even counts pre-lead-aware
   `skill_metrics` rows post-recalc.
4. Whatever check is added should stay **aggregate-only, sentinel-code-based**
   (station code `19999`, never a real code), consistent with the existing
   Phase 4 checks and the repo-wide sensitive-data constraint.

Illustrative (non-committed) shape of what check #1/#2 would need to answer —
for discussion, not for direct use:

```sql
-- Illustrative only. Column/table names and enum casing must be confirmed
-- against the current skill_metrics schema before use; this is not verified
-- against the live service schema.
SELECT
  horizon_type,
  horizon_value,
  MIN(date) AS first_date,
  MAX(date) AS last_date,
  COUNT(*) AS row_count
FROM skill_metrics
WHERE code = '19999'
  AND horizon_type IN ('MONTH', 'QUARTER', 'SEASON')
GROUP BY horizon_type, horizon_value
ORDER BY horizon_type, horizon_value;
```

## Ownership note

`skill_metrics`' schema lives in `sapphire/services/postprocessing/` (colleague-
managed per CLAUDE.md § Ownership Boundaries). Any concrete verification SQL —
including the illustrative query above — should be drafted for the service
owner's review before it is added to the runbook, not committed unilaterally
into `doc/prod/`. This issue documents the gap and the requirements a fix would
need to satisfy; it does not propose finished, ready-to-run SQL.

## Acceptance criteria

- [ ] `doc/prod/long_term_deploy_runbook.md` Phase 4 (or a linked section of
      `ppipe_ensemble_hv_deploy_runbook.md`) gains a `skill_metrics` check
      covering month/quarter/season and the configured per-deployment leads.
- [ ] The added check's date-range acceptance criterion matches the existing
      `long_forecasts` bucket check's rigor (explicit `MIN`/`MAX` bounds, not
      "looks populated").
- [ ] The check (or a documented follow-up) establishes whether old-convention
      `skill_metrics` rows remain after a lead-aware-enabled recalc.
- [ ] Any new SQL is reviewed by the `sapphire/services/postprocessing` owner
      before being committed to the runbook.
- [ ] All illustrative SQL in the runbook uses sentinel codes only.

## References

- `doc/prod/long_term_deploy_runbook.md` § "Phase 4 — Verify" (lines ~317-366)
- `doc/prod/ppipe_ensemble_hv_deploy_runbook.md` § "3. Aggregate-Only
  Verification" (lines 154-272) — confirmed no `skill_metrics` query present
- Related: `PP-038` (month long-term skill stratification by lead — the schema
  change this gate should be verifying landed correctly)
- Sibling draft: `high_prio_gi_draft_pp_recalc_silent_api_write_failure.md`
  (PP-051) — a different mechanism (silent write failure) that this gap also
  fails to catch
