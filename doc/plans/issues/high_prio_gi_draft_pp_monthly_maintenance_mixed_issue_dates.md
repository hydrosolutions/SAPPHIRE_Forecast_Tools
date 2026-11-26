## Monthly maintenance mixes issue dates and can miss latest-issue gaps (PP-076)

**Status**: Draft (2026-11-26)
**Priority**: High
**Module**: `apps/postprocessing_forecasts`
**Related**: [PP-057](high_prio_gi_draft_pp_operational_long_term_ensembles_read_q50_only.md)

Maintenance completed successfully and produced finite monthly ensembles after
PP-057, but readback retained earlier issue dates for some latest leads. Raw
members can carry multiple issue dates for the same station, target month, and
lead. This is a separate date-selection issue, not a failure of point fallback.

### What to inspect

- `src/ensemble_calculator.py`: `create_monthly_ensemble_forecasts` groups by
  `year`, `month`, `code`, and available `horizon_value`, omitting issue `date`.
  `_add_skilled_mean_monthly` and `_add_naive_mean_monthly` retain `date:first`,
  assuming each group belongs to a single issuance. Mixed issuances can therefore
  be pooled and labeled with an earlier date.
- `src/gap_detector.py`: `detect_missing_monthly_ensembles` omits issue `date`
  from gap identity. Its default flag-OFF key is station/target; flag ON adds
  lead, still without issue date. An older ensemble can satisfy either key.
- `postprocessing_maintenance_long_term.py`: `_run_monthly_gap_fill` passes
  detected gaps to monthly generation; inspect selection and merge-back together
  before changing either grain. `src/data_reader.py`:
  `read_latest_monthly_forecasts` selects the latest target month, which does not
  by itself select the latest issuance for every target/lead.

### Proposed narrow decision — approval required

Obtain user scope approval before semantic changes: define the latest eligible
issue for each station/target/lead and recover only its missing ensembles. Do not
backfill every historical issuance or assume that adding `date` to grouping alone
settles which members belong together. The lead-aware selection flag defaults OFF;
validation found no explicit override. Do not enable it as an incidental fix.

### Verification and contracts

- Use synthetic `SYNTHETIC_A`, target `2030-03`, lead `1`, and issue dates
  `2030-02-20` and `2030-02-25`. Verify an older stored ensemble cannot conceal
  the approved latest-issue gap, and generated latest rows do not mix members
  across those dates. Assert issue date, lead, composition, and expected point.
- Cover an already complete latest issue, different leads for the same target,
  and rerun idempotency; confirm only approved missing latest-issue rows change.
- Preserve PP-057's authoritative `q`, per-row `q50` fallback, nonnull derived
  points, and quantiles. Reuse stored skills and model outputs; no retraining,
  skill-policy changes, short-term changes, or unrelated aggregation changes.
