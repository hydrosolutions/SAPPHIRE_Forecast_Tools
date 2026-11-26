## Monthly maintenance mixes issue dates and can miss latest-issue gaps (PP-076)

**Status**: Review (2026-11-26)
**Priority**: High
**Module**: `apps/postprocessing_forecasts`
**Related**: [PP-057](high_prio_gi_draft_pp_operational_long_term_ensembles_read_q50_only.md)

PP-057 restored finite monthly ensemble creation. Readback then exposed a separate
issue: monthly builders grouped different issuances together and retained
`date:first`; maintenance gaps omitted issue date, letting older ensembles conceal
missing latest rows. The former target-month lookback could also exclude month_0
when month_3 was present.

### Approved scope and dependencies

The user approved recovery of only missing ensembles at the latest eligible raw
issue **per configured monthly lead, including month_0**. Select the maximum raw
issue date globally across stations and targets for each lead, no later than the
captured forecast date. Each mode may have a different latest issue. Never fall
back to an older station/model issue when the latest lacks enough members.

Use declared monthly modes from `available_monthly_batch_modes` and their
`operational_lead_for_mode` values; do not require an operational issue day.
The lookback now bounds **issue dates**, inclusively from forecast date minus the
configured number of months through forecast date. This necessary adjustment
retains month_0 alongside later target months without reopening older targets.
The behavior must hold with the lead-aware flag OFF or ON; do not enable flags.

### Implementation phases

- `src/ensemble_calculator.py`: monthly builders normalize calendar dates only
  in working pools, group by issue date as well as station/target/lead, and
  preserve raw input dates. Missing/invalid dates cannot join a valid issuance.
- `src/gap_detector.py`: select latest raw issues before point/skill eligibility;
  ensembles cannot choose the issue. Gap identity includes station, target year
  and month, lead, issue date, and ensemble type. Null/nonfinite points require
  recovery; zero is valid and missing quantiles alone are not a gap. Log aggregate
  exclusions for missing/invalid/future/out-of-window dates and invalid leads.
- `postprocessing_maintenance_long_term.py`: use the existing combined API
  snapshot for selection and construction; pass the entry point's captured date.
  Restrict raw input and generated output to actual gap keys. Write only new
  missing ensembles through the existing API writer; do not reread year forecasts,
  merge/rewrite historical rows, or swallow a failed API write.

- `src/data_reader.py`: only `_normalize_monthly_combined_forecasts` preserves
  original numeric/null leads around base normalization, so invalid API leads
  cannot become month_0. Historical monthly normalization remains unchanged.

### Acceptance and verification

- Synthetic fixtures cover leads 0/1/2/3 together, differing latest issues by mode,
  older targets not reopened, asynchronous member dates, insufficient latest
  members without older fallback, and consistent date/string normalization.
- For `SYNTHETIC_A`, target `2030-03`, lead `1`, and issue dates `2030-02-20` and
  `2030-02-25`, verify an older ensemble cannot conceal the latest gap; generated
  rows retain the selected issue, lead, composition, validity bounds, and point.
- Verify only the missing ensemble type is sent, nonfinite points are retried,
  successful reruns write nothing, invalid/future dates and leads are excluded,
  and API failure reaches the caller's errors. Assert final API payload records.
- Preserve PP-057's authoritative `q`, per-row `q50` fallback including zero,
  nonnull derived points, quantiles, stored skill gates and weighting. No
  retraining, historical skill changes, shared-writer changes, short-term changes,
  or quarter/season policy changes.

Implementation verified locally: **3303 passed, 1 existing xfail, zero skips**
in the full affected suite, including **34 new recovery regressions**. Ruff and
`git diff --check` pass. Full repository verification passed across all modules
with previously accepted skips and existing xfails. Local long-term maintenance
and a clean-context Docker build/smoke test passed; complete monthly rows were not
rewritten. Fresh independent review found no blockers. A remote read-only preview
confirmed latest-issue recovery across configured monthly leads, with forecast
writes and diagnostic CSV export blocked. No server forecast writes or default
image promotion occurred. Awaiting user review; deployment remains separate.
