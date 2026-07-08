# Monthly/seasonal bulletin: label an absent monthly norm as "N/A — monthly norm unavailable"

**Priority:** mid — user-facing polish; the last-year-runoff data fix (PREPQ-009) works
without it, but a blank/`0` norm reads as a data error to forecasters.
**Module:** `apps/forecast_dashboard` (fair game).
**Depends on:** PREPQ-009 P1 (norm-less month/quarter/season rows must exist to render).
Split out of PREPQ-009 as its sibling **fix #5 / phase P-FD**.
**Found:** 2026-07-08, during PREPQ-009 domain review.

## Summary

PREPQ-009 makes the long-horizon writer emit month/quarter/season hydrograph rows with
locally-computed `previous`/`current` even when the iEH-HF monthly **norm** is absent
(`norm = None`). The dashboard already reads and renders the month triad (last-year runoff
via `hydrate_month_hydrograph_stats` → `get_hydrograph_pentad_all("month", …)` →
`forecast_prevyear_q` / the `Q_LAST_YEAR` bulletin column), so observed runoff now populates.
But the **norm** and **percent-of-norm** cells for those rows are `None`.

Operational-hydrologist review: rendering an absent norm as `0`, a blank cell, or a bare dash
reads as a data error and erodes trust. Observed history and the climatological normal are
different products — the absence of the normal must not suppress or corrupt the observation,
and it must be shown as *explicitly unavailable*, not as a value.

## Proposed change

Where the monthly (and seasonal/quarterly) norm is `None`, the bulletin must:
1. **Preserve/display observed runoff** (`previous`/`current`) as normal.
2. Set norm-derived **percentage** fields to unavailable (not computed).
3. Render the norm and percent-of-norm cells as an explicit **"N/A — monthly norm
   unavailable"**, and add a footnote/tooltip:
   *"Observed runoff is from local daily discharge; the long-term monthly norm is not
   populated, so percent-of-norm is not calculated."*
4. **Never** render the norm as `0`, an empty cell, a bare `-`, `"None"`, or `"nan"`.

Locate where the month/season bulletin renders the norm and percent-of-norm columns (start in
`apps/forecast_dashboard/src/bulletins.py` around the `Q_LAST_YEAR` / norm tags and the
data-loader `hydrate_month_hydrograph_stats` in `dashboard/utils.py`) and add the N/A handling
at that presentation boundary. Do not change the writer or the data contract — this is display
only.

## Reconciliation with existing docs
- `doc/plans/working/runoff_long_horizon_hydrograph_dashboard_handoff.md` describes the general
  triad-display dashboard wiring (predates PREPQ-009 and its old "skip station when norm
  absent" behavior). This issue **extends** that: it adds the norm-absent N/A case, which did
  not exist when rows were skipped. Confirm whether the general triad wiring in that handoff is
  already implemented before starting (the read-side appears to be, since last-year runoff
  already renders).

## Acceptance criteria
- Render/output test (`run_tests.sh forecast_dashboard`, `today` passed explicitly, placeholder
  code `19999`): given a month row with finite `previous`/`current` and `norm=None`, the
  bulletin shows observed runoff and the explicit **"N/A — monthly norm unavailable"** label +
  footnote; it does **not** produce `0`, blank, `-`, `"None"`, or `"nan"` for the norm or
  percent-of-norm.
- Same handling verified for seasonal and quarterly rows.
- No change to rows that **do** have a numeric norm (existing display unchanged).
- No real station codes/discharge in code, tests, or fixtures.

## Notes
Cosmetic-but-trust-critical. Keep it strictly at the presentation layer — the writer already
emits `norm=None` intentionally (a genuine upstream iEH-HF gap), and PREPQ-010 may later fill
those norms from local derivation, at which point these cells become numeric with no further FD
change. Related: PREPQ-009 (data fix), PREPQ-010 (local norm derivation), FD-012 (missing
current-year data — adjacent empty-state handling).
