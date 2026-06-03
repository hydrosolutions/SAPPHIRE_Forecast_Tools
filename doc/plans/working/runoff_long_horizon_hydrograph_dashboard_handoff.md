# Runoff Long-Horizon Hydrograph — Dashboard Handoff

This note marks the end of the preprocessing-side work for
long-horizon (monthly + seasonal) runoff hydrograph rows. The
downstream dashboard plan is responsible for displaying the
new triad fields to operators and analysts.

## What this plan delivered

The preprocessing-side writer at
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
(commit `aeceebe`) now produces, for each configured station
that has monthly norms in iEH HF:

- **12 monthly hydrograph rows per target year** with
  `horizon_type="month"` and the full `(norm, previous, current)`
  triad. `previous` and `current` are arithmetic means of the
  daily SAPPHIRE runoff records for the same calendar month in
  `Y-1` and `Y`, subject to the per-month threshold rule
  (D-Q6: >=80% of calendar days populated with non-null finite
  values, otherwise the cell is `None`).
- **1 seasonal hydrograph row per target year** with
  `horizon_type="season"`, `date="{Y}-04-01"`,
  `horizon_value=1`, `horizon_in_year=1`. The seasonal
  `(norm, previous, current)` fields are arithmetic means of
  the six April-September monthly fields, subject to the
  strict-completeness rule (D1: if any one of the six monthly
  values is `None`, the seasonal field is `None`).

Stations whose iEH HF monthly norm call returns zero values or
raises are logged at WARNING and skipped (commit `aeceebe`).

## Downstream scope (next plan, not this plan)

The downstream dashboard plan should update the forecast
dashboard's monthly and seasonal data loaders so the new triad
fields appear in the existing UI:

- **`_get_data_monthly`** in `apps/forecast_dashboard/`
  (currently returns an empty hydrograph overlay DataFrame
  for the monthly tab). It should pull
  `/preprocessing/hydrograph/?horizon=month&code=<station>` and
  surface `norm`, `previous`, and `current` per month for the
  active station and target year.
- **`_get_data_season`** in `apps/forecast_dashboard/`
  (currently returns an empty hydrograph overlay DataFrame for
  the season tab). It should pull
  `/preprocessing/hydrograph/?horizon=season&code=<station>`
  and surface `norm`, `previous`, and `current` for the
  one-and-only April-September seasonal row per active station
  per target year.

Visualization style is the dashboard plan's call.
Recommendations: mirror the snow hydrograph display style
(min-max envelope or norm band, previous-year line,
current-year line, in-progress current shown as partial
trajectory). Reference the snow display plan (now archived
under `doc/plans/issues/archive/`) for component patterns.

## Explicitly out of scope (do NOT expand)

- **Quarter hydrograph triad**: no quarter records are stored
  or written by preprocessing. The preprocessing enum lacks
  `quarter` at
  `sapphire/services/preprocessing/app/models.py:6-13`, and
  the reservoir quarter card already reads monthly data through
  upstream PR #341. Do NOT add a quarter writer in this plan
  or the downstream dashboard plan.
- **API/schema changes**: the shared `Hydrograph` table at
  `sapphire/services/preprocessing/app/models.py:70-73` already
  has `norm`, `previous`, and `current` and the service already
  exposes shared `/hydrograph/` POST/GET endpoints; no service
  edits are needed for the dashboard work.
- **Operator wrapper edits**: the new wrapper
  `bin/yearly_runoff_hydrograph_aggregation.sh` lives in P4 of
  this plan and is not in the downstream dashboard plan's
  scope.

## Pointers for the downstream plan author

- **Live data shape**: see the P3 evidence at
  `doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`
  (commit `fc22f6d`) for the actual record counts and value
  ranges from the operator's local stack. As of 2026-06-02,
  53 of 63 configured kghm stations produced 12-month + 1-season
  triad rows for target year 2026 (636 monthly + 53 seasonal =
  689 records); the remaining 9 stations are an operator-side
  iEH HF data gap.
- **In-progress year semantics**: for any target year where the
  current calendar date falls inside April-September, the
  seasonal `current` will be `None` (D1 + D2). The dashboard
  must handle this gracefully — show the seasonal previous-year
  / climatology band but explicitly note that the current-year
  seasonal mean is not yet defined.
- **Per-month thresholds**: a station with chronic data gaps in
  some months but solid coverage in others will write
  meaningful means for the well-covered months and `None` for
  the sparse ones (D-Q6). The dashboard should NOT compute its
  own fallback values for these cells; trust the writer's
  `None` decisions and render the cell as missing.

## End of preprocessing-side work

After P4 (operator wrapper + Luigi task retirement) and P5
(this handoff note) commit, the runoff long-horizon hydrograph
plan is complete. The downstream dashboard plan is the next
deliverable but is out of scope for this plan.
