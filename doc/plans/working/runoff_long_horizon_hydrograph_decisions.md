# Runoff Long-Horizon Hydrograph Display — Committed Design Decisions

Source: `doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`
§Decisions Committed (rounds 1-2 reviewed; approved 2026-06-02).

These nine decisions are committed. Phase 0b, Phase 1, Phase 2,
Phase 3, Phase 4, and Phase 5 agents implement to them; do not
re-litigate.

## Decisions (User)

### D1 — Aggregation formula

Arithmetic mean of monthly values. Season norm = mean of 6 monthly
norms; season `previous` = mean of 6 monthly `previous`; season
`current` = mean of 6 monthly `current`.

### D2 — Monthly previous/current source

Mean discharge per month, computed from discharge history. For a
month-Y row, `previous` = mean discharge across all days in
month-Y in year Y-1; `current` = mean discharge across all days
in month-Y in year Y. For the in-progress current month (i.e.
`target_year == today.year` and `target_month == today.month`),
`current = None` regardless of days-so-far. Only completed months
receive `current`. A month is complete when
`(target_year, target_month) < (today.year, today.month)`.
Locked in by `test_current_is_none_for_in_progress_month` in
Phase 1.

### D3 — Season window

April-September (vegetation / high-flow), six months.

### D4 — Aggregation location

In `apps/preprocessing_runoff/`. The module already gathers data
from iEH HF and writes the hydrograph table; the new aggregations
join the same pipeline.

## Decisions Committed (Planner Defaults)

### Q-1 — Monthly mean discharge source

DEFAULT A, local SAPPHIRE daily runoff aggregation.

RATIONALE: the current long-horizon SDK usage only pins monthly
norms through `get_norm_for_site(..., norm_period="m")`
(`apps/iEasyHydroForecast/forecast_library.py:5379-5381`,
`apps/iEasyHydroForecast/forecast_library.py:5411-5415`). No
confirmed monthly-discharge SDK endpoint is present in the
existing path. Aggregating daily SAPPHIRE runoff records keeps
the implementation inside `apps/preprocessing_runoff/`, uses
already-synced data, and avoids adding operator env vars.

### Q-2 — Cadence

DEFAULT A, yearly maintenance cadence.

RATIONALE: existing `sync_monthly_norms.py` is designed to run
once a year (`apps/preprocessing_runoff/sync_monthly_norms.py:12`).
Norms change rarely, and a yearly long-horizon writer avoids
making every operational run perform broad daily-runoff reads.
Operators may still rerun with an explicit target year for
backfill or repair.

### Q-3 — Code organization

DEFAULT hybrid by responsibility.

RATIONALE: create `sync_long_horizon_hydrograph.py` in
`apps/preprocessing_runoff/` for the complete monthly triad and
seasonal aggregation. Retire the old `sync_monthly_norms.py`
path in Phase 4 so there is only one live monthly runoff
hydrograph writer. This keeps long-horizon runoff logic in one
preprocessing-runoff module while avoiding changes to the
iEasyHydroForecast helper, whose `_write_hydrograph_to_api`
accepts only `pentad`, `decade`, and `month`
(`apps/iEasyHydroForecast/forecast_library.py:3431-3444`).

### Q-4 — Missing previous/current behavior

DEFAULT C.

RATIONALE: write the station/month row when norm data exists,
populate whichever of `previous` or `current` can be computed,
and leave the missing field as `None`. This avoids silently
dropping stations and matches the API writer pattern of
serializing missing numeric fields as `None`
(`apps/iEasyHydroForecast/forecast_library.py:3517-3526`).

### Q-5 — Operator wrapper

DEFAULT new sibling wrapper.

RATIONALE: `bin/yearly_runoff_hydrograph_aggregation.sh` should
mirror the deployment style of
`bin/yearly_snow_norm_recalculation.sh` without coupling runoff
hydrograph aggregation to the snow recalculation job. The snow
wrapper currently owns snow-specific log naming, container
naming, and command text
(`bin/yearly_snow_norm_recalculation.sh:47-54`,
`bin/yearly_snow_norm_recalculation.sh:102-130`). Snow and runoff
use different source data, timing, logs, and failure modes.
