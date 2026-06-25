# Snow Visualization Population — Durable Self-Healing Design

**Status:** Validated design (brainstorming complete) — ready for issue planning.
**Date:** 2026-06-25

## Problem

On the forecast dashboard snow plot (display window is *hydrological*, e.g.
KGHM `SNOW_DISPLAY_START=09-01` → window `2025-09-01 … 2026-08-31`):

1. **Curve gap** — `value`/`current` displays correctly through ~Jan 2026, then a
   gap until ~mid-May 2026, after which the current-season curve resumes.
2. **Missing bands** — from mid-May onward the current-season curve and the
   forecast render, but `norm`, `previous`, and the percentile/min–max bands
   (`mean/min/max/q05..q95`) are absent for the whole 2026 portion.

Both are **operational population gaps, not dashboard bugs**. The service schema
and `/snow/` endpoint expose all fields correctly.

## Root cause

Two independent population paths, each structurally fragile:

- **Bands** are written only by `recalculate_snow_norms.py`, scheduled **once a
  year on Aug 31** (`run_periodic_maintenance.sh snow_norms`, cron `0 2 31 8 *`).
  The recalc writes records **per calendar year** (`{year}-01-01 … {year}-12-31`,
  `recalculate_snow_norms.py:142`) and defaults to the current year
  (`recalculate_snow_norms.py:381`). Because the display window is *hydrological*,
  it straddles two calendar years: the Aug-31 run writes year-N bands at the end
  of year N, so on Jan 1 of year N the bands for that year do not yet exist and
  will not until the following Aug 31. → symptom 2, recurring every January.

- **`value`/`current`** is written by the gateway snow sync. Operational mode
  writes only `date >= yesterday` (`dg_utils.py:819`); maintenance mode widens to
  `today-30` (`dg_utils.py:821`). The gateway *fetches* `today-365`
  (`snow_data_operational.py`), but the write filter discards all but the last 30
  days. There is **no path that heals a historical hole** — a stretch of missed
  daily writes (Jan→mid-May on this server) stays empty permanently. → symptom 1.

The two jobs already cooperate without clobbering: maintenance writes preserve
recalc's bands (`dg_utils.py:864`), and recalc preserves the existing `value`
(`recalculate_snow_norms.py:245`).

## Design (reuse existing machinery — no new programs)

### Change A — bands present year-round (cron cadence)

Retarget the existing `snow_norms` periodic-maintenance task from **Aug 31** to
**Jan 1**, targeting the current calendar year (co-schedule with the existing
`monthly_norms` Jan-1 task). The recalc is already current-year-aware and
idempotent; climatology is computed from completed prior years, so it is fully
available on Jan 1. One run per year at the start of the year covers the whole
Sep→Aug window (year N-1 bands cover Sep–Dec, year N bands cover Jan–Aug).

No `recalculate_snow_norms.py` change. Cron/doc change only.

### Change B — self-healing curve (widen the maintenance write window)

Widen the snow branch of the maintenance write filter in `dg_utils.py:821` from
`today-30` to **`today-365`**, matching the data the gateway already fetches and
the reanalysis-maintenance precedent (also 365 d). The existing
`daily_gateway_maintenance.sh` then rewrites the full window every run, so any
`value`/`current` gap self-heals at the maintenance cadence. No new script, no new
cron entry. (365 d covers the hydrological window in practice; data written in
earlier runs persists via upsert, so the window-start month remains populated even
when it ages past the 365-day lookback late in the year.)

## Rollout (steady-state fix + one-time remediation)

The cadence change is steady-state and does **not** retroactively populate the
current (2026) window. After deploying A + B, run once to heal this season:

1. Widened maintenance sync (fills `value`/`current` for the full window):
   `bash bin/daily_gateway_maintenance.sh <env>`
2. Current-year band recalc:
   `ieasyhydroforecast_SNOW_RECALC_YEAR=2026 bash bin/run_periodic_maintenance.sh snow_norms <env>`

Verify with the dashboard and/or the snow-table SQL counts (value/current/norm/
percentile/previous row counts over `2025-09-01 … 2026-08-31`).

## Files affected

- `apps/preprocessing_gateway/dg_utils.py` — maintenance snow filter `30 → 365` (Change B).
- Cron schedule + docs for `snow_norms` (Change A):
  `doc/deployment.md`, `doc/prod/update_deployment_checklist.md`,
  `doc/prod/first_deploy_checklist.md`, `doc/plans/deployment_new_hydromet_aws.md`,
  `bin/README.md`, and the docstrings in `bin/run_periodic_maintenance.sh` /
  `bin/yearly_snow_norm_recalculation.sh`.
- Tests: `apps/preprocessing_gateway/test/` (maintenance window width).

## Out of scope / non-goals

- No service (`sapphire/services/`) changes — fields already exist.
- No new scripts or cron entries — reuse `daily_gateway_maintenance.sh` and the
  `snow_norms` periodic task.
- The "why did operational snow writes stop Jan→mid-May on this server" history is
  a separate ops question; Change B makes it self-healing regardless of cause.

## Testing

- Unit: maintenance-mode snow filter returns the full 365-day window (not 30 d);
  operational mode still narrow (`date >= yesterday`); band-preservation and
  value-preservation cooperation unchanged.
- Recalc current-year default + `SNOW_RECALC_YEAR` override behavior (existing).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` — zero skips.
