# PREPQ-023: Quarterly/seasonal hydrograph norm has no read-merge preservation — a stored value can be nulled on rewrite

**Status**: Draft (2026-09-25)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: **Low** — this writer only ever derives `quarter`/`season` norms from `month` rows it
just built in the same run, so the gap has no known way to manifest through it today. Filed as a
pre-existing, latent structural gap found while mapping the module for PREPQ-022, not something
PREPQ-022 introduced or changed.
**Labels**: `preprocessing_runoff`, `long-horizon`, `norm`, `read-merge`
**Found**: 2026-09-25, out-of-loop diff review of PREPQ-022
(`high_prio_gi_draft_prepq_virtual_station_norms.md`).
**Related**: PREPQ-022 (virtual-station norms — the review that surfaced this while mapping the
same file's norm-preservation logic; PREPQ-022 does not touch quarter/season code at all).

## Problem

`write_station_monthly_hydrograph` preserves a previously-stored MONTH norm across a run where the
current lookup is absent (`NORM_ABSENT`) or the SDK call failed (`SDK_FAILED`): it read-merges via
`_read_existing_month_norms` (`sync_long_horizon_hydrograph.py:587-611`), called from
`write_station_monthly_hydrograph` at `:655`. No equivalent read-merge exists for the two rollup
horizons:

- `_seasonal_field_mean` (`:710-723`) computes the April–September seasonal norm as the mean of the
  6 constituent MONTH records' `norm` field, **all-or-nothing**: if any one of the 6 months is
  `None`, the whole seasonal norm is `None` (`:721-723`).
- `_quarterly_field_mean` (`:769-786`) does the same for each 3-month quarter (`:785-786`).
- `write_station_seasonal_hydrograph` (`:746-766`) and `write_station_quarterly_hydrograph`
  (`:819-839`) both call `client.write_hydrograph(...)` with whatever `build_seasonal_record`
  (`:726-743`) / `build_quarterly_records` (`:789-816`) computed — there is no `read_hydrograph` call
  anywhere in either function, and no fallback to a previously-stored `season`/`quarter` norm.

**Consequence.** If a `season` or `quarter` row's norm was ever written by something OTHER than this
same-run derivation from all 12 fresh month rows — e.g. a one-off migration, a manual backfill, or a
future writer that populates these rows independently — the next time this writer runs for that
station with even one month's norm absent (a legitimate `NORM_ABSENT`/`SDK_FAILED` outcome, which
PREPQ-022 and PREPQ-020/PREPQ-015 explicitly made a non-fatal, expected occurrence), the stored
`season`/`quarter` norm is silently overwritten with `null` via the API's field-by-field upsert —
the exact "read-merge to avoid clobbering a stored value" failure mode that
`_read_existing_month_norms` exists specifically to prevent for MONTH rows, but does not prevent
here.

**Why this is Low, not higher.** `write_long_horizon_hydrograph` always builds `monthly_records` in
the SAME call before building the seasonal/quarterly records from them (`write_long_horizon_hydrograph`,
`sync_long_horizon_hydrograph.py:842-905`), and no other code path in this module or elsewhere
in the repo writes a `season`/`quarter` hydrograph row. So today, a stored `season`/`quarter` norm is
*always* exactly what the all-or-nothing mean of that same run's 12 month norms produced — there is
no scenario in the current codebase where an "external" aggregate norm exists to be clobbered. The
gap is real but currently unreachable; it becomes live only if a future change writes these rows
some other way (a migration importer, a manual correction tool, etc.).

## Proposed direction (not implemented here — pick one, or document and close)

1. **Add read-merge preservation for `season`/`quarter`**, mirroring `_read_existing_month_norms`:
   before deriving the aggregate norm, read the existing stored `season`/`quarter` row for this
   station/year and fall back to its stored `norm` when the freshly-derived mean is `None`. Matches
   the MONTH-row precedent exactly, at the cost of two more `read_hydrograph` calls per station per
   run (one seasonal, one quarterly).
2. **Document instead of fixing**: state explicitly (in this module's docstring or README) that
   `season`/`quarter` norms are ALWAYS derived, same-run, all-or-nothing from the 12 month norms —
   never independently stored or preserved — so any future writer of these rows must either go
   through this same derivation or accept that this writer's next run can null its value. Lower
   effort, no behaviour change, but leaves the structural gap for whoever adds that future writer to
   rediscover the hard way.

Either direction is acceptable; the owner should decide based on whether a non-derived
`season`/`quarter` writer is anticipated.

## Acceptance criteria

- If direction 1 is chosen: a station with 11 of 12 month norms present (one `NORM_ABSENT`) and a
  previously-stored `season`/`quarter` norm from an EARLIER run keeps that stored norm instead of
  being nulled; a never-normed station's `season`/`quarter` stays `None` as today; a failed
  preservation read does not write nulls (same anti-clobber contract as the MONTH read-merge).
- If direction 2 is chosen: the documented invariant is stated in the module docstring (or README)
  near the existing MONTH read-merge documentation, so both design choices for THIS module live in
  one place.
- Either way: no change to the byte-for-byte output of the CURRENT test suite (all `season`/`quarter`
  rows are still same-run derivations there, so direction 1's read-merge would only ever fall back
  when the freshly-derived mean is already `None` — the existing derived-`None` behaviour, an
  intentional all-or-nothing design, must not be altered by the read-merge fallback itself).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero
  unexpected skips.

## Out of scope

Changing the all-or-nothing (any-month-absent-nulls-the-rollup) derivation rule itself; any other
module; PREPQ-022's virtual-station retry (unrelated call path).
