# PREPQ-023: Quarterly/seasonal hydrograph norm has no read-merge preservation — a stored value can be nulled on rewrite

**Status**: Draft (2026-09-25)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: **Low** — the preprocessing API's `POST /hydrograph/` accepts an independently-written
`quarter`/`season` norm today and does not enforce consistency with the constituent monthly rows (see
"Why this is Low, not higher" below), so the state this issue describes is API-seedable, not merely
hypothetical. Held at Low because no independent scheduled producer of such a row was found in this
repo, and no production occurrence has been verified — only the API-level seedability is confirmed.
Filed as a pre-existing, latent structural gap found while mapping the module for PREPQ-022, not
something PREPQ-022 introduced or changed.
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
same-run derivation from all 12 fresh month rows — e.g. a one-off migration, a partial historical
write, or a direct `POST /hydrograph/` call seeding it via the API (see below) — the next time this
writer runs for that station, IF a CONSTITUENT month (one of the season's 6 months, April–September,
or that quarter's 3 months) is STILL missing AFTER the monthly read-merge
(`_read_existing_month_norms`) has had its chance to preserve it, the stored `season`/`quarter` norm
is silently overwritten with `null` via the API's field-by-field upsert. An absent SDK response ALONE
does not erase an aggregate: if every constituent month's norm is complete (fresh or read-merge-
preserved), the aggregate derives and writes normally. This is the exact "read-merge to avoid
clobbering a stored value" failure mode that `_read_existing_month_norms` exists specifically to
prevent for MONTH rows, but does not prevent for `season`/`quarter` rows.

**Why this is Low, not higher.** `write_long_horizon_hydrograph` always builds `monthly_records` in
the SAME call before building the seasonal/quarterly records from them (`write_long_horizon_hydrograph`,
`sync_long_horizon_hydrograph.py:842-905`), and no PRODUCTION code path in this module or elsewhere in
the repo writes a `season`/`quarter` hydrograph row independently of this same-run derivation — so no
production occurrence of this gap has been verified.

However, the state is not merely hypothetical: the preprocessing service's `POST /hydrograph/`
endpoint (`sapphire/services/preprocessing/app/main.py:101-105`, read only) accepts a
`HydrographCreate` (`schemas.py:41-64`) with any `HorizonType` including `QUARTER`/`SEASON`
(`models.py:6-13`) and a `norm` field, with NO validation linking a `quarter`/`season` row to its
constituent `month` rows anywhere in the request/response schema. `crud.create_hydrograph`
(`crud.py:88`, field-by-field `setattr` upsert at `:110`) applies whatever fields are supplied,
independently of any other row. So an aggregate norm CAN be seeded today — by an operator's manual
`POST`, a migration/backfill tool, or a partial historical write — without this writer having
produced it; that seeded value is exactly what this issue's read-merge gap would then null on this
writer's next run under the trigger condition above. No independent SCHEDULED producer of such a
row was found in a repo sweep, and no production occurrence is verified — the gap is real and
API-reachable, but currently unexercised by any known automated path.

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

- If direction 1 is chosen, this fixture must pass: a station with an EXISTING stored `season` norm
  and an EXISTING stored Q2 (`quarter=2`, April–June) norm — pre-seeded via `POST /hydrograph/` (an
  operator/migration write, not this writer's own prior run) or via a partial historical write — and
  11 of its 12 month norms stored, with APRIL specifically the one missing; this run's SDK lookup
  returns absent for April. Expected: the stored `season` norm is preserved (April is one of its 6
  constituent months and is still missing after the monthly read-merge) AND the stored Q2 norm is
  preserved (April is also one of Q2's 3 constituent months) — while Q1/Q3/Q4 continue to derive
  normally (none of their constituent months are affected by April's absence). Also: a never-normed
  station's `season`/`quarter` stays `None` as today; a failed preservation read does not write nulls
  (same anti-clobber contract as the MONTH read-merge).
- If direction 2 is chosen: the documented invariant is stated in the module docstring (or README)
  near the existing MONTH read-merge documentation, so both design choices for THIS module live in
  one place.
- Either way, no ambiguity between the derivation helpers and the new preservation step:
  `_seasonal_field_mean`/`_quarterly_field_mean` STAY all-or-nothing exactly as today — they still
  return `None` if any constituent month is missing, and this issue does not change that. What
  direction 1 adds is a SEPARATE preservation step, applied AFTER derivation (the same shape as
  `_read_existing_month_norms` applied after `_lookup_monthly_norms`): it MAY replace a derived
  `None` in the OUTGOING RECORD with a previously stored value, exactly as the MONTH read-merge
  replaces a norm-absent month's `None` today. The derivation functions themselves are never
  modified to stop being all-or-nothing.
- No change to the byte-for-byte output of the CURRENT test suite: every `season`/`quarter` row built
  by today's tests is a same-run derivation with no pre-existing stored aggregate to fall back to, so
  direction 1's preservation step is a no-op for all of them (there is nothing to fall back to) and
  the existing derived-`None` behaviour for those cases is unchanged.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero
  unexpected skips.

## Out of scope

Changing the all-or-nothing (any-month-absent-nulls-the-rollup) derivation rule itself; any other
module; PREPQ-022's virtual-station retry (unrelated call path).
