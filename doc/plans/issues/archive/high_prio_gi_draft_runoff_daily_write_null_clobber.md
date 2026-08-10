# Daily runoff write clobbers stored `discharge` with NULL — no read-merge at all

**Priority:** high (silent data loss in `runoffs` DAY rows; no error, no warning).
**Module:** `apps/preprocessing_runoff/src/src.py` (`_write_runoff_to_api`, the DAILY path — a
different function from the pentad/decad `forecast_library._write_runoff_to_api`).
**Status:** **Complete** (merged `fb3e4bd3`, see Resolution below). IMPLEMENTED 2026-07-15 on branch
`fix_runoff_null_clobber_pagination` (with PREPQ-011,
via a shared paginated read-merge helper). Draft reviewed adversarially once (verdict
SOUND-BUT-INCOMPLETE — corrections below applied). Sibling of PREPQ-011. Out-of-loop verification
2026-07-23 confirmed correct + test-pinned (see Resolution).

> **✅ FIX SHIPPED (this branch):** the daily writer now calls the shared
> `_merge_preserve_existing_runoff(client, "day", records, preserve_fields=("discharge",))`
> (in `forecast_library.py`) gated to `maintenance`/`initial`, before `client.write_runoff`. The read
> is PAGINATED (skip/limit loop) and scoped to `horizon="day"`. Operational today-null is untouched.
> Red-phase-verified regression tests in `apps/preprocessing_runoff/test/test_api_write.py`.

## Resolution

**Status: Complete.** Fixed and merged to `origin/maxat_sapphire_2` in commit `fb3e4bd3`
("Fix runoff null-clobber: paginated read-merge for period + daily writers (PREPQ-011/012)"),
carried in via PR #424 (merge commit `8f43694c`). Shipped together with PREPQ-011 on the
shared `_merge_preserve_existing_runoff` helper (`apps/iEasyHydroForecast/forecast_library.py:3593`),
called from the daily writer at `apps/preprocessing_runoff/src/src.py:4434`.

**Verification:** an out-of-loop adversarial review (2026-07-23) confirmed the fix is correct.
Two tests exist in `apps/preprocessing_runoff/test/test_api_write.py`:
- `test_daily_maintenance_preserves_existing_discharge` (line 332) plants a stored-discharge
  victim and asserts it survives a null incoming row.
- `test_daily_initial_paginates` (line 411) asserts the read loop pages via increasing
  `skip`/`limit` for an existing-row count above one page.

**Deferred (soft spot, not a bug):** the review noted `test_daily_maintenance_preserves_existing_discharge`
mocks `read_runoff.return_value` as a single page (line 355) rather than a fake that honors
`skip`/`limit` — so it does not, by itself, prove the daily preservation path survives pagination;
that guarantee currently rides on the shared helper being proven by the period-path tests. Filed
as item #1 of **PREPQ-013** (`doc/plans/issues/low_prio_gi_draft_runoff_pagination_test_gaps.md`).

## Summary

The daily runoff write path has **no null-clobber protection of any kind**. Unlike the period
(pentad/decad) writer — which at least attempts a read-merge (and is being paginated in PREPQ-011) —
the daily writer builds records straight from the DataFrame and posts them, so any row whose
`discharge` is `None` overwrites a stored non-null value via the service's blind full-column upsert.

## Evidence (all on `origin/maxat_sapphire_2`)

1. **Records carry `discharge = None` for any unobserved/gap day.**
   `apps/preprocessing_runoff/src/src.py:4421` —
   `"discharge": (float(row["discharge"]) if pd.notna(row.get("discharge")) else None)`.
2. **Direct write, no read, no merge, no sync-mode preservation.**
   `src.py:4431` — `count = client.write_runoff(records)`. There is no `read_runoff` /
   merge step anywhere in this function (contrast `forecast_library._write_runoff_to_api`, which
   at least reads existing rows before writing).
3. **The service upsert is blind full-column.** `sapphire/services/preprocessing/app/crud.py:19`
   (`item.dict()`) + `:37` (`setattr` every key) — an incoming `None` becomes `NULL`.

Net: re-running a daily sync over a window where some days are unobserved (or CSV-gapped) **nulls
out previously-stored daily discharge** for those dates. No exception, run reports success.

## Why this is worse than PREPQ-011 in one respect

PREPQ-011's period path protects at least the oldest 100 rows per station (truncated read-merge).
The daily path protects **zero** — **for every emitted `discharge=None` row** it clobbers, at any
window size, including routine maintenance (which runs `SAPPHIRE_SYNC_MODE=maintenance`).

**Precise trigger (review correction):** null rows are NOT emitted for merely-absent dates — those
are never posted. They are emitted when the daily DataFrame carries a row whose discharge is
missing/NaN: source parse nulls, outliers marked NaN, and gaps **longer than 2 days** (only ≤2-day
gaps are interpolated). Those are exactly the rows that then NULL a previously-stored good value on
re-run.

## Fix options

1. **Apps-only read-merge** (mirror the period path — THIS is what shipped): before writing, read
   existing DAY rows and, for each outgoing row, keep the stored `discharge` when the incoming value
   is `None`. **Corrections from review (all applied in the shipped fix):**
   - The read MUST be **paginated** (skip/limit loop) — daily uniqueness is
     `(horizon_type, code, date)` and the service defaults `limit=100`, so an unpaginated read has the
     exact PREPQ-011 truncation bug. Constrain the read to `horizon="day"`; then `(code, date)` is a
     sufficient merge key (`horizon_value`/`horizon_in_year` are deterministic from date).
   - **Gate to `maintenance`/`initial` only.** The function already branches on `sync_mode`; an
     ungated read-merge would wrongly touch the operational today-null path.
   - Shares one helper with PREPQ-011 (`_merge_preserve_existing_runoff`) — not a copy.
2. **Omit-the-key** (if the intent is "don't touch discharge on this write"): omit `discharge` from
   the record rather than sending `None`. Requires the service to honor omission
   (`exclude_unset` / PATCH semantics — see PREPQ-011 "class fix"); does NOT work against today's
   blind upsert.
3. **Class fix** (preferred long-term, colleague-managed): omit-aware PATCH in the service that still
   allows explicit `null` to clear — removes this whole class. See PREPQ-011 Related.

Recommend option 1 now (apps-only, unblocks safe daily backfill), pursue option 3 with the service
owner.

## Acceptance criteria

Tests use placeholder station code `19999`; no real codes or discharge values.

1. **Regression:** a daily write for `19999` over a window where a date has a stored non-null
   `discharge` but the incoming row has `discharge=None`. Assert the stored value is **preserved**.
   Must FAIL against current code.
2. **Pagination loop exercised:** assert `read_runoff` is called with increasing `skip` and stops on
   a short/empty page (a single `limit=10000` one-shot would green a still-truncating fix).
3. **Operational today-null unaffected:** a not-yet-observed *today* row may still be written with
   `discharge=None` where no prior value exists — and NO read-merge is performed (assert `read_runoff`
   not called in operational mode).

> **Dropped criterion (review): "genuine null-clear still possible" is spurious for daily runoff.**
> There is no runoff PATCH/PUT/DELETE route (`RunoffUpdate` schema is unused), and the migrators
> deliberately **skip** null-discharge rows rather than send `discharge=null`
> (`doc/prod/update_data_migration_runbook.md`, `bin/utils/migration_py/runoff_day.py`). Daily
> discharge is never intentionally cleared, so preserve-always is correct here. (An explicit-clear
> path only becomes relevant under the service-side omit-aware PATCH class fix.)

## Related

- **PREPQ-011** (period pentad/decad path — pagination fix). This is its daily sibling; the two
  together are needed before `initialize_site_backfill.sh` (`SAPPHIRE_SYNC_MODE=initial`) is
  null-clobber-safe.
- `doc/plans/working/tajik_local_historical_backfill_plan.md` already flags this daily-path gap in
  its Problem/State section.
- Broader clobber class + the correct service-side PATCH design: see PREPQ-011 "class fix".
  Note `exclude_unset` alone will NOT help while writers explicitly set `"discharge": None`
  (`src.py:4421`) — they must omit the key; only the omit-aware PATCH class fix removes the class.
- **Related legacy writer (out of scope, colleague-managed):** the service-side
  `sapphire/services/preprocessing/app/data_migrator.py` daily migrator also prepares DAY rows with
  `"discharge": ... else None` and posts them (`:185-200`, `:118-128`) — same clobber shape, but it
  is a legacy migration path in `sapphire/services/`. The newer `bin/utils/migration_py/runoff_day.py`
  is safe (skips null rows). Flag to the service owner; not fixed by this apps-side change.
