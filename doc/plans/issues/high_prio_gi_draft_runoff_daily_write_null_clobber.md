# Daily runoff write clobbers stored `discharge` with NULL — no read-merge at all

**Priority:** high (silent data loss in `runoffs` DAY rows; no error, no warning).
**Module:** `apps/preprocessing_runoff/src/src.py` (`_write_runoff_to_api`, the DAILY path — a
different function from the pentad/decad `forecast_library._write_runoff_to_api`).
**Status:** Draft — found 2026-07-15 by the 2nd adversarial review of PREPQ-011. Sibling of
PREPQ-011; **not covered by PREPQ-011's fix.**

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
The daily path protects **zero** — every `None` row clobbers, at any window size, including routine
maintenance windows under 100 rows.

## Fix options

1. **Apps-only read-merge** (mirror the period path, paginated per PREPQ-011): before writing, read
   existing DAY rows for the `(code, date)` window and, for each outgoing row, keep the stored
   `discharge` when the incoming value is `None`. Simplest immediate mitigation; stays in `apps/`.
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
2. **Genuine clear still possible:** if the design keeps an explicit-null "clear" semantics, a test
   that an intentional clear still nulls the value (so the fix does not over-preserve).
3. **Operational today-null unaffected:** a not-yet-observed *today* row may still be written with
   `discharge=None` where no prior value exists (no spurious preservation of absent data).

## Related

- **PREPQ-011** (period pentad/decad path — pagination fix). This is its daily sibling; the two
  together are needed before `initialize_site_backfill.sh` (`SAPPHIRE_SYNC_MODE=initial`) is
  null-clobber-safe.
- `doc/plans/working/tajik_local_historical_backfill_plan.md` already flags this daily-path gap in
  its Problem/State section.
- Broader clobber class + the correct service-side PATCH design: see PREPQ-011 "class fix".
