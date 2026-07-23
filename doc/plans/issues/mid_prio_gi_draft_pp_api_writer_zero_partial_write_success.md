# PP-047 — `_write_combined_forecast_to_api` reports success on a zero/partial server write

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Medium (observability/correctness; weakens any write-success guarantee)
**Labels**: `postprocessing`, `data-integrity`, `observability`

---

## Summary

`api_writer._write_combined_forecast_to_api` returns `True` even when the server
persisted **zero** rows or **fewer** rows than were submitted. Callers therefore
cannot distinguish "wrote everything" from "wrote nothing/partial".

## Evidence

- `apps/postprocessing_forecasts/src/api_writer.py` (around `:405`): the function
  returns a truthy result regardless of the count the client reports written.
- Consequence for PP-045: the new `require_api=True` guard
  (`file_writer.save_forecast_data`) raises on API-unavailable, on an explicit
  falsy return, and on a raised exception under `SAPPHIRE_API_FAILURE_MODE=fail`
  — but a `True`-with-zero/partial-write slips through as success. So
  `require_api=True` proves "the call did not error", not "the rows persisted".

## Impact

- Any caller relying on the return value (operational best-effort; PP-045's
  backfill with `require_api=True`) can report success while the DB did not
  actually receive all rows. Surfaces as a silent partial backfill/operational
  write.
- Did **not** manifest in PP-045's real-write verification (every expected Tajik
  pentad/decad period read back correctly), but the guarantee is not airtight.

## Proposed direction (owner to confirm)

- Prefer: have `_write_combined_forecast_to_api` validate the server-reported
  written count against the submitted count and return `False` (or raise) on a
  shortfall. Then `require_api=True` becomes a real persistence guarantee.
- Alternatively / additionally: a post-write read-back verification step in the
  backfill (as `preprocessing_runoff/backfill_discharge_aggregation.py` does).
- Check whether the client already returns a usable count; if not, this may need
  coordination on the client/service response shape.

## Out of scope / notes

- If the fix needs a change to the API response contract, that is
  `sapphire/services/` territory → coordinate, do not edit service code directly.
- Changing the return semantics affects operational's best-effort path — verify
  operational still behaves acceptably (it currently ignores the return).

## References

- Found during PP-045 final independent review
  (review_gi_draft_pp_missed_boundary_period_gap.md, residual-risk section).
