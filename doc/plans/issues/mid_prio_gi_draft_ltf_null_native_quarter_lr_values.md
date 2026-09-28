# LTF-019: Some native calendar-quarter LR rows carry a NULL value although the LT module's CSV has one

**Status**: Draft (2026-09-27; re-checked 2026-09-28 after PP-064/FD-029 merged as #527/#528)
**Module**: `apps/long_term_forecasting` + `apps/postprocessing_forecasts` (root cause not yet localized)
**Priority**: Medium — unchanged on re-check (see note below). The affected rows are already native (pass
PP-064/PP-065's native-row rule), so these are cases where the card/bulletin would show an empty value for
an otherwise-correct row, not a row hidden by the calendar-window fixes.
**Related**: PP-064 (calendar-window validation), PP-065 (derived models, native-row selection), PP-061
(flag stamping on aggregated writer paths — **do not duplicate**, cross-reference only).

**Priority re-check (2026-09-28).** PP-064 (#527) and FD-029 (#528) are merged to trunk, but the overview's
rollout gate (owner decision 2026-09-28: do not deploy trunk before PP-065 P2) means neither is live on any
server yet — deployment is symmetric with the observation below. Severity is unchanged, not increased: the
underlying null-`q` defect is a separate root cause (LT producer write, the importer, or postprocessing's
aggregated-writer rewrite — still not localized) that PP-064/FD-029 do not touch, so the fix in this issue
is unaffected by their merge. Kept at Medium.

## Observation (dev DB, 2026-09-27, read-only; no station codes)

- **tjhm**: 17 native Q3-2026 `LR_Base`/`LR_SM` quarter rows have a `NULL` value in `long_forecasts`,
  although the LT module's own quarter forecast CSV has a value for the same station, model, `date` and
  `valid_from`.
- **kghm**: 16 Q2-2026, and **tjhm**: 3 Q3-2026, calendar-window LR rows are stored with a null `q`. **Since
  #527/#528** (PP-064 Chunk A / FD-029 P1, merged 2026-09-28), the quarter readers and the dashboard card
  *exclude* rolling-window rows *at read* (they are not deleted from the DB) — those stations would
  otherwise have fallen back on a rolling-window row, so on trunk today these stations already show **no**
  LR value at all for that quarter. This is not a future prediction; it is the present-tense state of
  `apps/postprocessing_forecasts/tests/test_quarter_calendar_window.py` and
  `apps/forecast_dashboard/src/db.py` on trunk, pending only the rollout gate (deployment, not code).

Also observed, in the same read-only pass: the postprocessing writer hardcodes `flag: 0` on quarter LR
rows regardless of the value's actual provenance — it rewrote roughly 3,970 kghm rows from flag 1 to 0 in
a local run. This is **PP-061's** territory (flag stamping on the aggregated writer path); noted here as
context for where to look, not duplicated as a separate finding.

## Proposed

Trace where the value is lost, with a read-only reproduction step comparing, for one affected
(station, model, quarter): the LT producer's own API write payload, the value the importer
(`bin/utils/migration_py/long_forecast.py`) would have written from the same CSV row, and the value after
postprocessing's aggregated-writer rewrite — to localize the loss to one of those three stages before
proposing a fix.

## Acceptance

- The stage that nulls the value is identified (producer write, importer, or postprocessing rewrite),
  with a minimal read-only repro against the dev DB.
- A fix plan follows once the root cause is known; this issue does not implement one.

## Out of scope

- PP-061's flag-stamping defect (cross-referenced, not fixed here).
- Any change to the native-row rule itself (PP-064/PP-065's scope).
