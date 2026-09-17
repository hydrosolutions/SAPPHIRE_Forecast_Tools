# PP-061: the aggregated long-forecast writer hardcodes `flag=0` and can clobber a recovery's audit trail

**Status**: Draft (2026-09-17)
**Module**: `apps/postprocessing_forecasts/src/api_writer.py`
**Priority**: High
**Labels**: `postprocessing`, `long-term`, `recovery`, `data-integrity`, `silent-overwrite`
**Found**: 2026-09-10, during the long-term recovery runbook review (`plan_issue_filing_rev4.md`,
after three out-of-loop `codex exec` review rounds on rev 1 and rev 2 of that plan).
**Related**:
- **PP-063** — owns *triggering* regeneration when a long-term aggregate's membership or values
  have changed, including groups whose regeneration yields no replacement row. Read it before
  designing a fix here: PP-063's detection work and this issue's write-time flag defect are
  different failure sites on the same `long_forecasts` write path.
- **PP-041** — owns *disposal* of a `long_forecasts` row that survives when regeneration no longer
  emits a replacement for it (tombstone production and display suppression).
- This issue is neither of those. It is about a live, currently-regenerated row being written with
  the wrong `flag` value at a key it shares with a row from a different write path.

---

## Problem

`_write_aggregated_forecasts_to_api` (`apps/postprocessing_forecasts/src/api_writer.py:1070`,
shared by `_write_quarterly_ensemble_to_api` at `:1027` and `_write_seasonal_ensemble_to_api` at
`:1048`) builds one `long_forecasts` record per input row and hardcodes:

```python
record = {
    "horizon_type": horizon_type,
    "horizon_value": horizon_value,
    "code": code,
    "date": record_date,
    "model_type": model_type,
    "valid_from": valid_from,
    "valid_to": valid_to,
    "flag": 0,
    ...
}
```

(`flag: 0` at `:1214`, verified at `89a6ffc7`). Both `_write_quarterly_ensemble_to_api` and
`_write_seasonal_ensemble_to_api` are documented, in their own docstrings (`:1030`, `:1051`), as
writing **"both individual model aggregates and ensemble rows"** — i.e. this is not an
ensemble-only path; ordinary member-model forecast rows go through the same hardcoded `flag: 0`
assignment.

Separately, `apps/long_term_forecasting/lt_recovery.py` defines:

```python
RECOVERY_FLAG = 1        # lt_recovery.py:99
MISSING_VALUE_FLAG = 2   # lt_recovery.py:101
OPERATIONAL_FLAG = 0     # lt_recovery.py:103
```

and `apply_success_flag` (`lt_recovery.py:547`) stamps a recovery run's own rows with
`RECOVERY_FLAG` (or `MISSING_VALUE_FLAG` for an all-NaN row) rather than `OPERATIONAL_FLAG`,
specifically so a recovered row is distinguishable from an ordinary operational one after the
fact.

`long_forecasts` is written upsert-only, keyed on the table's own unique constraint:
`(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`
(`sapphire/services/postprocessing/app/models.py:157-203`, `UniqueConstraint` at `:193-202`, read
only — this service module is colleague-owned and was not edited to verify this). Where a row
written by the aggregated writer lands on the same key as a row a recovery run wrote earlier, the
upsert overwrites every field on the existing row with the incoming one — confirmed by reading
`create_long_forecast` in `sapphire/services/postprocessing/app/crud.py:107-158`, which looks up
existing rows by exactly this seven-column key (`:111-124`) and, on a match, does
`setattr(existing, k, v)` for every field in the incoming record when any field differs
(`:132-136`). `flag` is an ordinary field in that record; nothing exempts it. So where the two
write paths collide on key, **the aggregated writer's `flag: 0` silently overwrites the
recovery's `flag: 1`, and the audit trail that a given row was produced by a dated recovery run is
erased** — with no error, no log line naming the collision, and no visible change in row count.

## Verified collision mechanics — read this before assuming "quarter/season can't collide"

1. **Collision is decided by the complete seven-column natural key, not by horizon type alone.**
   The writer sets `record_date = valid_from` by default (`:1199`), and overrides it to the row's
   own `date` only when `horizon_type == "season"`, **or** (`horizon_type == "quarter"` **and**
   `skill_lead_aware_enabled()`), and only when that `date` is non-null (`:1200-1204`, verified at
   `89a6ffc7`). `skill_lead_aware_enabled()` (`apps/iEasyHydroForecast/skill_lead_aware_flag.py:35`)
   defaults to **OFF** when `SAPPHIRE_SKILL_LEAD_AWARE` is unset.

2. **Do not assume default-quarter processing (lead-aware flag OFF) cannot collide with a
   recovery.** Flag OFF only means the key's `date` component is `valid_from` (the first day of
   the quarter), not that `valid_from` is guaranteed to differ from a recovered run's own issue
   date. A deployment configured with issue day 1 and lead 0 makes the recovered issue date and
   `valid_from` the same calendar date. Nothing in `lt_recovery.py`'s scheduling forbids issuing a
   recovery on that date. The exposure is a stated condition, not a blanket exemption either way.

3. **The monthly writer is exempt from this specific defect, not from clobbering in general.**
   `_write_monthly_ensemble_to_api` (`:885`) filters its input to
   `ensemble_models = {"EM", "Naive Mean", "Skilled Mean"}` (`:902`, mask applied `:929`) and never
   writes individual member-model rows. Recovery only ever guards on and reads back member-model
   rows — `lt_recovery.py`'s own `AGGREGATE_MODEL_NAMES` comment (`:105-108`) states ensemble
   aggregates "are derived from members by the postprocessing maintenance job, never produced by
   `run_forecast.py`, so they are neither guarded on nor read back." So a monthly ensemble row and
   a monthly recovery row never share a natural key today. This exemption is specific to *this*
   defect; it says nothing about whether monthly is affected by other defects (see PP-063/PP-041
   above), and must not be read as "monthly is unaffected."

4. **The blast radius is history-wide, not scoped to the recovered period.** Verified by reading
   `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py`: the quarterly gap-fill
   block reads the full historical dataset with `data_reader.read_quarterly_combined_forecasts(
   codes=codes)` (`:295`, scoped only by station code, not by date or period), concatenates it with
   any freshly regenerated gap rows into `q_merged` (`:353-373`), and passes that whole frame to
   `file_writer.save_quarterly_forecast_data(q_merged)` (`:376`) →
   `api_writer._write_quarterly_ensemble_to_api` (`file_writer.py:853`) →
   `_write_aggregated_forecasts_to_api`. The seasonal block follows the identical shape:
   `read_seasonal_combined_forecasts` per issue lead (`:397-399`), concatenated into `s_combined`
   (`:403-407`), merged into `s_merged` (`:489-492`), passed whole to
   `file_writer.save_seasonal_forecast_data(s_merged)` (`:503`). Every row in that history-wide
   frame — not only rows for the period the run is actually filling gaps for — is re-emitted
   through the same hardcoded `flag: 0` record construction. Any historical member row at
   `flag=1` or `flag=2` whose key happens to be present in `q_merged`/`s_merged` is exposed to the
   same clobber on every maintenance run that reaches this code, not only on the run immediately
   following a recovery.

## Cross-reference boundary — read together, restated in neither file

- **PP-063** owns deciding whether a long-term aggregate needs to be regenerated at all when its
  membership or input values have changed, including the case where regeneration yields no
  replacement row.
- **PP-041** owns what happens to a `long_forecasts` row that survives when regeneration no longer
  emits a replacement for it — tombstone production and display suppression.
- **This issue (PP-061)** is neither of those: it is about a row that *is* being correctly
  (re)computed, at a key that legitimately still exists, being written with the wrong `flag` value
  because the writer never inspects what it might be overwriting.

## Acceptance criteria

Row-population counts are explicitly insufficient — a clobber does not change how many rows exist,
and a naive fix could raise counts through wrong-date duplicates while the clobber itself goes
unnoticed. Any fix must be verified with a **before/after comparison keyed on the complete
seven-column natural key** (`horizon_type, horizon_value, code, date, model_type, valid_from,
valid_to`):

1. **Recovered rows retain their flag.** Construct a fixture where a member-model row is written
   with `flag=RECOVERY_FLAG` (1) at a key that a subsequent aggregated-writer call (quarterly or
   seasonal) will also target — including the collision condition from point 2 above (issue day
   1 / lead 0, lead-aware flag OFF, so `date == valid_from`). Run the aggregated writer over a
   history-wide frame containing that row. Assert that, keyed on all seven columns, the row's
   `flag` is still 1 after the write, not silently reset to 0.
2. **Unrelated historical rows are untouched.** In the same fixture, include other historical
   member rows at `flag=0` (ordinary operational), `flag=1` (a different, unrelated recovery), and
   `flag=2` (`MISSING_VALUE_FLAG`) at keys the aggregated writer also re-emits because they are
   part of the history-wide `q_merged`/`s_merged` frame. Assert, again on the complete seven-column
   key, that every one of these rows' `flag` values is unchanged after the write — not merely that
   the row still exists or that its `q` value is correct.
3. **A monthly control case.** Confirm the same fixture shape does not apply to
   `_write_monthly_ensemble_to_api` because it only ever emits `{EM, Naive Mean, Skilled Mean}`
   rows, and that this exemption is not extended to any other defect in review or in test naming.
4. **The fix must not weaken recovery's own idempotency.** A fix that instead reads back
   `flag` before writing must still allow an *operational* run to legitimately overwrite an
   `OPERATIONAL_FLAG` (0) row with an updated value at the same key — the requirement is that
   `RECOVERY_FLAG`/`MISSING_VALUE_FLAG` rows are not silently downgraded to `OPERATIONAL_FLAG` by a
   write path that has no knowledge a recovery ever touched that key, not that the aggregated
   writer becomes unable to update rows at all.
5. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.

## Out of scope

- Detecting or regenerating aggregates whose membership has changed (**PP-063**).
- Tombstoning or suppressing a surviving row when regeneration no longer emits a replacement
  (**PP-041**).
- Any change to `sapphire/services/postprocessing/app/crud.py` or `models.py` — that service is
  colleague-owned; if the fix needs the service to distinguish "no incoming value for this field"
  from "explicitly overwrite with 0," that is an API-contract question to raise with the service
  owner, not to implement here.
