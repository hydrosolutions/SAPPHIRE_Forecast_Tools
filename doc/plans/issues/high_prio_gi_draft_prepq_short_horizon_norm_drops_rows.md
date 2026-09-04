# PREPQ-020: Short-horizon hydrograph — a missing/failing norm drops the station's whole pentad or decad batch

**Status**: Draft (2026-09-03, revised after out-of-loop review)
**Module**: `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py`
**Priority**: **High** — this runs on every operational `preprocessing_runoff` run that has iEH-HF
SDK access (not only `--maintenance`, not only locally), and it silently discards observed discharge
that was already available locally. Measured on kyg 2026-09-03: pentad rows written for **10 of 62**
stations, decade for **55 of 62**.
**Labels**: `preprocessing_runoff`, `short-horizon`, `data-loss`, `silent-skip`
**Found**: 2026-09-03, while diagnosing a `maintenance:preprocessing_runoff` FAIL row (see INFRA-044).
**Related**: **PREPQ-009** (archived, `issues/archive/high_prio_gi_draft_runoff_longhorizon_norm_decouple.md`,
PR #409) broke exactly this coupling for the **long-horizon** `NORM_ABSENT` branch. **PREPQ-015**
(PR #475) broke it for the long-horizon `SDK_FAILED` branch. **This issue is the short-horizon twin
that neither touched.** **PREPQ-014** explains *why* the SDK raises for four kyg stations; this issue
does not depend on it and is not fixed by it. **INFRA-044** is the reporting half of the same
investigation.

---

## Summary

`write_station_short_horizon` returns an empty list — dropping the station's entire 72-row pentad or
36-row decad batch — whenever the iEH-HF norm lookup either raises or returns the wrong number of
values. The station's `current`/`previous` observed values and the whole daily-derived envelope
(`mean`, `min`, `max`, `q05`, `q25`, `q75`, `q95`) are computed from **local daily runoff**, not from
the norm, and are read *after* the norm check. So a missing norm withholds data it has no bearing on.
This is the same defect class PREPQ-009 fixed for the long-horizon path.

**Confirmed clobber premise**: the API updates existing hydrograph rows field-by-field including
`norm=None` (`sapphire/services/preprocessing/app/crud.py:107-111`, locked by
`sapphire/services/preprocessing/tests/test_hydrograph_norm_preservation.py:79`). So writing rows
with a naive `None` norm would *erase* a stored norm — which is why C2's read-merge is mandatory,
not cosmetic.

## Current behaviour

`sync_short_horizon_hydrograph.py:619-671` (`write_station_short_horizon`):

```python
    try:
        norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period=config["norm_period"])
    except Exception as exc:
        logger.warning("... SDK norm call failed for site %s (%s), skipping. ...")
        return []                      # <-- (a) whole batch dropped   (:630-641)

    if len(norms) != config["periods_per_year"]:
        logger.warning("... expected %d norm values ... got %d - skipping this site.")
        return []                      # <-- (b) whole batch dropped   (:643-652)
```

Everything that would have produced the observed data comes *after* both returns:
`_read_daily_by_year` (`:654`), `_fetch_sdk_period_actuals` (`:655-657`), the builder (`:659`), and
`client.write_hydrograph(records)` (`:669`).

### A third, unhandled branch (found in review)

`len(norms)` at `:643` is **outside** the `try`. A successful SDK response that is `None` or any
other unsized object raises `TypeError` there. That is not in `_API_READ_WRITE_ERRORS`, so it
escapes `write_short_horizon_hydrograph`'s handler (`:705`), reaches
`_write_short_horizon_hydrograph_records`'s bare `except Exception`
(`preprocessing_runoff.py:223-230`), and aborts the short-horizon write for **every remaining
station**, not just this one. Pre-existing and latent; C1 must close it rather than preserve it.

### The skip is invisible to every status channel

- `write_short_horizon_hydrograph` (`:674-723`) **pops the station back off**
  `attempted_station_codes` when it produced no records (`:699-702`, pop at `:700`), so the station
  vanishes from the attempted count rather than being counted as skipped. That also silently
  understates the denominator of the API-failure ratio at `:716-722`.
- There is no status enum, no run-summary block, and no counts line — the long-horizon module's
  `LongHorizonStationWriteStatus` / `LONG-HORIZON RUN SUMMARY` machinery
  (`sync_long_horizon_hydrograph.py:65-69`, `:659-676`) has no short-horizon counterpart.
- `preprocessing_runoff.py:192-230` discards the return value and treats a normal return as success.

### Measured impact (kyg, 2026-09-03, 62 stations, one run)

| Horizon | Stations skipped | Cause | Rows never written |
|---|---:|---|---:|
| pentad | 52 | 48 × `expected 72 norm values … got 0`, 4 × SDK raise | 3744 |
| decade |  7 |  3 × `expected 36 norm values … got 0`, 4 × SDK raise |  252 |

`expected N norm values … got 0` also occurs on tjhm (19 occurrences in the 2026-08-17 cross-org
run), so this is not kyg-specific.

## Why the branches are the same defect but not the same cause

Keep the distinction PREPQ-014 established — it changes the *messaging*, not the fix:

- **(a) the raise** is a path-resolution failure, byte-identical for a genuinely unregistered site
  and for any non-200 (`ieasyhydro_sdk/sdk_endpoint_definitions.py:90-109` returns `None` for ANY
  non-200; `sdk_base.py:64` then raises). It may be transient.
- **(b) the wrong length / wrong shape** is a data-availability statement from a successful call.

Both must stop dropping rows. Neither may be silently reclassified as the other — reclassification
was proposed three times for the long-horizon path and refuted three times (PREPQ-014).

## The contract

**C1 — classify the norm response; never drop rows over it.** Replace both early returns with a
classifier modelled on `_classify_monthly_norms` (`sync_long_horizon_hydrograph.py:277-308`),
returning one of `VALID` / `NORM_ABSENT` / `SDK_FAILED`:

- the SDK call raises → `SDK_FAILED`;
- the response is **not a `list`/`tuple`** (position carries meaning — the builder indexes
  `norm_values[period - 1]`, so a set or a dict would be silently mis-ordered or mis-keyed), or is
  unsized, or is not `periods_per_year` long, or any element is non-numeric (`bool` counts as
  non-numeric here) or non-finite → `NORM_ABSENT`. Compute the length inside a guard so `TypeError` on `len(None)` can no
  longer escape (see "A third, unhandled branch"). `_json_safe` passes strings and booleans through
  unchanged, so element validation must be explicit — a 72-element list of strings must **not** be
  treated as `VALID`;
- otherwise `VALID`.

Neither failing classification returns early. Both fall through to the existing daily/actuals
pipeline and write the full 72 (pentad) or 36 (decad) records. Only the `norm` field is affected.

**C2 — a previously stored norm is preserved, never overwritten with `None`.** Add
`_read_existing_period_norms(client, code, horizon_type, target_year)` returning a
`periods_per_year`-length list keyed by `horizon_in_year`, missing periods left `None`. Use it for
**both** failing classifications.

> **The read window is NOT the calendar year** — this is the trap that makes the obvious
> implementation wrong. Period 1 of `target_year` is stamped with the **preceding 31 December**:
> `fl.get_issue_date_from_pentad(1, 2026)` → `2025-12-31`, and `get_issue_date_from_decad(1, 2026)`
> → `2025-12-31` (measured 2026-09-03; generated at `iEasyHydroForecast/forecast_library.py:4546` for pentad and `:4592-4595`
> for decad). Copying
> `_read_existing_month_norms`'s `{year}-01-01`…`{year}-12-31` bounds
> (`sync_long_horizon_hydrograph.py:317-323`) would **miss period 1 and ingest the next year's
> period 1** (`get_issue_date_from_pentad(1, 2027)` = `2026-12-31`, inside those bounds).
> Derive the bounds from the config instead — `config["get_issue_date"](1, target_year)` through
> `config["get_issue_date"](periods_per_year, target_year)` — and match each returned row against
> the exact expected `(issue_date, horizon_in_year)` pair for the target year, not on
> `horizon_in_year` alone. Test 3's fake **must assert the requested date bounds**, not return rows
> regardless of them; a fake that ignores its arguments cannot catch this class of bug.

Verified plumbing (2026-09-03): `SapphirePreprocessingClient.read_hydrograph`
(`sapphire_api_client/preprocessing.py:158-188`) validates `horizon` against `VALID_HORIZONS` =
`{"day","pentad","decade","month","quarter","season","year"}` (`validators.py:14-19`) — **both
`"pentad"` and `"decade"` are valid** and are the same strings `_HORIZON_CONFIG` (`:66-93`) is keyed
by, so no name mapping is needed. Use `limit=1000` (72 rows/station/year fits). Add `_iter_daily_rows`
(`sync_long_horizon_hydrograph.py:147-152`) to the existing top-level
`from sync_long_horizon_hydrograph import (...)` block (`:47-53`); that import direction is already
established and is not the one guarded against circularity (long→short is the call-time import at
`sync_long_horizon_hydrograph.py:377-380`).

**C2a — a failed read-merge is an `API_FAILED`, and that horizon is NOT written.** This is the one
place where "write the rows anyway" is the wrong answer, and it is worth being explicit about why.

The API upserts field-by-field including `norm=None` (`crud.py:107-111`), so writing
`[None] * periods_per_year` after a *failed* preservation read would **erase every stored norm for
that station-horizon** — destroying exactly what C2 exists to protect, and doing it precisely when
we are blind to what is stored. An earlier revision of this contract said to fall back to all-`None`
and keep writing; that was wrong and is retracted.

So: wrap the `read_hydrograph` call in `except _API_READ_WRITE_ERRORS`, and on failure record
`API_FAILED` for that `(code, horizon)`, write nothing for it, and continue to the next horizon and
station. This matches the long-horizon module, which lets a failed `_read_existing_month_norms`
propagate into the same outer API handler (`sync_long_horizon_hydrograph.py:365-372` → `:591-604`).

That is not a regression to the defect this issue fixes. Today rows are dropped on **norm absence**,
which is the common case (52/62 stations on kyg). After this change rows are dropped only on an
**API read failure**, which is rare, is itself an API-side problem, almost certainly means the
subsequent write would fail too, and is now counted and reported instead of silent.

The read-merge is only reached for `NORM_ABSENT` / `SDK_FAILED`; a `VALID` norm never touches it, so
this path cannot affect a healthy station.

**C3 — the run is summarised, per horizon.** Record one terminal status per **`(code, horizon)`**
pair, not per station. Emit a counts-only block at the end of `write_short_horizon_hydrograph`:

```
SHORT-HORIZON RUN SUMMARY
total_attempted=62
pentad_written=10 pentad_norm_absent=48 pentad_sdk_failed=4 pentad_api_failed=0
decade_written=55 decade_norm_absent=3 decade_sdk_failed=4 decade_api_failed=0
DEGRADED: pentad discharge norms unavailable for 52/62 stations; observed runoff written; norm unavailable.
DEGRADED: decade discharge norms unavailable for 7/62 stations; observed runoff written; norm unavailable.
END SHORT-HORIZON RUN SUMMARY
```

**One summary line per degraded horizon**, each emitted only when that horizon's
`norm_absent + sdk_failed > 0`; no line at all when both horizons are clean.

> **Log level, per the owner decision of 2026-09-04** ("a missing norm is not our problem; it may be
> an informational log, not an error"): the **counts block is neutral and always printed**. The
> per-horizon line is **INFO** when the only contributors are `norm_absent` — an upstream absence,
> not our failure. Reserve WARNING for `sdk_failed`, which means the lookup *raised* and we cannot
> tell absence from an outage. Do not label the norm-absent case `DEGRADED` or `ERROR`; the word
> `DEGRADED` in the long-horizon module predates that decision and is not a precedent to copy here.

**C3a — each `(code, horizon)` needs its own exception boundary.** Today both horizons run inside a
single `try` (`:688-704`), so a pentad `_API_READ_WRITE_ERRORS` jumps to the handler at `:705` and
decade is never attempted or classified — which would leave C3's counts unable to sum to
`total_attempted`. Move the boundary inside the horizon loop, assign that horizon's terminal status
there, and aggregate to the station afterwards.

**C4 — no new exit code, ever.** `_write_short_horizon_hydrograph_records`
(`preprocessing_runoff.py:192-230`) documents that this write "must NEVER abort the operational
run". C3 is logging only. Do **not** add a return-status contract, do **not** make
`preprocessing_runoff.py` inspect the counts, and do **not** propagate a non-zero exit. Changing
that is a separate decision with production blast radius and is out of scope.

**C5 — remove the `attempted_station_codes.pop()` (`:699-702`) *and* fix the CLI diagnosis it feeds.**
A station that produced no records was still attempted. But the standalone CLI reads
`completed == 0 and attempted > 0` as *"All N attempted station(s) had short-horizon hydrograph API
read/write failures"* and exits 2 (`:780-788`) — a diagnosis that is only true because the pop
currently hides non-API empties. Removing the pop without changing that branch would make the CLI
misreport a norm-only degradation as a total API outage. Change the branch to test the actual
`API_FAILED` count from C3's status tally instead of inferring it from list lengths.

## Files that may be modified

- `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py` (including its `main()`, for C5)
- `apps/preprocessing_runoff/test/test_short_horizon_write_failure_visibility.py` (extend)
- new `apps/preprocessing_runoff/test/test_short_horizon_norm_decoupling.py`

**Do not** change `preprocessing_runoff.py`, `sync_long_horizon_hydrograph.py`, or any function
signature outside `sync_short_horizon_hydrograph.py`. Do not change `_build_short_horizon_records`,
`period_actuals`, or the record schema. `_build_short_horizon_records` already length-checks
(`:365-369`) and passes each value through `_json_safe` (`:393`), which maps `None` → `None`
(`sync_long_horizon_hydrograph.py:113-119`), so a list of `None`s is a valid input and the builder
needs no change.

## Tests

Use `19999` / `19998` as station codes — never a real code.

1. **Norm raises → rows still written.** Fake SDK whose `get_norm_for_site` raises; assert
   `write_hydrograph` received 72 records, every `current`/`previous`/envelope field matches the
   no-failure baseline, and every `norm` is `None`.
2. **Norm wrong length (`[]`) → rows still written.** Same assertions.
2b. **Norm unsized (`None`) and norm invalid (72 strings) → rows still written**, classified
   `NORM_ABSENT`, and **no exception escapes** to the caller. Guards the third branch above.
3. **Read-merge preserves a stored norm, over the correct window.** Fake client whose
   `read_hydrograph` returns stored pentad rows with norms for periods 1-3 **and asserts the
   `start_date`/`end_date` it was called with**. Assert the bounds against **literal dates**
   (`2025-12-31` and `2026-12-25` for `target_year=2026`), not against a value re-derived from
   `get_issue_date_from_pentad` — deriving the expectation from the same helper under test is what
   lets a boundary bug pass unnoticed, and is why the existing
   `test_short_horizon_actuals_m2.py:531` does not independently lock this. Assert the written
   records carry those three norms, `None` elsewhere, and that **period 1 is among the preserved
   ones**.
4. **A failed read-merge does not clobber and does not abort (C2a).** `read_hydrograph` raises a
   `_API_READ_WRITE_ERRORS` member → **no `write_hydrograph` call is made for that horizon** (assert
   on the mock: zero calls — this is the anti-clobber assertion), the `(code, horizon)` is
   `API_FAILED`, the station's **other** horizon is still attempted, and the loop continues to the
   next station.
5. **Status/summary counts and log levels.** A three-station run (one valid, one norm-absent, one
   SDK-raise) produces the exact `SHORT-HORIZON RUN SUMMARY` counts; per-horizon counts each sum to
   `total_attempted`; the per-horizon lines are absent when all stations are valid; **both** appear
   when both horizons degrade. **Assert the levels**: a norm-absent-only horizon logs at INFO, a
   horizon with any `sdk_failed` logs at WARNING.
6. **Exit-code invariant (regression guard for C4).** `write_short_horizon_hydrograph` returns
   normally and `_write_short_horizon_hydrograph_records` returns `None` even when every station is
   norm-absent.
7. **API failure is per-horizon and still counted.** `write_hydrograph` raises for **pentad only** →
   pentad is `API_FAILED`, **decade is still attempted and classified**, the loop continues to the
   next station. Guards C3a.
8. **CLI diagnosis (C5).** Drive `main()` with a `write_short_horizon_hydrograph` result whose
   metadata is *attempted > 0, completed == 0, `API_FAILED` count == 0* — construct that state
   directly rather than relying on a norm-absent run, which after C1 produces records and completed
   stations and would therefore pass whether or not C5 was implemented. Assert the "all attempted
   station(s) had short-horizon hydrograph API read/write failures" message is **not** emitted.

## Acceptance criteria

- [ ] On a kyg run with a healthy API, pentad rows are written for **62/62** stations and decade
      for **62/62**; the count of stations with a non-null pentad `norm` is unchanged from
      before the fix (10). (A station-horizon whose preservation read fails is `API_FAILED` and
      correctly absent — see C2a.)
- [ ] No station loses a previously stored norm, **including period 1** (test 3).
- [ ] `SHORT-HORIZON RUN SUMMARY` appears once per run, per-horizon counts summing to
      `total_attempted`.
- [ ] `preprocessing_runoff.py --maintenance` and a plain operational run both still exit 0 with a
      fully norm-absent station set.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures,
      zero unexpected skips; then the full `run_tests.sh`.
- [ ] `ruff check` / `ruff format --check` clean on every changed file.

## Phases

- **P1 — classify + decouple (C1, C2, C2a).** Files: `sync_short_horizon_hydrograph.py`, new test
  file. Depends on: none. Agents: 1. Accept: tests 1, 2, 2b, 3 pass, plus a **P1-scoped form of test
  4** asserting only the anti-clobber half (a failed read-merge makes no `write_hydrograph` call and
  does not abort the loop). The status half of test 4 belongs to P2 — asserting `API_FAILED` before
  P2 introduces the status tally would leave P1 red.
- **P2 — per-horizon boundary, statuses and summary (C3, C3a).** Files:
  `sync_short_horizon_hydrograph.py`, both test files. Depends on: P1. Agents: 1. Accept: tests 5, 7
  pass, and test 4 is promoted to its full form (status `API_FAILED`, other horizon still attempted).
- **P3 — CLI diagnosis + attempted-list fix (C5).** Files: `sync_short_horizon_hydrograph.py`
  (`main()`), test files. Depends on: P2 (needs the status tally). Agents: 1. Accept: test 8.
- **P4 — invariant guard (C4).** Files: test files only. Depends on: P1-P3. Agents: 1.
  Accept: test 6 passes; no non-test file outside `sync_short_horizon_hydrograph.py` was touched.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P1", "P2", "P3"], "parallel_agents": 1 }
  }
}
```

## Scope notes and corrections applied after out-of-loop review (2026-09-03)

- The header previously said "every operational run". Qualified: the short-horizon write is outside
  the maintenance gate, but is skipped when `ieasyhydroforecast_connect_to_iEH == "True"` (legacy
  connection) or the HF SDK is unavailable (`preprocessing_runoff.py:544-560`).
- An earlier revision claimed the per-station warnings are the only trace because the root logger is
  capped at WARNING (INFRA-029). **That is false for this entry point**: `preprocessing_runoff.py`
  installs its own handlers with the console at the configured level, default INFO (`:74-99`) — the
  2026-09-03 kyg log does show the short-horizon INFO lines. It may still hold for the standalone
  `sync_short_horizon_hydrograph.py` CLI, whose logging is configured at import time (`:54-58`).
  The defect is the **absence of an aggregate**, not log suppression.
- C5 grew to include `main()` after review showed the pop removal alone would corrupt the CLI's
  failure diagnosis.

## Corrections applied after the confirm-fixes pass (2026-09-03)

- **C2a was reversed.** It previously said a failed read-merge should fall back to
  `[None] * periods_per_year` and keep writing. Given the confirmed field-by-field upsert
  (`crud.py:107-111`), that would have clobbered every stored norm for the station-horizon — the
  exact loss C2 exists to prevent, triggered precisely when we cannot see what is stored. A failed
  read-merge is now `API_FAILED` with no write for that horizon.
- The norm classifier now requires an ordered `list`/`tuple`; a set or dict of the right size could
  otherwise have been graded `VALID` and written in arbitrary order.
- Test 3 asserts literal boundary dates instead of re-deriving them from the helper under test.
- Test 8 constructs the attempted/no-completion metadata directly, because after C1 a norm-absent
  run produces records and would pass regardless of whether C5 shipped.
- Test 4 was split across P1/P2 so no phase boundary leaves the suite red.

## Correction applied 2026-09-04

The owner decision that a missing norm is not our failure was not reflected here: C3 and its test
still required the norm-absent case to be "loud". Now: counts are always printed and neutral, a
norm-absent-only horizon logs at **INFO**, and WARNING is reserved for `sdk_failed`, where the
lookup raised and absence cannot be distinguished from an outage. The long-horizon module's
`DEGRADED` wording predates the decision and is not a precedent to copy.

## Out of scope

- Making the short-horizon path exit non-zero (C4).
- Fixing *why* the SDK raises for the four kyg stations — PREPQ-014, upstream.
- Getting norms into iEH HF for the 48 pentad-normless stations — upstream data entry. Per the
  2026-09-04 decision this is **not** reported as our failure; the counts are recorded so the gap is
  visible and can be raised with iEH HF.
- Percent-of-norm for virtual stations — PREPQ-010.
