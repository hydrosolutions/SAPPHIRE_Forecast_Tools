# PREPQ-013: Runoff pagination read-merge — test-coverage and defensive gaps (follow-up to PREPQ-011/012)

**Status**: Draft (2026-07-23)
**Module**: `apps/iEasyHydroForecast/forecast_library.py` (`_merge_preserve_existing_runoff`),
`apps/preprocessing_runoff/test/test_api_write.py`, `apps/linear_regression/test/test_forecast_library_api.py`
**Priority**: Low
**Labels**: `test-coverage`, `hardening`, `runoff`, `follow-up`
**ID note:** `PREPQ-013` is provisional — re-check `doc/plans/module_issues.md` for collisions
before publishing to GitHub, in case a parallel branch has also claimed a PREPQ id.

---

## This is NOT a reopened bug

**PREPQ-011** (period read-merge-write clobbers non-null `discharge`/`predictor` past row 100) and
**PREPQ-012** (daily writer clobbers with no read-merge at all) are **fixed and merged** to
`origin/maxat_sapphire_2` in commit `fb3e4bd3` ("Fix runoff null-clobber: paginated read-merge for
period + daily writers (PREPQ-011/012)"). An out-of-loop adversarial review (2026-07-23) confirmed
the fix is **correct and non-vacuously test-pinned** — see the Resolution sections of
`doc/plans/issues/archive/high_prio_gi_draft_runoff_read_merge_pagination_clobber.md` and
`doc/plans/issues/archive/high_prio_gi_draft_runoff_daily_write_null_clobber.md`.

This issue captures three **LOW**-priority soft spots the same review found in the test suite and
in the read loop's defensiveness. **None of them reopen the data-loss bug for any realistically
sized station.** Do not read this issue as "the fix is incomplete" — it means "the fix's safety
margin isn't fully exercised by tests / has no belt-and-suspenders bound," which is a different
(and much lower-stakes) kind of gap.

### Why the margin is comfortable: the `page_size=10000` reframe

The shared helper (`apps/iEasyHydroForecast/forecast_library.py:3593`,
`_merge_preserve_existing_runoff`) pages the existing-row read with `page_size=10000`
(`forecast_library.py` while-loop at `:3626-3649`). Every real station's archive is far smaller
than one page:

| Horizon | Approx. rows / 16-yr archive |
|---------|------------------------------|
| pentad  | ~1,150 |
| decade  | ~575 |
| daily   | ~5,840 |

So in production this loop essentially never executes more than one iteration — the fix is
effectively "raise the truncation cap from the old service default of 100 to 10000," and that
raised cap is exactly what the row-149-of-150 victim test
(`apps/linear_regression/test/test_forecast_library_api.py:1678`,
`test_maintenance_backfill_preserves_newest_row_beyond_first_page`) pins. The gaps below are about
the loop *mechanism* being under-exercised in combination with a value-preservation check, and
about defensive bounds for inputs that don't occur with any real, honest API implementation — not
about the shipped behavior being wrong for any station in production today.

## Soft spot #1 — Daily preservation not independently pinned across pages

**Evidence:** `apps/preprocessing_runoff/test/test_api_write.py:332`
(`test_daily_maintenance_preserves_existing_discharge`) mocks
`mock_client.read_runoff.return_value = existing` at line 355 — a single fixed return value that
does **not** honor the `skip`/`limit` kwargs the real client/service contract requires. The
pagination-loop mechanics for the daily path are instead tested separately by
`test_daily_initial_paginates` (`test_api_write.py:411`), which asserts call count and increasing
`skip`/`limit` but plants no discharge victim to preserve.

**Consequence:** the daily path's "a victim beyond page 1 survives" guarantee is not proven by a
daily-specific test — it currently rides entirely on the shared helper being proven correct by the
*period* test (`test_forecast_library_api.py:1678`). If the shared helper's daily call site were
ever accidentally reverted to a single unpaginated `read_runoff(limit=100)` call while the period
call site kept its loop, `test_daily_maintenance_preserves_existing_discharge` would still pass
(its victim is on the only page the mock returns), silently missing the regression.

**Acceptance criterion to close:** rewrite (or add alongside)
`test_daily_maintenance_preserves_existing_discharge` so `read_runoff` is a fake that honors
`skip`/`limit` (mirroring the `_paged_read_runoff` pattern already used in
`test_daily_initial_paginates` and in `test_forecast_library_api.py`'s pagination tests), with the
discharge victim placed beyond the first page (e.g. row 149 of 150, matching the period test's
convention). The new/updated test must **FAIL** if the daily call site is reverted to a single
`limit=100` read — that revert-and-confirm-red step is required evidence before closing this item.

## Soft spot #2 — No test combines multi-page concat with a page-2 victim

**Evidence:** the two existing period-path tests exercise the two properties separately:
- `test_maintenance_backfill_paginates_existing_read`
  (`apps/linear_regression/test/test_forecast_library_api.py:1614`) uses
  `n_existing = page_size + 5` (10,005 rows, spanning two pages) but every row carries the same
  `discharge=1.0`/`predictor=2.0` — there is no distinguishing victim value, so it only proves the
  loop's call sequence (`:1671,1673-1676`), not that a merge value surviving from page 2 is
  correctly concatenated and preserved.
- `test_maintenance_backfill_preserves_newest_row_beyond_first_page`
  (`test_forecast_library_api.py:1678`) uses `n_existing = 150` with the real default
  `page_size=10000` — 150 rows fit in a **single** page (`existing.iloc[skip : skip + limit]` with
  `limit=10000` returns all 150 rows in one call), so despite the victim being "beyond row 100" in
  date order, the read that finds it is never actually paginated across two `read_runoff` calls.

**Consequence:** "a victim at `skip=page_size` (i.e. genuinely on page 2, requiring the
`pd.concat(all_existing, ...)` step at `forecast_library.py` to have assembled multiple pages
before the merge) survives concat + merge" is unverified. This is the one gap with the most direct
line to the shipped code (`pd.concat` behavior, key-normalization after concat), even though it is
still very unlikely to matter given the `page_size=10000` reframe.

**Acceptance criterion to close:** one test with `n_existing` deliberately larger than a single
`page_size` (e.g. reuse the `page_size + 5` setup from
`test_maintenance_backfill_paginates_existing_read`, or shrink `page_size` via a fixture/monkeypatch
to force multiple pages over a smaller row count) that plants a distinguishing victim value on the
**second** page and asserts it appears correctly in the written payload after the merge. Must FAIL
if pagination is removed or if `pd.concat` / the `(code, date)` re-keying after concat is broken.

## Soft spot #3 — No max-iteration guard on the read loop

**Evidence:** the `while True:` loop in `_merge_preserve_existing_runoff`
(`apps/iEasyHydroForecast/forecast_library.py:3626-3649`) only terminates on:
- `existing_page is None` (break),
- `existing_page` not a `pd.DataFrame` (logs a warning, break),
- `existing_page.empty` (break), or
- `len(existing_page) < page_size` (break after appending).

There is no upper bound on iteration count or on total rows accumulated. A service or client stub
that ignored `skip` and always returned a full `page_size`-length page would loop unboundedly,
accumulating memory and never terminating.

**Severity note:** this is **theoretical, not observed** — the real `sapphire-api-client` /
preprocessing service both honor `skip`/`limit` correctly (this is not in dispute; it's the same
contract the pagination fix itself relies on). This is purely a defensive-programming gap for a
hypothetical future client/service regression, not a live risk. The original PREPQ-011 issue
already flagged a related defensive idea (a warning if a read returns exactly `limit` rows, as the
truncation signature) as a "defensive extra" that was never acted on — this item generalizes it.

**Acceptance criterion to close:** add a max-iteration (or max-total-rows) bound to the loop that
raises or logs an error and stops instead of looping forever, plus a warning log when a page comes
back at exactly `page_size` rows for enough consecutive iterations to suggest `skip` is being
ignored. A test can fake a `read_runoff` that always returns a full page regardless of `skip` and
assert the loop terminates (raises/stops) within the bound rather than hanging.

## Why these were not fixed alongside PREPQ-011/012

The reviewer that found them explicitly classified the fix as sound; these are hardening items
discovered *while verifying* that fix, not defects in it. Fixing them now would have expanded the
scope of an already-shipped, already-tested change. Filing them separately keeps PREPQ-011/012's
history clean (bug found → fixed → verified) and lets this hardening work be picked up
independently, at low priority, whenever test-coverage time is available.

## Acceptance criteria (when picked up)

- [ ] Soft spot #1: daily preservation test rewritten/added with a paginating fake + victim beyond
      page 1; confirmed to fail on revert to a single `limit=100` read.
- [ ] Soft spot #2: one test combining multi-page concat with a page-2 victim; confirmed to fail if
      pagination or post-concat re-keying breaks.
- [ ] Soft spot #3: max-iteration/row bound added to the read loop, with a fake-client test proving
      termination, plus a warning log on a full-page-at-exactly-`page_size` signature.
- [ ] No change to production behavior for any real, honest client/service response — these are
      test and defensive-code additions only.

## Related

- **PREPQ-011** (`doc/plans/issues/archive/high_prio_gi_draft_runoff_read_merge_pagination_clobber.md`) —
  the bug this hardens test coverage for; fixed in `fb3e4bd3`.
- **PREPQ-012** (`doc/plans/issues/archive/high_prio_gi_draft_runoff_daily_write_null_clobber.md`) —
  sibling daily-writer fix, same commit.
