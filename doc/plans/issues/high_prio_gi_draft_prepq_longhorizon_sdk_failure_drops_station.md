# PREPQ-015: Long-horizon hydrograph: an SDK-raise still drops the whole station (PREPQ-009's other half)

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: High — affected stations get **zero** month/quarter/season rows, which reproduces
the exact symptom PREPQ-009 fixed (empty last-year runoff, no percent-of-norm in the long-term
monthly bulletin), just reached by a different branch of the same classifier.
**Labels**: `preprocessing_runoff`, `long-horizon`, `data-loss`, `silent-skip`
**Found**: 2026-08-20, filed as a follow-up while implementing INFRA-037.
**Related**: **PREPQ-009** (archived, `issues/archive/high_prio_gi_draft_runoff_longhorizon_norm_decouple.md`,
merged PR #409 `c894edcd`) fixed exactly this asymmetry for the **NORM_ABSENT** branch — this
issue is the residual **SDK_FAILED** branch it left standing. **PREPQ-014** (Low, Draft) —
plausible but unconfirmed upstream cause for *why* the SDK raises for specific stations.

---

## The asymmetry

`write_station_monthly_hydrograph` (`sync_long_horizon_hydrograph.py:337-417`) classifies the
iEH-HF monthly-norm SDK call into one of three outcomes via `_lookup_monthly_norms` /
`_classify_monthly_norms` (`:277-308`):

| Classification | SDK behavior | Rows written? | Stored norm preserved? |
|---|---|---|---|
| `VALID` | returns 12 finite numbers | Yes | n/a (fresh norm used) |
| `NORM_ABSENT` | returns successfully, but not 12 finite numbers | **Yes** — from local data | **Yes**, via `_read_existing_month_norms` read-merge (`:369-375`) |
| `SDK_FAILED` | **raises** an exception | **No** — `records=[]` | n/a — nothing written at all |

The `SDK_FAILED` branch (`:354-366`):

```python
if norm_classification is _NormClassification.SDK_FAILED:
    exc = norm_lookup.exception
    logger.warning(
        "write_station_monthly_hydrograph: SDK call failed for site %s, skipping. "
        "Error: %s: %s", code, type(exc).__name__, exc,
    )
    return LongHorizonStationWriteResult(
        status=LongHorizonStationWriteStatus.SDK_FAILED,
        records=[],
    )
```

returns before reaching the local-data build (`build_monthly_records`, `:399-409`) that the
`NORM_ABSENT` branch falls through to. But `previous`/`current` — the observed monthly runoff the
dashboard actually displays — come from **local daily SAPPHIRE runoff**
(`_read_daily_runoff`, `:377-378`), not from the SDK. A raised SDK call has no bearing on whether
that local data is available. The station's local observations are discarded purely because the
*norm* lookup raised, exactly the coupling PREPQ-009 decoupled for the non-raising case.

The caller, `write_long_horizon_hydrograph` (`:552-622`), confirms the asymmetry downstream:
on `SDK_FAILED` it pops the station from `attempted_station_codes` and skips both the seasonal and
quarterly writers entirely (`:572-579`, `"Skipping seasonal hydrograph for station %s without
monthly records"`); on `NORM_ABSENT` it proceeds to `write_station_seasonal_hydrograph` /
`write_station_quarterly_hydrograph` using the local-data monthly records exactly as `VALID` does.

## PREPQ-009's own framing of this residual

PREPQ-009's terminology table (`archive/high_prio_gi_draft_runoff_longhorizon_norm_decouple.md`)
already named the target state precisely: *"A **valid monthly norm** is a sequence of exactly 12
finite numeric values → status **written**. Every other **successful** SDK return is
**norm-absent** … A **raised** SDK call is **sdk-failed** (skip station)."* That issue **deliberately
kept SDK_FAILED skipping the station** — its Phase P1 acceptance criterion 3 states: *"**sdk-failed**
(SDK raises): **zero** writes for that station across month+season+quarter — no partial row set."*
So the current behavior is not an oversight relative to PREPQ-009 — it was the explicit design at
the time, on the reasoning that a raised call is "unexpected, retryable" and the run's exit code
should key on it (`_exit_code_for_long_horizon_summary`, `:639-645`, returns 4 when
`SDK_FAILED >= 1` and no station is `API_FAILED`; `API_FAILED` is checked first and returns 5).
This issue proposes revisiting that choice: a raise from the *norm* endpoint
specifically does not need to withhold data the norm endpoint has nothing to do with.

## Proposed fix

Make `SDK_FAILED` behave like `NORM_ABSENT` for row-writing purposes: fall through to
`build_monthly_records` using local daily runoff, with `norms` sourced from
`_read_existing_month_norms` (the same read-merge `NORM_ABSENT` already uses to preserve any
previously stored numeric norm) rather than the (unavailable) fresh SDK value. Concretely, treat
the two classifications identically from `:369` onward — the `if norm_classification is
_NormClassification.NORM_ABSENT:` branch's read-merge should also fire for `SDK_FAILED`, and the
`SDK_FAILED`-only early return at `:354-366` should log the same warning but no longer return
`records=[]`.

**Verified detail that de-risks this fix**: `write_station_monthly_hydrograph` also calls
`shh._fetch_sdk_period_actuals(iehhf_sdk, code, "decade", target_year)` (`:386-388`) further down
the same function, using the same `iehhf_sdk` object. That helper (`_fetch_sdk_period_actuals`,
defined in `sync_short_horizon_hydrograph.py:490-616`) wraps its own SDK call in a `try/except Exception`
(`:529-560`) that **degrades to empty `sdk_current`/`sdk_previous` dicts with a logged warning**
rather than re-raising. So during a systemic outage, falling through past the norm lookup does not
risk a second uncaught exception from this later SDK call — it independently fails soft.

### Required guard — do not let this regress the systemic-outage signal

Today, `_exit_code_for_long_horizon_summary` returns exit code 4 when **any** attempted station
has `SDK_FAILED` status **and zero stations have `API_FAILED` status** (`:641-645` — `API_FAILED
>= 1` is checked first and returns 5, so a run with both failure kinds exits 5, not 4) — absent
any API failures, a single flaky station's SDK raise already makes the whole run report non-zero.
Exit 4 is therefore a guarantee that no API read/write failures occurred this run — that invariant
is exactly what `run_locally.sh`'s maintenance runner relies on to downgrade exit 4 to non-fatal
(rc stays 0) while still treating exit 5 as fatal (`run_maintenance_preprocessing_runoff`,
`apps/run_locally.sh:918-934`); this fix must
preserve that invariant, not just the exit-4-on-SDK_FAILED behavior in isolation. If this fix
simply reclassifies `SDK_FAILED` to be treated
identically to `NORM_ABSENT` everywhere (including the exit-code function, which today does *not*
key on `NORM_ABSENT` at all), the run would degrade silently to exit 0 even when the SDK is
**completely down and every station's norm lookup is failing** — writing every station with
whatever `_read_existing_month_norms` happens to preserve (frozen/stale) and reporting success.
That is the specific failure mode this fix must not introduce.

**Design constraint, not yet an implementation decision**: the fix must preserve some way to
detect and fail loudly on a systemic outage — at minimum, when **all** attempted stations end up
`SDK_FAILED`, the run must still exit non-zero (a single flaky station recovering into a
`NORM_ABSENT`-shaped write is a reasonable relaxation; all stations failing the same way in the
same run is much more likely a transport/tunnel outage — see the iEH-HF SSH tunnel + Docker bridge
networking trap recorded elsewhere in this project's operational history — and must not be
absorbed into a quiet DEGRADED line). Whether that's implemented as "keep exit 4 on `SDK_FAILED
>= 1`" (i.e., don't relax the exit code even though rows now get written), "exit 4 only when
`SDK_FAILED == total_attempted`", or a new status distinguishing "wrote from stale/no norm because
SDK raised" from "wrote from stale/no norm because SDK returned garbage" is an open implementation
choice — but *some* form of "all-failed still exits non-zero" must be decided and tested before
this ships, not deferred.

## Operational impact (today, unfixed)

Any station whose norm lookup raises gets **no** month, quarter, or season hydrograph rows for
that run — not degraded rows, none at all. In the long-term monthly bulletin this reproduces
PREPQ-009's original symptom for that subset of stations: empty last-year runoff and no
percent-of-norm, indistinguishable in the dashboard from "never configured." Unlike `NORM_ABSENT`
(which at least writes observed data), an `SDK_FAILED` station silently loses a whole
maintenance/yearly cycle's worth of otherwise-computable local data.

## Upstream cause — hypothesis, not a finding

**PREPQ-014** (Low, Draft) documents the SDK norm lookup raising `No path provided or the provided
path is None` for a small number of kghm sites, attributed (proven from code, not yet tied to
specific sites) to a two-registry seam where `site_uuid` resolves only via the hydrological
registry while the long-horizon work list also injects codes from the virtual registry. It is
**plausible but unconfirmed** that PREPQ-014 is (one of) the mechanism(s) producing `SDK_FAILED`
classifications in production — PREPQ-014's own file states the failing set of 4 sites is not
confirmed, and other causes (transport/tunnel outages, auth expiry, upstream API errors) raise
through the identical `except Exception` at `_lookup_monthly_norms:298-304` and are
indistinguishable from PREPQ-014's cause by classification alone. Do not treat PREPQ-014 as this
issue's root cause without independent confirmation — this issue's fix (write rows regardless of
*why* the SDK raised) is valid independent of which upstream cause turns out to dominate.

## Out of scope

- Diagnosing which upstream condition(s) actually produce `SDK_FAILED` in production — that is
  PREPQ-014's job, not this issue's.
- Changing `_classify_monthly_norms`'s VALID/NORM_ABSENT boundary — untouched by this fix.
- The `API_FAILED` status and its exit code (`:641-642`) — a distinct failure class (write/read
  errors against the SAPPHIRE API itself, not the iEH-HF SDK norm lookup).
- Local monthly-norm derivation (PREPQ-010) — a different, longer-term fix for the *absence* of
  norms; orthogonal to whether an SDK raise should withhold local data.

## Acceptance criteria

- [ ] A station whose monthly-norm SDK call raises still gets 12 month + 1 season + 4 quarter
      rows written from local daily runoff, with any previously stored numeric norm preserved via
      the existing read-merge (same identity-level checks PREPQ-009 P1 used: exact row counts,
      correct `date`/`horizon_value`/`horizon_in_year`, no duplicate keys, `previous`/`current`
      matching local aggregates).
- [ ] When **all** attempted stations are `SDK_FAILED` in one run, the run still exits non-zero —
      test this explicitly, not just the per-station write behavior.
- [ ] A mixed batch (some `VALID`/`NORM_ABSENT`, some `SDK_FAILED`) writes rows for all of them and
      the exit-code decision reflects whatever guard was chosen (all-failed vs any-failed) —
      record the chosen semantics here before implementation, per this draft's open design
      constraint.
- [ ] No regression to PREPQ-009's existing `NORM_ABSENT` behavior or its norm-preservation
      read-merge test coverage.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures,
      zero unexpected skips.
- [ ] No real station codes, discharge values, or credentials in code, tests, fixtures, or logs
      (placeholder `19999` only).
