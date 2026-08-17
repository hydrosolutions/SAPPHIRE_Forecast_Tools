## Module failures are unattributable: causing events logged at DEBUG, specific exit codes overwritten to 1 (INFRA-024)

**Status**: Draft (2026-08-17)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`, `apps/run_locally.sh`
**Priority**: **Medium** — no data is lost or corrupted, but both defects cost real diagnostic time
and one of them silently contradicts a contract restated across several issue files.
**Labels**: `infra`, `observability`, `exit-codes`, `logging`
**Found**: 2026-08-17, during the PREPQ-014 root-cause investigation. Defect B was surfaced by an
out-of-loop `codex exec` review and re-verified by direct code reading.
**Related**: PREPQ-014 (where both were found), PREPG-009 (partial-failure reporting), PP-051
(silent-success family), INFRA-023.

---

## Why these are filed together

Two independent defects, one operator-facing problem: **when a run fails, the log tells you which
module and roughly what kind, but not which events caused it nor — machine-readably — which class.**
Defect A hides *which events*; defect B discards the *machine-readable class*. Fixing either alone
still leaves a failure that cannot be attributed without reading source code — which is exactly what
PREPQ-014 required.

The title says "unattributable", which is shorthand: log *text* does preserve the module and a broad
failure class. What is lost is event-level cause and any status a script could branch on.

They are in different files and ship independently. Split them if that suits the work better; they
are together because a fixer should know both exist before deciding what "reported properly" means.

## Defect A — the events that cause exit 4 are invisible at default log level

`write_station_monthly_hydrograph` logs an SDK norm failure at **`logger.debug`**:

```python
if norm_classification is _NormClassification.SDK_FAILED:
    exc = norm_lookup.exception
    logger.debug(
        "write_station_monthly_hydrograph: SDK call failed for site %s, skipping. "
        "Error: %s: %s", code, type(exc).__name__, exc,
    )
```
— `sync_long_horizon_hydrograph.py:354-362`

Default level is INFO (`:100-104`), so **nothing is emitted**. The run then exits 4 with only an
aggregate count:

```
ERROR - Long-horizon monthly hydrograph ingestion completed with 4 SDK norm lookup failure(s).
```

Note the asymmetry with the sibling status: `API_FAILED` gets an aggregate `logger.warning` naming
the affected station count (`:616-621`), while `SDK_FAILED` gets **no** per-station or aggregate
warning at all — only the terminal count in `main()`. The status with a dedicated exit code is the
one that reports least.

**Consequence, observed:** in PREPQ-014, the visible WARNINGs came from the *short-horizon* module
and drive no exit code, while the four events that actually caused exit 4 were never printed. The
two signals looked unrelated. Attributing them took a multi-round investigation and a source read.

`_lookup_monthly_norms` catches bare `Exception` and maps everything to `SDK_FAILED` (`:295-304`) —
connection and auth failures, station-lookup non-200, malformed JSON, missing keys,
float-conversion errors, norm-endpoint non-200. So the count alone cannot distinguish an outage from
a per-site configuration gap.

## Defect B — `run_locally.sh` normalises a recorded-FAIL module's exit code to 1

The inner maintenance function receives the module's specific code and logs the right diagnostic:

```bash
elif [ $lt_rc -eq 4 ]; then
    log ERROR "Long-horizon hydrograph sync had SDK norm lookup failure(s)"
    rc=$lt_rc
```
— `run_locally.sh:725-727`

But `print_summary` returns 1 whenever any module failed (`:1585-1588`), and the caller does:

```bash
print_summary "$pipeline_elapsed" || exit_code=1   # :1974
...
exit $exit_code
```

so the specific code is discarded before the script exits. **The process exits 1.** The distinction
survives only in log text.

**Scope the claim carefully.** This holds for any module whose failure is *recorded in the summary*
— which is the PREPQ-014 path. It is **not** universal: a target that returns a specific code
without recording a FAIL result can leave `exit_code` intact, because `print_summary` then returns 0
(see `run_locally.sh:918-955`, `:1928-1929`, `:1973-1977`). An earlier draft of this issue claimed
"every module's exit code", which is wrong. Establish the true set before designing the fix.

**Blast radius, stated precisely:** `run_locally.sh` is the local/dev runner and production
schedules go through cron → Luigi (`doc/deployment.md:912`), so on the **documented** architecture
this does not affect production alerting. That scoping is *inferred from the docs, not verified* —
no deployed crontab was inspected, and INFRA-023 is a live demonstration that scheduling docs drift
from installed reality. Treat "dev-facing only" as the likely case, not an established one. What it
demonstrably affects is developer and reviewer trust: several issue files state a "contract" that exit
codes 2/4/5 "are consumed by `run_locally.sh`", and anyone reasoning about `$?` from a wrapper
invocation on that basis gets the wrong answer. PREPQ-014's own "Contract not to break" said this
before it was corrected.

**Related shell worth reading, but not an equivalent implementation:**
`bin/yearly_runoff_hydrograph_aggregation.sh:190-211` reads the container's true status via
`docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}'` rather than `$?` after a `| tee`
pipeline. That solves a *different* problem — recovering one container's status through a pipeline —
and does **not** address `run_locally.sh`'s actual question, which is what a *multi-module summary*
should exit with. Do not treat it as a drop-in reference.

## What to inspect

1. Whether `SDK_FAILED` should log per-station at WARNING, or emit a counts-only summary keyed by a
   **normalised reason/stage** — e.g. `station_lookup_non_200`, `norm_path_unresolved`,
   `norm_endpoint_non_200` — rather than by exception class. **Exception class alone is not
   sufficient:** the installed SDK raises `ValueError` for a missing path *and* for a non-200 norm
   endpoint, and a non-200 station lookup also becomes the missing-path `ValueError`, so
   `ValueError=4` would not distinguish an outage from a config gap. An earlier draft of this issue
   proposed exactly that and was wrong. The goal remains a summary that discriminates cause
   **without putting station codes in logs**.
2. Whether the same DEBUG-only pattern hides causes elsewhere. `sync_long_horizon_hydrograph.py:370`
   (norm absent) and `:607` (per-station API failure) are the same shape; audit siblings in
   `preprocessing_runoff` and the PP-051 recalc family before deciding a convention.
3. Whether `run_locally.sh` should propagate the first non-zero module code, the highest, or a
   dedicated aggregate — and what `print_summary` should return when it must both print and not
   clobber. This is a design choice, not a bug fix; do not just delete the `|| exit_code=1`.
4. Whether any caller (CI, a developer script, `bin/*`) actually consumes `run_locally.sh`'s exit
   code today. If nothing does, defect B is documentation-only and should be fixed by correcting the
   claim rather than the code.

## Acceptance criteria

- A run that exits non-zero prints, at default log level, enough to attribute the failure to a
  class — without emitting real station codes.
- `SDK_FAILED` reports at least as loudly as `API_FAILED` does today.
- Either `run_locally.sh` propagates the specific exit code, **or** every document claiming it does
  is corrected. Not neither.
- A **shell-level** test asserting the final exit contract of `run_locally.sh`. The existing
  `apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py` inspects only the
  function body and cannot catch this class of defect.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` green.

## Contract not to break

- Exit codes 2 / 4 / 5 from `sync_long_horizon_hydrograph.py` are branched on by
  `run_maintenance_preprocessing_runoff`; do not renumber without updating that mapping — and note
  that the mapping currently affects log text only (defect B).
- Do not raise `SDK_FAILED` per-station logging to WARNING **and** keep the terminal aggregate ERROR
  without checking the volume: a deployment with many virtual stations would emit one line per
  station per horizon. Counts-only is likely the right shape.
- **Requested behaviour change, NOT an existing contract: no real station codes at WARNING or
  above.** Today's short-horizon warnings *do* include codes
  (`sync_short_horizon_hydrograph.py:633-650`, and `:716-722` can list failed codes), so this is a
  new requirement, not an invariant to preserve. An earlier draft listed it as a contract not to
  break, which would have misled a fixer into thinking the codebase already complied. If the owner
  wants it, audit every existing warning path as separate work — several deployments ship logs
  off-host.
- `bin/yearly_runoff_hydrograph_aggregation.sh`'s `docker inspect` exit-status handling must be
  preserved if that job is ever rerouted; `$?` after a `| tee` is the pipeline's code, not the
  container's.
