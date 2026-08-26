# PREPQ-019: Discharge-aggregation backfill discards the writers' statuses and exits 0 over failed norm lookups

**Status**: Draft (2026-08-26)
**Module**: `apps/preprocessing_runoff/backfill_discharge_aggregation.py`
**Priority**: **Mid** — silent success, and the data it silently gets wrong is *historical* and
persisted. Held at Mid rather than High because the tool is operator-invoked and one-time (not on
any cron), the checklist requires a `--dry-run` and a diff review first, and it has a `verified`
flag — mitigations that reduce exposure but, as shown below, do **not** cover this gap.
**Labels**: `preprocessing_runoff`, `long-horizon`, `backfill`, `silent-success`, `exit-code`
**Found**: 2026-08-26, out-of-loop review of the kghm operator handover (PR #480), while
establishing which callers reach `write_long_horizon_hydrograph`.
**Related**: **PREPQ-015** (Implemented, PR #475) changes what this path captures for an
SDK-failed station — see "What PREPQ-015 changes" below. Same defect family as **PP-051**,
**ML-021**, **PP-054**: a run that writes nothing, or the wrong thing, and exits 0.

---

## The defect

`compute_backfill_records` (`apps/preprocessing_runoff/backfill_discharge_aggregation.py:91-110`)
runs both hydrograph writers against a `_CapturingClient` and returns only the captured records:

```python
sync_shh.write_short_horizon_hydrograph(codes, iehhf_sdk, capturing_client, target_year, today)
sync_lhh.write_long_horizon_hydrograph(codes, iehhf_sdk, capturing_client, target_year, today)
return list(capturing_client.captured)
```

Discarding the return values is **deliberate and documented** — the docstring states the captured
list "not the writers' own return values -- is the source of truth, since it is exactly what would
have been sent to `write_hydrograph`." That reasoning is sound for deciding *what to write*. It is
not sound for deciding *whether the run succeeded*, and nothing else in this module recovers the
status signal.

`write_long_horizon_hydrograph` classifies each station as `WRITTEN`, `NORM_ABSENT`, `SDK_FAILED`
or `API_FAILED`, and its own `main()` maps those to graded exit codes (4 for SDK failure, 5 for
API failure). Routed through this backfill, all of that is dropped at `:109`. `main()`
(`:477-538`) then logs a per-year summary — `record_count`, `added`, `unchanged`, `changed`,
`verified`, `snapshot_file` (`:511-524`) — none of which carries a station-status field, and calls
`sys.exit(0)` unconditionally at `:525`. The only non-zero exits are `RuntimeError` → 1
(`:527-533`), an argument error → 2 (`:496`), and an unexpected exception → 3 (`:536-538`).

**Failure scenario.** An iEasyHydro-HF outage, an expired credential, or a partial API read failure
part-way through a multi-year backfill:

1. Each affected station's norm lookup raises. `_lookup_monthly_norms`
   (`sync_long_horizon_hydrograph.py:295-300`) catches `Exception` broadly and classifies it
   `SDK_FAILED`; an API read failure inside the write loop is caught and classified `API_FAILED`.
   Neither raises out of the writer.
2. The writer continues and still produces records for those stations (post-PREPQ-015), with norms
   read-merged from whatever the API already held — for a station with no stored norm, none.
3. `compute_backfill_records` returns those records. The status that said "this station's norm is
   missing because the SDK was down, not because it has no norm" is gone.
4. The diff report shows a plausible `added`/`changed` count, `verified` passes, and the process
   **exits 0**.

The persisted result is a historical aggregation silently missing norms for an arbitrary subset of
stations, indistinguishable in the report from stations that legitimately have none.

**Why the existing mitigations do not cover it.** The dry-run diff shows what *was* captured, not
what *failed to be* captured — a station whose norm lookup died contributes rows to the diff, so
nothing in the report looks anomalous. `verified` re-reads what was written and compares it against
the same captured records, so it confirms the write, not the input. And because the writers log the
SDK failure at WARNING, the evidence exists in the log — but the operator is given an exit 0 and a
clean summary as the signal to trust.

## What PREPQ-015 changes

Before PREPQ-015, a station whose norm lookup raised was dropped by the writer and contributed
**zero** records, so the same outage showed up as a smaller `added` count — equally silent, and
arguably harder to notice. After PREPQ-015 the station contributes its full 12/1/4 rows with an
absent-or-preserved norm. PREPQ-015 does not introduce this defect and does not worsen the exit
code; it changes the shape of the wrong output from "rows missing" to "rows present, norms
missing." Stated plainly so the fix is not misattributed.

## Proposed fix (not implemented here)

Recover the status signal at the seam that currently drops it, without changing what gets written:

- Have `compute_backfill_records` return the writers' status summaries alongside the captured
  records (a small dataclass or tuple), leaving the captured list as the source of truth for
  *content*.
- Surface per-status counts in the per-year summary logged at `:511-524` — at minimum
  `sdk_failed` and `api_failed` — so the diff report an operator reviews shows them.
- Exit non-zero when any station reached a non-`WRITTEN`/`NORM_ABSENT` terminal status. Reusing
  `sync_lhh._exit_code_for_long_horizon_summary`'s grading (5 for API failure taking precedence
  over 4 for SDK failure) keeps one convention across both entrypoints; a plain non-zero would also
  be an improvement over today.

**Constraint the fix must preserve**: `--dry-run` must keep writing nothing, and the captured
records must remain the sole determinant of *what* is written in apply mode. This issue is about
reporting and exit status, not about changing the backfill's output.

**Open design question for the owner**: whether a `SDK_FAILED` station should abort the backfill
before the apply phase rather than merely report afterwards. Given the tool rewrites historical
rows, refusing to persist a knowingly-degraded backfill may be the correct behaviour — but that is
a scope/semantics decision, not a defect fix, and is deliberately left open here.

## Acceptance criteria

- A backfill run in which at least one station's norm lookup raises exits **non-zero**, in both
  `--dry-run` and apply mode.
- The per-year summary logged by `main()` includes station-status counts covering at minimum
  `sdk_failed` and `api_failed`.
- A run where every station is `WRITTEN` or `NORM_ABSENT` still exits **0** — a legitimately
  norm-absent station must not become a failure (this is the LR-010 trap: do not convert a valid
  skip into a false alarm).
- `--dry-run` still performs no writes, and the set of records written in apply mode is byte-identical
  to today's for a run with no failures.
- A regression test drives `compute_backfill_records` with an SDK that raises for one station and
  asserts both the non-zero exit and the status count in the summary.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero
  unexpected skips.
