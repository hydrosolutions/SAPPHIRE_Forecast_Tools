# INFRA-048: the two ingest paths trust data quality they never check, and hide the failures they do detect

**Status**: Draft (2026-09-07)
**Modules**: `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py`,
`apps/preprocessing_gateway/Quantile_Mapping_OP.py`, `apps/preprocessing_runoff/preprocessing_runoff.py`
**Priority**: **Medium** — none of these fires on a healthy day, and none was introduced by the work
that surfaced them. They matter because every one of them turns a *bad* day into either silently
wrong stored data or an invisible failure, on paths that write to the operational database on every
run.
**Labels**: `infra`, `preprocessing_runoff`, `preprocessing_gateway`, `data-quality`, `silent-failure`
**Found**: 2026-09-03/07, across five out-of-loop review rounds on **PREPQ-020** (PR #488) and
**PREPG-023** (PR #489). Both are merged. **Nothing here is a regression from either** — they are
pre-existing weaknesses those reviews walked past, recorded here so they are not re-derived a third
time.
**Related**: **PREPQ-018** (the same narrow exception tuple, filed for the long-horizon module — F1
is its short-horizon twin), **PREPG-016** (ensemble completeness checked only in aggregate),
**INFRA-038** (the `run_CM_models` truth-value split — F6 is a newly reachable consequence).

> Verified against trunk at `613cf4c4` (the PREPG-023 merge). Every citation below was re-checked on
> that tree; several were first reported against pre-merge code and have moved.

---

## Why one issue and not seven

These are the same defect wearing six hats: **the ingest code decides whether a write succeeded from
the absence of an exception, never from the content of what it wrote or read.** Fixing them
one-at-a-time invites the shape this project keeps hitting — a guard added at one call site while
the identical hole stays open two functions away. Fix the principle, in one pass, with one set of
tests.

There is a second, related theme: **failures that ARE detected do not reach the operator**, because
the layer that knows discards its result before the layer that reports gets it (F4).

---

## F1 — the exception tuple is narrower than the failures it must catch

`_API_READ_WRITE_ERRORS` is `(SapphireAPIError, ConnectionError, Timeout)`
(`sync_long_horizon_hydrograph.py:51-56`, imported by the short-horizon module). The installed
client can raise outside that set from a **successful** response — notably
`requests.exceptions.JSONDecodeError` while decoding a 200 body, and `ValueError` from DataFrame
construction.

PREPQ-020 closed this for the *preservation read* by introducing a narrow
`_ShortHorizonNormReadError` and catching it at the per-horizon boundary. **Every other call site
still has the hole**, including `client.write_hydrograph` itself (`sync_short_horizon_hydrograph.py:899`)
and the daily reads.

**This is PREPQ-018 in a second module.** Fix them together or the next reviewer files it a third
time. Do not simply widen the tuple to `Exception` — that would swallow programming errors; the
PREPQ-020 pattern (a narrow named exception raised at the boundary of the fragile call) is the
precedent to follow.

## F2 — `write_hydrograph`'s return value is discarded, so a partial write reads as success

`client.write_hydrograph(records)` (`sync_short_horizon_hydrograph.py:899`) returns the number of
records written. Nothing checks it. The horizon is classified `WRITTEN` and the station counted
complete purely because no exception was raised.

A client that returns `0`, or `71` for a 72-row request, produces a run that logs "Wrote 72 records",
reports `api_failed=0`, and exits 0 with the data missing.

**Note the test-fixture trap**: every fake client in the current suite returns `len(records)`, so no
existing test can detect this. A test that fixes the discrepancy is part of the fix.

## F3 — a batch built from unusable inputs is still written

PREPQ-020 added a guard that refuses to write when a station-horizon has **no** usable daily runoff
for any year. It checks whether rows came back — **not whether they are usable**.

Daily values are coerced with `pd.to_numeric(..., errors="coerce")`, so a year of non-numeric
discharge becomes a year of `NaN`, passes the guard, and produces a batch whose envelope and
`current`/`previous` are null. The API upserts field-by-field, so that batch overwrites previously
good stored values.

**The better guard is on the output, not the input**: refuse when the *built* batch carries no
usable values at all. That subsumes the case PREPQ-020 already closed. Take care that a station with
genuinely sparse history — normal here — is not refused; the condition is "nothing usable anywhere",
not "some fields null".

## F4 — the operational wrapper throws away the failure tally it is given

`_write_short_horizon_hydrograph_records` (`preprocessing_runoff.py:212-218`) calls
`write_short_horizon_hydrograph` and **discards its return value**, then logs
`"Pentad/decad hydrograph rows written."` and returns `None`.

PREPQ-020 gave that function a real per-`(code, horizon)` status tally and an `api_failed_count`.
**Only the standalone CLI reads them.** So on an operational run, every station can fail its API
write and the maintenance output still reports success — the same class of blindness PREPQ-020 fixed
one layer down.

**Hard constraint, and the reason this needs care**: that wrapper documents that this write
"must NEVER abort the operational run", and PREPQ-020 was explicitly forbidden from changing it. The
fix is to **report**, not to abort — fold the counts into the existing
`SHORT_HORIZON_HYDROGRAPH_WRITE_FAILED` note and the maintenance-mode validation output, keeping the
return contract and the exit code exactly as they are.

## F5 — nothing verifies that a norm's position matches its period

`build_*_records` assigns `norm_values[period - 1]` (`sync_short_horizon_hydrograph.py:486`). The
classifier PREPQ-020 added checks that the payload is an ordered sequence of the right length of
finite numbers — it **cannot** check that element *i* is period *i+1*, because the SDK discards
period identity and preserves server response order.

If the upstream order ever changed, every norm would be written to the wrong period, silently, and
the run would report success. There is no cheap client-side detection; the honest options are to
request period-identified norms upstream, or to add a sanity check against previously stored norms
and warn on a wholesale reordering.

**This is a latent risk, not an observed bug.** It is recorded so nobody later reads the classifier
as a stronger guarantee than it is.

## F6 — the ensemble gate can open while the conceptual model is not scheduled

PREPG-023's gate reads `ieasyhydroforecast_run_CM_models` case-insensitively.
`pipeline_docker.py:827` schedules `ConceptualModel` only for **exactly** `"True"`.

So `run_CM_models=true` makes the gateway download and process ensembles while Luigi never runs the
consumer — reinstating the wasted work PREPG-023 removed, for that spelling.

Pre-existing (**INFRA-038**: the variable is read four incompatible ways across the repo), and
PREPG-023 was explicitly forbidden from adding a fifth reading. But it is **newly reachable**: before
the gate, the spelling did not matter because ensembles were always processed. Resolve it as part of
INFRA-038 rather than patching one more site.

## F7 — the ensemble CSV pair is written non-atomically

`{code}_P_ensemble_forecast.csv` and `{code}_T_ensemble_forecast.csv` are written in sequence
(`Quantile_Mapping_OP.py:1221`, `:1225`). A failure between them leaves a new precipitation file
beside an old, truncated or absent temperature file, in a directory the conceptual model reads.

PREPG-023 made process-wide faults (`ENOSPC`, `EROFS`, `EDQUOT`, `ENOMEM`) stop the run rather than
continue, which narrows the window but does not close it. Write to temporary files and rename both
into place, or write both then rename, so a consumer never sees a mixed-generation pair.

---

## Files that may be modified

- `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py` (F1, F2, F3, F5)
- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` (F1 — the shared tuple)
- `apps/preprocessing_runoff/preprocessing_runoff.py` (**F4 only**, and only the reporting; the
  never-abort contract and the return type stay)
- `apps/preprocessing_gateway/Quantile_Mapping_OP.py` (F7)
- the corresponding test modules

**Do not** attempt F6 here — it belongs to INFRA-038.

## Acceptance criteria

- [ ] A client returning fewer records than submitted fails the horizon rather than reporting
      success (F2), and no fake in the suite masks it.
- [ ] A batch whose values are entirely unusable is not written, and a station with sparse-but-real
      history still is (F3).
- [ ] An operational run in which every API write fails reports that failure through the maintenance
      output — **while still exiting 0 and returning the same type** (F4).
- [ ] No `JSONDecodeError` or DataFrame `ValueError` from any client call escapes its per-station or
      per-horizon boundary (F1), in both the short- and long-horizon modules.
- [ ] The ensemble CSV pair is never observable in a mixed-generation state (F7).
- [ ] F5 is either mitigated or explicitly documented as an accepted upstream dependency.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.

## Phases

- **P1 — the exception boundary (F1).** Files: both sync modules + tests. Depends on: none.
  Coordinate with **PREPQ-018**; if that is implemented first, this becomes its second call site.
- **P2 — write verification and the output guard (F2, F3).** Files:
  `sync_short_horizon_hydrograph.py` + tests. Depends on: none.
- **P3 — operator visibility (F4).** Files: `preprocessing_runoff.py` + tests. Depends on: P2 (it
  reports the tally P2 makes trustworthy).
- **P4 — atomic ensemble write (F7).** Files: `Quantile_Mapping_OP.py` + tests. Depends on: none.
- **P5 — F5 decision.** No code until the owner decides mitigate-or-document.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": [], "parallel_agents": 1 },
    "P5": { "depends_on": [], "parallel_agents": 1 }
  }
}
```

## A note for whoever implements this

Five review rounds across #488 and #489 produced **five tests that passed with their fix reverted**.
Every one had the same shape: they asserted a post-condition that was *already true* — "the function
returned None", "the variable is absent", "72 rows were written" — instead of establishing the
failing condition and checking it was handled.

Three of the findings above (F2, F3, F4) are precisely about code that infers success from a
non-event. Write the tests for this issue so they fail on the current code first, and check it.

## Out of scope

- **INFRA-038** (F6, and the `run_CM_models` truth-value split generally).
- **PREPG-016** (ensemble completeness in aggregate — related to F2's theme but its own issue).
- Making the short-horizon write abort the operational run. It must not; F4 is about reporting.
