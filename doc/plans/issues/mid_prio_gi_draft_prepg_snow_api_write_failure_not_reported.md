## A failed snow API write does not fail the task (PREPG-026)

**Status**: Draft (2026-09-05) — **REVIEWED 2026-09-08, NOT safe to implement as written.** An
out-of-loop `codex exec` pass plus an in-loop pass agree: the central instruction below ("a `False`
from `write_snow_to_api` means delivery failed") is **false about the code**, and following it
literally would break this issue's own first contract. Five confirmed defects in the plan are
recorded under "Review findings" before the § Problem section stands as a work order. **Do not
implement until the owner answers the decisions listed there.**
**Module**: `apps/preprocessing_gateway` (`snow_data_operational.py`)
**Priority**: **Medium** — the preprocessing API can go entirely stale while every run reports
success. Not data corruption: the CSV is still written, so nothing is lost, only unpublished.
**Labels**: `preprocessing_gateway`, `snow`, `silent-success`
**Found**: 2026-09-05, by the adversarial diff review of the PREPG-025/009/024 implementation.
Reported as pre-existing, correctly — it is the deliberate scope boundary recorded in **PREPG-009**,
filed here rather than widened into that issue.
**Related**: **PREPG-009** (made task *fetch* failures exit non-zero; explicitly left API delivery
alone). Same silent-success family as PP-051 / PP-054 / LR-010.

---

## Review findings (2026-09-08) — read before the rest of this document

Verified against the code at trunk `dd30c568`. Every finding below was checked at file:line; the
line references are current.

**1. The proposed failure signal does not exist. (Blocking.)** The decision table further down says
a `False` from `write_snow_to_api` means "delivery genuinely failed". It does not. `False` is
returned for **eight** distinct conditions, most of which are not failures:

| `dg_utils.py` | Condition | Delivery failure? |
|---|---|---|
| `:1122-1124` | `sapphire-api-client` not installed | **No** — the one skip CLAUDE.md sanctions |
| `:1126-1129` | `SAPPHIRE_API_ENABLED=false` | **No** — deliberately not writing |
| `:1134-1141` | readiness check failed | **Yes** |
| `:1143-1145` | input frame empty | **No** |
| `:1184-1204` | no rows inside the selected sync window | **No** — normal on a quiet operational run |
| `:1270-1273`, `:1337-1339` | no rows carrying publishable values | **No** |

The docstring (`:1105`) promises only "True if records were written, False otherwise" — a
description, not a failure signal. Propagating the bare boolean would fail the task whenever the API
is deliberately disabled, whenever the client is absent, and on every quiet operational window —
**directly contradicting this issue's first "contract not to break"**. Any implementation must first
make the outcomes distinguishable (an outcome enum, or raising on genuine failure); that is a
prerequisite this plan did not state.

**2. The obvious implementation of "fail the task" silently breaks PREPG-009. (Blocking.)**
`main()` has **no exception boundary** around the task call (`snow_data_operational.py:856-866`). If
"fail on `SapphireAPIError`" is implemented by removing the `except` at `:773` or by re-raising, the
first API rejection aborts the loop and the remaining five HRU/variable tasks never run — they do
not even fetch or write their CSVs. PREPG-009's contract is run-all-then-aggregate. The fix must
convert the error into a falsey **task result**, never an escaping exception — while preserving the
deliberate immediate re-raise of `SnowPreservationReadError` at `:764-772` (PREPG-020).

**3. A partial publication still reports success, and says so out loud.** `write_snow_to_api` calls
`count = client.write_snow(records)` and returns `True` for any returned count without ever
comparing it to `len(records)` (`dg_utils.py:1329-1339`). It then **prints** "Successfully wrote
{count} snow records". Under `SAPPHIRE_SYNC_MODE=initial`, 2000 records selected and 1000 written
is a green run with a success message. This plan asks whether partial writes should fail but records
no decision, so as written it would leave the reported problem half-fixed.

**4. The consistency check verifies the wrong window in two of three sync modes.**
`_check_snow_consistency` hard-codes a `date >= yesterday` window (`snow_data_operational.py:111-126`)
and never consults `SAPPHIRE_SYNC_MODE`. Under `maintenance` or `initial` the writer selects a
historical window the check then finds empty, logs "nothing to verify", and returns `True`. A
missing historical publication therefore *cannot* produce the consistency failure this plan proposes
to handle. Any consistency-related acceptance criterion must either cover all three modes or say
explicitly that the contract is operational-mode-only.

Related: the check also returns `False` when the readback request itself **raises**
(`snow_data_operational.py:180-182`). That is "verification unavailable", not "readback disagrees" —
a third meaning the plan's table does not distinguish.

**5. The same hole exists in `snow_data_renalysis.py`, and it is worse there. (Scope decision.)**
That script shares the writer and repeats the pattern exactly — `False` discarded, `SapphireAPIError`
caught and logged, consistency result dropped, `return True` (`snow_data_renalysis.py:366-387`).
Worse, **it is also PREPG-009 before the fix**: its `main()` logs failures but builds no failure
list, ends with `logger.info("Snow reanalysis processing complete (%d tasks)", total)` counting
*attempted* (`:501`) — the exact wording PREPG-009 identified as the defect — returns nothing, and
is invoked as bare `main()` with no `sys.exit()`. It exits 0 unconditionally. So this issue's claim
to close "the last silent-success hole in the snow path" is **false as scoped**.

### Corrections to this document's own framing

- **`SAPPHIRE_API_ENABLED=false` is no longer "a documented supported mode".** The owner's INFRA-049
  decision deprecates it: the API is the only supported sink, and a missing API is an error. The
  *contract* below is still right for this issue — INFRA-049's P1 deliberately keeps the disabled
  path exiting 0 and flips it in P4 — but the stated reason must not be "it is supported".
- The "6/6 succeeded" example is configuration-dependent, not a constant: the task count is
  `len(SNOW_HRUS) * len(SNOW_VARS)` (`snow_data_operational.py:844`).
- **Rejected, on verification:** the reviewer stated that no production forecast model consumes the
  published snow rows, having checked `machine_learning` only. `apps/long_term_forecasting/data_interface.py:230`
  (`get_snow_data`) reads snow **from the database**, and is called at `:308` and `:1103`. The
  long-term models are real consumers, which raises the stakes of a silent publication failure
  rather than lowering them.

### Minimal design — owner instruction 2026-09-08: "keep changes minimal"

This supersedes the four questions below; they are kept as the record of what was asked. The
outcome-enum idea is **rejected** as too large: it would change a public return type, two production
call sites and seven test files.

**Two production lines change.**

1. `dg_utils.write_snow_to_api` — the readiness-check failure at `:1134-1141` **raises
   `SapphireAPIError`** instead of returning `False`. This is the one genuine delivery failure that
   is currently indistinguishable from the benign cases; raising separates it without touching the
   meaning of the other seven `False` returns, which all stay benign.
2. `snow_data_operational.py:773-775` — the existing `except SapphireAPIError` logs as it does now
   and then **`return False`** instead of continuing.

**Why this is enough.** The plan's concrete failure is "the API is unreachable for all six
HRU/variable calls". Unreachable means `readiness_check()` is false, so today it returns `False` and
is silently indistinguishable from "the flag is off". After (1) it raises, and after (2) the task
reports failure and PREPG-009 turns it into a non-zero exit. Fixing only the `except` clause would
be a one-line change that leaves the reported bug unfixed.

**Why it breaks nothing.**

- `SAPPHIRE_API_ENABLED=false` (`:1126`), client absent (`:1122`), empty input (`:1143`), quiet sync
  window (`:1184`), no publishable values (`:1270`, `:1337`) all still return `False` and still exit
  0. The first contract holds.
- The result is a falsey **task result**, not an escaping exception, so PREPG-009's
  run-all-then-aggregate survives (Review finding 2).
- `SnowPreservationReadError` is caught earlier at `:764-772` and still re-raises (PREPG-020).
- `snow_data_renalysis.py:366-387` catches `SapphireAPIError` and continues, so its behaviour is
  **unchanged**: today readiness-false returns `False` and it returns `True`; after this change it
  catches the raise and still returns `True`. The blast radius stays inside the operational path.
- `extend_era5_reanalysis.py` does not use this writer.

**Test work.** **Two** tests assert readiness-false returns `False` for *this* writer and must be
deliberately inverted to expect the raise: `test_api_integration.py:62` and `:271`. **Four** tests
share the name `test_api_not_ready_returns_false`; the other two (`:656`, `:1214`) exercise
`extend_era5_reanalysis._write_meteo_to_api` and `Quantile_Mapping_OP._write_meteo_to_api` — the
meteo writers, which this change does not touch. **Do not modify those two.** One existing test is **vacuous and must be fixed either way**:
`test_api_failure_non_fatal_csv_still_written` (`:911-935`) sets `get_operational` to raise, so the
function returns at the fetch stage and never reaches the API write — the `SapphireAPIError` it
mocks never fires, and it therefore pins nothing. New tests: API unreachable exits non-zero; flag
off exits 0; client absent exits 0; and **all six tasks are still attempted after the first one
fails its write**.

**Deliberately not done, on the same instruction:**

- **Partial writes** (Review finding 3) keep today's behaviour. Still a real hole; needs its own
  decision.
- **The consistency check** keeps today's behaviour — warn only, operational window only (Review
  findings 4). No path is made to fail on it.
- **`snow_data_renalysis.py`** (Review finding 5) is left alone and should be filed separately: it
  needs the PREPG-009 treatment (failure list, `sys.exit(main())`) more than it needs this change.
  **Until that is done, this issue does not close "the last silent-success hole in the snow path"**
  — it closes the operational one.

### Decisions originally raised (superseded by the minimal design above)

1. How should each of the eight `False` reasons be classified — failure, benign skip, or benign
   no-op? (This is the blocking one; nothing can be built without it.)
2. Should a **partial** batch write fail the task?
3. Is the consistency contract operational-mode-only, or must it follow `SAPPHIRE_SYNC_MODE`?
4. Does this issue also fix `snow_data_renalysis.py` — including giving it the PREPG-009 treatment —
   or does that become its own issue? (Fixing only the operational path leaves the reanalysis path
   green while stale.)

---

## Problem

`get_snow_data_operational` returns `True` even when the snow data never reached the API. Three
paths do this:

- `write_snow_to_api` returns `False` — the return is assigned and then not acted on;
- a `SapphireAPIError` is caught and logged, and execution continues;
- the consistency check returns `False` — the result is discarded.

The function then returns `True`. After PREPG-009 that `True` is counted as a succeeded task, the
summary reports `6/6 succeeded`, and the process exits 0.

**Concrete failure**: with `SAPPHIRE_API_ENABLED=true`, the API is unreachable for all six
HRU/variable calls. Each writes its CSV, posts nothing, returns `True`; the run exits 0. Automation
reports success while the preprocessing API — which the dashboard and the models read — stays
completely stale.

This is the last silent-success hole in the snow path. PREPG-025 made a missing upstream day
recoverable; PREPG-009 made a failed *fetch* visible; a failed *publish* is still invisible.

## Why it was deliberately excluded from PREPG-009

Counting API delivery as task failure changes what a green run means, and the change is not free:

- `SAPPHIRE_API_ENABLED=false` is a documented supported mode. Whatever is built must leave that
  mode exiting 0, or every CSV-only deployment starts failing.
- The consistency check is opt-in (`SAPPHIRE_CONSISTENCY_CHECK`) and its failure is a *mismatch*
  signal, not a delivery failure. Do not conflate the three paths.
- A partial batch write is a different condition again from "no write at all".

So this needs its own decision, not a widened scope on an issue that had already been settled.

## Owner decision needed before implementation

**Which of the three paths should fail the task?** They are not the same condition:

| Path | Meaning | Suggested |
|---|---|---|
| `write_snow_to_api` returns `False` | **eight different conditions — see Review finding 1; this row is wrong as written** | must be split before any decision is possible |
| `SapphireAPIError` caught | delivery genuinely failed | should fail the task |
| consistency check `False` | written, but readback disagrees | probably warn, not fail — different condition |

And: should `SAPPHIRE_API_ENABLED=false` exit 0 (almost certainly yes), and should a *partial*
batch write count as failure?

## Contract not to break

- **`SAPPHIRE_API_ENABLED=false` must still exit 0 — for now.** Not because it is supported: the
  owner's INFRA-049 decision deprecates it. It stays exit-0 here because INFRA-049 sequences this
  issue as its P1 and flips the disabled path to an error in its P4. Turning it red here would
  land that change out of order, ahead of the policy documents.
- **Do not stop writing the CSV.** The CSV write happens before the API write and must continue to,
  so a delivery failure never costs data — only publication.
- **Do not weaken PREPG-009 or PREPG-025.** A fetch failure must still fail the task, and an
  incomplete fallback window must still write nothing. Specifically: **all six tasks must still
  run even when one fails its API write** — see Review finding 2, which is the trap in the
  obvious implementation.
- Keep the three paths distinguishable in the log; an operator needs to know whether the API was
  unreachable, rejected the payload, or disagreed on readback.

## Acceptance criteria

- With the API unreachable and `SAPPHIRE_API_ENABLED=true`, the run exits non-zero.
- With `SAPPHIRE_API_ENABLED=false`, the run exits 0 and behaves exactly as today.
- The CSV is still written in both cases.
- Whichever paths the owner decides should fail are pinned by tests that fail if the change is
  reverted; whichever should only warn are pinned as *not* failing.
- **All tasks still run after one fails its API write** — a test asserting six attempts, not one.
- With `sapphire-api-client` absent, the run still exits 0 (the dependency-gated skip CLAUDE.md
  sanctions), distinct from "the API was reachable and refused".
- Whatever is decided for partial writes and for the reanalysis path is pinned by a test either way.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.
