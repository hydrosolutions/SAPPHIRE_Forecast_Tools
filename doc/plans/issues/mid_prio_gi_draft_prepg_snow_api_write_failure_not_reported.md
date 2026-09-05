## A failed snow API write does not fail the task (PREPG-026)

**Status**: Draft (2026-09-05)
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
| `write_snow_to_api` returns `False` | delivery genuinely failed | should fail the task |
| `SapphireAPIError` caught | delivery genuinely failed | should fail the task |
| consistency check `False` | written, but readback disagrees | probably warn, not fail — different condition |

And: should `SAPPHIRE_API_ENABLED=false` exit 0 (almost certainly yes), and should a *partial*
batch write count as failure?

## Contract not to break

- **`SAPPHIRE_API_ENABLED=false` must still exit 0.** It is a supported mode; breaking it would
  turn every CSV-only deployment red.
- **Do not stop writing the CSV.** The CSV write happens before the API write and must continue to,
  so a delivery failure never costs data — only publication.
- **Do not weaken PREPG-009 or PREPG-025.** A fetch failure must still fail the task, and an
  incomplete fallback window must still write nothing.
- Keep the three paths distinguishable in the log; an operator needs to know whether the API was
  unreachable, rejected the payload, or disagreed on readback.

## Acceptance criteria

- With the API unreachable and `SAPPHIRE_API_ENABLED=true`, the run exits non-zero.
- With `SAPPHIRE_API_ENABLED=false`, the run exits 0 and behaves exactly as today.
- The CSV is still written in both cases.
- Whichever paths the owner decides should fail are pinned by tests that fail if the change is
  reverted; whichever should only warn are pinned as *not* failing.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.
