## A partial ensemble is accepted as success: nothing checks that 50 members arrived (PREPG-016)

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Low — DORMANT.** *Was Medium-latent; the blocking question is now answered
empirically (§ How likely is this, really) and the answer closes it.*
**Labels**: `preprocessing_gateway`, `data-quality`, `silent-failure`
**Found**: 2026-08-20, by the out-of-loop adversarial review of the PREPG-010 diff. Not observed in
production; no log or output artifact is known to exhibit it.
**Related**: PREPG-009 (same module, same family — a total snow-task outage reported as success).
PREPG-010 (the retry fix whose review surfaced this; see § Relationship to PREPG-010 — the two are
orthogonal and 010 neither causes nor worsens this).

---

## The defect

The ensemble download loop requests 50 members one at a time and appends whatever each call
returns (`Quantile_Mapping_OP.py:913`, and `:939` on the `yesterday` fallback path). Nothing
anywhere then checks how many members actually arrived.

`merge_ensemble_forecast` validates only **aggregates**:

| Guard | Line | What it catches |
|---|---|---|
| `if not files_downloaded` | `:264` | the flattened list is completely empty |
| `if P_ensemble.empty` | `:293` | no precipitation rows at all |
| `if T_ensemble.empty` | `:296` | no temperature rows at all |

There is no count, no `nunique()` on `ensemble_member`, and no comparison against the 50 that were
requested — verified by search across the module. So **one** member's P/T pair is sufficient to
pass every guard. The run then writes ensemble output (`:998-999`) and exits 0.

## Why it matters

The output is a forecast *distribution*. Its whole purpose is spread across members. A one-member
or six-member "ensemble" is not a degraded distribution — it is a point forecast wearing the shape
of a distribution, and nothing downstream can tell the difference. The conceptual model consumes
these files filtering only on `ensemble_member` (`functions_operational.R:492`), so it will happily
run on whatever arrived.

This is the same failure *shape* as PREPG-009: the run is loud about nothing, exits 0, and the
damage is wrong data rather than a visible error.

## How likely is this, really — ANSWERED 2026-08-20, and the answer is "it cannot happen today"

The question was: can `get_ensemble_forecast` return **successfully with an empty file list** —
an HTTP 200 carrying no links — rather than raising? Only a 200-with-empty-list arms this issue,
because `_call_api` turns every non-200 into a `ValueError` (`client_base.py:59`).

**Measured against the live Data Gateway 2026-08-20.** Every no-data and invalid case returns
**HTTP 400**, never 200-with-empty-list:

| request | result |
|---|---|
| valid HRU + date | 200, 2 entries |
| date 400 days in the past | 200, 2 entries |
| date 10 days in the **future** | **400** |
| model 51 (outside 1..50) | **400** |
| nonexistent HRU | **400** |

The no-data body is
`{"message": "Couldn't find any files for the given HRU code, date and models! ", "success": false}`
— i.e. the **same 400 that drives the today→yesterday fallback**. So an unavailable member raises;
it does not return `[]`.

**Verdict: dormant.** The loop cannot silently skip a member, so the one-member-ensemble scenario
is unreachable through this path. Reduced to **Low**.

**What would re-arm it**, and why the issue is kept rather than closed: a gateway change that
starts returning 200 with an empty or partial list, or a caller that begins swallowing the
`ValueError`. The aggregate-only guards at `:264`/`:293`/`:296` are still the only thing standing
between a partial download and a written output — nothing in *our* code enforces member count.
**Five probes are not a contract.**

## Relationship to PREPG-010 — orthogonal, worth stating explicitly

PREPG-010's retry does **not** create, worsen, or mask this. A transport fault now either succeeds
on retry or propagates and kills the run loudly — it never yields a silently-short ensemble. This
issue concerns the case where **no exception is raised at all** and the member is simply absent
from a successful response.

One honest caveat about the test suite PREPG-010 added: those tests deliberately return `[]` for 49
members and one real P/T pair for the recovering member, because the aggregate-only guards permit
it and that keeps the fixture cheap. That is a legitimate way to test *retry* behaviour, but it
means **no test in the module asserts that a complete ensemble reaches the operator**. Fixing this
issue should add that assertion; it should not require rewriting PREPG-010's retry tests.

## Proposed fix

- After the flatten, compare distinct `ensemble_member` values against the 50 requested.
- **Decide the policy explicitly, with the owner** — this is the real content of the issue, not the
  check itself:
  - fail loudly on any shortfall (safest, but one flaky member kills a run that PREPG-010 was
    written to keep alive — these two must not fight each other); or
  - proceed with a **prominent** warning above a documented threshold and fail below it; or
  - always proceed but record the realised member count alongside the output so a consumer can
    see what it is holding.
- Whichever is chosen, the member count must become **visible** rather than assumed.

## Acceptance criteria

- A run in which only a subset of the 50 members returns data does not silently produce ensemble
  output that looks complete.
- The realised member count is asserted by a test at the `main()` level, not only in a unit test of
  the merge helper.
- The chosen shortfall policy is pinned by a test at both boundaries (just-acceptable and
  just-unacceptable).
- PREPG-010's existing retry tests still pass unmodified — if the new check forces them to change,
  the check is wrong or the policy is too strict.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not make a shortfall silently fatal to the whole gateway run without deciding it deliberately.**
  The module `break`s on first failure, so ERA5 extension and snow ride on this exit status —
  the same coupling that made PREPG-010 worth fixing.
- The `ensemble_member` column and its values are consumed by the conceptual model
  (`functions_operational.R:492`); do not renumber or reindex members as part of adding a count.
