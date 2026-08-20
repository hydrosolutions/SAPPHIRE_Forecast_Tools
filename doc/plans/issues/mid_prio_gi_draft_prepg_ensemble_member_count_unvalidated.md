## A partial ensemble is accepted as success: nothing checks that 50 members arrived (PREPG-016)

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Medium — latent, not observed.** See § How likely is this, really — the severity
turns on a question about Data Gateway behaviour that has not been answered.
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

## How likely is this, really — the open question

**Do not assign a firm severity before answering this**, and do not let the vividness of the
scenario substitute for evidence:

> Can `client.ecmwf_ens.get_ensemble_forecast(...)` return **successfully with an empty or short
> file list** for a member — an HTTP 200 whose body contains no links — rather than raising?

- If it always raises when a member is unavailable (the DG client turns a non-200 into a
  `ValueError`, and the today→yesterday fallback is built on exactly that), then the loop cannot
  silently skip members and this issue is **dormant** — worth a cheap assertion, nothing more.
- If a 200-with-empty-list is reachable, the loop swallows it as `[]` and the defect is **live**.

That question is answerable from a DG response capture or from the client's own behaviour; it is
not answerable by reading `Quantile_Mapping_OP.py`, which is why this is filed rather than fixed.

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
