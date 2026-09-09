# INFRA-050: `validate_pipeline` exits by traceback on unwritable paths and a malformed baseline

**Status**: Draft (2026-09-08)
**Module**: `apps/validate_pipeline/validate_pipeline.py`
**Priority**: **Low** — both need an operator to supply a bad path or a corrupted baseline file, and
`validate_pipeline` has no production invoker (it runs only from `apps/run_locally.sh`, so this is a
dev gate). Filed because they are the same shape as INFRA-045's findings and were found while
reviewing it.
**Labels**: `infra`, `validate_pipeline`, `robustness`, `exit-contract`
**Found**: 2026-09-08, second out-of-loop review of INFRA-045.
**Related**: **INFRA-045** (the configuration-robustness issue these were split out of — see its
decision **D6**), **INFRA-024** (exit-code attribution).

---

## Why this is separate from INFRA-045

INFRA-045 had already been re-scoped twice (two findings withdrawn, three owner decisions) when
these surfaced. Owner decision **D6, 2026-09-08**: file separately rather than grow it again, and
stop describing INFRA-045 as closing configuration robustness *generally* — it closes five named
findings.

## The module's contract

```
Exit codes:
    0 — all checks passed (or skipped/warned)
    1 — at least one check FAILed
```

Both findings below violate it the same way INFRA-045's F2 and F4 did: the process exits via an
uncaught traceback instead of a `[FAIL]` row.

## F1 — output and baseline writes are unguarded

`Path.write_text()` is called without a guard at `validate_pipeline.py:253` and `:1542`
(re-derive with `grep -n` at implementation time). An unwritable or non-existent parent directory
raises `OSError`, which escapes to the CLI.

**Reachable how**: `--output-json /no/such/dir/out.json`, a read-only destination, or a full disk.

**Fix**: catch and convert to a `[FAIL]` row naming the path and the OS error, exit 1.

**Read side too (added 2026-09-08 after review — it was omitted from the first draft).**
`Path.read_text()` at `:278` can raise `PermissionError`, `IsADirectoryError` and other `OSError`s,
while the post-phase catch handles only `FileNotFoundError` and `ValueError` (`:1567`). Same family,
same fix.

## F2 — a syntactically valid but wrong-shaped baseline raises `AttributeError`

`baseline.get(...)` at `:278` assumes the parsed JSON is an object. A baseline file containing a
valid JSON *array* — `[]` — parses fine and then raises `AttributeError`, **outside** the
post-phase catch.

**Reachable how**: a hand-edited or wrong-file baseline. (**Not** an ordinarily truncated file —
that raises `JSONDecodeError`, which is already caught as `ValueError`. The gap is JSON that parses
successfully but is not an object.) Note INFRA-045's F1 shows one
way to produce a corrupted baseline in the first place (`--output-json` and `--baseline` pointing at
the same path), so these two interact.

**Fix**: validate the parsed baseline's **shape at every level it is indexed**, not just the top —
checking only that the top level is a mapping still leaves `_meta=[]` failing at `meta.get`, and a
per-check entry such as `"Runoff (day)": []` failing in `compute_deltas` (`:309`). State and test
the required shape for the top level, for `_meta`, and for each result entry. A malformed baseline
becomes a `[FAIL]` naming the file and what was wrong with it, exit 1.

## Tests

- `--output-json` at an unwritable path → `[FAIL]` naming the path, exit 1, no traceback.
- `--baseline` pointing at a file containing `[]` → `[FAIL]` naming the file, exit 1, no traceback.
- A valid baseline still loads and compares unchanged.
- Each proven by its own subprocess assertion over combined stdout+stderr with the exact exit code —
  not a repo-wide traceback grep, which cannot show the intended validation actually ran.

## Sequencing — depends on INFRA-045

INFRA-045's **D4** adds resolved-horizon metadata to the baseline and refuses a baseline whose
horizons do not match. That changes the same serialisation and loading path this issue guards, so
**do this after INFRA-045's P1, or rebase onto its horizon-metadata contract**. Doing them
independently risks two incompatible baseline-shape validators.

## Control flow — DECIDED 2026-09-08

**Appending a `[FAIL]` row is not sufficient to make these exit 1.** `exit_code` is computed before
the output/baseline writes happen, and `--phase pre` then returns 0 unconditionally, so a write
failure would be printed and the process would still exit 0.

**Owner decision: fail fast.** On a write or read failure, report what went wrong and **exit
non-zero at that point** — do not continue and recompute the exit code at the end. One place decides
the outcome, so a later step cannot overwrite it. Consequence to state in the implementing PR: when
`--output-json` fails during `--phase pre`, the baseline is **not** written either, because the run
stops at the failure. That is intended — a run that could not produce its output should not leave a
baseline implying it succeeded.

**Test it**: an unwritable `--output-json` under `--phase pre` exits non-zero **and** leaves no new
baseline behind.

## Files that may be modified

- `apps/validate_pipeline/validate_pipeline.py`
- `apps/validate_pipeline/test/test_validate_pipeline.py`

## Acceptance criteria

- [ ] Neither case produces a traceback; both produce a `[FAIL]` row and exit 1.
- [ ] No existing behaviour changes for a well-formed baseline or a writable path.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] `ruff check` / `ruff format --check` clean on changed files.

## Out of scope

- INFRA-045's five findings.
- Whether `--phase pre` should return non-zero (INFRA-045 F5 decided: no).
- Exit-code attribution generally (INFRA-024).
