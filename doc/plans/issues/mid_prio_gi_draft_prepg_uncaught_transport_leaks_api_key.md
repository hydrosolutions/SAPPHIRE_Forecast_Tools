## An uncaught transport exception prints the API key to stderr (PREPG-017)

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_gateway`
**Priority**: **Medium** — same credential, same persistence as PREPG-015, but a different and
**unredactable** path: the message never passes through any of our logging statements.
**Labels**: `preprocessing_gateway`, `security`, `logging`
**Found**: 2026-08-20, by the out-of-loop adversarial review of the PREPG-015 diff. **Split out of
PREPG-015 by owner decision** — it needs a top-level handler design, not a one-line redaction.
**Related**: PREPG-015 (redacts the key at statements we control — this is the path that bypasses
them). PREPG-010 (shipped in #459; its retry re-raises the exception that takes this path).
PREPG-014 (upstream: the client should never put a credential in an exception at all).

---

## The path

`requests` transport exceptions embed the **full request URL** in their message, and the Data
Gateway client appends `&api_key=<live key>` to that URL (`client_base.py:55`). Verified
empirically in the module venv:

```
class: ConnectionError
KEY IN MESSAGE: True
HTTPSConnectionPool(host='…', port=443): Max retries exceeded with url:
    /x?hru=1&api_key=SECRETKEY (Caused by NewConnectionError(…))
```

Both ensemble handlers in `Quantile_Mapping_OP.py` catch **`ValueError` only** — deliberately, so
that the today→yesterday date fallback stays distinguishable from a transport fault. So a
`ConnectionError`, `SSLError`, `ProxyError`, `ChunkedEncodingError` or `Timeout` that survives
PREPG-010's retry is **re-raised, caught by nothing, and printed by Python's default excepthook**.

`run_locally.sh` redirects stderr through `tee` into its run log, so the key persists in a file —
the same outcome PREPG-015 exists to prevent, reached without passing through a single statement
PREPG-015 can redact.

## Why PREPG-015 cannot fix this

PREPG-015 redacts strings we format ourselves. Here nobody formats anything: the interpreter prints
the traceback. **There is no statement to wrap.** That asymmetry is the whole reason this is a
separate issue rather than a sixth site.

## Not caused by PREPG-010, and not made worse by it

Worth stating so this is not misread as a regression in #459. Before that change an unretried
transport fault propagated on the first failure; after it, one that survives two attempts
propagates. Same exception, same path, same exposure — the retry changes *when*, not *whether*.

## The design question — this is the actual work

A redaction helper already exists (`dg_utils.redact_api_key`, PREPG-015). The question is where to
attach it so nothing escapes, without swallowing failures:

1. **`sys.excepthook` installed at each entry point.** Catches everything including bugs we did not
   anticipate. But it is process-global, and a handler that rewrites tracebacks can obscure
   debugging if it is careless.
2. **A `try/except` at each `main()`** that logs redacted and re-raises or exits non-zero. More
   explicit and more local, but it must be added to every entry point and will be forgotten on the
   next one.
3. **Do nothing here; fix it upstream** (PREPG-014 — the client stops embedding the credential).
   That is the only real cure, but it is a separate repo and consumers move only on a relock, so it
   should not be the sole mitigation.

**Whichever is chosen, the failure must stay loud.** The gateway target breaks on first failure and
ERA5 extension and snow ride on that exit status. A handler that redacts and then swallows would
convert a loud failure into a silent one — PREPG-009's defect, reintroduced in the name of security.

## Acceptance criteria

- An **uncaught** `requests.exceptions.ConnectionError` whose message contains an `api_key`-bearing
  URL does not put the credential on stdout or stderr.
- The process still **exits non-zero**, and the traceback is still useful — exception type, and the
  call site, must survive; only the credential goes.
- Covered by a test that invokes the entry point and asserts on **stderr**, not only stdout. The
  existing PREPG-015 tests all assert on `capsys.out`, which is exactly why this path went unnoticed.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not widen any `except ValueError` to catch transport faults** as a shortcut to getting a
  redactable statement. That conflation is what PREPG-010 explicitly forbids: the `ValueError`
  message is load-bearing for the today→yesterday fallback.
- Do not change process exit status on any path.

## Not covered

Log files already written. As with PREPG-015, the key must be treated as exposed on any machine
that has run the gateway; rotating or scrubbing those files is an operational task.
