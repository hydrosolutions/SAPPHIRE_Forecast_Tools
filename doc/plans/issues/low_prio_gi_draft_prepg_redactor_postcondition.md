## `redact_api_key` should assert its postcondition, not just enumerate shapes (PREPG-019)

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_gateway` (`dg_utils.py`)
**Priority**: **Low** — hardening, not a live defect. **No shape is known to escape redaction
today.** The value is catching the shapes nobody enumerated; see § Why bother.
**Labels**: `preprocessing_gateway`, `security`, `hardening`
**Found**: 2026-08-20. Not from a failure — from a peer session's independent result on a shell
validator (§ Where this idea comes from), which generalises to any code that transforms its input.
**Related**: **PREPG-015** (shipped, PR #460 — the redactor this hardens;
[`archive/mid_prio_gi_draft_prepg_dg_api_key_written_to_logs.md`](archive/mid_prio_gi_draft_prepg_dg_api_key_written_to_logs.md)).
**PREPG-014** — the upstream cure. **PREPG-017** — the path the redactor cannot reach at all.

---

## The change

`dg_utils.redact_api_key` runs two passes: a literal replacement of the live key value, then an
`api_key=` pattern substitution. Both are **enumerations** — they redact the shapes we thought of.
PREPG-015 ships three documented limitations, and every one of them is an enumeration limitation.

Add a **postcondition** at the return: *the live key value does not appear in the returned string.*

```python
result = <existing literal pass, then pattern pass>

if live_key and len(live_key) >= _MIN_LITERAL_KEY_LENGTH:
    if live_key in result:
        # a shape neither pass anticipated -- do not return it
        <fail loudly, or hard-scrub and log that this happened>
else:
    logger.debug("redact_api_key: postcondition SKIPPED (no usable key available)")
return result
```

## Why bother, when nothing is known to escape

Because the two passes can only catch what was enumerated, and the postcondition catches what was
not. **This is the entire argument** — if you are unconvinced by it, do not implement this issue,
because there is no failing case to point at.

**Evidence that it is categorically different, from the peer session that produced the idea:** they
had found *five* fail-open defects by enumeration in one ~40-line shell validator. Replacing the
per-path guards with a single postcondition, they then tested four broken-transform stubs — and one
of them, a **wildcard**, was a case they had never written a test for. The postcondition rejected it
anyway. Enumeration catches what you thought of.

## Two things to get right — both cost the peer session a round

1. **GUARD THE GUARD: a postcondition that cannot run must say so.** Written naively, the check does
   nothing when the live key is unavailable — which is *precisely* the condition PREPG-015's second
   documented limitation describes (env unset, or shorter than `_MIN_LITERAL_KEY_LENGTH`). A reader
   of the logs then cannot distinguish **"verified absent"** from **"could not verify"**. Log the
   skip. An unrunnable check that looks like a passing one is the same defect this issue exists to
   prevent.
2. **Do not let a green postcondition imply more than it proves.** A key that was URL-encoded or
   otherwise transformed *before* it reached the message will not be found by a literal containment
   check — the literal is not there. **That limitation survives and must stay in the docstring.**
   State what the check does and does not establish.

## What it does NOT do

- It does not replace the two passes. It verifies them.
- It does not fix PREPG-017 (an uncaught exception never reaches this function).
- It does not make the helper a containment guarantee. PREPG-015's framing stands: **this reduces
  exposure; the cure is upstream (PREPG-014).**

## Acceptance criteria

- A crafted message where **both passes fail** but the literal key is present is **not returned**
  as-is — pinned by a test that patches the passes or supplies a shape they miss.
- The skip path is **observable**: with no usable key, the debug line fires and the function still
  returns the pattern-redacted string. Assert the log, not just the return value.
- With a usable key and an ordinary message, output is **byte-identical** to today — the
  postcondition must not change the happy path.
- The docstring states what the postcondition establishes and what it cannot (the pre-encoded case).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Never log, raise, or embed the key itself** when the postcondition fails. Report *that* it
  failed and the exception/message class — not the value. A security check that leaks the secret in
  its own failure path would be a self-defeating fix.
- The helper sits on **error paths**. It must not raise where it previously returned, or a logged
  error becomes a crash — decide deliberately between hard-scrub-and-continue and fail-loudly, and
  state which in the docstring.

## Where this idea comes from

A parallel session hardening a shell validator (`validate_dashboard_origins`, INFRA-033) found five
fail-open defects by enumeration, then switched to asserting a postcondition and immediately caught
a sixth class it had not enumerated. Their postcondition was **structural** ("still parses as
HOST[:PORT]"), which is a proxy — it can pass for a well-formed *wrong* value. This one is the
**requirement itself** ("the secret is absent"), which cannot pass while the secret is present.
When the postcondition *is* the requirement, the class of unanticipated failures it covers is
everything.
