## The Data Gateway API key is written to local log files in plaintext (PREPG-015)

**Status**: Draft (2026-08-18)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Medium** — a live credential lands in readable files on every dev machine and
server that has run the gateway. **No repo exposure**: `apps/logs` is gitignored and the key has
never been committed (verified).
**Labels**: `preprocessing_gateway`, `security`, `logging`
**Found**: 2026-08-18, while assessing PREPG-010's expected impact — spotted in a real
`run_locally` log from 2026-06-03.
**Related**: PREPG-014 (the upstream half of this — the client is what puts the key in the message).

---

## What lands in the log

```
ERROR - Exiting the program due to error: Failed to get data from
api/calculations/operational/template/RSMinerva?hru_code=…&start_date=…&api_key=<LIVE KEY>:
{"message": "Operational data for HRU … is not available…", "success": false}
```

The key is in cleartext, in a file that persists. Log rotation keeps 30 backups per run, so it
accumulates across months.

## How it gets there

Three steps, only the last of which is ours:

1. `client_base.py:55` appends the credential to the request path:
   `endpoint = endpoint + f"&api_key={self.api_key}"`
2. `client_base.py:59` embeds that whole endpoint in the exception message:
   `raise ValueError(f"Failed to get data from {endpoint}: {response.text}")`
3. **We log the exception verbatim.**

Note this fires on a **routine** condition — "operational data for this HRU is not available for
this date" is an ordinary daily occurrence, not a rare error. The key is written often.

## The sites — EIGHT statements across FOUR files

**This scope has been wrong at every revision.** 2 → 3 → 5 (the traceback prints re-leak what the
line above redacts) → **8 across 4 files** (an out-of-loop review found three more consumers of the
same client). Recorded because the pattern matters more than the count: the issue kept naming
*statements* when the defect is a *property* — anything in this module that renders a DG exception.
**If you are extending this, search the module for the property; do not trust this list.**

### `Quantile_Mapping_OP.py` — 5 statements

| Site | Context |
|---|---|
| `Quantile_Mapping_OP.py:796` (was `:719`) | `logger.error(f"Exiting the program due to error: {e}")` — the observed one, and it fires on the **routine** "data not published yet" condition |
| `Quantile_Mapping_OP.py:956` (was `:854`) | `print(f"Unexpected error for date {today}: {e}")` — same client, same exposure |
| `Quantile_Mapping_OP.py:949` | `print(f"Error for date {yesterday}: {e2}")` — **newly identified**; the `yesterday`-fallback branch, reached after today's data was absent, i.e. on the same routine path as the first site |

These go to a file: the first via the module logger, the two `print`s via `run_locally.sh`, which
tees stdout into its run log. **Plus the two `print(traceback.format_exc())` calls** that follow the
two prints (see below).

### Three more files — same client, same exposure

| Site | Context |
|---|---|
| `snow_data_operational.py:331` | `logger.error("Error getting snow data … %s", …, e)` — the **active daily** snow pipeline |
| `snow_data_renalysis.py:316` | same shape, inside a **five-year batch loop**, so one credential can be written many times |
| `get_era5_reanalysis_data.py:~163` | the DG call had **no exception boundary at all** — the credential went straight to the default traceback handler |

The helper therefore lives in **`dg_utils.py`**, not in `Quantile_Mapping_OP.py`, so all four files
share one implementation.

### FIVE statements, not three — the traceback prints re-leak it

*Found 2026-08-20 during implementation. This is the finding that decides whether this issue
achieves anything: redacting only the three statements above fixes ONE site of three.*

Each of the two `print` sites is immediately followed by `print(traceback.format_exc())`
(currently `:984` and `:991`). **`traceback.format_exc()` renders the exception's own `str()` as
its final line**, so the unredacted key is printed one line after the redacted message. Verified in
the module venv:

```
LAST LINE: ValueError: Failed to get data from endpoint?api_key=LIVEKEY123: {"msg": "nope"}
KEY PRESENT IN TRACEBACK: True
```

So the redaction must be applied to the traceback output too — `print(_redact_api_key(traceback.format_exc()))`.
**Owner decision 2026-08-20: fix in this change**, rather than shipping a partial fix and filing a
follow-up. A security fix that retires the issue while leaving the credential on the routine daily
path is worse than not shipping, because nobody looks again.

The `logger.error` site is not affected — no traceback is rendered on that branch.

**Consequence for testing**: a `capsys` assertion that inspects only the redacted line proves
nothing while the traceback is live. Assert against the WHOLE captured stdout, and assert the
traceback's structural markers survive so the test cannot pass merely because nothing printed.

**Not** a site: the `else` branch added at `:809-815` by PREPG-010 logs `type(e).__name__` only,
deliberately, and has a regression test asserting no `api_key` reaches the log. Leave it that way
until this issue lands a redaction helper — then it may be widened to include a redacted message.

Other exception logs in the module (`extend_era5_reanalysis.py:353`, `:650`, `:659`) are on the
**SAPPHIRE API** client, not the DG client, and are out of scope here.

## The fix

A small redaction helper in `dg_utils.py`, applied at **all eight** statements — replace
`api_key=<value>` with `api_key=***` before the message is logged or printed.

### The trap: what terminates the key

Get this wrong and you either leak part of the key or destroy the diagnostics. In the observed
message the credential is the **last** query parameter and is followed by `: ` and the server's
JSON body:

```
...&start_date=…&api_key=<LIVE KEY>: {"message": "Operational data for HRU … is not available…"}
```

So a naive `api_key=[^&\s]*` runs past the key and eats the colon and the leading part of the JSON,
destroying exactly the response text the acceptance criteria require you to preserve. But the key is
**not** guaranteed to be last — `_call_api` appends it to whatever endpoint it was handed
(`client_base.py:55`), and a future endpoint could append further parameters, in which case `&` does
terminate it.

Terminate on `&` **or** whitespace **or** `:` — whichever comes first — and pin all three shapes
with a test: key-last-then-colon (the observed case), key-followed-by-`&`, and key-at-end-of-string.

**Keep it local and keep it small.** Do not build a logging filter subsystem, and do not change
what is logged apart from the credential: the rest of the message (endpoint, HRU code, the
server's explanation) is diagnostically useful and is why these lines exist.

The root cause is upstream — the client should not put a credential in an exception message at
all — but that is a separate repo (**PREPG-014**) and consumers only move on a relock. Redact
locally regardless; it is cheap and does not depend on the client changing.

## Acceptance criteria

- A DG-client `ValueError` whose message contains `api_key=<something>` is logged with the value
  replaced, at **all eight** statements across the four files listed above. **Match on the
  statements, not on line numbers** — they have drifted twice this week. *Every earlier revision of
  this issue understated the scope; a fix that satisfies an out-of-date list closes the issue with
  the credential still being written.*
- **The redaction is applied to the logged string, never to the exception object.** The
  `ValueError` message is load-bearing for the today→yesterday fallback (see Contract below);
  rewriting the exception itself would be a control-flow change wearing a logging fix's clothes.
- The three termination shapes above are each pinned by a test.
- The rest of the message is preserved — endpoint path, HRU code, and the server's response text
  must still appear, or the log stops being useful.
- A message with **no** `api_key=` is passed through unchanged.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- **Do not swallow or downgrade the error.** `:796` exits the program deliberately; redaction must
  not change control flow.
- The `ValueError` message text is **load-bearing elsewhere**: `Quantile_Mapping_OP.py` matches on
  `"Couldn't find any files for the given HRU code, date and models!"` to drive the
  today→yesterday fallback. Redaction must not alter matching behaviour.

## Known limitations of the redactor — accepted, not overlooked

Found by the final adversarial pass 2026-08-20 and **accepted by owner decision** rather than fixed.
All three are real; none is a reason to withhold the fix, and all three fail in bounded ways.

1. **The literal pass is unscoped.** It replaces every occurrence of the live key value, so a
   credential that happens to be a substring of ordinary text redacts that text too — with a key of
   `disabled`, `{"message":"model disabled"}` becomes `{"message":"model ***"}`. This **fails in the
   safe direction**: it hides a diagnostic word rather than leaking a credential. A real gateway
   credential being a common English substring is the unlikely case we accept.

2. **The length threshold is a trade with no correct value.** Raising `_MIN_LITERAL_KEY_LENGTH`
   reduces limitation 1 and worsens its opposite — a short real credential falls back to
   pattern-only redaction, which is *partial* for a key containing `": "`. Lowering it does the
   reverse. **The two failure modes cannot both be closed by tuning the number.** The fallback is
   deliberately silent: a log line announcing that the key is short is its own small disclosure.

3. **The environment is read, not the client's captured key.** If the environment were rotated
   after a client was constructed, that client's exception would carry the old key while the helper
   searched for the new one, and the literal pass would silently miss. Nothing in this codebase
   mutates the environment mid-run without rebuilding the client — which is why this is documented
   rather than fixed. **A caller that ever does so must pass the key explicitly.**

The durable point behind all three: this helper reduces exposure, it does not guarantee
containment. **The only cure is upstream — PREPG-014, where the client stops putting a credential
in an exception message at all.** Treat the key as exposed on any machine that has run the gateway.

## Not covered

Existing log files already contain the key. Rotating or scrubbing them is an operational task, not
a code change — and the key should be treated as exposed on any machine that has run the gateway.
