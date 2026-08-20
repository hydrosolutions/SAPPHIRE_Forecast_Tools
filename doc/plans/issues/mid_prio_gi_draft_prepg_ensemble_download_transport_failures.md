## A transient transport fault kills `preprocessing_gateway`: no DG download is retried, and two of the three sites lose the cause (PREPG-010)

**Status**: Draft (2026-08-20) — **re-widened after out-of-loop review**; ready to implement
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Scope**: all three Data Gateway download sites — control member (`:712`) and both ensemble loops
(`:811` today, `:830` yesterday). *Widened 2026-08-20 by owner decision; the previous revision
covered only the two ensemble loops.*
**Priority**: **Medium** — one transient network fault fails the whole module, and because the
gateway target runs its three scripts with `break`-on-first-failure, ERA5 extension **and** snow
do not run either. The failure **is** reported (`run_locally.sh:506-507` logs ERROR/FAIL) — loud,
not silent.
**Labels**: `preprocessing_gateway`, `error-handling`, `network`
**Found**: 2026-08-14, reported by a colleague running `run_locally.sh` on a dev machine; code
confirmed on trunk 2026-08-18.
**Related**: PREPG-014 (client-side timeouts and link-status validation — split out of this issue,
requires the separate `sapphire-dg-client` repo). PREPG-015 (the DG `ValueError` carries the API
key; anything this fix logs must be redacted). PREPG-009 (same module, opposite direction —
that one reports success on failure).

**Revision note (2026-08-20).** An out-of-loop `codex exec` review found three factual errors and
one missing call site in the previous revision. All four are corrected below; the corrections are
called out inline where an implementer would otherwise trust the old text. Two owner decisions were
taken at the same time: **one retry, not two** (§ The fix) and **widen to the control-member
download** (§ Three transport call sites).

---

## What happened

```
Processing HRU Ensemble: <code>
  File "Quantile_Mapping_OP.py", line 811, in main
    files = client.ecmwf_ens.get_ensemble_forecast(
  File "sapphire_dg_client/client_base.py", line 43, in _call_api_and_save_file
    resp = requests.get(file_resp.get("link"))
requests.exceptions.ConnectionError: ('Connection aborted.',
    ConnectionResetError(54, 'Connection reset by peer'))
[ERROR] preprocessing_gateway failed (exit 1) after 2m 4s
```

One member's TLS connection was reset. The module died; ERA5 extension and snow never ran.

## Root cause

`Quantile_Mapping_OP.py:808-856` wraps the download in `try / except ValueError`. That handler
exists for a **different** condition — "today's data isn't published yet, fall back to yesterday" —
and a `requests.exceptions.ConnectionError` is not a `ValueError`, so it bypasses the block and
propagates out of `main()`.

## THREE transport call sites, not one — and not two

This is the detail an implementation is most likely to get wrong. *A previous revision listed only
the two ensemble loops; the control-member download was missed.*

| Site | Date | Context |
|---|---|---|
| `Quantile_Mapping_OP.py:712-725` | `start_date` | **control member** — runs **before** the ensemble block, and fails differently (see below) |
| `Quantile_Mapping_OP.py:811` | `today` | ensemble, the normal path |
| `Quantile_Mapping_OP.py:830` | `yesterday` | ensemble, **inside** the `ValueError` fallback |

The two ensemble sites are near-identical 50-iteration loops. **A fix applied to only the first
leaves the defect live on the fallback path**, which runs whenever today's data is not yet
published. (How often that happens operationally is **not** established here — the code proves the
path is supported, not its frequency.) The commented-out batch calls at `:818` and `:840` are dead code — do not "fix" them.

### The control-member site fails differently, and worse

```python
except Exception as e:                                  # :717
    if "Operational data for HRU" in str(e):
        logger.error(f"Exiting the program due to error: {e}")
        sys.exit(1)
                                                        # <-- no else, no re-raise
if not control_member_era5:                             # :723
    logger.error(f"Control Member Data for HRU {c_m_hru} not available.")
    sys.exit(1)
```

A `ConnectionError` here is caught by the bare `except Exception`, **fails the message test, and is
then silently discarded**. `control_member_era5` stays `None`, and the run exits 1 reporting
*"Control Member Data for HRU X not available"* — a data-availability message for what was a
network fault. The exit is loud, but **the cause is destroyed**, which is why the reported incident
was diagnosable only from the ensemble traceback.

So this site needs two things, not one: the same retry, **and** the real exception preserved in the
failure message when the retry is exhausted. Fixing only the retry here would leave a transport
fault still masquerading as missing data.

## The fix

**One small bounded retry helper, applied at all three call sites.** Nothing more.

- **Exactly one retry — two attempts total, hard-coded. No env var, no config surface, no CLI
  flag.** *(Owner decision 2026-08-20; a previous revision said three.)* One retry clears a
  transient reset, which is the observed fault. It also halves the hang exposure the retry
  introduces — see § Hang exposure. An implementation that attempts three times does **not**
  conform.
- **Retry the individual call**, not an enclosing loop. Re-running all 50 members after a fault on
  member 37 is a different (and worse) behaviour.
- **Retryable set**: `requests.exceptions.ConnectionError` **and
  `requests.exceptions.ChunkedEncodingError`** — two classes, no exclusions. `SSLError` and
  `ProxyError` are `ConnectionError` subclasses and are therefore retried with it; that is
  deliberate (see below), not an oversight to be "tightened" later.
- **On exhaustion, re-raise the original exception unchanged** — same type, same message. Do not
  convert it to `sys.exit`, a `ValueError`, or a custom class: the ensemble sites depend on
  `ValueError` remaining distinguishable, and the control site's whole problem is a lost cause.
- **Never log the raw exception or endpoint from the retry helper.** The DG `ValueError` embeds
  `&api_key=<live key>` (`client_base.py:55-60`) — that is **PREPG-015**. A retry warning may name
  the attempt number, date, HRU, model index, and exception **class**; nothing else.

### `ChunkedEncodingError` is NOT a `ConnectionError` — and it is the likelier class here

Verified, not assumed: `class ChunkedEncodingError(RequestException)` (`requests/exceptions.py:120`)
is a **sibling** of `ConnectionError`, not a subclass. Requests converts a `ProtocolError` raised
while consuming a response body into `ChunkedEncodingError` (`requests/models.py:818-830`), and a
non-streaming `requests.get` reads the body eagerly before returning (`sessions.py:748-751`).

A file download is a case where a reset **can** land mid-body. So `except ConnectionError` alone
would catch the handshake-phase reset in the reported traceback but miss a body-phase one entirely.
Which is more frequent in this deployment is **not** established — the point is that one whole
class of the same fault would go unretried, not that it is the commoner class.
### `SSLError` is retried — DECIDED 2026-08-20 (reversal; read this before "fixing" it)

`requests.exceptions.SSLError` **subclasses** `ConnectionError` (`requests/exceptions.py:60`,
`:68`) — verified, not assumed. So a plain `except requests.exceptions.ConnectionError` already
catches it, and **no exclusion clause is needed or wanted**:

```python
except (requests.exceptions.ConnectionError,        # incl. SSLError, ProxyError
        requests.exceptions.ChunkedEncodingError):
    ...  # one retry, then re-raise unchanged
```

**Owner decision, reversing an earlier exclusion.** The reason the exclusion was dropped:
**the class hierarchy does not separate transient from permanent faults.** A connection reset
*during* a TLS handshake can surface as either `SSLError` or plain `ConnectionError` depending on
where in the exchange it lands — so excluding `SSLError` would have declined to retry a share of
exactly the fault this issue exists to survive, while a genuinely permanent TLS
misconfiguration costs only **one** wasted extra attempt before failing loudly with its original
message. One attempt is a cheap price for not silently splitting the observed failure mode in two.

Two consequences an implementer must not undo:

- **Do not add an `SSLError` re-raise** as a "safety" refinement. It is the reverted design.
- **Do not discriminate on the underlying `ssl` cause** either. That would mean matching on
  exception text or private cause types, which is fragile in a way the one wasted attempt is not.

## Coverage today is ZERO, and the harness is NOT a small fixture tweak

Measured 2026-08-18. **Nothing exercises the code this issue changes:**

- **No test references `get_ensemble_forecast` or `ecmwf_ens`.**
- `test_dg_download_failure_exits` calls `qm.main()`, but makes the **control** download raise, so
  it dies long before `:808`.
- `test_qm_writes_both_P_and_T_csvs` runs `main()` successfully — but `gateway_env` sets
  `ieasyhydroforecast_HRU_ENSEMBLE: "None"` (`test_integration_preprocessing_gateway.py:318`) and
  the loop breaks on that value (`Quantile_Mapping_OP.py:806`) **before** the download.
- **The today→yesterday fallback has no test at all** — the contract this fix most must not break.

### Budget the harness properly

Reaching `:808` requires **all** of the following to succeed first:

- `load_environment()` neutralised; API key present.
- Every path env var non-`None`; CM/ENS/DG directories writable.
- Models/QM path env vars present. **If the QM directory exists, both control-member parameter
  CSVs must be valid** — the current fixture sidesteps this by leaving it absent.
- Both HRU env vars present, ensemble **not** `"None"`.
- Mapping path env vars present; any existing mapping file must be valid JSON containing
  `gateway_name_twins`.
- The control-member download must return a truthy, existing, valid DG-format CSV, and its
  transform and CM output writes must complete.

### The fallback test needs TWO real files, not 100 — corrected 2026-08-20

*A previous revision claimed all 50 `yesterday` calls must return real files on disk. That is
wrong, and it inflated this test's cost enough to justify deferring it. Do not deprioritise it on
those grounds.*

A plain `MagicMock` return still will not work — `merge_ensemble_forecast` calls `sys.exit(1)` on an
empty list (`Quantile_Mapping_OP.py:188-191`) and a default mock flattens to `[]`. But every check
in that function is on the **aggregate**, never on member count or completeness:

- `if not files_downloaded` (`:188`) — the flattened list, after `:815-816` unnests it.
- `if P_ensemble.empty` / `if T_ensemble.empty` (`:217-223`).

So a legitimate fake returns `[]` for 49 of the 50 calls and **one real P/T pair** for the
recovering member. Two files on disk, real merge and output logic exercised, no internal
`MagicMock` chain. The factory in `test/test_ensemble_transforms.py` is still module-local and must
be shared or rebuilt, but that is a small refactor rather than a 100-file fixture.

**Consequence for sequencing:** there is no longer an expensive half to defer. The date-fallback
regression guard lands **with** the retry, not after it — which is also what the repo requires,
since full affected-scope tests are a precondition for review eligibility, not a follow-up.

One caveat on the cheap test: an **always**-failing transport call does not "invert" when the fix
lands — it still raises, just after two calls instead of one. The test that inverts is
**fail-then-succeed**.

## This fix creates a small NEW hang exposure — know it before implementing

`requests` has **no default timeout**, and the client sets none (`client_base.py:43`, `:56`), so a
peer that accepts a connection and never responds blocks forever. That is **PREPG-014**, not this
issue — but the retry interacts with it, and not neutrally:

| Scenario | Today | After this fix |
|---|---|---|
| Attempt 1 **hangs** | blocks forever | blocks forever — **unchanged**; the retry never fires because the call never returns |
| Attempt 1 **resets** | module dies in seconds — loud, and cron can retry | attempt 2 runs, and **attempt 2 can hang** |

So the fix does not make a hang worse; it **doubles the opportunities to meet one**, on a path
that previously terminated immediately. The bad trade it can produce is converting a fast, loud,
cron-retryable failure into an indefinite silent block.

**This is not a reason to skip the fix** — a reset is the observed failure and hangs are
unobserved — but two things follow:

1. **One retry, not two.** Every extra attempt is another chance to hang, and this is a second,
   independent reason not to make the count configurable: an operator raising it to 10 would
   multiply the exposure without seeing the connection.
2. **PREPG-014 stops being merely "the other half" and becomes this fix's safety net.** Land it, or
   accept the exposure knowingly.

**There is no local workaround — do not reach for one.** `socket.setdefaulttimeout(N)` was proposed
in a previous revision and **does not work for this client**. Requests always constructs an explicit
`TimeoutSauce(connect=timeout, read=timeout)` (`requests/adapters.py:642`), so urllib3 receives an
explicit `None` rather than its `_DEFAULT_TIMEOUT` sentinel, and
`urllib3/util/connection.py:69-70` therefore calls `sock.settimeout(None)` — re-blocking the socket
and overriding the process default. Confirmed empirically in the module venv: the default reaches a
bare socket, but `Timeout(connect=None).connect_timeout` is `None`. **A per-request timeout in the
client (PREPG-014) is the only fix.**

## Acceptance criteria

Tightened 2026-08-20: the previous set could be satisfied by several wrong implementations — a
three-attempt loop, one that converts exhaustion to `sys.exit`, or one that retries unrelated
`ValueError`s all passed it.

**Recovery**

- A `requests.exceptions.ConnectionError` on one member is retried **once** and the run continues.
  Use the class Requests actually raises, **not** the built-in `ConnectionError` — a test against
  the built-in proves nothing about handler dispatch.
- A `requests.exceptions.ChunkedEncodingError` is retried on the same terms, pinned separately.
- **The `yesterday` fallback path is covered too**: `today` raises the fallback `ValueError` →
  `yesterday` hits a transport fault → the retry succeeds. *Without this case, fixing only the
  first loop would pass.*
- **The control-member site (`:712`) is covered**, including that an exhausted transport fault is
  reported with its own cause and **not** as "Control Member Data for HRU X not available".
- **Output values are asserted after recovery** — the resulting P/T CSVs contain the expected
  values. Exit status alone does not prove the retried data reached the output.

**Counts and types**

- **Call counts are asserted exactly, and the two scenarios have DIFFERENT totals** — assert them
  per date, not as one aggregate, or the fallback case will look like an off-by-one:

  | Scenario | `today` calls | `yesterday` calls | Total |
  |---|---|---|---|
  | Normal path, one member fails then succeeds | 51 (50 members + 1 retry) | 0 | **51** |
  | Fallback path, one `yesterday` member fails then succeeds | **1** (model 1 raises the fallback `ValueError`, ending that loop) | 51 | **52** |

  In both, only the failing member repeats and the other 49 are not re-requested. On exhaustion:
  **exactly 2** calls for that member — an implementation attempting 3 must fail this test.
- On exhaustion the module fails **loudly**, propagating the **original exception type and
  message** — not a silent skip (cf. PREPG-009), and not a rewrapped or swallowed cause.
- **A `requests.exceptions.SSLError` IS retried** on the same terms as any other
  `ConnectionError` — pinned by a test asserting **2** calls on exhaustion, so that a later
  "tightening" that re-adds an exclusion clause fails visibly.
- **A non-matching `ValueError` is not retried — asserted as exactly 1 call**, not as a
  propagating exception. Be precise about where it is observed: the helper must let it through
  untouched, but `main()` never re-raises it — the `else` branch at `:851-856` prints and calls
  `sys.exit(1)`, so at `main()` level the observable outcome is **`SystemExit(1)` with the
  "Unexpected error" message**, unchanged from today. Assert the call count plus that exit; do not
  assert that a `ValueError` escapes `main()`, because it never has.

**Hygiene**

- No retry log line contains `api_key`, the endpoint, or the raw exception text (PREPG-015).
- The forecast date is **fixed/patched** in these tests — `today`/`yesterday` assertions must not
  depend on the wall clock (see CLAUDE.md § The Forecast Date Rule).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **The `yesterday` date-fallback must keep working.** It handles "data not published yet", a
  normal daily condition. Do not collapse it into the transport retry.
- **Do not widen the `except` to bare `Exception`.** The `ValueError` for "no files for this
  HRU/date" must stay distinguishable from a transport fault; conflating them re-creates
  PREPG-009's problem of an unexplained condition reported as routine. Note the control-member site
  (`:717`) **already** has this defect — the fix there is to narrow and re-raise, never to copy the
  pattern to the ensemble sites.
- **Preserve fail-fast across HRUs.** Today an escaping exception ends the HRU loop
  (`Quantile_Mapping_OP.py:799`). Keep that. Switching to "continue" would change
  operator-visible partial output, since results are written per HRU (`:912-917`) — a separate
  decision, not this fix.
- The one-model-at-a-time loop is deliberate. The only evidence is the comment at `:817-823`
  (batched requests time out server-side) — unconfirmed against the server, but **keep it**: this
  issue is not the place to re-test that claim.
- **A small local loop, not `tenacity`** — a retry framework for three call sites is
  disproportionate.
- **Declare `requests` in `apps/preprocessing_gateway/pyproject.toml` and relock** (DECIDED
  2026-08-20). It is absent today and reaches the venv only transitively via the DG client, whose
  own metadata does not declare it either — so the exception classes this fix dispatches on would
  rest on a dependency nobody has promised to keep. The DG client is pinned to a moving `@main`,
  which makes that promise weaker still. Do not skip the relock.
- **A short fixed pause before the single retry** (DECIDED 2026-08-20) — a couple of seconds,
  hard-coded alongside the attempt count, no backoff schedule and no config surface. With only one
  retry, an immediate re-dial is likely to meet the same condition before it clears, which would
  make the retry decorative. **Inject or patch the sleeper** so tests add no real wall-clock delay;
  CLAUDE.md forbids `sleep()` in tests, and a two-second pause on 50 members would otherwise be
  felt in the suite.
- **No HTTP-status retry (429/502/503/504).** It cannot work from here: the client turns a non-200
  *metadata* response into an undifferentiated `ValueError` and never raises on a bad *link*. That
  is PREPG-014's territory.

## State on failure — no new cleanup needed

**A transport fault cannot leave a truncated file** — corrected 2026-08-20. The previous rationale
("writes immediately, a retry overwrites harmlessly") had the mechanism backwards. Requests buffers
the whole body *before* `_save_file` opens the file (`client_base.py:26-30`, mode `wb`), so a
`ConnectionError` or `ChunkedEncodingError` during transfer means the file was never opened. A
failed call also returns no list, so the outer loop appends nothing and a successful retry supplies
its own paths. Files from earlier members persist, but the in-memory `files_downloaded` list is
lost, so no merge or final ensemble CSV is produced for that HRU.
**No code resumes from partial files**, and the next run *attempts* to delete everything under
`OUTPUT_PATH_DG` before downloading (`Quantile_Mapping_OP.py:625`) — deletion failures are caught
and suppressed (`:637`), so it is an attempt, not a guarantee.

---

## Implementation phases

Required by CLAUDE.md § Orchestration Protocol. One phase — this is a single small change with one
test surface, and splitting it would create a window where the retry exists without its regression
guard.

### P1 — Retry helper at all three DG download sites

**Goal**: A transient transport fault at any Data Gateway download is retried once and, if it still
fails, propagates with its original type and message.

**Files** (the complete allowed set):
- `apps/preprocessing_gateway/Quantile_Mapping_OP.py` — the helper plus its three call sites
  (`:712`, `:811`, `:830`), and narrowing the control site's `except Exception`.
- `apps/preprocessing_gateway/pyproject.toml` + its `uv.lock` — declare `requests`.
- `apps/preprocessing_gateway/test/test_integration_preprocessing_gateway.py` — the `main()`-level
  cases.
- `apps/preprocessing_gateway/test/` — a new unit-test module for the helper, and sharing the
  ensemble-file factory currently module-local to `test_ensemble_transforms.py`.

**Do NOT change** any existing function signature, the data flow through the transform/merge
chain, the today→yesterday fallback logic, or the one-model-at-a-time loop. The change is a
wrapper around three call sites plus one narrowed `except`; everything else is additive.

**Depends on**: nothing. PREPG-014 is a *safety net*, not a prerequisite — this fix is correct
without it and the hang exposure is documented above.

**Agents**: 1.

**Acceptance criteria**: the § Acceptance criteria section above, in full.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 }
  }
}
```
